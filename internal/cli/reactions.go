package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/guard"
)

// maxEmojiList caps the emoji catalog rows the --list text output
// prints; the JSON output is untruncated.
const maxEmojiList = 200

// ReactionsHandler runs `whinny reactions`: with --list it reports the
// transport's reaction support (capabilities and the custom emoji
// catalog); otherwise it sets the caller's complete emoji set on one
// message — an empty argument list clears. The send-path guard chain
// runs before the connection is opened. The server names what the
// caller got wrong (an unknown message_id, an unknown custom emoji),
// so its invalid_params refusals surface as usage errors.
func (d Deps) ReactionsHandler(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
	if inv.Bools["list"] {
		return d.reactionsList(ctx, inv)
	}
	messageID := inv.Flags["message"]
	if messageID == "" {
		return nil, UsageErrorf("reactions: missing --message <message_id>")
	}
	emojis := inv.Args
	// The guard chain runs before connecting, exactly as post: a denial
	// is a non-zero exit carrying the typed denial text, and a refused
	// rate-limit is a bad send outcome that counts toward the breaker.
	st, owned, err := d.storeOwned()
	if err != nil {
		return nil, err
	}
	if owned {
		defer st.Close()
	}
	bucket, denial, err := d.guard(ctx, st)
	if err != nil {
		return nil, err
	}
	if bucket == nil {
		if denial.Kind == guard.KindRefusedRateLimit {
			if reason, tripped := d.tripOnFailure(ctx, st); tripped {
				return nil, OperationalErrorf("%s: %s", guard.KindRefusedBreaker, guard.BreakerRefusalText(reason))
			}
		}
		return nil, OperationalErrorf("%s: %s", denial.Kind, denial.Text)
	}
	// failSend mirrors post: a send that did not complete after the
	// guard allowed it refunds the token and counts toward the breaker.
	failSend := func(err error) (any, error) {
		bucket.Refund()
		if reason, tripped := d.tripOnFailure(ctx, st); tripped {
			return nil, OperationalErrorf("%s: %s", guard.KindRefusedBreaker, guard.BreakerRefusalText(reason))
		}
		return nil, err
	}
	client, owned, err := d.connect(ctx, inv)
	if err != nil {
		var uerr *Error
		if errors.As(err, &uerr) && uerr.Class == ClassUsage {
			bucket.Refund()
			return nil, err
		}
		return failSend(err)
	}
	if owned {
		defer client.Close()
	}
	rooms, err := client.RoomList(ctx, apronws.RoomListParams{})
	if err != nil {
		return failSend(OperationalErrorf("room list: %v", err))
	}
	room, err := ResolveRoom(rooms.Joined, rooms.NotJoined, inv.Flags["room"])
	if err != nil {
		// A room-resolution failure is a caller mistake, not a bad
		// send outcome: refund without counting toward the breaker.
		bucket.Refund()
		return nil, err
	}
	if !client.HasCapability("reactions") {
		// A server without the capability would refuse every request
		// server-side, and each refusal would count toward the shared
		// breaker, blocking posts too. Refuse here as a usage error.
		bucket.Refund()
		return nil, UsageErrorf("reactions: this server does not offer the reactions capability (see `whinny reactions --list`)")
	}
	if err := client.SetReactions(ctx, messageID, emojis); err != nil {
		// A caller mistake is a usage error that refunds without
		// counting toward the breaker: a transport refusal (slack), or
		// the server's invalid_params — the react request names what
		// the caller got wrong, such as `Unknown message "m9"` for an
		// id from another room (§4.7 resolves through the caller's
		// visible messages).
		var uerr *Error
		if errors.As(err, &uerr) && uerr.Class == ClassUsage {
			bucket.Refund()
			return nil, err
		}
		var aerr *apronws.Error
		if errors.As(err, &aerr) && aerr.Code == apronws.CodeInvalidParams {
			bucket.Refund()
			return nil, UsageErrorf("reactions: %s", aerr.Message)
		}
		return failSend(OperationalErrorf("reactions: %v", err))
	}
	// The send is confirmed: reset the consecutive failure counter (the
	// latch is sticky; clearing is manual). A reset failure is logged,
	// not returned: the reaction succeeded, and a non-zero exit would
	// send the agent to retry a completed send.
	if err := st.ResetPostFailures(ctx); err != nil {
		log.Printf("whinny: post-failure reset: %v", err)
	}
	if inv.Bools["json"] {
		return map[string]any{"message_id": messageID, "room_id": room.ID, "cleared": len(emojis) == 0}, nil
	}
	if len(emojis) == 0 {
		return fmt.Sprintf("cleared reactions on %s", messageID), nil
	}
	return fmt.Sprintf("set reactions on %s: %s", messageID, strings.Join(emojis, " ")), nil
}

// reactionsList reports what the connected transport offers for
// reactions: whether the server advertises the reactions capability,
// and the custom emoji catalog. It is read-only and runs no guard.
func (d Deps) reactionsList(ctx context.Context, inv clicontract.Invocation) (any, error) {
	client, owned, err := d.connect(ctx, inv)
	if err != nil {
		return nil, err
	}
	if owned {
		defer client.Close()
	}
	reactions := client.HasCapability("reactions")
	emoji, err := client.EmojiCatalog(ctx)
	if err != nil {
		return nil, OperationalErrorf("emoji catalog: %v", err)
	}
	if inv.Bools["json"] {
		return map[string]any{"reactions": reactions, "emoji": emoji}, nil
	}
	var b strings.Builder
	if reactions {
		b.WriteString("reactions: supported (apron §4.7; the request sets the complete emoji set, an empty set clears)\n")
	} else {
		b.WriteString("reactions: not supported on this transport\n")
	}
	if len(emoji) == 0 {
		b.WriteString("custom emoji: none")
	} else {
		names := make([]string, 0, len(emoji))
		for name := range emoji {
			names = append(names, name)
		}
		sort.Strings(names)
		if len(names) > maxEmojiList {
			names = append(names[:maxEmojiList], fmt.Sprintf("… (%d more)", len(names)-maxEmojiList))
		}
		b.WriteString(fmt.Sprintf("custom emoji: %s", strings.Join(names, " ")))
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}

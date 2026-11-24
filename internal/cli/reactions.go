package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/guard"
)

// maxEmojiList caps the emoji catalog rows the --list text output
// prints; the JSON output is untruncated.
const maxEmojiList = 200

// historyProbeLimit is how much recent history the visibility check
// fetches. Reactions land on recent messages — a wake or a history
// page carried the id — so a bounded window is the honest probe.
const historyProbeLimit = 50

// ReactionsHandler runs `whinny reactions`: with --list it reports the
// transport's reaction support (capabilities and the custom emoji
// catalog); otherwise it sets the caller's complete emoji set on one
// message — an empty argument list clears. The send-path guard chain
// runs before the connection is opened, and the message's visibility
// in the target room is checked first: a message_id from another room
// would fail server-side with an error the caller cannot fix from the
// message.
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
	// The reactions request carries no room_id, so a message_id from
	// another room fails server-side as "unknown message" — true but
	// unactionable. The room's history is the visibility proof: refuse
	// here instead, naming what the caller can fix.
	h, err := client.History(ctx, apronws.HistoryParams{RoomID: room.ID, Limit: historyProbeLimit})
	if err != nil {
		return failSend(OperationalErrorf("history: %v", err))
	}
	visible := false
	for _, m := range h.Messages {
		if m.MessageID == messageID {
			visible = true
			break
		}
	}
	if !visible {
		bucket.Refund()
		return nil, UsageErrorf("reactions: message %s not found in %s's recent history; reply-to ids and message ids arrive in wakes and `whinny history --json`", messageID, room.ID)
	}
	if err := client.SetReactions(ctx, messageID, emojis); err != nil {
		var uerr *Error
		if errors.As(err, &uerr) && uerr.Class == ClassUsage {
			// A transport refusal (slack) is a caller mistake: refund
			// without counting toward the breaker.
			bucket.Refund()
			return nil, err
		}
		return failSend(OperationalErrorf("reactions: %v", err))
	}
	// The send is confirmed: reset the consecutive failure counter (the
	// latch is sticky; clearing is manual).
	if err := st.ResetPostFailures(ctx); err != nil {
		return nil, OperationalErrorf("post-failure reset: %v", err)
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
	reactions := false
	if c, ok := client.(interface {
		Capabilities() []string
	}); ok {
		for _, cap := range c.Capabilities() {
			if cap == "reactions" {
				reactions = true
				break
			}
		}
	}
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

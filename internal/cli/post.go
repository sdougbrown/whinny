package cli

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/guard"
	"github.com/sdougbrown/whinny/internal/store"
)

// DefaultRatePerMinute is the send-path rate limit when
// WHINNY_RATE_PER_MINUTE is unset.
const DefaultRatePerMinute = 20

// PostHandler runs `whinny post`: it evaluates the send-path guard
// chain, resolves the target room, posts (creating a thread first when
// --thread-of is set), and joins the posted room.
func PostHandler(d Deps) clicontract.HandleFunc {
	return func(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
		if len(inv.Args) == 0 {
			return nil, UsageErrorf("post: missing text")
		}
		text := inv.Args[0]
		if text == "-" {
			r := d.Stdin
			if r == nil {
				r = os.Stdin
			}
			b, err := io.ReadAll(r)
			if err != nil {
				return nil, OperationalErrorf("post: read stdin: %v", err)
			}
			text = strings.TrimRight(string(b), " \t\r\n")
		}
		if text == "" {
			return nil, UsageErrorf("post: empty text")
		}
		replyTo := inv.Flags["reply-to"]
		threadOf := inv.Flags["thread-of"]
		if replyTo != "" && threadOf != "" {
			return nil, UsageErrorf("post: --reply-to and --thread-of are mutually exclusive")
		}
		// Guard: evaluate the send-path guard chain before connecting.
		// A denial is a non-zero exit carrying the typed denial text.
		// The store stays open for the whole handler so a failed send
		// can refund the consumed rate token.
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
			// A refused-rate-limit denial is a bad outcome: the agent
			// saw the typed denial and is retrying anyway. A quiet or
			// breaker denial is not (operator silence; already latched).
			if denial.Kind == guard.KindRefusedRateLimit {
				if reason, tripped := d.tripOnFailure(ctx, st); tripped {
					return nil, OperationalErrorf("%s: %s", guard.KindRefusedBreaker, guard.BreakerRefusalText(reason))
				}
			}
			return nil, OperationalErrorf("%s: %s", denial.Kind, denial.Text)
		}
		// failSend handles a send that did not complete after the guard
		// allowed it: refund the consumed rate token, bump the
		// consecutive failure counter, and — when the bump trips the
		// breaker — refuse this post with the new latch instead of the
		// transport error.
		failSend := func(err error) (any, error) {
			bucket.Refund()
			if reason, tripped := d.tripOnFailure(ctx, st); tripped {
				return nil, OperationalErrorf("%s: %s", guard.KindRefusedBreaker, guard.BreakerRefusalText(reason))
			}
			return nil, err
		}
		client, owned, err := d.connect(ctx, inv)
		if err != nil {
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
			// ResolveRoom only returns *RoomError (usage errors the
			// caller can fix from the message) or nil — never a send-
			// path failure — so refund without counting toward the
			// breaker.
			bucket.Refund()
			return nil, err
		}
		postRoom := room.ID
		var outRoomID string
		if threadOf != "" {
			t, err := client.CreateThread(ctx, apronws.CreateThreadParams{
				ParentRoomID: room.ID,
				Title:        threadTitle(text),
			})
			if err != nil {
				return failSend(OperationalErrorf("create thread: %v", err))
			}
			postRoom = t.ID
			outRoomID = t.ID
		}
		var rt *apronws.ReplyTo
		if anchor := replyTo; anchor != "" {
			rt = &apronws.ReplyTo{MessageID: anchor}
		} else if threadOf != "" {
			// The thread's first reply carries the parent message.
			rt = &apronws.ReplyTo{MessageID: threadOf}
		}
		res, err := client.PostMessage(ctx, apronws.PostParams{
			RoomID:  postRoom,
			Body:    apronws.Body{Text: text},
			ReplyTo: rt,
		})
		if err != nil {
			return failSend(OperationalErrorf("post: %v", err))
		}
		// The send is confirmed: reset the consecutive failure counter
		// (the latch is sticky; clearing is manual). A JoinRoom failure
		// is after the confirmed send, so no refund.
		if err := st.ResetPostFailures(ctx); err != nil {
			log.Printf("whinny: post-failure reset: %v", err)
		}
		if err := client.JoinRoom(ctx, postRoom); err != nil {
			return nil, OperationalErrorf("join %s: %v", postRoom, err)
		}
		if inv.Bools["json"] {
			out := map[string]any{"message_id": res.MessageID}
			if outRoomID != "" {
				out["room_id"] = outRoomID
			}
			return out, nil
		}
		return res.MessageID, nil
	}
}

// guard evaluates the send-path guard chain before a post connects:
// the persisted breaker, the persisted rate bucket, and the quiet gate.
// A denial returns the typed denial and a nil bucket (no token was
// consumed); an allowed send returns the bucket so a failed send can
// refund the consumed token. The caller owns the store's lifecycle.
func (d Deps) guard(ctx context.Context, st *store.Store) (*guard.RateBucket, guard.Denial, error) {
	brk, err := st.GetBreaker(ctx)
	if err != nil {
		return nil, guard.Denial{}, OperationalErrorf("breaker: %v", err)
	}
	perMinute := DefaultRatePerMinute
	if v := d.env()("WHINNY_RATE_PER_MINUTE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, guard.Denial{}, UsageErrorf("post: WHINNY_RATE_PER_MINUTE must be a positive integer, got %q", v)
		}
		perMinute = n
	}
	bucket := guard.NewStoreBucket(st, perMinute)
	quietStr, err := st.GetMeta(ctx, MetaQuietUntil)
	if err != nil {
		return nil, guard.Denial{}, OperationalErrorf("quiet: %v", err)
	}
	var quietUntil int64
	if quietStr != "" {
		quietUntil, err = strconv.ParseInt(quietStr, 10, 64)
		if err != nil {
			return nil, guard.Denial{}, OperationalErrorf("quiet_until: %v", err)
		}
	}
	chain := guard.NewChain(guard.Config{PerMinute: perMinute})
	denial, denied := chain.Evaluate(guard.GuardInput{
		BreakerLatched: brk.Latched,
		BreakerReason:  brk.Reason,
		Bucket:         bucket,
		QuietUntil:     quietUntil,
		Now:            d.now(),
	})
	if denied {
		return nil, denial, nil
	}
	return bucket, guard.Denial{}, nil
}

// breakerTripReason is the outcome-neutral latch reason: both trip
// paths (rate-limit denials and failed posts) bump the same
// consecutive-failure counter, so the reason names the shared count,
// not the last outcome.
const breakerTripReason = "bad send outcomes"

// breakerTripThreshold is the consecutive bad-outcome count (rate-
// limit denials or failed posts) that latches the send-path breaker.
// It lives beside its enforcement point: the guard only reads the
// latch, it never trips one.
const breakerTripThreshold = 5

// tripOnFailure bumps the consecutive post-failure counter and, when
// the bump reaches the trip threshold, latches the breaker with the
// outcome-neutral reason. It returns the latch reason when the
// breaker tripped. A bump or latch error is logged, not returned: the
// caller reports the send outcome (the original error or the denial)
// either way.
func (d Deps) tripOnFailure(ctx context.Context, st *store.Store) (string, bool) {
	n, err := st.BumpPostFailures(ctx)
	if err != nil {
		log.Printf("whinny: post-failure count: %v", err)
		return "", false
	}
	if n < breakerTripThreshold {
		return "", false
	}
	reason := fmt.Sprintf("%d consecutive %s", n, breakerTripReason)
	if err := st.SetBreaker(ctx, store.Breaker{
		Latched:   true,
		LatchedAt: d.now().Unix(),
		Reason:    reason,
	}); err != nil {
		log.Printf("whinny: breaker latch: %v", err)
		return "", false
	}
	return reason, true
}

// threadTitle is the first line of the text, truncated to 80 runes.
func threadTitle(text string) string {
	line := text
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	line = strings.TrimSpace(line)
	r := []rune(line)
	if len(r) > 80 {
		r = r[:80]
	}
	return string(r)
}

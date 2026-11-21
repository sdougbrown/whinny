package cli

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/guard"
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
		format := inv.Flags["format"]
		if format == "" {
			format = "markdown"
		}
		if format != "markdown" && format != "plain" {
			return nil, UsageErrorf("post: --format must be markdown or plain, got %q", format)
		}
		// Guard: evaluate the send-path guard chain before connecting.
		// A denial is a non-zero exit carrying the typed denial text.
		if err := d.guard(ctx); err != nil {
			return nil, err
		}
		client, owned, err := d.connect(ctx, inv)
		if err != nil {
			return nil, err
		}
		if owned {
			defer client.Close()
		}
		rooms, err := client.RoomList(ctx, apronws.RoomListParams{})
		if err != nil {
			return nil, OperationalErrorf("room list: %v", err)
		}
		room, err := ResolveRoom(rooms.Joined, rooms.NotJoined, inv.Flags["room"])
		if err != nil {
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
				return nil, OperationalErrorf("create thread: %v", err)
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
			return nil, OperationalErrorf("post: %v", err)
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
// A denial returns the typed denial text as an operational error
// (non-zero exit); an allowed send leaves the consumed bucket state
// persisted by the bucket's save hook.
func (d Deps) guard(ctx context.Context) error {
	st, err := d.store()
	if err != nil {
		return err
	}
	if d.Store == nil {
		defer st.Close()
	}
	brk, err := st.GetBreaker(ctx)
	if err != nil {
		return OperationalErrorf("breaker: %v", err)
	}
	perMinute := DefaultRatePerMinute
	if v := d.env()("WHINNY_RATE_PER_MINUTE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return UsageErrorf("post: WHINNY_RATE_PER_MINUTE must be a positive integer, got %q", v)
		}
		perMinute = n
	}
	bucket := guard.NewPersistedBucket(
		func() (float64, time.Time, error) { return st.RateState(ctx) },
		func(tokens float64, last time.Time) error { return st.SetRateState(ctx, tokens, last) },
		perMinute,
	)
	quietStr, err := st.GetMeta(ctx, MetaQuietUntil)
	if err != nil {
		return OperationalErrorf("quiet: %v", err)
	}
	var quietUntil int64
	if quietStr != "" {
		quietUntil, err = strconv.ParseInt(quietStr, 10, 64)
		if err != nil {
			return OperationalErrorf("quiet_until: %v", err)
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
		return OperationalErrorf("%s: %s", denial.Kind, denial.Text)
	}
	return nil
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

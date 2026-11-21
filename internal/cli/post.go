package cli

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
)

// PostHandler runs `whinny post`: it resolves the target room, posts
// (creating a thread first when --thread-of is set), and joins the
// posted room unless --no-subscribe is set.
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
		if !inv.Bools["no-subscribe"] {
			if err := client.JoinRoom(ctx, postRoom); err != nil {
				return nil, OperationalErrorf("join %s: %v", postRoom, err)
			}
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

package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
)

// historyMessage is one history message's JSON projection.
type historyMessage struct {
	MessageID string       `json:"message_id"`
	LogID     int64        `json:"log_id"`
	RoomID    string       `json:"room_id"`
	From      apronws.From `json:"from"`
	Text      string       `json:"text"`
	ReplyTo   string       `json:"reply_to,omitempty"`
	Deleted   bool         `json:"deleted,omitempty"`
}

// HistoryHandler runs `whinny history`: it resolves the room, fetches
// the room's history, and applies the --since/--before cursor filters
// client-side.
func HistoryHandler(d Deps) clicontract.HandleFunc {
	return func(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
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
		limit := inv.Ints["limit"]
		if limit <= 0 {
			limit = 50
		}
		h, err := client.History(ctx, apronws.HistoryParams{RoomID: room.ID, Limit: limit})
		if err != nil {
			return nil, OperationalErrorf("history: %v", err)
		}
		msgs := h.Messages
		if before := inv.Flags["before"]; before != "" {
			msgs, err = filterByCursor(msgs, before, false)
			if err != nil {
				return nil, err
			}
		}
		if since := inv.Flags["since"]; since != "" {
			msgs, err = filterByCursor(msgs, since, true)
			if err != nil {
				return nil, err
			}
		}
		if inv.Bools["json"] {
			out := make([]historyMessage, 0, len(msgs))
			for _, m := range msgs {
				out = append(out, projectMessage(m))
			}
			return out, nil
		}
		var b strings.Builder
		for _, m := range msgs {
			from := m.From.Name
			if from == "" {
				from = m.From.UserID
			}
			text := ""
			if m.Body != nil {
				text = m.Body.Text
			}
			fmt.Fprintf(&b, "%s: %s\n", from, text)
		}
		return strings.TrimSuffix(b.String(), "\n"), nil
	}
}

// filterByCursor keeps the messages strictly on one side of the cursor
// message's log id: newer when the since flag is set, older when the
// before flag is set. A cursor the fetched window does not contain is a
// usage error the caller can fix by widening --limit.
func filterByCursor(msgs []apronws.Message, cursorID string, newer bool) ([]apronws.Message, error) {
	var cursorLog int64
	found := false
	for _, m := range msgs {
		if m.MessageID == cursorID {
			cursorLog = int64(m.LogID)
			found = true
			break
		}
	}
	if !found {
		return nil, UsageErrorf("cursor %s not in fetched history; increase --limit", cursorID)
	}
	out := make([]apronws.Message, 0, len(msgs))
	for _, m := range msgs {
		if (newer && int64(m.LogID) > cursorLog) || (!newer && int64(m.LogID) < cursorLog) {
			out = append(out, m)
		}
	}
	return out, nil
}

func projectMessage(m apronws.Message) historyMessage {
	out := historyMessage{
		MessageID: m.MessageID,
		LogID:     int64(m.LogID),
		RoomID:    m.RoomID,
		From:      m.From,
		Deleted:   m.Deleted,
	}
	if m.Body != nil {
		out.Text = m.Body.Text
	}
	if m.ReplyTo != nil {
		out.ReplyTo = m.ReplyTo.MessageID
	}
	return out
}

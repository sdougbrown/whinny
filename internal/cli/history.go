package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/apronws"
)

// DefaultHistoryLimit is the history limit when --limit is omitted or
// non-positive.
const DefaultHistoryLimit = 50

// historyMessage is one history message's JSON projection.
type historyMessage struct {
	MessageID string                `json:"message_id"`
	LogID     int64                 `json:"log_id"`
	RoomID    string                `json:"room_id"`
	From      apronws.From          `json:"from"`
	Text      string                `json:"text"`
	ReplyTo   string                `json:"reply_to,omitempty"`
	Deleted   bool                  `json:"deleted,omitempty"`
	Reactions []apronws.ReactionSet `json:"reactions,omitempty"`
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
			limit = DefaultHistoryLimit
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
		reactions := foldReactions(msgs, h.Reactions)
		if inv.Bools["json"] {
			out := make([]historyMessage, 0, len(msgs))
			for _, m := range msgs {
				out = append(out, projectMessage(m, reactions[m.MessageID]))
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
			for _, set := range reactions[m.MessageID] {
				who := set.From.Name
				if who == "" {
					who = set.From.UserID
				}
				fmt.Fprintf(&b, "  %s reacted %s\n", who, strings.Join(set.Emojis, " "))
			}
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

func projectMessage(m apronws.Message, reactions []apronws.ReactionSet) historyMessage {
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
	out.Reactions = reactions
	return out
}

// foldReactions folds a history window's reaction-set records onto the
// message ids they react to, in log order: each record carries one
// author's complete set at that position, so a later record from the
// same author replaces their earlier one — the fold's result is the
// current sets per message. A record whose message is not in the
// window is dropped.
func foldReactions(msgs []apronws.Message, records []apronws.ReactionsRecord) map[string][]apronws.ReactionSet {
	inWindow := make(map[string]bool, len(msgs))
	for _, m := range msgs {
		inWindow[m.MessageID] = true
	}
	// An author's complete set replaces their previous one: track per
	// (message, author) position in each message's slice.
	index := make(map[string]map[string]int) // messageID -> userID -> slice position
	out := make(map[string][]apronws.ReactionSet)
	for _, rec := range records {
		if !inWindow[rec.MessageID] {
			continue
		}
		if index[rec.MessageID] == nil {
			index[rec.MessageID] = make(map[string]int)
			out[rec.MessageID] = nil
		}
		for _, set := range rec.Reactions {
			if pos, ok := index[rec.MessageID][set.From.UserID]; ok && pos < len(out[rec.MessageID]) && out[rec.MessageID][pos].From.UserID == set.From.UserID {
				// Same author, later complete set: replace in place;
				// an empty set removes their entry.
				if len(set.Emojis) == 0 {
					sets := out[rec.MessageID]
					out[rec.MessageID] = append(sets[:pos], sets[pos+1:]...)
					index[rec.MessageID][set.From.UserID] = 0
					delete(index[rec.MessageID], set.From.UserID)
				} else {
					out[rec.MessageID][pos] = set
				}
				continue
			}
			if len(set.Emojis) == 0 {
				continue
			}
			index[rec.MessageID][set.From.UserID] = len(out[rec.MessageID])
			out[rec.MessageID] = append(out[rec.MessageID], set)
		}
	}
	for id, sets := range out {
		if len(sets) == 0 {
			delete(out, id)
		}
	}
	return out
}

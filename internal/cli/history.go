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

// foldReactions folds a history window's reaction records onto the
// message ids they react to. Each record carries one author's complete
// set at that log position — an empty set is that author's clear — so
// the last record naming an author wins; the fold's result is the
// current sets per message. A record whose message is not in the
// window is dropped.
func foldReactions(msgs []apronws.Message, records []apronws.ReactionsRecord) map[string][]apronws.ReactionSet {
	inWindow := make(map[string]bool, len(msgs))
	for _, m := range msgs {
		inWindow[m.MessageID] = true
	}
	// The current state is one entry per author: the last record that
	// names an author carries their whole set, so it simply overwrites.
	current := make(map[string]map[string]apronws.ReactionSet) // messageID -> userID -> set
	// order keeps each message's first-appearance author order, so the
	// projection is deterministic regardless of map iteration.
	order := make(map[string][]string)
	for _, rec := range records {
		if !inWindow[rec.MessageID] {
			continue
		}
		if current[rec.MessageID] == nil {
			current[rec.MessageID] = make(map[string]apronws.ReactionSet)
		}
		for _, set := range rec.Reactions {
			userID := set.From.UserID
			if len(set.Emojis) == 0 {
				// The clear drops the author's state and their order
				// entry, so a later re-add appends fresh and cannot
				// leave a stale name in the projection.
				delete(current[rec.MessageID], userID)
				for i, id := range order[rec.MessageID] {
					if id == userID {
						order[rec.MessageID] = append(order[rec.MessageID][:i], order[rec.MessageID][i+1:]...)
						break
					}
				}
				continue
			}
			if _, seen := current[rec.MessageID][userID]; !seen {
				order[rec.MessageID] = append(order[rec.MessageID], userID)
			}
			current[rec.MessageID][userID] = set
		}
	}
	out := make(map[string][]apronws.ReactionSet)
	for messageID, authors := range current {
		sets := make([]apronws.ReactionSet, 0, len(authors))
		for _, userID := range order[messageID] {
			if set, ok := authors[userID]; ok {
				sets = append(sets, set)
			}
		}
		if len(sets) > 0 {
			out[messageID] = sets
		}
	}
	return out
}

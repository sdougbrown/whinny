package daemon

import (
	"context"
	"sort"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/wake"
)

// KindMessage is the LinkEvent kind for an inbound chat message. The
// policy treats app mentions and normal messages alike, so both arrive
// as messages.
const KindMessage = "message"

// KindReaction is the LinkEvent kind for an inbound reaction-set push
// on a message. Msg carries the reacted-to message's id with an empty
// Text; Rec carries the full reaction-set record.
const KindReaction = "reaction"

// Link is the room transport the daemon runs over. The daemon is the
// consumer: it needs inbound message events, posting, history (for
// catch-up and reply-author lookups), and the room list; everything
// transport-specific (Slack threads, apron log ids) stays behind the
// implementations.
type Link interface {
	// Events delivers inbound message events; other kinds are filtered
	// by the implementation. The channel closes when the transport
	// stops or ctx is done.
	Events(ctx context.Context) (<-chan LinkEvent, error)
	// Post sends text to a room, optionally as a reply; returns the
	// message id.
	Post(ctx context.Context, roomID, text, replyTo string) (string, error)
	// History returns message and reaction events for a room strictly
	// newer than after (a cursor; 0 = from the start), oldest first,
	// capped. Reaction events carry the full record in Rec.
	History(ctx context.Context, roomID string, after int64, limit int) ([]LinkEvent, error)
	// Rooms lists the rooms the bot is in.
	Rooms(ctx context.Context) ([]RoomRef, error)
}

// LinkEvent is one inbound event from a Link. Cursor is the transport's
// monotonic position for the message (apron log_id, Slack ts in
// microseconds); the daemon stores it as the room cursor for dedupe
// and catch-up.
type LinkEvent struct {
	Kind   string
	RoomID string
	Msg    wake.Msg
	Cursor int64
	// ImageURL is the first image attachment's URL, "" when none.
	ImageURL string
	// Rec is the full reaction-set record on a KindReaction event.
	Rec *apronws.ReactionsRecord
}

// RoomRef is one room the bot is in.
type RoomRef struct{ ID, Title string }

// apronLink adapts an apronws client to Link.
type apronLink struct {
	client *apronws.Client
	// tombstoneFeed, if set, is called with a delete tombstone's author
	// (messageID, roomID, author) so the reply-author cache can resolve
	// a deleted message's author. The tombstone itself is never
	// evaluated.
	tombstoneFeed func(messageID, roomID, author string)
}

// newApronLink wraps an apron client.
func newApronLink(client *apronws.Client) *apronLink { return &apronLink{client: client} }

// SetOnReconnect forwards the reconnect hook to the underlying client
// (the daemon's catch-up wiring). It satisfies the optional hook the
// Run loop looks for on any Link.
func (l *apronLink) SetOnReconnect(fn func()) { l.client.SetOnReconnect(fn) }

// SetTombstoneFeed sets the callback invoked for delete tombstones (the
// daemon wires it to the reply-author cache).
func (l *apronLink) SetTombstoneFeed(fn func(messageID, roomID, author string)) {
	l.tombstoneFeed = fn
}

// feedTombstone records a delete tombstone's author (the tombstone is
// never evaluated, but its author feeds the reply-author cache).
func (l *apronLink) feedTombstone(rec apronws.Record) {
	if rec.Kind != apronws.KindMessage || rec.Message == nil || !rec.Message.Deleted {
		return
	}
	if l.tombstoneFeed != nil {
		m := rec.Message
		l.tombstoneFeed(m.MessageID, m.RoomID, m.From.UserID)
	}
}

// Events pumps the client's record stream, keeping only message records.
func (l *apronLink) Events(ctx context.Context) (<-chan LinkEvent, error) {
	out := make(chan LinkEvent)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case rec, ok := <-l.client.Records():
				if !ok {
					return
				}
				l.feedTombstone(rec)
				ev, keep := apronEvent(rec)
				if !keep {
					continue
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// apronEvent maps one record to a LinkEvent. Room updates, activities,
// and delete tombstones are dropped: the record contract has the daemon
// ignoring them, and a tombstone carries no body to evaluate. A
// reactions record becomes a KindReaction event; the wake decision runs
// on the main loop like every other event, so cursor writes stay in
// log order.
func apronEvent(rec apronws.Record) (LinkEvent, bool) {
	if rec.Kind == apronws.KindReactions {
		if rec.Reactions == nil {
			return LinkEvent{}, false
		}
		return LinkEvent{
			Kind:   KindReaction,
			RoomID: rec.Reactions.RoomID,
			Cursor: int64(rec.Reactions.LogID),
			Rec:    rec.Reactions,
		}, true
	}
	if rec.Kind != apronws.KindMessage || rec.Message == nil || rec.Message.Deleted {
		return LinkEvent{}, false
	}
	m := rec.Message
	ev := LinkEvent{
		Kind:   KindMessage,
		RoomID: m.RoomID,
		Cursor: int64(m.LogID),
		Msg: wake.Msg{
			MessageID: m.MessageID,
			RoomID:    m.RoomID,
			From:      wake.From{UserID: m.From.UserID, Name: m.From.Name},
		},
	}
	if m.ReplyTo != nil {
		ev.Msg.ReplyTo = m.ReplyTo.MessageID
	}
	if m.Body != nil {
		ev.Msg.Text = m.Body.Text
		ev.Msg.Mentions = apronws.DecodeMentions(m.Body.Mentions)
		for _, e := range apronws.DecodeEmbeds(m.Body.Embeds) {
			ev.Msg.Embeds = append(ev.Msg.Embeds, wake.Embed{Title: e.Title, Caption: e.Caption})
			if e.Kind == "image" && ev.ImageURL == "" {
				ev.ImageURL = e.URL
			}
		}
	}
	return ev, true
}

// Post posts to a room, or to the thread the given message started.
func (l *apronLink) Post(ctx context.Context, roomID, text, replyTo string) (string, error) {
	p := apronws.PostParams{RoomID: roomID, Body: apronws.Body{Text: text}}
	if replyTo != "" {
		p.ReplyTo = &apronws.ReplyTo{MessageID: replyTo}
	}
	res, err := l.client.PostMessage(ctx, p)
	if err != nil {
		return "", err
	}
	return res.MessageID, nil
}

// History fetches a room's history and keeps the messages strictly
// newer than the after cursor, oldest first.
func (l *apronLink) History(ctx context.Context, roomID string, after int64, limit int) ([]LinkEvent, error) {
	res, err := l.client.History(ctx, apronws.HistoryParams{RoomID: roomID, Limit: limit})
	if err != nil {
		return nil, err
	}
	var out []LinkEvent
	for i := range res.Messages {
		rec := apronws.Record{Kind: apronws.KindMessage, Message: &res.Messages[i]}
		l.feedTombstone(rec)
		ev, ok := apronEvent(rec)
		if ok && ev.Cursor > after {
			out = append(out, ev)
		}
	}
	// The log's reaction records ride the same window: without them a
	// reaction during a disconnect gap would never wake, while a
	// mention in the same gap replays through catch-up.
	for i := range res.Reactions {
		rec := apronws.Record{Kind: apronws.KindReactions, Reactions: &res.Reactions[i]}
		ev, ok := apronEvent(rec)
		if ok && ev.Cursor > after {
			out = append(out, ev)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Cursor < out[j].Cursor })
	return out, nil
}

// Rooms lists the joined rooms.
func (l *apronLink) Rooms(ctx context.Context) ([]RoomRef, error) {
	res, err := l.client.RoomList(ctx, apronws.RoomListParams{})
	if err != nil {
		return nil, err
	}
	out := make([]RoomRef, 0, len(res.Joined))
	for _, r := range res.Joined {
		out = append(out, RoomRef{ID: r.ID, Title: r.Title})
	}
	return out, nil
}

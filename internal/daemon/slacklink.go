package daemon

import (
	"context"
	"fmt"
	"sort"

	"strings"

	"github.com/sdougbrown/whinny/internal/slack"
	"github.com/sdougbrown/whinny/internal/slack/slackws"
	"github.com/sdougbrown/whinny/internal/wake"
)

// slackLink adapts a Slack Web API client plus a Socket Mode client to
// Link. The bot's user id comes from auth.test (Identify) and drives
// the self-post firebreak and app_mention mention attribution.
type slackLink struct {
	api        *slack.Client
	ws         *slackws.Client
	selfUserID string
	botID      string
}

// newSlackLink wraps the two Slack clients.
func newSlackLink(api *slack.Client, ws *slackws.Client) *slackLink {
	return &slackLink{api: api, ws: ws}
}

// Identify resolves the bot's identity via auth.test and remembers the
// user id and bot id; it must be called before events flow.
func (l *slackLink) Identify(ctx context.Context) error {
	res, err := l.api.AuthTest(ctx)
	if err != nil {
		return err
	}
	if res.UserID == "" {
		return fmt.Errorf("slack auth.test returned no user_id")
	}
	l.selfUserID = res.UserID
	l.botID = res.BotID
	return nil
}

// SelfUserID returns the user id resolved by Identify.
func (l *slackLink) SelfUserID() string { return l.selfUserID }

// BotID returns the bot id resolved by Identify.
func (l *slackLink) BotID() string { return l.botID }

// SetOnReconnect forwards the reconnect hook to the underlying Socket
// Mode client (the daemon's catch-up wiring). It satisfies the optional
// hook the Run loop looks for on any Link, and is called after the
// daemon's link and history cache are set, so the hook never runs
// against a nil dependency.
func (l *slackLink) SetOnReconnect(fn func()) { l.ws.SetOnReconnect(fn) }

// Events pumps Socket Mode events, keeping message and app_mention
// events.
func (l *slackLink) Events(ctx context.Context) (<-chan LinkEvent, error) {
	out := make(chan LinkEvent)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-l.ws.Records():
				if !ok {
					return
				}
				le, keep := l.slackEvent(ev)
				if !keep {
					continue
				}
				select {
				case out <- le:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// slackEvent maps one Socket Mode event to a LinkEvent. Edits unwrap to
// the original ts and so dedupe against the room cursor; deletes are
// dropped (nothing to evaluate).
func (l *slackLink) slackEvent(ev slackws.Event) (LinkEvent, bool) {
	switch ev.Type {
	case "message":
		m, err := slack.DecodeMessage(ev.Payload)
		if err != nil || m.DeletedTS != "" {
			return LinkEvent{}, false
		}
		// Room-metadata subtypes (join/leave/topic/name/purpose, the
		// channel_ and group_ families) are not chat: apron drops the
		// equivalent room-update records, and letting them through
		// would let pattern watches fire on join/leave text.
		if strings.HasPrefix(m.Subtype, "channel_") || strings.HasPrefix(m.Subtype, "group_") {
			return LinkEvent{}, false
		}
		return LinkEvent{
			Kind:   KindMessage,
			RoomID: m.RoomID,
			Msg:    l.wakeMsg(m),
			Cursor: slack.TSToCursor(m.MessageID),
			// ImageURL is left empty on Slack: Slack image embeds need
			// bearer-authenticated fetches the transport-agnostic
			// deliverer doesn't carry, so Slack watch hits deliver
			// text-only envelopes (apron fills it; auth-aware fetches
			// are follow-up work).
		}, true
	case "app_mention":
		// Decode through the same path as the message branch so both
		// duplicates of one mention build identical wake.Msg values
		// (bot attribution, thread-root normalization), then overlay
		// the mention: an app_mention is by definition directed at
		// the bot.
		m, err := slack.DecodeMessage(ev.Payload)
		if err != nil {
			return LinkEvent{}, false
		}
		msg := l.wakeMsg(m)
		msg.Mentions = []string{l.selfUserID}
		return LinkEvent{
			Kind:   KindMessage,
			RoomID: m.RoomID,
			Msg:    msg,
			Cursor: slack.TSToCursor(m.MessageID),
		}, true
	default:
		return LinkEvent{}, false
	}
}

// wakeMsg projects a Slack message onto the policy's Msg. Bot and app
// messages carry no user: the bot's own posts (its bot_id) attribute to
// the bot's user id (driving the self-post firebreak), while other
// bots' posts attribute to "bot:"+bot_id so they evaluate as bot peers
// instead of being swallowed by the firebreak. Mentions come from the
// decode (slack.DecodeMessage parses <@USERID> tokens from the text):
// a message and its app_mention share a ts, so whichever wins the
// cursor dedupe race carries the mention.
func (l *slackLink) wakeMsg(m slack.Msg) wake.Msg {
	from := slack.AuthorID(m.FromUserID, m.BotID, l.selfUserID, l.botID)
	return wake.Msg{
		MessageID: m.MessageID,
		RoomID:    m.RoomID,
		From:      wake.From{UserID: from},
		Text:      m.Text,
		ReplyTo:   m.ReplyTo,
		Mentions:  m.Mentions,
	}
}

// Post posts to a channel, or to the thread the given ts started.
func (l *slackLink) Post(ctx context.Context, roomID, text, replyTo string) (string, error) {
	res, err := l.api.PostMessage(ctx, slack.PostParams{Channel: roomID, Text: text, ThreadTS: replyTo})
	if err != nil {
		return "", err
	}
	return res.TS, nil
}

// History fetches a channel's history and keeps the messages strictly
// newer than the after cursor (a ts in microseconds), oldest first.
// Slack's oldest param is inclusive, so the filter is applied here.
func (l *slackLink) History(ctx context.Context, roomID string, after int64, limit int) ([]LinkEvent, error) {
	res, err := l.api.ConversationsHistory(ctx, slack.HistoryParams{
		Channel: roomID,
		Oldest:  slack.CursorToTS(after),
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	var out []LinkEvent
	for _, m := range res.Messages {
		if m.DeletedTS != "" || m.Subtype == "message_deleted" {
			continue
		}
		cursor := slack.TSToCursor(m.MessageID)
		if cursor <= after {
			continue
		}
		out = append(out, LinkEvent{
			Kind:   KindMessage,
			RoomID: m.RoomID,
			Msg:    l.wakeMsg(m),
			Cursor: cursor,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Cursor < out[j].Cursor })
	return out, nil
}

// Rooms lists every unarchived conversation the bot is in, following
// cursors — the same listing the CLI's send path resolves rooms from,
// so receive and send agree on which rooms exist. Archived channels
// cannot receive posts, so catch-up over them would never deliver.
func (l *slackLink) Rooms(ctx context.Context) ([]RoomRef, error) {
	// Known limitation: under a bot token conversations.list returns no
	// MPIMs, so this listing (and reconnect catch-up over it) misses
	// them — but live message.mpim events still wake. Documented in the
	// README's known-gaps list.
	types := []string{"public_channel", "private_channel", "mpim", "im"}
	var out []RoomRef
	cursor := ""
	for {
		res, err := l.api.ConversationsList(ctx, slack.ListParams{Types: types, Cursor: cursor, Limit: 200, ExcludeArchived: true})
		if err != nil {
			return nil, err
		}
		for _, ch := range res.Channels {
			out = append(out, RoomRef{ID: ch.ID, Title: ch.Name})
		}
		if res.NextCursor == "" {
			return out, nil
		}
		cursor = res.NextCursor
	}
}

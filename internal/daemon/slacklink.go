package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
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
		return LinkEvent{
			Kind:   KindMessage,
			RoomID: m.RoomID,
			Msg:    l.wakeMsg(m),
			Cursor: TSToCursor(m.MessageID),
		}, true
	case "app_mention":
		var w struct {
			Channel  string `json:"channel"`
			User     string `json:"user"`
			Text     string `json:"text"`
			TS       string `json:"ts"`
			ThreadTS string `json:"thread_ts"`
		}
		if err := json.Unmarshal(ev.Payload, &w); err != nil {
			return LinkEvent{}, false
		}
		return LinkEvent{
			Kind:   KindMessage,
			RoomID: w.Channel,
			Msg: wake.Msg{
				MessageID: w.TS,
				RoomID:    w.Channel,
				From:      wake.From{UserID: w.User},
				Text:      w.Text,
				// An app_mention is by definition directed at the bot.
				Mentions: []string{l.selfUserID},
				ReplyTo:  w.ThreadTS,
			},
			Cursor: TSToCursor(w.TS),
		}, true
	default:
		return LinkEvent{}, false
	}
}

// mentionRe matches a Slack user mention token, <@USERID>.
var mentionRe = regexp.MustCompile(`<@([A-Z0-9]+)>`)

// wakeMsg projects a Slack message onto the policy's Msg. Bot and app
// messages carry no user: the bot's own posts (its bot_id) attribute to
// the bot's user id (driving the self-post firebreak), while other
// bots' posts attribute to "bot:"+bot_id so they evaluate as bot peers
// instead of being swallowed by the firebreak. <@USERID> tokens in the
// text are parsed into Mentions: a message and its app_mention share a
// ts, so whichever wins the cursor dedupe race carries the mention.
func (l *slackLink) wakeMsg(m slack.Msg) wake.Msg {
	from := m.FromUserID
	if from == "" && m.BotID != "" {
		if m.BotID == l.botID {
			from = l.selfUserID
		} else {
			from = "bot:" + m.BotID
		}
	}
	return wake.Msg{
		MessageID: m.MessageID,
		RoomID:    m.RoomID,
		From:      wake.From{UserID: from},
		Text:      m.Text,
		ReplyTo:   m.ReplyTo,
		Mentions:  parseMentions(m.Text),
	}
}

// parseMentions extracts every <@USERID> token from text, in order.
func parseMentions(text string) []string {
	var out []string
	for _, m := range mentionRe.FindAllStringSubmatch(text, -1) {
		out = append(out, m[1])
	}
	return out
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
		Oldest:  CursorToTS(after),
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
		cursor := TSToCursor(m.MessageID)
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

// Rooms lists every conversation the bot is in, following cursors.
func (l *slackLink) Rooms(ctx context.Context) ([]RoomRef, error) {
	types := []string{"public_channel", "private_channel", "mpim", "im"}
	var out []RoomRef
	cursor := ""
	for {
		res, err := l.api.ConversationsList(ctx, slack.ListParams{Types: types, Cursor: cursor, Limit: 200})
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

// TSToCursor converts a Slack ts ("1234567890.123456") to a cursor:
// epoch seconds and the fraction, in microseconds. The fraction is
// right-padded to 6 digits (or truncated past 6), which keeps the
// per-channel ordering Slack guarantees.
func TSToCursor(ts string) int64 {
	sec, frac, _ := strings.Cut(ts, ".")
	s, err := strconv.ParseInt(sec, 10, 64)
	if err != nil {
		return 0
	}
	frac = (frac + "000000")[:6]
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return s * 1_000_000
	}
	return s*1_000_000 + f
}

// CursorToTS converts a cursor back to a Slack ts string.
func CursorToTS(c int64) string {
	if c <= 0 {
		return ""
	}
	return fmt.Sprintf("%d.%06d", c/1_000_000, c%1_000_000)
}

package cli

import (
	"context"
	"log"
	"sort"
	"sync"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/slack"
)

// SlackClient is the RoomClient over the Slack Web API: the CLI's send
// path for the slack transport. It translates the apron-shaped
// interface to Slack calls: rooms are channels (conversations.list),
// posts are chat.postMessage, history is conversations.history.
//
// Slack threads are not rooms: CreateThread is a usage error, and
// JoinRoom is a no-op (posting to a channel the bot is in needs no
// join). NotJoined holds the channels the bot is not a member of
// (is_member false), so room resolution by id still reaches them.
type SlackClient struct {
	api *slack.Client
	// selfMu guards the bot identity resolved lazily by History so
	// bot-authored messages attribute the same way the daemon
	// attributes them (shared slack.AuthorID rule).
	selfMu     sync.Mutex
	selfUserID string
	selfBotID  string
}

// NewSlackClient wraps a Slack Web API client as a RoomClient.
func NewSlackClient(api *slack.Client) *SlackClient {
	return &SlackClient{api: api}
}

// PostMessage posts p.Body.Text to the channel p.RoomID, threading it
// under p.ReplyTo.MessageID when set. The result's MessageID is the
// posted message's ts.
func (c *SlackClient) PostMessage(ctx context.Context, p apronws.PostParams) (apronws.PostResult, error) {
	threadTS := ""
	if p.ReplyTo != nil {
		threadTS = p.ReplyTo.MessageID
	}
	res, err := c.api.PostMessage(ctx, slack.PostParams{
		Channel:  p.RoomID,
		Text:     p.Body.Text,
		ThreadTS: threadTS,
	})
	if err != nil {
		return apronws.PostResult{}, err
	}
	return apronws.PostResult{MessageID: res.TS}, nil
}

// RoomList lists unarchived public and private channels, following
// cursors, and splits them by is_member: joined holds the channels the
// bot is a member of, not-joined holds the rest.
func (c *SlackClient) RoomList(ctx context.Context, _ apronws.RoomListParams) (apronws.RoomListResult, error) {
	var joined, notJoined []apronws.Room
	cursor := ""
	for {
		res, err := c.api.ConversationsList(ctx, slack.ListParams{
			Types:           []string{"public_channel", "private_channel"},
			Cursor:          cursor,
			Limit:           200,
			ExcludeArchived: true,
		})
		if err != nil {
			return apronws.RoomListResult{}, err
		}
		for _, ch := range res.Channels {
			room := apronws.Room{ID: ch.ID, Title: ch.Name}
			if ch.IsMember {
				joined = append(joined, room)
			} else {
				notJoined = append(notJoined, room)
			}
		}
		if res.NextCursor == "" {
			break
		}
		cursor = res.NextCursor
	}
	return apronws.RoomListResult{Joined: joined, NotJoined: notJoined}, nil
}

// History fetches the channel's message history and maps each Slack
// message to the apron shape: the message id is the ts, the author is
// the user id, and the thread parent is ReplyTo.
func (c *SlackClient) History(ctx context.Context, p apronws.HistoryParams) (apronws.HistoryResult, error) {
	res, err := c.api.ConversationsHistory(ctx, slack.HistoryParams{
		Channel: p.RoomID,
		Limit:   p.Limit,
	})
	if err != nil {
		return apronws.HistoryResult{}, err
	}
	// Slack returns newest first; the history contract is oldest
	// first, matching the daemon's slackLink ordering.
	sort.Slice(res.Messages, func(i, j int) bool {
		return slack.TSToCursor(res.Messages[i].MessageID) < slack.TSToCursor(res.Messages[j].MessageID)
	})
	if err := c.resolveSelf(ctx); err != nil {
		// Attribution is the only thing the identity drives: a
		// transient auth.test failure must not throw away the
		// already-fetched history. The bot's own posts then
		// attribute as bot:<bot id>.
		log.Printf("whinny: resolve slack identity: %v", err)
	}
	msgs := make([]apronws.Message, 0, len(res.Messages))
	for _, m := range res.Messages {
		if m.DeletedTS != "" || m.Subtype == "message_deleted" {
			// A tombstone replaces the message it removes: the id
			// is the deleted message's ts and the body is nil,
			// matching the apron contract.
			id := m.DeletedTS
			if id == "" {
				id = m.MessageID
			}
			msgs = append(msgs, apronws.Message{
				LogID:     apronws.LogID(slack.TSToCursor(id)),
				MessageID: id,
				RoomID:    m.RoomID,
				Deleted:   true,
			})
			continue
		}
		// The ts converts to a log position so --since/--before
		// filtering works on Slack; per-channel ordering is preserved.
		from := slack.AuthorID(m.FromUserID, m.BotID, c.selfUserID, c.selfBotID)
		msg := apronws.Message{
			LogID:     apronws.LogID(slack.TSToCursor(m.MessageID)),
			MessageID: m.MessageID,
			RoomID:    m.RoomID,
			From:      apronws.From{UserID: from},
			Body:      &apronws.Body{Text: m.Text},
		}
		if m.ReplyTo != "" {
			msg.ReplyTo = &apronws.ReplyTo{MessageID: m.ReplyTo}
		}
		msgs = append(msgs, msg)
	}
	return apronws.HistoryResult{Messages: msgs}, nil
}

// CreateThread is a usage error: Slack threads are not rooms, so the
// --thread-of flow has no Slack equivalent.
func (c *SlackClient) CreateThread(_ context.Context, _ apronws.CreateThreadParams) (apronws.Room, error) {
	return apronws.Room{}, UsageErrorf("thread rooms are not supported on the Slack transport (Slack threads are not rooms)")
}

// JoinRoom is a no-op: posting to a Slack channel the bot is in
// requires no join.
func (c *SlackClient) JoinRoom(_ context.Context, _ string) error { return nil }

// Close is a no-op: the Web API client holds no connection.
func (c *SlackClient) Close() {}

// resolveSelf resolves the bot's identity once via auth.test; the
// attribution rule needs it to map the bot's own posts to its user id.
func (c *SlackClient) resolveSelf(ctx context.Context) error {
	c.selfMu.Lock()
	defer c.selfMu.Unlock()
	if c.selfUserID != "" {
		return nil
	}
	res, err := c.api.AuthTest(ctx)
	if err != nil {
		return err
	}
	c.selfUserID, c.selfBotID = res.UserID, res.BotID
	return nil
}

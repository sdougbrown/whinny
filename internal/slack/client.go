// Package slack is the Slack transport for whinny: a Web API client and
// the message-event mapping the daemon consumes. The Socket Mode client
// lives in the slackws subpackage and shares this package's APIError.
package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const defaultBaseURL = "https://slack.com/api"

// Client is a Slack Web API client. It is safe for concurrent use.
type Client struct {
	token   string
	baseURL string
	http    *http.Client
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithBaseURL overrides the Web API base URL (tests).
func WithBaseURL(u string) ClientOption {
	return func(c *Client) { c.baseURL = strings.TrimSuffix(u, "/") }
}

// WithHTTPClient overrides the HTTP client (tests).
func WithHTTPClient(h *http.Client) ClientOption {
	return func(c *Client) { c.http = h }
}

// NewClient returns a Web API client authenticated with the bot token.
func NewClient(botToken string, opts ...ClientOption) *Client {
	c := &Client{token: botToken, baseURL: defaultBaseURL, http: http.DefaultClient}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// PostParams is the params for chat.postMessage.
type PostParams struct {
	Channel  string
	Text     string
	ThreadTS string
}

// PostResult is the chat.postMessage result.
type PostResult struct {
	TS      string
	Channel string
}

// PostMessage posts a message to a channel or thread. An empty ThreadTS
// posts a top-level message.
func (c *Client) PostMessage(ctx context.Context, p PostParams) (PostResult, error) {
	var out struct {
		TS      string `json:"ts"`
		Channel string `json:"channel"`
	}
	params := map[string]string{"channel": p.Channel, "text": p.Text}
	if p.ThreadTS != "" {
		params["thread_ts"] = p.ThreadTS
	}
	err := c.call(ctx, "chat.postMessage", params, &out)
	if err != nil {
		return PostResult{}, err
	}
	return PostResult{TS: out.TS, Channel: out.Channel}, nil
}

// AuthResult is the auth.test result.
type AuthResult struct {
	UserID string
	BotID  string
}

// AuthTest authenticates the token and returns the caller's identity:
// the bot's user id and bot id.
func (c *Client) AuthTest(ctx context.Context) (AuthResult, error) {
	var out struct {
		UserID string `json:"user_id"`
		BotID  string `json:"bot_id"`
	}
	if err := c.call(ctx, "auth.test", map[string]string{}, &out); err != nil {
		return AuthResult{}, err
	}
	return AuthResult{UserID: out.UserID, BotID: out.BotID}, nil
}

// ListParams is the params for conversations.list.
type ListParams struct {
	Types  []string
	Cursor string
	Limit  int
}

// Channel is one room from conversations.list.
type Channel struct {
	ID   string
	Name string
	IsIM bool
}

// ListResult is the conversations.list result.
type ListResult struct {
	Channels   []Channel
	NextCursor string
}

// ConversationsList lists channels, private channels, IMs, and MPIMs as
// requested.
func (c *Client) ConversationsList(ctx context.Context, p ListParams) (ListResult, error) {
	params := map[string]any{
		"types": strings.Join(p.Types, ","),
	}
	if p.Cursor != "" {
		params["cursor"] = p.Cursor
	}
	if p.Limit > 0 {
		params["limit"] = p.Limit
	}
	var out struct {
		Channels []struct {
			ID        string `json:"id"`
			Name      string `json:"name"`
			IsIM      bool   `json:"is_im"`
			IsChannel bool   `json:"is_channel"`
			IsGroup   bool   `json:"is_group"`
		} `json:"channels"`
		ResponseMetadata struct {
			NextCursor string `json:"next_cursor"`
		} `json:"response_metadata"`
	}
	if err := c.call(ctx, "conversations.list", params, &out); err != nil {
		return ListResult{}, err
	}
	res := ListResult{NextCursor: out.ResponseMetadata.NextCursor}
	for _, ch := range out.Channels {
		res.Channels = append(res.Channels, Channel{ID: ch.ID, Name: ch.Name, IsIM: ch.IsIM})
	}
	return res, nil
}

// HistoryParams is the params for conversations.history.
type HistoryParams struct {
	Channel string
	Cursor  string
	Oldest  string
	Limit   int
}

// HistoryResult is the conversations.history result.
type HistoryResult struct {
	Messages   []Msg
	NextCursor string
	HasMore    bool
}

// ConversationsHistory fetches a channel's message history. Messages are
// mapped to the daemon-facing Msg shape; the channel fills Msg.RoomID.
func (c *Client) ConversationsHistory(ctx context.Context, p HistoryParams) (HistoryResult, error) {
	params := map[string]any{"channel": p.Channel}
	if p.Cursor != "" {
		params["cursor"] = p.Cursor
	}
	if p.Oldest != "" {
		params["oldest"] = p.Oldest
	}
	if p.Limit > 0 {
		params["limit"] = p.Limit
	}
	var out struct {
		Messages         []json.RawMessage `json:"messages"`
		HasMore          bool              `json:"has_more"`
		ResponseMetadata struct {
			NextCursor string `json:"next_cursor"`
		} `json:"response_metadata"`
	}
	if err := c.call(ctx, "conversations.history", params, &out); err != nil {
		return HistoryResult{}, err
	}
	res := HistoryResult{NextCursor: out.ResponseMetadata.NextCursor, HasMore: out.HasMore}
	for _, raw := range out.Messages {
		m, err := DecodeMessage(raw)
		if err != nil {
			return HistoryResult{}, fmt.Errorf("conversations.history: %w", err)
		}
		m.RoomID = p.Channel
		res.Messages = append(res.Messages, m)
	}
	return res, nil
}

// ConversationsOpen opens (or finds) the IM channel with a user and
// returns its channel id.
func (c *Client) ConversationsOpen(ctx context.Context, userID string) (string, error) {
	var out struct {
		Channel struct {
			ID string `json:"id"`
		} `json:"channel"`
	}
	params := map[string]string{"users": userID}
	if err := c.call(ctx, "conversations.open", params, &out); err != nil {
		return "", err
	}
	return out.Channel.ID, nil
}

// User is one user from users.list.
type User struct {
	ID      string
	Name    string
	IsBot   bool
	Deleted bool
}

// UsersList lists all users, following cursors.
func (c *Client) UsersList(ctx context.Context) ([]User, error) {
	var users []User
	cursor := ""
	for {
		params := map[string]any{"limit": 200}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var out struct {
			Members []struct {
				ID      string `json:"id"`
				Name    string `json:"name"`
				IsBot   bool   `json:"is_bot"`
				Deleted bool   `json:"deleted"`
			} `json:"members"`
			ResponseMetadata struct {
				NextCursor string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		if err := c.call(ctx, "users.list", params, &out); err != nil {
			return nil, err
		}
		for _, u := range out.Members {
			users = append(users, User{ID: u.ID, Name: u.Name, IsBot: u.IsBot, Deleted: u.Deleted})
		}
		if out.ResponseMetadata.NextCursor == "" {
			return users, nil
		}
		cursor = out.ResponseMetadata.NextCursor
	}
}

// call performs one Web API call: POST <base>/<method> with a JSON body,
// then decodes Slack's response envelope. Slack signals failures three
// ways: an ok:false envelope on 200, HTTP 429 with Retry-After, and other
// non-200 statuses; all become APIError.
func (c *Client) call(ctx context.Context, method string, params any, out any) error {
	body, err := json.Marshal(params)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/"+method, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		apiErr := &APIError{Method: method, SlackError: "rate_limited", RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
		return apiErr
	}
	if resp.StatusCode != http.StatusOK {
		return &APIError{Method: method, SlackError: fmt.Sprintf("http_%d", resp.StatusCode)}
	}
	var envelope struct {
		OK    bool            `json:"ok"`
		Error string          `json:"error"`
		Raw   json.RawMessage `json:"-"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("slack %s: decode response: %w", method, err)
	}
	if !envelope.OK {
		return &APIError{Method: method, SlackError: envelope.Error}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

// parseRetryAfter parses a Retry-After header in seconds; absent or
// unparseable values are 0.
func parseRetryAfter(h string) time.Duration {
	secs, err := strconv.Atoi(strings.TrimSpace(h))
	if err != nil || secs < 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}

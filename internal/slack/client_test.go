package slack

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// apiFake is a fake Slack Web API server. It records the last request's
// Authorization header and decoded JSON body per method, and serves
// canned responses from handler.
type apiFake struct {
	t  *testing.T
	ts *httptest.Server

	mu      sync.Mutex
	auths   map[string]string
	bodies  map[string]json.RawMessage
	handler func(w http.ResponseWriter, r *http.Request)
}

func newAPIFake(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) *apiFake {
	f := &apiFake{
		t:       t,
		auths:   map[string]string{},
		bodies:  map[string]json.RawMessage{},
		handler: handler,
	}
	f.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auths[r.URL.Path] = r.Header.Get("Authorization")
		var body json.RawMessage
		if r.Body != nil {
			raw, _ := io.ReadAll(r.Body)
			body = raw
			f.bodies[r.URL.Path] = body
		}
		f.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(f.ts.Close)
	return f
}

func (f *apiFake) auth(method string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.auths["/"+method]
}

func (f *apiFake) body(method string) json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bodies["/"+method]
}

func (f *apiFake) client() *Client {
	return NewClient("xoxb-test", WithBaseURL(f.ts.URL))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func TestPostMessage(t *testing.T) {
	f := newAPIFake(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat.postMessage" {
			t.Errorf("path = %q", r.URL.Path)
		}
		writeJSON(w, 200, map[string]any{"ok": true, "ts": "1712345678.000100", "channel": "C123"})
	})
	c := f.client()
	res, err := c.PostMessage(context.Background(), PostParams{Channel: "C123", Text: "hello", ThreadTS: "1712345678.000050"})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	if res.TS != "1712345678.000100" || res.Channel != "C123" {
		t.Fatalf("PostResult = %+v", res)
	}
	if got, want := f.auth("chat.postMessage"), "Bearer xoxb-test"; got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
	var body map[string]string
	if err := json.Unmarshal(f.body("chat.postMessage"), &body); err != nil {
		t.Fatal(err)
	}
	for _, want := range [][2]string{{"channel", "C123"}, {"text", "hello"}, {"thread_ts", "1712345678.000050"}} {
		if body[want[0]] != want[1] {
			t.Errorf("body[%q] = %q, want %q", want[0], body[want[0]], want[1])
		}
	}
}

func TestPostMessageOmitsThreadTS(t *testing.T) {
	f := newAPIFake(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true, "ts": "1.1", "channel": "C1"})
	})
	if _, err := f.client().PostMessage(context.Background(), PostParams{Channel: "C1", Text: "hi"}); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	var body map[string]string
	if err := json.Unmarshal(f.body("chat.postMessage"), &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["thread_ts"]; ok {
		t.Errorf("thread_ts sent for a top-level message: %v", body)
	}
}

func TestAPIErrorOKFalse(t *testing.T) {
	f := newAPIFake(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": false, "error": "channel_not_found"})
	})
	_, err := f.client().PostMessage(context.Background(), PostParams{Channel: "C404", Text: "hi"})
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("err = %T (%v), want *APIError", err, err)
	}
	if apiErr.Method != "chat.postMessage" || apiErr.SlackError != "channel_not_found" || apiErr.RetryAfter != 0 {
		t.Fatalf("APIError = %+v", apiErr)
	}
}

func TestAPIErrorRateLimited(t *testing.T) {
	f := newAPIFake(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"ok":false,"error":"rate_limited"}`))
	})
	_, err := f.client().PostMessage(context.Background(), PostParams{Channel: "C1", Text: "hi"})
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("err = %T (%v), want *APIError", err, err)
	}
	if apiErr.SlackError != "rate_limited" || apiErr.Method != "chat.postMessage" {
		t.Fatalf("APIError = %+v", apiErr)
	}
	if apiErr.RetryAfter != 7*time.Second {
		t.Fatalf("RetryAfter = %v, want 7s", apiErr.RetryAfter)
	}
}

func TestAPIErrorRateLimitedNoHeader(t *testing.T) {
	f := newAPIFake(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{}`))
	})
	_, err := f.client().PostMessage(context.Background(), PostParams{Channel: "C1", Text: "hi"})
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("err = %T (%v), want *APIError", err, err)
	}
	if apiErr.SlackError != "rate_limited" || apiErr.RetryAfter != 0 {
		t.Fatalf("APIError = %+v", apiErr)
	}
}

func TestAPIErrorNon200(t *testing.T) {
	f := newAPIFake(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	_, err := f.client().PostMessage(context.Background(), PostParams{Channel: "C1", Text: "hi"})
	apiErr, ok := err.(*APIError)
	if !ok {
		t.Fatalf("err = %T (%v), want *APIError", err, err)
	}
	if apiErr.SlackError != "http_500" {
		t.Fatalf("SlackError = %q, want http_500", apiErr.SlackError)
	}
}

func TestConversationsList(t *testing.T) {
	var f *apiFake
	f = newAPIFake(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Types  string `json:"types"`
			Cursor string `json:"cursor"`
			Limit  int    `json:"limit"`
		}
		_ = json.Unmarshal(f.body("conversations.list"), &body)
		if body.Cursor != "" || body.Types != "public_channel,private_channel" || body.Limit != 100 {
			t.Errorf("params = %+v", body)
		}
		writeJSON(w, 200, map[string]any{
			"ok": true,
			"channels": []map[string]any{
				{"id": "C1", "name": "general"},
				{"id": "D1", "name": "", "is_im": true},
			},
			"response_metadata": map[string]any{"next_cursor": "cur2"},
		})
	})
	c := f.client()
	// The caller follows the cursor; the client maps one page per call.
	res, err := c.ConversationsList(context.Background(), ListParams{Types: []string{"public_channel", "private_channel"}, Limit: 100})
	if err != nil {
		t.Fatalf("ConversationsList: %v", err)
	}
	if len(res.Channels) != 2 {
		t.Fatalf("channels = %+v, want 2", res.Channels)
	}
	if res.Channels[0].ID != "C1" || res.Channels[0].Name != "general" {
		t.Errorf("channels[0] = %+v", res.Channels[0])
	}
	if !res.Channels[1].IsIM {
		t.Errorf("channels[1].IsIM = false, want true")
	}
	if res.NextCursor != "cur2" {
		t.Errorf("NextCursor = %q, want cur2", res.NextCursor)
	}
}

func TestConversationsHistory(t *testing.T) {
	f := newAPIFake(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{
			"ok":       true,
			"has_more": true,
			"messages": []map[string]any{
				{"type": "message", "user": "U1", "text": "hello", "ts": "1.1", "thread_ts": "0.9"},
				{"type": "message", "bot_id": "B1", "subtype": "bot_message", "text": "beep", "ts": "1.2"},
			},
			"response_metadata": map[string]any{"next_cursor": "next-here"},
		})
	})
	res, err := f.client().ConversationsHistory(context.Background(), HistoryParams{Channel: "C1", Oldest: "0.9", Limit: 50})
	if err != nil {
		t.Fatalf("ConversationsHistory: %v", err)
	}
	if !res.HasMore || res.NextCursor != "next-here" {
		t.Fatalf("result = %+v", res)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("messages = %+v", res.Messages)
	}
	m := res.Messages[0]
	if m.RoomID != "C1" || m.MessageID != "1.1" || m.FromUserID != "U1" || m.Text != "hello" || m.ReplyTo != "0.9" {
		t.Errorf("messages[0] = %+v", m)
	}
	b := res.Messages[1]
	if b.BotID != "B1" || b.FromUserID != "" || b.Subtype != "bot_message" {
		t.Errorf("messages[1] = %+v", b)
	}
	var body map[string]any
	if err := json.Unmarshal(f.body("conversations.history"), &body); err != nil {
		t.Fatal(err)
	}
	if body["channel"] != "C1" || body["oldest"] != "0.9" || body["limit"] != float64(50) {
		t.Errorf("body = %v", body)
	}
}

func TestConversationsOpen(t *testing.T) {
	f := newAPIFake(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true, "channel": map[string]any{"id": "D123"}})
	})
	ch, err := f.client().ConversationsOpen(context.Background(), "U42")
	if err != nil {
		t.Fatalf("ConversationsOpen: %v", err)
	}
	if ch != "D123" {
		t.Fatalf("channel = %q, want D123", ch)
	}
	var body map[string]string
	if err := json.Unmarshal(f.body("conversations.open"), &body); err != nil {
		t.Fatal(err)
	}
	if body["users"] != "U42" {
		t.Errorf("body = %v", body)
	}
}

func TestUsersListPaging(t *testing.T) {
	pages := []map[string]any{
		{
			"ok": true,
			"members": []map[string]any{
				{"id": "U1", "name": "alice"},
				{"id": "U2", "name": "bot", "is_bot": true},
			},
			"response_metadata": map[string]any{"next_cursor": "p2"},
		},
		{
			"ok": true,
			"members": []map[string]any{
				{"id": "U3", "name": "gone", "deleted": true},
			},
		},
	}
	calls := 0
	var f *apiFake
	f = newAPIFake(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Cursor string `json:"cursor"`
			Limit  int    `json:"limit"`
		}
		_ = json.Unmarshal(f.body("users.list"), &body)
		if calls == 1 && body.Cursor != "p2" {
			t.Errorf("page 2 cursor = %q, want p2", body.Cursor)
		}
		if body.Limit != 200 {
			t.Errorf("limit = %d, want 200", body.Limit)
		}
		writeJSON(w, 200, pages[calls])
		calls++
	})
	users, err := f.client().UsersList(context.Background())
	if err != nil {
		t.Fatalf("UsersList: %v", err)
	}
	if len(users) != 3 {
		t.Fatalf("users = %+v", users)
	}
	if users[0].ID != "U1" || users[0].Name != "alice" {
		t.Errorf("users[0] = %+v", users[0])
	}
	if !users[1].IsBot {
		t.Errorf("users[1].IsBot = false, want true")
	}
	if !users[2].Deleted {
		t.Errorf("users[2].Deleted = false, want true")
	}
}

func TestParseRetryAfter(t *testing.T) {
	cases := []struct {
		header string
		want   time.Duration
	}{
		{"", 0},
		{"  5 ", 5 * time.Second},
		{"0", 0},
		{"abc", 0},
		{"-3", 0},
		{strconv.Itoa(2), 2 * time.Second},
	}
	for _, c := range cases {
		if got := parseRetryAfter(c.header); got != c.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", c.header, got, c.want)
		}
	}
}

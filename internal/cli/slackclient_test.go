package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/slack"
)

// fakeSlackAPI plays the Web API endpoints the SlackClient adapter
// calls, recording the post and list requests for assertions.
type fakeSlackAPI struct {
	mu        sync.Mutex
	posts     []slack.PostParams
	lists     []slack.ListParams
	histories []slack.HistoryParams
	// historyJSON overrides the canned conversations.history payload
	// when set (tests that need specific message shapes).
	historyJSON string
}

func newFakeSlackAPI(t *testing.T) (*fakeSlackAPI, *slack.Client) {
	t.Helper()
	f := &fakeSlackAPI{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/chat.postMessage":
			var p struct {
				Channel  string `json:"channel"`
				Text     string `json:"text"`
				ThreadTS string `json:"thread_ts"`
			}
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				t.Errorf("decode post body: %v", err)
			}
			f.mu.Lock()
			f.posts = append(f.posts, slack.PostParams{Channel: p.Channel, Text: p.Text, ThreadTS: p.ThreadTS})
			f.mu.Unlock()
			w.Write([]byte(`{"ok":true,"ts":"1700000000.000100","channel":"` + p.Channel + `"}`))
		case "/conversations.list":
			var p struct {
				Types           string `json:"types"`
				Cursor          string `json:"cursor"`
				ExcludeArchived bool   `json:"exclude_archived"`
			}
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				t.Errorf("decode list body: %v", err)
			}
			f.mu.Lock()
			f.lists = append(f.lists, slack.ListParams{
				Types:           strings.Split(p.Types, ","),
				Cursor:          p.Cursor,
				ExcludeArchived: p.ExcludeArchived,
			})
			f.mu.Unlock()
			w.Write([]byte(`{"ok":true,"channels":[
				{"id":"C1","name":"general","is_member":true},
				{"id":"C2","name":"random","is_member":false}
			]}`))
		case "/conversations.history":
			var p struct {
				Channel string `json:"channel"`
				Limit   int    `json:"limit"`
			}
			if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
				t.Errorf("decode history body: %v", err)
			}
			f.mu.Lock()
			f.histories = append(f.histories, slack.HistoryParams{Channel: p.Channel, Limit: p.Limit})
			f.mu.Unlock()
			if f.historyJSON != "" {
				w.Write([]byte(f.historyJSON))
				return
			}
			w.Write([]byte(`{"ok":true,"messages":[
				{"type":"message","user":"U1","text":"hello","ts":"1700000000.000100"},
				{"type":"message","user":"U2","text":"reply","ts":"1700000000.000200","thread_ts":"1700000000.000100"},
				{"type":"message","bot_id":"B1","text":"from the bot","ts":"1700000000.000300"}
			]}`))
		case "/auth.test":
			w.Write([]byte(`{"ok":true,"user_id":"U-SELF","bot_id":"B-SELF"}`))
		default:
			w.Write([]byte(`{"ok":false,"error":"not_mocked"}`))
		}
	}))
	t.Cleanup(ts.Close)
	return f, slack.NewClient("xoxb-test", slack.WithBaseURL(ts.URL))
}

func TestSlackClientPost(t *testing.T) {
	f, api := newFakeSlackAPI(t)
	c := NewSlackClient(api)

	// Top-level: no reply-to means no thread_ts.
	res, err := c.PostMessage(context.Background(), apronws.PostParams{
		RoomID: "C1",
		Body:   apronws.Body{Text: "hello"},
	})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	if res.MessageID != "1700000000.000100" {
		t.Fatalf("MessageID = %q, want the ts", res.MessageID)
	}

	// Reply: the reply-to message id becomes the thread_ts.
	if _, err := c.PostMessage(context.Background(), apronws.PostParams{
		RoomID:  "C1",
		Body:    apronws.Body{Text: "reply"},
		ReplyTo: &apronws.ReplyTo{MessageID: "1700000000.000100"},
	}); err != nil {
		t.Fatalf("PostMessage (reply): %v", err)
	}

	if len(f.posts) != 2 {
		t.Fatalf("posts = %d, want 2", len(f.posts))
	}
	if f.posts[0].Channel != "C1" || f.posts[0].Text != "hello" || f.posts[0].ThreadTS != "" {
		t.Fatalf("first post = %+v, want channel C1, text hello, no thread_ts", f.posts[0])
	}
	if f.posts[1].ThreadTS != "1700000000.000100" {
		t.Fatalf("second post thread_ts = %q, want the reply-to id", f.posts[1].ThreadTS)
	}
}

func TestSlackClientRoomList(t *testing.T) {
	f, api := newFakeSlackAPI(t)
	c := NewSlackClient(api)

	res, err := c.RoomList(context.Background(), apronws.RoomListParams{})
	if err != nil {
		t.Fatalf("RoomList: %v", err)
	}
	if len(res.Joined) != 1 || res.Joined[0].ID != "C1" || res.Joined[0].Title != "general" {
		t.Fatalf("joined = %+v, want the is_member channel", res.Joined)
	}
	if len(res.NotJoined) != 1 || res.NotJoined[0].ID != "C2" {
		t.Fatalf("notJoined = %+v, want the non-member channel", res.NotJoined)
	}
	if len(f.lists) != 1 {
		t.Fatalf("lists = %d, want 1", len(f.lists))
	}
	l := f.lists[0]
	if len(l.Types) != 2 || l.Types[0] != "public_channel" || l.Types[1] != "private_channel" {
		t.Fatalf("types = %v, want public_channel,private_channel", l.Types)
	}
	if !l.ExcludeArchived {
		t.Fatalf("exclude_archived = false, want true")
	}
}

func TestSlackClientHistory(t *testing.T) {
	f, api := newFakeSlackAPI(t)
	c := NewSlackClient(api)

	res, err := c.History(context.Background(), apronws.HistoryParams{RoomID: "C1", Limit: 20})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(res.Messages) != 3 {
		t.Fatalf("messages = %d, want 3", len(res.Messages))
	}
	m0 := res.Messages[0]
	if m0.MessageID != "1700000000.000100" || m0.RoomID != "C1" {
		t.Fatalf("first message = id %q room %q, want the ts and channel", m0.MessageID, m0.RoomID)
	}
	if m0.From.UserID != "U1" {
		t.Fatalf("from = %+v, want user id U1", m0.From)
	}
	if m0.Body == nil || m0.Body.Text != "hello" {
		t.Fatalf("body = %+v, want hello", m0.Body)
	}
	if m0.ReplyTo != nil {
		t.Fatalf("reply-to = %+v, want nil for a top-level message", m0.ReplyTo)
	}
	m1 := res.Messages[1]
	if m1.ReplyTo == nil || m1.ReplyTo.MessageID != "1700000000.000100" {
		t.Fatalf("second reply-to = %+v, want the thread parent ts", m1.ReplyTo)
	}
	// The ts converts to a log position so cursor filtering works, and
	// the mapping keeps the contract's oldest-first order (Slack's wire
	// order is newest first).
	if m0.LogID != apronws.LogID(slack.TSToCursor("1700000000.000100")) {
		t.Fatalf("first log_id = %d, want the ts-derived cursor", m0.LogID)
	}
	if m1.LogID <= m0.LogID {
		t.Fatalf("log ids = %d then %d, want ascending (oldest first)", m0.LogID, m1.LogID)
	}
	m2 := res.Messages[2]
	if m2.From.UserID != "bot:B1" {
		t.Fatalf("bot message from = %+v, want bot:B1 (no blank author)", m2.From)
	}
	if len(f.histories) != 1 || f.histories[0].Channel != "C1" || f.histories[0].Limit != 20 {
		t.Fatalf("histories = %+v, want channel C1 limit 20", f.histories)
	}
}

func TestSlackClientCreateThread(t *testing.T) {
	_, api := newFakeSlackAPI(t)
	c := NewSlackClient(api)

	_, err := c.CreateThread(context.Background(), apronws.CreateThreadParams{ParentRoomID: "C1"})
	if err == nil {
		t.Fatalf("CreateThread: want a usage error, got nil")
	}
	if e, ok := err.(*Error); !ok || e.Class != ClassUsage {
		t.Fatalf("err = %v, want a usage-class error", err)
	}
	if !strings.Contains(err.Error(), "not supported on the Slack transport") {
		t.Fatalf("err = %q, want the thread-rooms-not-supported text", err)
	}
}

func TestSlackClientJoinRoom(t *testing.T) {
	_, api := newFakeSlackAPI(t)
	c := NewSlackClient(api)

	if err := c.JoinRoom(context.Background(), "C1"); err != nil {
		t.Fatalf("JoinRoom = %v, want a no-op", err)
	}
	c.Close()
}

// TestSlackClientHistorySelfBotAttribution: the resolving bot's own
// posts attribute to its user id (not bot:<bot id>), matching the
// daemon's wake evaluation — one identity per sender on both paths.
func TestSlackClientHistorySelfBotAttribution(t *testing.T) {
	f, api := newFakeSlackAPI(t)
	f.historyJSON = `{"ok":true,"messages":[
		{"type":"message","bot_id":"B-SELF","text":"my own post","ts":"1700000000.000400"}
	]}`
	c := NewSlackClient(api)

	res, err := c.History(context.Background(), apronws.HistoryParams{RoomID: "C1", Limit: 20})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(res.Messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(res.Messages))
	}
	if got := res.Messages[0].From.UserID; got != "U-SELF" {
		t.Fatalf("from = %q, want the bot's own user id U-SELF", got)
	}
}

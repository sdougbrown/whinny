package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sdougbrown/whinny/internal/slack"
	"github.com/sdougbrown/whinny/internal/slack/slackws"
)

// slackAPIFake plays the Web API endpoints the Slack link calls. Each
// map key is an API method; the handler's response is the JSON body.
func slackAPIFake(methods map[string]string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := strings.TrimPrefix(r.URL.Path, "/")
		w.Header().Set("Content-Type", "application/json")
		body, ok := methods[method]
		if !ok {
			w.Write([]byte(`{"ok":false,"error":"not_mocked"}`))
			return
		}
		w.Write([]byte(body))
	}))
}

func TestSlackIdentify(t *testing.T) {
	srv := slackAPIFake(map[string]string{
		"auth.test": `{"ok":true,"user_id":"U9","bot_id":"B1"}`,
	})
	defer srv.Close()
	l := newSlackLink(slack.NewClient("x-token", slack.WithBaseURL(srv.URL)), nil)
	if err := l.Identify(context.Background()); err != nil {
		t.Fatalf("Identify: %v", err)
	}
	if l.SelfUserID() != "U9" {
		t.Errorf("self = %q, want U9", l.SelfUserID())
	}
}

func TestSlackEventMessageMapping(t *testing.T) {
	l := &slackLink{selfUserID: "U9"}
	ev, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(
		`{"type":"message","channel":"C1","user":"U1","text":"hello","ts":"1700000000.000100","thread_ts":"1700000000.000050"}`)})
	if !ok {
		t.Fatal("message event dropped")
	}
	if ev.Kind != KindMessage || ev.RoomID != "C1" {
		t.Errorf("event = %+v", ev)
	}
	if ev.Msg.From.UserID != "U1" || ev.Msg.Text != "hello" || ev.Msg.ReplyTo != "1700000000.000050" {
		t.Errorf("msg = %+v", ev.Msg)
	}
	// ts 1700000000.000100 → 1700000000 s + 100 µs.
	if ev.Cursor != 1700000000_000100 {
		t.Errorf("cursor = %d, want %d", ev.Cursor, 1700000000_000100)
	}
}

func TestSlackEventBotMessageAttribution(t *testing.T) {
	l := &slackLink{selfUserID: "U9"}
	ev, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(
		`{"type":"message","channel":"C1","bot_id":"B1","subtype":"bot_message","text":"my post","ts":"1700000000.000200"}`)})
	if !ok {
		t.Fatal("bot message event dropped")
	}
	if ev.Msg.From.UserID != "U9" {
		t.Errorf("from = %q, want the bot's own id U9", ev.Msg.From.UserID)
	}
}

func TestSlackEventAppMention(t *testing.T) {
	l := &slackLink{selfUserID: "U9"}
	ev, ok := l.slackEvent(slackws.Event{Type: "app_mention", Payload: json.RawMessage(
		`{"type":"app_mention","channel":"C1","user":"U1","text":"<@U9> do the thing","ts":"1700000000.000300","thread_ts":"1700000000.000100"}`)})
	if !ok {
		t.Fatal("app_mention event dropped")
	}
	if len(ev.Msg.Mentions) != 1 || ev.Msg.Mentions[0] != "U9" {
		t.Errorf("mentions = %v, want [U9]", ev.Msg.Mentions)
	}
	if ev.Msg.From.UserID != "U1" || ev.Msg.ReplyTo != "1700000000.000100" {
		t.Errorf("msg = %+v", ev.Msg)
	}
}

func TestSlackEventSkipsDeletesAndUnknown(t *testing.T) {
	l := &slackLink{selfUserID: "U9"}
	if _, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(
		`{"type":"message","channel":"C1","ts":"1700000000.000001","subtype":"message_deleted","deleted_ts":"1700000000.000000"}`)}); ok {
		t.Error("delete kept")
	}
	if _, ok := l.slackEvent(slackws.Event{Type: "reaction_added", Payload: json.RawMessage(`{}`)}); ok {
		t.Error("non-message event kept")
	}
}

func TestSlackPostThreadTS(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in map[string]any
		json.NewDecoder(r.Body).Decode(&in)
		if r.URL.Path == "/chat.postMessage" {
			got = in
			w.Write([]byte(`{"ok":true,"ts":"1700000000.000900","channel":"C1"}`))
			return
		}
		w.Write([]byte(`{"ok":false,"error":"not_mocked"}`))
	}))
	defer srv.Close()
	l := newSlackLink(slack.NewClient("x-token", slack.WithBaseURL(srv.URL)), nil)
	ts, err := l.Post(context.Background(), "C1", "here you go", "1700000000.000500")
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	if ts != "1700000000.000900" {
		t.Errorf("ts = %q", ts)
	}
	if got["channel"] != "C1" || got["text"] != "here you go" || got["thread_ts"] != "1700000000.000500" {
		t.Errorf("post params = %v", got)
	}
}

func TestSlackHistory(t *testing.T) {
	var gotOldest string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth.test":
			w.Write([]byte(`{"ok":true,"user_id":"U9","bot_id":"B1"}`))
			return
		case "/conversations.history":
			var in map[string]any
			json.NewDecoder(r.Body).Decode(&in)
			gotOldest, _ = in["oldest"].(string)
			// Slack returns newest first.
			w.Write([]byte(`{"ok":true,"messages":[
				{"user":"U1","text":"newer","ts":"1700000000.000200"},
				{"user":"U1","text":"at cursor","ts":"1700000000.000100"},
				{"user":"U1","text":"older","ts":"1700000000.000050"},
				{"bot_id":"B1","subtype":"bot_message","text":"mine","ts":"1700000000.000300"}
			]}`))
		default:
			w.Write([]byte(`{"ok":false,"error":"not_mocked"}`))
		}
	}))
	defer srv.Close()
	l := newSlackLink(slack.NewClient("x-token", slack.WithBaseURL(srv.URL)), nil)
	if err := l.Identify(context.Background()); err != nil {
		t.Fatalf("Identify: %v", err)
	}
	events, err := l.History(context.Background(), "C1", TSToCursor("1700000000.000100"), 50)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	// The oldest param is the after cursor's ts; the filter is strict.
	if gotOldest != "1700000000.000100" {
		t.Errorf("oldest = %q, want the after cursor's ts", gotOldest)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2 (strictly newer than the cursor)", len(events))
	}
	// Oldest first: .000200 then .000300, the bot message attributed to
	// the bot's own id.
	if events[0].Msg.Text != "newer" || events[1].Msg.Text != "mine" || events[1].Msg.From.UserID != "U9" {
		t.Errorf("events not oldest-first or bot misattributed: %+v", events)
	}
}

func TestSlackRooms(t *testing.T) {
	page := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/conversations.list" {
			w.Write([]byte(`{"ok":false,"error":"not_mocked"}`))
			return
		}
		page++
		if page == 1 {
			w.Write([]byte(`{"ok":true,"channels":[{"id":"C1","name":"general"},{"id":"C2","name":"ops","is_im":false}],"response_metadata":{"next_cursor":"next"}}`))
			return
		}
		w.Write([]byte(`{"ok":true,"channels":[{"id":"D1","name":"","is_im":true}],"response_metadata":{}}`))
	}))
	defer srv.Close()
	l := newSlackLink(slack.NewClient("x-token", slack.WithBaseURL(srv.URL)), nil)
	rooms, err := l.Rooms(context.Background())
	if err != nil {
		t.Fatalf("Rooms: %v", err)
	}
	if len(rooms) != 3 || rooms[0] != (RoomRef{ID: "C1", Title: "general"}) || rooms[2].ID != "D1" {
		t.Errorf("rooms = %+v", rooms)
	}
}

func TestTSToCursorRoundTrip(t *testing.T) {
	for _, ts := range []string{"1700000000.000100", "1700000000.1", "1700000000.000000", "1700000000.123456"} {
		if got := CursorToTS(TSToCursor(ts)); got != padTS(ts) {
			t.Errorf("round trip %q = %q, want %q", ts, got, padTS(ts))
		}
	}
	if TSToCursor("1700000000.1234567") != 1700000000_123456 {
		t.Error("fraction longer than 6 digits must truncate")
	}
	if CursorToTS(0) != "" {
		t.Error("zero cursor must map to an empty ts")
	}
}

// padTS normalizes a test ts to the canonical 6-digit fraction shape.
func padTS(ts string) string {
	sec, frac, _ := strings.Cut(ts, ".")
	for len(frac) < 6 {
		frac += "0"
	}
	return sec + "." + frac
}

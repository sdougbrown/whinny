package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sdougbrown/whinny/internal/slack"
	"github.com/sdougbrown/whinny/internal/slack/slackws"
	"github.com/sdougbrown/whinny/internal/wake"
	"github.com/sdougbrown/whinny/internal/watch"
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
	if l.BotID() != "B1" {
		t.Errorf("bot id = %q, want B1", l.BotID())
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
	l := &slackLink{selfUserID: "U9", botID: "B1"}
	// The bot's own post (its bot_id) attributes to its user id.
	ev, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(
		`{"type":"message","channel":"C1","bot_id":"B1","subtype":"bot_message","text":"my post","ts":"1700000000.000200"}`)})
	if !ok {
		t.Fatal("bot message event dropped")
	}
	if ev.Msg.From.UserID != "U9" {
		t.Errorf("self bot from = %q, want the bot's own id U9", ev.Msg.From.UserID)
	}
	// A foreign bot's post attributes to "bot:"+bot_id, not the self id.
	ev2, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(
		`{"type":"message","channel":"C1","bot_id":"B2","subtype":"bot_message","text":"their post","ts":"1700000000.000210"}`)})
	if !ok {
		t.Fatal("foreign bot message event dropped")
	}
	if ev2.Msg.From.UserID != "bot:B2" {
		t.Errorf("foreign bot from = %q, want bot:B2", ev2.Msg.From.UserID)
	}

	// The foreign bot evaluates as a bot peer: a plain text with no
	// matching watch is ignored (the policy's bot firebreak), but a
	// matching watch fires.
	peers := wake.Peers{"bot:B2": {Name: "otherbot", IsBot: true}}
	self := wake.Self{UserID: "U9"}
	if dec := wake.Evaluate(context.Background(), wake.In{Msg: ev2.Msg, Self: self, Peers: peers}); dec.Wake {
		t.Errorf("foreign bot plain text woke (%+v); want ignored (bot peer, no watch match)", dec)
	}
	bTrue := true
	watches := []watch.Watch{{ID: "w1", RoomID: "C1", Kind: watch.KindPattern, Pattern: "their", Note: "n", Once: &bTrue}}
	dec := wake.Evaluate(context.Background(), wake.In{Msg: ev2.Msg, Self: self, Peers: peers, Watches: watches})
	if !dec.Wake || dec.Kind != wake.KindWatch {
		t.Errorf("foreign bot with matching watch = %+v; want a watch wake", dec)
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

// TestSlackMessageMentionParsed: a plain message carrying a <@USERID>
// token has the mention parsed into Mentions (so the message event wins
// the dedupe race against its app_mention without losing the mention),
// and the parsed mention wakes.
func TestSlackMessageMentionParsed(t *testing.T) {
	l := &slackLink{selfUserID: "U9", botID: "B1"}
	ev, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(
		`{"type":"message","channel":"C1","user":"U1","text":"<@U9> look at this and <@U1> too","ts":"1700000000.000400"}`)})
	if !ok {
		t.Fatal("message event dropped")
	}
	if len(ev.Msg.Mentions) != 2 || ev.Msg.Mentions[0] != "U9" || ev.Msg.Mentions[1] != "U1" {
		t.Errorf("mentions = %v, want [U9 U1]", ev.Msg.Mentions)
	}
	// The parsed mention wakes: mentioned() matches the self user id.
	dec := wake.Evaluate(context.Background(), wake.In{
		Msg:  ev.Msg,
		Self: wake.Self{UserID: "U9"},
	})
	if !dec.Wake || dec.Kind != wake.KindMention {
		t.Errorf("decision = %+v; want a mention wake", dec)
	}
	// A message with no mention token has no Mentions.
	ev2, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(
		`{"type":"message","channel":"C1","user":"U1","text":"plain text","ts":"1700000000.000410"}`)})
	if !ok {
		t.Fatal("message event dropped")
	}
	if len(ev2.Msg.Mentions) != 0 {
		t.Errorf("mentions = %v, want none", ev2.Msg.Mentions)
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
	events, err := l.History(context.Background(), "C1", slack.TSToCursor("1700000000.000100"), 50)
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
		if got := slack.CursorToTS(slack.TSToCursor(ts)); got != padTS(ts) {
			t.Errorf("round trip %q = %q, want %q", ts, got, padTS(ts))
		}
	}
	if slack.TSToCursor("1700000000.1234567") != 1700000000_123456 {
		t.Error("fraction longer than 6 digits must truncate")
	}
	if slack.CursorToTS(0) != "" {
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

func TestSlackEventDropsRoomMetadataSubtypes(t *testing.T) {
	l := &slackLink{selfUserID: "USELF", botID: "B1"}
	for _, subtype := range []string{"channel_join", "channel_leave", "channel_topic", "group_join"} {
		payload := fmt.Sprintf(`{"type":"message","channel":"C1","user":"U2","text":"<@USELF> has joined","ts":"1700000000.000100","subtype":%q}`, subtype)
		if _, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(payload)}); ok {
			t.Fatalf("subtype %q delivered as a chat event; want dropped", subtype)
		}
	}
	plain := `{"type":"message","channel":"C1","user":"U2","text":"hello","ts":"1700000000.000100"}`
	if ev, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(plain)}); !ok {
		t.Fatal("plain message dropped; want delivered")
	} else if ev.Msg.Text != "hello" {
		t.Fatalf("plain message text = %q", ev.Msg.Text)
	}
}

// TestSlackEventImageURL: a message with an image file carries the
// file's url_private in ImageURL; a message with an attachment
// image_url carries that; a message with neither (or only a
// non-image file) carries an empty ImageURL.
func TestSlackEventImageURL(t *testing.T) {
	l := &slackLink{selfUserID: "U9"}
	ev, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(
		`{"type":"message","channel":"C1","user":"U1","text":"look","ts":"1700000000.000500","files":[{"mimetype":"image/png","url_private":"https://files.slack.com/abc.png"}]}`)})
	if !ok {
		t.Fatal("message event dropped")
	}
	if ev.ImageURL != "https://files.slack.com/abc.png" {
		t.Errorf("image url = %q, want the file's url_private", ev.ImageURL)
	}
	ev2, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(
		`{"type":"message","channel":"C1","user":"U1","text":"look","ts":"1700000000.000510","attachments":[{"image_url":"https://files.slack.com/thumb.png"}]}`)})
	if !ok {
		t.Fatal("message event dropped")
	}
	if ev2.ImageURL != "https://files.slack.com/thumb.png" {
		t.Errorf("image url = %q, want the attachment's image_url", ev2.ImageURL)
	}
	ev3, ok := l.slackEvent(slackws.Event{Type: "message", Payload: json.RawMessage(
		`{"type":"message","channel":"C1","user":"U1","text":"plain","ts":"1700000000.000520","files":[{"mimetype":"application/pdf","url_private":"https://files.slack.com/x.pdf"}]}`)})
	if !ok {
		t.Fatal("message event dropped")
	}
	if ev3.ImageURL != "" {
		t.Errorf("image url = %q, want empty (no image file)", ev3.ImageURL)
	}
}

package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/store"
	"github.com/sdougbrown/whinny/internal/wake"
)

func TestApronEventMessageMapping(t *testing.T) {
	rec := apronws.Record{Kind: apronws.KindMessage, Message: &apronws.Message{
		MessageID: "m1",
		LogID:     990001,
		RoomID:    "r1",
		From:      apronws.From{UserID: "u1", Name: "doug"},
		Body: &apronws.Body{
			Text:     "squire check the deploy",
			Mentions: json.RawMessage(`["u9"]`),
		},
		ReplyTo: &apronws.ReplyTo{MessageID: "m0"},
	}}
	ev, ok := apronEvent(rec)
	if !ok {
		t.Fatal("apronEvent dropped a message record")
	}
	if ev.Kind != KindMessage || ev.RoomID != "r1" || ev.Cursor != 990001 {
		t.Errorf("event = %+v, want kind message room r1 cursor 990001", ev)
	}
	if ev.Msg.MessageID != "m1" || ev.Msg.ReplyTo != "m0" || ev.Msg.From.UserID != "u1" {
		t.Errorf("msg = %+v", ev.Msg)
	}
	if len(ev.Msg.Mentions) != 1 || ev.Msg.Mentions[0] != "u9" {
		t.Errorf("mentions = %v, want [u9]", ev.Msg.Mentions)
	}
}

func TestApronEventImageURL(t *testing.T) {
	rec := apronws.Record{Kind: apronws.KindMessage, Message: &apronws.Message{
		MessageID: "m1",
		LogID:     990001,
		RoomID:    "r1",
		From:      apronws.From{UserID: "u1"},
		Body: &apronws.Body{
			Embeds: json.RawMessage(`[{"kind":"file","title":"notes"},{"kind":"image","url":"https://x/i.png"}]`),
		},
	}}
	ev, ok := apronEvent(rec)
	if !ok {
		t.Fatal("apronEvent dropped a message record")
	}
	if ev.ImageURL != "https://x/i.png" {
		t.Errorf("image URL = %q, want https://x/i.png", ev.ImageURL)
	}
	if len(ev.Msg.Embeds) != 2 || ev.Msg.Embeds[0].Title != "notes" {
		t.Errorf("embeds = %+v", ev.Msg.Embeds)
	}
}

func TestApronEventSkipsNonMessages(t *testing.T) {
	if _, ok := apronEvent(apronws.Record{Kind: apronws.KindRoomUpdate}); ok {
		t.Error("room update kept")
	}
	if _, ok := apronEvent(apronws.Record{Kind: apronws.KindActivity}); ok {
		t.Error("activity kept")
	}
	tomb := apronws.Record{Kind: apronws.KindMessage, Message: &apronws.Message{
		MessageID: "m1", LogID: 990001, RoomID: "r1", Deleted: true,
	}}
	if _, ok := apronEvent(tomb); ok {
		t.Error("delete tombstone kept")
	}
}

// fakeLink is an in-memory Link for daemon tests: events are pushed
// into a channel, history is a canned per-room list, and posts are
// recorded.
type fakeLink struct {
	events  chan LinkEvent
	mu      sync.Mutex
	history []LinkEvent
	rooms   []RoomRef
	posted  []postCall
}

type postCall struct {
	roomID, text, replyTo string
}

func newFakeLink() *fakeLink {
	return &fakeLink{events: make(chan LinkEvent, 8), rooms: []RoomRef{{ID: "r1", Title: "general"}}}
}

func (f *fakeLink) Events(_ context.Context) (<-chan LinkEvent, error) { return f.events, nil }

func (f *fakeLink) Post(_ context.Context, roomID, text, replyTo string) (string, error) {
	f.posted = append(f.posted, postCall{roomID: roomID, text: text, replyTo: replyTo})
	return "posted-1", nil
}

func (f *fakeLink) History(_ context.Context, roomID string, after int64, _ int) ([]LinkEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []LinkEvent
	for _, ev := range f.history {
		if ev.RoomID == roomID && ev.Cursor > after {
			out = append(out, ev)
		}
	}
	return out, nil
}

func (f *fakeLink) Rooms(_ context.Context) ([]RoomRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rooms, nil
}

// setHistory seeds the canned history (safe after Run started).
func (f *fakeLink) setHistory(evs []LinkEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.history = evs
}

func (f *fakeLink) push(ev LinkEvent) { f.events <- ev }

// startLinkHarness runs the daemon over a fakeLink.
func startLinkHarness(t *testing.T, cfg Config) (*fakeLink, *harness) {
	t.Helper()
	h := &harness{
		dlv:    newFakeDeliverer(),
		clock:  newFakeClock(),
		ticker: newTestTicker(),
		done:   make(chan error, 1),
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	h.st = st
	t.Cleanup(func() { h.st.Close() })
	link := newFakeLink()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	t.Cleanup(h.cancel)
	go func() {
		h.done <- Run(ctx, cfg, Deps{
			Link:    link,
			Store:   h.st,
			Deliver: h.dlv,
			Clock:   h.clock,
			Now:     h.clock.Now,
			Ticker:  func(time.Duration) Ticker { return h.ticker },
			Wait:    func(time.Duration) {},
		})
	}()
	return link, h
}

// msgEvent builds a message LinkEvent.
func msgEvent(room, from, text string, cursor int64, mentions ...string) LinkEvent {
	return LinkEvent{
		Kind: KindMessage, RoomID: room, Cursor: cursor,
		Msg: wake.Msg{
			MessageID: strconv.FormatInt(cursor, 10),
			RoomID:    room,
			From:      wake.From{UserID: from, Name: from},
			Text:      text,
			Mentions:  mentions,
		},
	}
}

func TestLinkMentionWake(t *testing.T) {
	cfg := baseCfg()
	link, h := startLinkHarness(t, cfg)

	link.push(msgEvent("r1", "doug", "squire can you look at the roster", 100, "u9"))
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=mention") || !strings.Contains(d.env.Text, "squire can you look") {
		t.Errorf("envelope = %q, want a mention wake", d.env.Text)
	}
	cursor, err := h.st.GetCursor(context.Background(), "r1")
	if err != nil || cursor != 100 {
		t.Errorf("cursor = %d, err %v; want 100", cursor, err)
	}
	h.stop(t)
}

func TestLinkCursorDedupe(t *testing.T) {
	cfg := baseCfg()
	link, h := startLinkHarness(t, cfg)

	link.push(msgEvent("r1", "doug", "squire hello", 100, "u9"))
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=mention") {
		t.Fatalf("envelope = %q, want a mention wake", d.env.Text)
	}
	// A replayed event at the same cursor is a duplicate.
	link.push(msgEvent("r1", "doug", "squire hello", 100, "u9"))
	link.push(msgEvent("r1", "doug", "room noise", 101))
	waitCursorAt(t, h.st, "r1", 101)
	if got := h.dlv.count(); got != 1 {
		t.Errorf("deliveries = %d, want 1 (duplicate at cursor must not re-wake)", got)
	}
	h.stop(t)
}

// waitCursorAt polls until the room cursor reaches want.
func waitCursorAt(t *testing.T, st *store.Store, room string, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		cursor, err := st.GetCursor(context.Background(), room)
		if err == nil && cursor >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cursor never reached %d (at %d)", want, cursor)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestLinkCatchUpOverHistory(t *testing.T) {
	cfg := baseCfg()
	link, h := startLinkHarness(t, cfg)
	// The cursor sits at 40: catch-up must replay only newer events.
	if err := h.st.SetCursor(context.Background(), "r1", 40); err != nil {
		t.Fatalf("set cursor: %v", err)
	}
	link.setHistory([]LinkEvent{
		msgEvent("r1", "doug", "too old", 40, "u9"),
		msgEvent("r1", "doug", "squire missed this one", 50, "u9"),
		msgEvent("r1", "doug", "and this", 60, "u9"),
	})
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "missed this one") {
		t.Errorf("first catch-up wake = %q", d.env.Text)
	}
	<-h.dlv.nowc
	if got := h.dlv.count(); got != 2 {
		t.Errorf("deliveries = %d, want 2 (events at or below the cursor excluded)", got)
	}
	cursor, err := h.st.GetCursor(context.Background(), "r1")
	if err != nil || cursor != 60 {
		t.Errorf("cursor = %d, err %v; want 60", cursor, err)
	}
	h.stop(t)
}

func TestSlackTransportForcesAmbientOff(t *testing.T) {
	hits := 0
	classifier := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Write([]byte(`{"directed":true}`))
	}))
	defer classifier.Close()

	cfg := baseCfg()
	cfg.Transport = TransportSlack
	// The injected link bypasses the Slack connection, but the config
	// must still pass backend validation.
	cfg.BotToken, cfg.AppToken = "xoxb-test", "xapp-test"
	cfg.Ambient = AmbientConfig{Endpoint: classifier.URL, MinSpacing: time.Minute, MaxPerHour: 2}
	link, h := startLinkHarness(t, cfg)
	link.rooms = nil // no catch-up noise

	link.push(msgEvent("r1", "doug", "anyone around?", 100))
	link.push(msgEvent("r1", "doug", "still there?", 101))
	waitCursorAt(t, h.st, "r1", 101)
	if got := h.dlv.count(); got != 0 {
		t.Errorf("deliveries = %d, want 0 (ambient gate forced off)", got)
	}
	if hits != 0 {
		t.Errorf("classifier hits = %d, want 0 (gate must not be consulted)", hits)
	}
	// Mentions still wake: the gate is off, not the policy.
	link.push(msgEvent("r1", "doug", "squire ping", 102, "u9"))
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=mention") {
		t.Errorf("envelope = %q, want a mention wake", d.env.Text)
	}
	h.stop(t)
}

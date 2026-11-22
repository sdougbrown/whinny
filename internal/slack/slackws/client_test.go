package slackws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/sdougbrown/whinny/internal/slack"
)

// wsServer is a fake Socket Mode WebSocket endpoint: it accepts
// connections and hands them to the test.
type wsServer struct {
	url   string
	conns chan *websocket.Conn
}

func newWSServer(t *testing.T) *wsServer {
	s := &wsServer{conns: make(chan *websocket.Conn, 4)}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		s.conns <- conn
	}))
	t.Cleanup(ts.Close)
	s.url = "ws" + strings.TrimPrefix(ts.URL, "http")
	return s
}

// openFake is a fake apps.connections.open endpoint: it returns the
// configured URLs in order (the last repeats) and records the auth
// header of each call.
type openFake struct {
	ts   *httptest.Server
	urls []string

	mu    sync.Mutex
	calls []string
}

func newOpenFake(t *testing.T, urls ...string) *openFake {
	f := &openFake{urls: urls}
	f.ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.calls = append(f.calls, r.Header.Get("Authorization"))
		i := len(f.calls) - 1
		f.mu.Unlock()
		if i >= len(f.urls) {
			i = len(f.urls) - 1
		}
		writeJSON(w, map[string]any{"ok": true, "url": f.urls[i]})
	}))
	t.Cleanup(f.ts.Close)
	return f
}

func (f *openFake) callsMade() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(ctx context.Context, t *testing.T, conn *websocket.Conn, into any) {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	typ, data, err := conn.Read(rctx)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("read frame: binary message")
	}
	if err := json.Unmarshal(data, into); err != nil {
		t.Fatalf("decode frame %s: %v", data, err)
	}
}

func sendJSON(ctx context.Context, t *testing.T, conn *websocket.Conn, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func envelope(id string, payload any) map[string]any {
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return map[string]any{"type": "events_api", "envelope_id": id, "payload": json.RawMessage(raw)}
}

func readEvent(ctx context.Context, t *testing.T, records <-chan Event) Event {
	t.Helper()
	select {
	case ev, ok := <-records:
		if !ok {
			t.Fatal("records closed while waiting for an event")
		}
		return ev
	case <-ctx.Done():
		t.Fatal("timed out waiting for an event")
		return Event{}
	}
}

func TestHandshakeAndAck(t *testing.T) {
	ws := newWSServer(t)
	api := newOpenFake(t, ws.url)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c, err := Connect(ctx, Options{AppToken: "xapp-test", BaseURL: api.ts.URL})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	calls := api.callsMade()
	if len(calls) != 1 || calls[0] != "Bearer xapp-test" {
		t.Fatalf("apps.connections.open calls = %v, want one Bearer xapp-test", calls)
	}
	conn := <-ws.conns

	sendJSON(ctx, t, conn, envelope("e1", map[string]any{"type": "message", "channel": "C1", "text": "hi", "ts": "1.1"}))
	var ack struct {
		Type       string `json:"type"`
		EnvelopeID string `json:"envelope_id"`
	}
	readJSON(ctx, t, conn, &ack)
	if ack.Type != "ack" || ack.EnvelopeID != "e1" {
		t.Fatalf("ack = %+v, want ack e1", ack)
	}
	ev := readEvent(ctx, t, c.Records())
	if ev.Type != "message" || ev.EnvelopeID != "e1" {
		t.Fatalf("event = %+v", ev)
	}
	var payload map[string]any
	if err := json.Unmarshal(ev.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["channel"] != "C1" || payload["text"] != "hi" {
		t.Errorf("payload = %v", payload)
	}
}

func TestDuplicateEnvelopeDeliveredOnce(t *testing.T) {
	ws := newWSServer(t)
	api := newOpenFake(t, ws.url)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c, err := Connect(ctx, Options{AppToken: "xapp-test", BaseURL: api.ts.URL})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
	conn := <-ws.conns

	sendJSON(ctx, t, conn, envelope("e1", map[string]any{"type": "message", "ts": "1.1"}))
	readEvent(ctx, t, c.Records())
	var ack1 struct {
		EnvelopeID string `json:"envelope_id"`
	}
	readJSON(ctx, t, conn, &ack1)

	// Slack resends an un-acked payload; the resend is acked but the
	// event is not delivered again.
	sendJSON(ctx, t, conn, envelope("e1", map[string]any{"type": "message", "ts": "1.1"}))
	var ack2 struct {
		EnvelopeID string `json:"envelope_id"`
	}
	readJSON(ctx, t, conn, &ack2)
	sendJSON(ctx, t, conn, envelope("e2", map[string]any{"type": "message", "ts": "1.2"}))
	ev := readEvent(ctx, t, c.Records())
	if ev.EnvelopeID != "e2" {
		t.Fatalf("event after resend = %+v, want e2 only", ev)
	}
}

func TestDisconnectRefreshReconnects(t *testing.T) {
	ws1 := newWSServer(t)
	ws2 := newWSServer(t)
	api := newOpenFake(t, ws1.url, ws2.url)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	reconnected := make(chan struct{})
	c, err := Connect(ctx, Options{AppToken: "xapp-test", BaseURL: api.ts.URL, OnReconnect: func() { close(reconnected) }})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
	conn1 := <-ws1.conns

	sendJSON(ctx, t, conn1, map[string]any{"type": "disconnect", "reason": "refresh_requested"})
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("timed out waiting for reconnect")
	}

	// The second apps.connections.open returns the fresh URL, and the
	// replacement connection delivers events.
	calls := api.callsMade()
	if len(calls) != 2 {
		t.Fatalf("apps.connections.open calls = %d, want 2", len(calls))
	}
	conn2 := <-ws2.conns
	sendJSON(ctx, t, conn2, envelope("e2", map[string]any{"type": "message", "ts": "1.2"}))
	ev := readEvent(ctx, t, c.Records())
	if ev.EnvelopeID != "e2" {
		t.Fatalf("event after reconnect = %+v", ev)
	}
}

func TestReadErrorReconnects(t *testing.T) {
	ws1 := newWSServer(t)
	ws2 := newWSServer(t)
	api := newOpenFake(t, ws1.url, ws2.url)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	reconnected := make(chan struct{})
	c, err := Connect(ctx, Options{AppToken: "xapp-test", BaseURL: api.ts.URL, OnReconnect: func() { close(reconnected) }})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
	conn1 := <-ws1.conns

	// An abrupt server-side close feeds the reconnect loop.
	conn1.CloseNow()
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("timed out waiting for reconnect")
	}
	conn2 := <-ws2.conns
	sendJSON(ctx, t, conn2, envelope("e3", map[string]any{"type": "app_mention", "ts": "1.3"}))
	ev := readEvent(ctx, t, c.Records())
	if ev.Type != "app_mention" {
		t.Fatalf("event after reconnect = %+v", ev)
	}
}

func TestInteractiveAckedNotDelivered(t *testing.T) {
	ws := newWSServer(t)
	api := newOpenFake(t, ws.url)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c, err := Connect(ctx, Options{AppToken: "xapp-test", BaseURL: api.ts.URL})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
	conn := <-ws.conns

	sendJSON(ctx, t, conn, map[string]any{"type": "interactive", "envelope_id": "i1", "payload": map[string]any{}})
	var ack struct {
		Type       string `json:"type"`
		EnvelopeID string `json:"envelope_id"`
	}
	readJSON(ctx, t, conn, &ack)
	if ack.Type != "ack" || ack.EnvelopeID != "i1" {
		t.Fatalf("ack = %+v", ack)
	}
	// A later event proves the interactive payload was not delivered.
	sendJSON(ctx, t, conn, envelope("e4", map[string]any{"type": "message", "ts": "1.4"}))
	ev := readEvent(ctx, t, c.Records())
	if ev.EnvelopeID != "e4" {
		t.Fatalf("event = %+v, want e4 (interactive must not be delivered)", ev)
	}
}

func TestCloseAndContextCancel(t *testing.T) {
	ws := newWSServer(t)
	api := newOpenFake(t, ws.url)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c, err := Connect(ctx, Options{AppToken: "xapp-test", BaseURL: api.ts.URL})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	conn := <-ws.conns

	// Close is idempotent and drains Records.
	c.Close()
	c.Close()
	select {
	case _, ok := <-c.Records():
		if ok {
			t.Fatal("records still open after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for records to close")
	}
	_ = conn

	// A canceled context stops the client the same way.
	ctx2, cancel2 := context.WithCancel(context.Background())
	c2, err := Connect(ctx2, Options{AppToken: "xapp-test", BaseURL: api.ts.URL})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c2.Close()
	<-ws.conns
	cancel2()
	select {
	case _, ok := <-c2.Records():
		if ok {
			t.Fatal("records still open after ctx cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for records to close")
	}
}

func TestOpenFailure(t *testing.T) {
	fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"ok": false, "error": "invalid_auth"})
	}))
	defer fail.Close()
	_, err := Connect(context.Background(), Options{AppToken: "xapp-bad", BaseURL: fail.URL})
	apiErr, ok := err.(*slack.APIError)
	if !ok {
		t.Fatalf("err = %T (%v), want *slack.APIError", err, err)
	}
	if apiErr.Method != "apps.connections.open" || apiErr.SlackError != "invalid_auth" {
		t.Fatalf("APIError = %+v", apiErr)
	}
}

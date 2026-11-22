package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/sdougbrown/whinny/internal/store"
	"github.com/sdougbrown/whinny/internal/wake"
)

// fakeSlackAPI plays the Web API endpoints the Slack link calls.
func newFakeSlackAPI(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/auth.test":
			w.Write([]byte(`{"ok":true,"user_id":"U9","bot_id":"B1"}`))
		case "/conversations.list":
			w.Write([]byte(`{"ok":true,"channels":[{"id":"C1","name":"general"}]}`))
		case "/conversations.history":
			w.Write([]byte(`{"ok":true,"messages":[]}`))
		default:
			w.Write([]byte(`{"ok":false,"error":"not_mocked"}`))
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

// fakeSlackWS plays the Socket Mode open endpoint plus the WebSocket
// it hands back; the accepted connection is delivered to the test.
type fakeSlackWS struct {
	open *httptest.Server
	conn chan *websocket.Conn
}

func newFakeSlackWS(t *testing.T) *fakeSlackWS {
	t.Helper()
	f := &fakeSlackWS{conn: make(chan *websocket.Conn, 4)}
	wsTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		f.conn <- c
	}))
	t.Cleanup(wsTS.Close)
	wsURL := "ws" + strings.TrimPrefix(wsTS.URL, "http")
	f.open = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true,"url":"` + wsURL + `"}`))
	}))
	t.Cleanup(f.open.Close)
	return f
}

// push sends one events_api envelope over the connection.
func (f *fakeSlackWS) push(t *testing.T, c *websocket.Conn, id string, payload map[string]any) {
	t.Helper()
	raw, _ := json.Marshal(payload)
	env := map[string]any{"type": "events_api", "envelope_id": id, "payload": json.RawMessage(raw)}
	data, _ := json.Marshal(env)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// waitWatchNote polls until a watch with the given note exists.
func waitWatchNote(t *testing.T, st *store.Store, note string) store.Watch {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ws, err := st.ListWatches(context.Background())
		if err == nil {
			for _, w := range ws {
				if w.Note == note {
					return w
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("watch %q never appeared (watches %+v, err %v)", note, ws, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSlackSelfIDReachesDaemon runs the daemon over the Slack path with
// an empty config self: the resolved id (from auth.test) must reach the
// daemon's copy so the firebreak and the evaluation both see it.
func TestSlackSelfIDReachesDaemon(t *testing.T) {
	api := newFakeSlackAPI(t)
	ws := newFakeSlackWS(t)

	cfg := Config{
		Agent:     "squire",
		Transport: TransportSlack,
		BotToken:  "xoxb-test",
		AppToken:  "xapp-test",
		Self:      wake.Self{}, // empty: the daemon must resolve it
		ExecCmd:   []string{"true"},
	}
	dlv := newFakeDeliverer()
	clock := newFakeClock()
	ticker := newTestTicker()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, cfg, Deps{
			Store:          st,
			Deliver:        dlv,
			Clock:          clock,
			Now:            clock.Now,
			Ticker:         func(time.Duration) Ticker { return ticker },
			Wait:           func(time.Duration) {},
			SlackBaseURL:   api.URL,
			SlackWSBaseURL: ws.open.URL,
		})
	}()

	// Wait for the socket to connect (the daemon is live).
	var conn *websocket.Conn
	select {
	case conn = <-ws.conn:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the socket to connect")
	}

	// Firebreak: a self-post (the bot's own bot_message, with a
	// thread_ts) arms a thread subscription only when the daemon's
	// self id matches the resolved id.
	ws.push(t, conn, "e1", map[string]any{
		"type": "message", "channel": "C1", "bot_id": "B1",
		"subtype": "bot_message", "text": "my post",
		"ts": "1700000000.000100", "thread_ts": "1700000000.000050",
	})
	w := waitWatchNote(t, st, "my thread post")
	if w.Kind != "subscription" {
		t.Errorf("watch kind = %q, want subscription", w.Kind)
	}

	// Evaluation: an app_mention is attributed to the bot's user id;
	// it wakes only when the daemon's self id is the resolved id.
	ws.push(t, conn, "e2", map[string]any{
		"type": "app_mention", "channel": "C1", "user": "U1",
		"text": "<@U9> do the thing", "ts": "1700000000.000200",
	})
	d := dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=mention") {
		t.Errorf("envelope = %q, want a mention wake", d.env.Text)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not shut down")
	}
}

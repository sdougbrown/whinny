// Package apronfake is a shared test-support fake for the apron
// WebSocket protocol: a fake server, its accepted connections, and raw
// I-JSON frames. It is a normal package (not _test) so tests in
// several packages can import it.
package apronfake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Frame is a raw I-JSON frame as observed on the wire.
type Frame struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
	Data   json.RawMessage `json:"data"`
}

// Conn is one accepted WebSocket connection on a Server.
type Conn struct {
	conn   *websocket.Conn
	recv   chan []byte
	closed chan struct{}

	// AutoPong, when set, replies to ping frames with a pong.
	AutoPong atomic.Bool
	// WantToken is the token the handshake asserts the client sends;
	// RotToken is the rotated token the auth reply returns.
	WantToken string
	RotToken  string
	// YouUserID and YouName are the auth reply's "you" user.
	YouUserID string
	YouName   string
}

// Server is a WebSocket server for tests. It accepts connections and
// forwards raw frames to the test, which drives the protocol.
//
// The room_list and history responses are configured per server:
//
//   - JoinedRooms is the room_list response's joined list; nil means
//     none (the catch-up finds no rooms to replay).
//   - HistoryByRoom is the history response's messages, keyed by room
//     id; a missing room yields an empty list.
//   - HistoryGate, when non-nil, blocks the history response until it
//     is closed: a test holds the replay to simulate a live push
//     landing between the reconnect and the history response.
//   - HistoryGateHit, when non-nil, is signaled (non-blocking) when a
//     history request reaches the gate.
//   - HistoryFailures, when > 0, makes the next N history requests
//     fail with an error frame (a flaky-transport simulation).
type Server struct {
	ts      *httptest.Server
	mu      sync.Mutex
	conns   []*Conn
	newConn chan struct{}

	// errMu guards errs: errors recorded by server-side goroutines,
	// drained at test teardown by CheckErrors (registered by New), so a
	// late failure cannot log after the test has completed.
	errMu sync.Mutex
	errs  []error

	// historyCalls counts every history request the server sees.
	historyCalls atomic.Int64

	// AutoRespond, when set, makes the server answer room_list and
	// history requests itself (from JoinedRooms/HistoryByRoom); when
	// unset, those frames are forwarded to the test, which drives the
	// replies manually.
	AutoRespond bool

	JoinedRooms     []map[string]any
	HistoryByRoom   map[string][]map[string]any
	HistoryGate     chan struct{}
	HistoryGateHit  chan struct{}
	HistoryFailures int
}

// New starts a fake server on a mux serving /ws (the client appends
// the path itself). Per-connection defaults: AutoPong on, WantToken
// "test-token", RotToken "tok-2", you u1/Test.
func New(t *testing.T) *Server {
	t.Helper()
	f := &Server{newConn: make(chan struct{}, 16)}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fc := &Conn{
			conn:      conn,
			recv:      make(chan []byte, 256),
			closed:    make(chan struct{}),
			WantToken: "test-token",
			RotToken:  "tok-2",
			YouUserID: "u1",
			YouName:   "Test",
		}
		fc.AutoPong.Store(true)
		f.mu.Lock()
		f.conns = append(f.conns, fc)
		f.mu.Unlock()
		f.newConn <- struct{}{}
		f.runConn(fc)
	})
	f.ts = httptest.NewServer(mux)
	t.Cleanup(f.ts.Close)
	t.Cleanup(func() { f.CheckErrors(t) })
	return f
}

// ReportError records an error from a server-side goroutine. It is
// drained at test teardown (CheckErrors), so a late failure cannot log
// after the test has completed (a t.Error from a goroutine after the
// test body exits panics and obscures the real failure).
func (f *Server) ReportError(err error) {
	f.errMu.Lock()
	f.errs = append(f.errs, err)
	f.errMu.Unlock()
}

// CheckErrors reports the errors recorded by server-side goroutines.
func (f *Server) CheckErrors(t *testing.T) {
	t.Helper()
	f.errMu.Lock()
	errs := f.errs
	f.errs = nil
	f.errMu.Unlock()
	for _, err := range errs {
		t.Error(err)
	}
}

// HistoryCallCount reports how many history requests the server has
// seen: the tests use it to prove a lookup did (or did not) happen.
func (f *Server) HistoryCallCount() int64 {
	return f.historyCalls.Load()
}

// WSURL is the client-facing base URL (no path: the client appends /ws).
func (f *Server) WSURL() string {
	return f.ts.URL
}

// WaitConn waits for the n-th accepted connection (1-indexed).
func (f *Server) WaitConn(n int) (*Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		f.mu.Lock()
		if len(f.conns) >= n {
			fc := f.conns[n-1]
			f.mu.Unlock()
			return fc, nil
		}
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for connection %d", n)
		case <-f.newConn:
		}
	}
}

// ConnAt waits for the n-th accepted connection (1-indexed) and
// returns it, failing the test on timeout.
func (f *Server) ConnAt(t *testing.T, n int) *Conn {
	t.Helper()
	fc, err := f.WaitConn(n)
	if err != nil {
		t.Fatalf("connection %d: %v", n, err)
	}
	return fc
}

// SetCatchUp configures the room_list and history responses for the
// catch-up.
func (f *Server) SetCatchUp(rooms []map[string]any, historyByRoom map[string][]map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.JoinedRooms = rooms
	f.HistoryByRoom = historyByRoom
}

// SetHistoryFailures makes the next n history requests fail with an
// error frame.
func (f *Server) SetHistoryFailures(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.HistoryFailures = n
}

func (f *Server) runConn(fc *Conn) {
	defer close(fc.closed)
	for {
		typ, data, err := fc.conn.Read(context.Background())
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var fr Frame
		if json.Unmarshal(data, &fr) != nil {
			continue
		}
		if fr.Method == "ping" && fc.AutoPong.Load() {
			pong, _ := json.Marshal(map[string]any{"method": "pong"})
			fc.write(pong)
		}
		if fr.Method == "history" {
			f.historyCalls.Add(1)
		}
		// skip suppresses the recv push for a failed history auto-reply.
		skip := false
		if f.autoRespond() {
			switch fr.Method {
			case "room_list":
				f.mu.Lock()
				joined := f.JoinedRooms
				f.mu.Unlock()
				if joined == nil {
					joined = []map[string]any{}
				}
				reply, _ := json.Marshal(map[string]any{"id": fr.ID, "result": map[string]any{"joined": joined}})
				fc.write(reply)
			case "history":
				var p struct {
					RoomID string `json:"room_id"`
				}
				_ = json.Unmarshal(fr.Params, &p)
				f.mu.Lock()
				msgs := f.HistoryByRoom[p.RoomID]
				gate := f.HistoryGate
				fail := f.HistoryFailures > 0
				if fail {
					f.HistoryFailures--
				}
				f.mu.Unlock()
				if fail {
					errReply, _ := json.Marshal(map[string]any{"id": fr.ID, "error": map[string]any{"code": -32000, "message": "history flake"}})
					fc.write(errReply)
					skip = true
				} else {
					if gate != nil {
						select {
						case f.HistoryGateHit <- struct{}{}:
						default:
						}
						<-gate
					}
					if msgs == nil {
						msgs = []map[string]any{}
					}
					reply, _ := json.Marshal(map[string]any{"id": fr.ID, "result": map[string]any{"messages": msgs, "latest_log_id": "0"}})
					fc.write(reply)
				}
			}
		}
		if !skip {
			select {
			case fc.recv <- data:
			default:
			}
		}
	}
}

func (f *Server) autoRespond() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.AutoRespond
}

func (fc *Conn) write(b []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = fc.conn.Write(ctx, websocket.MessageText, b)
}

// SendFrame sends a raw JSON frame to the client.
func (fc *Conn) SendFrame(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return fc.conn.Write(ctx, websocket.MessageText, b)
}

// RecvFrame waits for the next frame from the client.
func (fc *Conn) RecvFrame(ctx context.Context) (Frame, error) {
	select {
	case b := <-fc.recv:
		var fr Frame
		if err := json.Unmarshal(b, &fr); err != nil {
			return Frame{}, err
		}
		return fr, nil
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	}
}

// Close closes the connection from the server side.
func (fc *Conn) Close() {
	fc.conn.CloseNow()
}

// WaitForClose waits until the server observes the connection die.
func (fc *Conn) WaitForClose() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-fc.closed:
		return nil
	case <-ctx.Done():
		return errors.New("timed out waiting for connection close")
	}
}

// Handshake performs the server side of the apron handshake: it sends
// the server push, waits for the auth request (asserting its shape),
// and sends the auth reply.
func (fc *Conn) Handshake(ctx context.Context, ping int) error {
	if err := fc.SendFrame(map[string]any{
		"method": "server",
		"params": map[string]any{
			"apron":        9,
			"agent":        "fake",
			"capabilities": []string{"history"},
			"ping":         ping,
		},
	}); err != nil {
		return err
	}
	for {
		f, err := fc.RecvFrame(ctx)
		if err != nil {
			return err
		}
		if f.Method != "auth" {
			continue
		}
		var ap struct {
			Scheme string `json:"scheme"`
			Token  string `json:"token"`
		}
		if err := json.Unmarshal(f.Params, &ap); err != nil {
			return err
		}
		if ap.Scheme != "token" || ap.Token != fc.WantToken {
			return fmt.Errorf("auth params = %+v, want scheme=token token=%s", ap, fc.WantToken)
		}
		return fc.SendFrame(map[string]any{
			"id": f.ID,
			"result": map[string]any{
				"you":   map[string]any{"user_id": fc.YouUserID, "name": fc.YouName},
				"token": fc.RotToken,
			},
		})
	}
}

// Push sends a push frame to the client.
func (fc *Conn) Push(method string, payload any) {
	b, _ := json.Marshal(map[string]any{"method": method, "params": payload})
	fc.write(b)
}

// PushMessage sends a message push.
func (fc *Conn) PushMessage(id, room, from, text string, mentions []string) {
	body := map[string]any{"text": text}
	if mentions != nil {
		body["mentions"] = mentions
	}
	fc.Push("message", map[string]any{
		"message_id": id,
		"log_id":     time.Now().UnixNano(),
		"room_id":    room,
		"from":       map[string]any{"user_id": from, "name": from},
		"body":       body,
	})
}

// PushMessageAt sends a message push with an explicit log id.
func (fc *Conn) PushMessageAt(id, room, from, text string, mentions []string, logID int64) {
	body := map[string]any{"text": text}
	if mentions != nil {
		body["mentions"] = mentions
	}
	fc.Push("message", map[string]any{
		"message_id": id,
		"log_id":     logID,
		"room_id":    room,
		"from":       map[string]any{"user_id": from, "name": from},
		"body":       body,
	})
}

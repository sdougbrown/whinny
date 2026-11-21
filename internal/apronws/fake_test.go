package apronws

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

// testFrame is a raw I-JSON frame as observed on the wire.
type testFrame struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
	Data   json.RawMessage `json:"data"`
}

// fakeConn is one accepted WebSocket connection on a fakeServer.
type fakeConn struct {
	conn     *websocket.Conn
	send     chan []byte
	recv     chan []byte
	closeCh  chan struct{}
	closed   chan struct{}
	autoPong atomic.Bool
	// wantToken is the token the handshake asserts the client sends; rotToken
	// is the rotated token the auth reply returns.
	wantToken string
	rotToken  string
}

// fakeServer is a WebSocket server for tests. It accepts connections and
// forwards raw frames to the test, which drives the protocol.
type fakeServer struct {
	ts      *httptest.Server
	mu      sync.Mutex
	conns   []*fakeConn
	newConn chan struct{}
}

func newFakeServer(t *testing.T) *fakeServer {
	f := &fakeServer{
		newConn: make(chan struct{}, 16),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fc := &fakeConn{
			conn:    conn,
			send:    make(chan []byte),
			recv:    make(chan []byte),
			closeCh: make(chan struct{}, 1),
			closed:  make(chan struct{}),
		}
		fc.autoPong.Store(true)
		fc.wantToken = "test-token"
		fc.rotToken = "tok-2"
		f.mu.Lock()
		f.conns = append(f.conns, fc)
		f.mu.Unlock()
		f.newConn <- struct{}{}
		f.runConn(fc)
	})
	f.ts = httptest.NewServer(mux)
	t.Cleanup(f.ts.Close)
	return f
}

// wsURL is the client-facing base URL (no path: the client appends /ws).
func (f *fakeServer) wsURL() string {
	return f.ts.URL
}

// waitConn waits for the n-th accepted connection (1-indexed).
func (f *fakeServer) waitConn(n int) (*fakeConn, error) {
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

func (f *fakeServer) runConn(fc *fakeConn) {
	defer close(fc.closed)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			typ, data, err := fc.conn.Read(context.Background())
			if err != nil {
				return
			}
			if typ != websocket.MessageText {
				continue
			}
			fc.recv <- data
			var fr testFrame
			if json.Unmarshal(data, &fr) == nil && fr.Method == "ping" && fc.autoPong.Load() {
				pong, _ := json.Marshal(map[string]any{"method": "pong"})
				_ = fc.conn.Write(context.Background(), websocket.MessageText, pong)
			}
		}
	}()
	for {
		select {
		case b := <-fc.send:
			if err := fc.conn.Write(context.Background(), websocket.MessageText, b); err != nil {
				return
			}
		case <-fc.closeCh:
			fc.conn.CloseNow()
			return
		case <-readDone:
			return
		}
	}
}

// sendFrame sends a raw JSON frame to the client.
func (fc *fakeConn) sendFrame(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case fc.send <- b:
		return nil
	case <-ctx.Done():
		return errors.New("timed out sending frame")
	}
}

// recvFrame waits for the next frame from the client.
func (fc *fakeConn) recvFrame(ctx context.Context) (testFrame, error) {
	select {
	case b := <-fc.recv:
		var fr testFrame
		if err := json.Unmarshal(b, &fr); err != nil {
			return testFrame{}, err
		}
		return fr, nil
	case <-ctx.Done():
		return testFrame{}, ctx.Err()
	}
}

// close closes the connection from the server side.
func (fc *fakeConn) close() {
	fc.closeCh <- struct{}{}
}

// waitForClose waits until the server observes the connection die.
func (fc *fakeConn) waitForClose() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-fc.closed:
		return nil
	case <-ctx.Done():
		return errors.New("timed out waiting for connection close")
	}
}

// handshake performs the server side of the apron handshake: it sends the
// server push, waits for the auth request (asserting its shape), and sends
// the auth reply.
func (fc *fakeConn) handshake(ctx context.Context, ping int) error {
	if err := fc.sendFrame(map[string]any{
		"method": "server",
		"data": map[string]any{
			"apron":        9,
			"agent":        "fake",
			"capabilities": []string{"history"},
			"ping":         ping,
		},
	}); err != nil {
		return err
	}
	f, err := fc.recvFrame(ctx)
	if err != nil {
		return err
	}
	if f.Method != "auth" {
		return fmt.Errorf("first client frame = %q, want auth", f.Method)
	}
	var ap struct {
		Scheme string `json:"scheme"`
		Token  string `json:"token"`
	}
	if err := json.Unmarshal(f.Params, &ap); err != nil {
		return err
	}
	if ap.Scheme != "token" || ap.Token != fc.wantToken {
		return fmt.Errorf("auth params = %+v, want scheme=token token=%s", ap, fc.wantToken)
	}
	return fc.sendFrame(map[string]any{
		"id": f.ID,
		"result": map[string]any{
			"you":   map[string]any{"user_id": "u1", "name": "Test"},
			"token": fc.rotToken,
		},
	})
}

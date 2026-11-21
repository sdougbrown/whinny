package apronws

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/sdougbrown/whinny/internal/apronws/apronfake"
	"testing"
	"time"
)

func TestConnectHandshake(t *testing.T) {
	f := apronfake.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		fc, err := f.WaitConn(1)
		if err != nil {
			t.Error(err)
			return
		}
		if err := fc.Handshake(ctx, 30); err != nil {
			t.Error(err)
		}
	}()

	c, err := Connect(ctx, Options{URL: f.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	s := c.Server()
	if s.Apron != 9 {
		t.Errorf("Apron = %d, want 9", s.Apron)
	}
	if s.Agent != "fake" {
		t.Errorf("Agent = %q, want fake", s.Agent)
	}
	if len(s.Capabilities) != 1 || s.Capabilities[0] != "history" {
		t.Errorf("Capabilities = %v, want [history]", s.Capabilities)
	}
	if s.Ping != 30*time.Second {
		t.Errorf("Ping = %v, want 30s", s.Ping)
	}
}

// TestSendFailsBeforeAuth exercises the auth serialization across a
// reconnect: a request issued while the replacement connection is
// unauthenticated fails client-side and is never sent to the server.
func TestSendFailsBeforeAuth(t *testing.T) {
	f := apronfake.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go func() {
		fc, err := f.WaitConn(1)
		if err != nil {
			t.Error(err)
			return
		}
		if err := fc.Handshake(ctx, 30); err != nil {
			t.Error(err)
		}
	}()
	c, err := Connect(ctx, Options{URL: f.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	// Force a close: the client enters its reconnect window, unauthenticated.
	fc1, err := f.WaitConn(1)
	if err != nil {
		t.Fatal(err)
	}
	fc1.Close()
	if err := fc1.WaitForClose(); err != nil {
		t.Fatal(err)
	}

	// Once the client has dialed the replacement, the old connection is
	// torn down and auth is incomplete.
	fc2, err := f.WaitConn(2)
	if err != nil {
		t.Fatal(err)
	}
	fc2.WantToken = "tok-2" // the client re-auths with the rotated token

	// A request before re-auth fails client-side and is never sent.
	if _, err := c.Send(ctx, "history", nil); !errors.Is(err, ErrNotAuthenticated) {
		t.Fatalf("Send before auth = %v, want ErrNotAuthenticated", err)
	}

	reconnected := make(chan struct{})
	c.SetOnReconnect(func() { close(reconnected) })
	go func() {
		if err := fc2.Handshake(ctx, 30); err != nil {
			t.Error(err)
			return
		}
		f, err := fc2.RecvFrame(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		if f.Method != "history" {
			t.Errorf("first post-auth frame = %q, want history", f.Method)
			return
		}
		if err := fc2.SendFrame(map[string]any{"id": f.ID, "result": map[string]any{"ok": true}}); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("timed out waiting for reconnect")
	}

	raw, err := c.Send(ctx, "history", nil)
	if err != nil {
		t.Fatalf("Send after re-auth: %v", err)
	}
	var r struct{ OK bool }
	if err := json.Unmarshal(raw, &r); err != nil || !r.OK {
		t.Fatalf("result = %s, want ok=true", raw)
	}
}

func TestSendReplyAndError(t *testing.T) {
	f := apronfake.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		fc, err := f.WaitConn(1)
		if err != nil {
			t.Error(err)
			return
		}
		if err := fc.Handshake(ctx, 30); err != nil {
			t.Error(err)
			return
		}

		f, err := fc.RecvFrame(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		if f.Method != "message" {
			t.Errorf("first request = %q, want message", f.Method)
			return
		}
		var p struct {
			RoomID string `json:"room_id"`
			Body   struct {
				Text string `json:"text"`
			} `json:"body"`
		}
		if err := json.Unmarshal(f.Params, &p); err != nil {
			t.Error(err)
			return
		}
		if p.RoomID != "r1" || p.Body.Text != "hi" {
			t.Errorf("params = %+v, want room r1 body hi", p)
			return
		}
		if err := fc.SendFrame(map[string]any{"id": f.ID, "result": map[string]any{"message_id": "m1"}}); err != nil {
			t.Error(err)
			return
		}

		f, err = fc.RecvFrame(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		if f.Method != "history" {
			t.Errorf("second request = %q, want history", f.Method)
			return
		}
		err = fc.SendFrame(map[string]any{
			"id": f.ID,
			"error": map[string]any{
				"code":    429,
				"message": "slow down",
				"data":    map[string]any{"retry_after": 1.5},
			},
		})
		if err != nil {
			t.Error(err)
		}
	}()

	c, err := Connect(ctx, Options{URL: f.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	res, err := c.PostMessage(ctx, PostParams{RoomID: "r1", Body: Body{Text: "hi"}})
	if err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	if res.MessageID != "m1" {
		t.Fatalf("MessageID = %q, want m1", res.MessageID)
	}
	if res.Raw == nil {
		t.Error("Raw = nil, want the full result object")
	}

	_, err = c.History(ctx, HistoryParams{Limit: 10})
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("History error = %v, want *Error", err)
	}
	if apiErr.Code != 429 {
		t.Errorf("Code = %d, want 429", apiErr.Code)
	}
	if apiErr.Message != "slow down" {
		t.Errorf("Message = %q, want slow down", apiErr.Message)
	}
	if apiErr.RetryAfter != 1500*time.Millisecond {
		t.Errorf("RetryAfter = %v, want 1.5s", apiErr.RetryAfter)
	}
}

func TestPushDelivery(t *testing.T) {
	f := apronfake.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		fc, err := f.WaitConn(1)
		if err != nil {
			t.Error(err)
			return
		}
		if err := fc.Handshake(ctx, 30); err != nil {
			t.Error(err)
		}
	}()
	c, err := Connect(ctx, Options{URL: f.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	fc, err := f.WaitConn(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := fc.SendFrame(map[string]any{"method": "frobnicate", "params": map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	if err := fc.SendFrame(map[string]any{
		"method": "message",
		"params": map[string]any{
			"message_id": "m9",
			"log_id":     9,
			"room_id":    "r1",
			"from":       map[string]any{"user_id": "u1", "name": "Ann"},
			"body":       map[string]any{"text": "pushed"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := fc.SendFrame(map[string]any{"method": "room_update", "params": map[string]any{"kind": "joined", "room_id": "r2"}}); err != nil {
		t.Fatal(err)
	}

	rec := readRecord(t, c.Records(), ctx)
	if rec.Kind != KindMessage || rec.Message == nil || rec.Message.MessageID != "m9" {
		t.Fatalf("first record = %+v, want message m9", rec)
	}
	if rec.Message.Body == nil || rec.Message.Body.Text != "pushed" {
		t.Fatalf("Body = %+v, want text pushed", rec.Message.Body)
	}
	rec = readRecord(t, c.Records(), ctx)
	if rec.Kind != KindRoomUpdate || rec.RoomUpdate == nil || rec.RoomUpdate.Kind != "joined" {
		t.Fatalf("second record = %+v, want room_update joined", rec)
	}
}

func readRecord(t *testing.T, ch <-chan Record, ctx context.Context) Record {
	t.Helper()
	select {
	case rec, ok := <-ch:
		if !ok {
			t.Fatal("records channel closed")
		}
		return rec
	case <-ctx.Done():
		t.Fatal("timed out waiting for record")
		return Record{}
	}
}

package apronws

import (
	"context"
	"testing"
	"time"
)

func TestReconnect(t *testing.T) {
	f := newFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go func() {
		fc, err := f.waitConn(1)
		if err != nil {
			t.Error(err)
			return
		}
		if err := fc.handshake(ctx, 30); err != nil {
			t.Error(err)
		}
	}()
	c, err := Connect(ctx, Options{URL: f.wsURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	reconnected := make(chan struct{})
	c.mu.Lock()
	c.OnReconnect = func() { close(reconnected) }
	c.mu.Unlock()

	fc1, err := f.waitConn(1)
	if err != nil {
		t.Fatal(err)
	}
	fc1.close()
	if err := fc1.waitForClose(); err != nil {
		t.Fatal(err)
	}

	go func() {
		fc2, err := f.waitConn(2)
		if err != nil {
			t.Error(err)
			return
		}
		if err := fc2.handshake(ctx, 30); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("timed out waiting for reconnect")
	}

	// Push delivery resumes on the replacement connection.
	fc2, err := f.waitConn(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := fc2.sendFrame(map[string]any{
		"method": "message",
		"data": map[string]any{
			"message_id": "m2",
			"log_id":     2,
			"room_id":    "r1",
			"from":       map[string]any{"user_id": "u1", "name": "Ann"},
			"body":       map[string]any{"text": "after reconnect"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rec := readRecord(t, c.Records(), ctx)
	if rec.Kind != KindMessage || rec.Message == nil || rec.Message.MessageID != "m2" {
		t.Fatalf("record after reconnect = %+v, want message m2", rec)
	}
}

func TestKeepalive(t *testing.T) {
	f := newFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go func() {
		fc, err := f.waitConn(1)
		if err != nil {
			t.Error(err)
			return
		}
		if err := fc.handshake(ctx, 1); err != nil { // server suggests a 1s interval
			t.Error(err)
		}
	}()
	c, err := Connect(ctx, Options{URL: f.wsURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	reconnected := make(chan struct{})
	c.mu.Lock()
	c.OnReconnect = func() { close(reconnected) }
	c.mu.Unlock()

	fc, err := f.waitConn(1)
	if err != nil {
		t.Fatal(err)
	}

	// Cadence: the client pings on the server-suggested 1s interval.
	for i := 0; i < 2; i++ {
		f, err := fc.recvFrame(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if f.Method != "ping" {
			t.Fatalf("frame %d = %q, want ping", i+1, f.Method)
		}
	}

	// A missed pong must kill the connection and trigger a reconnect.
	fc.autoPong.Store(false)
	go func() {
		fc2, err := f.waitConn(2)
		if err != nil {
			t.Error(err)
			return
		}
		if err := fc2.handshake(ctx, 1); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("timed out waiting for missed-pong reconnect")
	}
}

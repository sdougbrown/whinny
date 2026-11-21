package apronws

import (
	"context"
	"github.com/sdougbrown/whinny/internal/apronws/apronfake"
	"testing"
	"time"
)

func TestReconnect(t *testing.T) {
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

	reconnected := make(chan struct{})
	c.SetOnReconnect(func() { close(reconnected) })

	fc1, err := f.WaitConn(1)
	if err != nil {
		t.Fatal(err)
	}
	fc1.Close()
	if err := fc1.WaitForClose(); err != nil {
		t.Fatal(err)
	}

	go func() {
		fc2, err := f.WaitConn(2)
		if err != nil {
			t.Error(err)
			return
		}
		fc2.WantToken = "tok-2" // the client re-auths with the rotated token
		if err := fc2.Handshake(ctx, 30); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("timed out waiting for reconnect")
	}

	// Push delivery resumes on the replacement connection.
	fc2, err := f.WaitConn(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := fc2.SendFrame(map[string]any{
		"method": "message",
		"params": map[string]any{
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

// TestSetOnReconnectAfterConnect swaps the reconnect hook while the
// client is running: the first reconnect runs the hook set via Options,
// the second runs the swapped-in hook.
func TestSetOnReconnectAfterConnect(t *testing.T) {
	f := apronfake.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first := make(chan struct{})
	second := make(chan struct{})
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
	c, err := Connect(ctx, Options{URL: f.WSURL(), Token: "test-token", OnReconnect: func() { close(first) }})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	// Reconnect #1: the hook set via Options fires.
	fc1, err := f.WaitConn(1)
	if err != nil {
		t.Fatal(err)
	}
	fc1.Close()
	if err := fc1.WaitForClose(); err != nil {
		t.Fatal(err)
	}
	go func() {
		fc2, err := f.WaitConn(2)
		if err != nil {
			t.Error(err)
			return
		}
		fc2.WantToken = "tok-2" // the client re-auths with the rotated token
		if err := fc2.Handshake(ctx, 30); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-first:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the first reconnect hook")
	}

	// Swap the hook while the client runs.
	c.SetOnReconnect(func() { close(second) })

	// Reconnect #2: the swapped-in hook fires.
	fc2, err := f.WaitConn(2)
	if err != nil {
		t.Fatal(err)
	}
	fc2.Close()
	if err := fc2.WaitForClose(); err != nil {
		t.Fatal(err)
	}
	go func() {
		fc3, err := f.WaitConn(3)
		if err != nil {
			t.Error(err)
			return
		}
		fc3.WantToken = "tok-2" // the client re-auths with the rotated token
		if err := fc3.Handshake(ctx, 30); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-second:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the swapped reconnect hook")
	}
}

func TestKeepalive(t *testing.T) {
	f := apronfake.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	go func() {
		fc, err := f.WaitConn(1)
		if err != nil {
			t.Error(err)
			return
		}
		if err := fc.Handshake(ctx, 1); err != nil { // server suggests a 1s interval, clamped to the 5s minimum
			t.Error(err)
		}
	}()
	c, err := Connect(ctx, Options{URL: f.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	reconnected := make(chan struct{})
	c.SetOnReconnect(func() { close(reconnected) })

	fc, err := f.WaitConn(1)
	if err != nil {
		t.Fatal(err)
	}

	// Cadence: the client pings on the server-suggested interval, clamped to
	// the 5s minimum.
	for i := 0; i < 2; i++ {
		f, err := fc.RecvFrame(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if f.Method != "ping" {
			t.Fatalf("frame %d = %q, want ping", i+1, f.Method)
		}
	}

	// A missed pong must kill the connection and trigger a reconnect.
	fc.AutoPong.Store(false)
	go func() {
		fc2, err := f.WaitConn(2)
		if err != nil {
			t.Error(err)
			return
		}
		fc2.WantToken = "tok-2" // the client re-auths with the rotated token
		if err := fc2.Handshake(ctx, 1); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("timed out waiting for missed-pong reconnect")
	}
}

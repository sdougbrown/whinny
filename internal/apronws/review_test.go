package apronws

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestInFlightFailure exercises the two in-flight failure modes: a Send in
// flight when the connection drops returns ErrConnectionLost, and a Send in
// flight at Close returns ErrClientClosed.
func TestInFlightFailure(t *testing.T) {
	f := newFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	fc, err := f.waitConn(1)
	if err != nil {
		t.Fatal(err)
	}

	reconnected := make(chan struct{})
	c.mu.Lock()
	c.OnReconnect = func() { close(reconnected) }
	c.mu.Unlock()

	// Part 1: a Send in flight when the connection drops → ErrConnectionLost.
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Send(ctx, "history", nil)
		errCh <- err
	}()
	// The request reaching the server proves the Send is in flight.
	fr, err := fc.recvFrame(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fr.Method != "history" {
		t.Fatalf("frame = %q, want history", fr.Method)
	}
	fc.close()
	if err := fc.waitForClose(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrConnectionLost) {
			t.Fatalf("Send on drop = %v, want ErrConnectionLost", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for in-flight Send (drop)")
	}

	// Reconnect and re-auth; the client now holds the rotated token.
	fc2, err := f.waitConn(2)
	if err != nil {
		t.Fatal(err)
	}
	fc2.wantToken = "tok-2"
	go func() {
		if err := fc2.handshake(ctx, 30); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-reconnected:
	case <-ctx.Done():
		t.Fatal("timed out waiting for reconnect")
	}

	// Part 2: a Send in flight at Close → ErrClientClosed.
	errCh2 := make(chan error, 1)
	go func() {
		_, err := c.Send(ctx, "history", nil)
		errCh2 <- err
	}()
	f2, err := fc2.recvFrame(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if f2.Method != "history" {
		t.Fatalf("frame = %q, want history", f2.Method)
	}
	c.Close()
	select {
	case err := <-errCh2:
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("Send on close = %v, want ErrClientClosed", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for in-flight Send (close)")
	}
}

// TestRecordsClosedAfterClose verifies the Records channel is closed when the
// client stops.
func TestRecordsClosedAfterClose(t *testing.T) {
	f := newFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	c.Close()

	// No pushes were sent, so the buffer is empty; the read blocks until the
	// channel is closed, then returns ok=false.
	_, ok := <-c.Records()
	if ok {
		t.Fatal("records channel not closed after Close")
	}
}

// TestBackpressure verifies a full records buffer stalls the read loop, and
// draining it resumes the loop.
func TestBackpressure(t *testing.T) {
	f := newFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
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

	fc, err := f.waitConn(1)
	if err != nil {
		t.Fatal(err)
	}

	// Fill the 256-buffer and one more; the 257th stalls the read loop.
	for i := 0; i < 257; i++ {
		if err := fc.sendFrame(map[string]any{"method": "activity", "params": map[string]any{"i": i}}); err != nil {
			t.Fatal(err)
		}
	}

	// Prove the read loop is stalled: a reply is sent but not delivered.
	shortCtx, shortCancel := context.WithTimeout(ctx, 2*time.Second)
	defer shortCancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Send(shortCtx, "history", nil)
		errCh <- err
	}()
	fr, err := fc.recvFrame(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fr.Method != "history" {
		t.Fatalf("frame = %q, want history", fr.Method)
	}
	if err := fc.sendFrame(map[string]any{"id": fr.ID, "result": map[string]any{"ok": true}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Send while stalled = %v, want context.DeadlineExceeded", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for stalled Send")
	}

	// Drain all 257 records; the read loop resumes.
	for i := 0; i < 257; i++ {
		select {
		case <-c.Records():
		case <-ctx.Done():
			t.Fatalf("timed out draining record %d", i)
		}
	}

	// Prove the read loop resumed: a reply is delivered.
	resCh := make(chan struct{}, 1)
	errCh2 := make(chan error, 1)
	go func() {
		if _, err := c.Send(ctx, "history", nil); err != nil {
			errCh2 <- err
			return
		}
		resCh <- struct{}{}
	}()
	f2, err := fc.recvFrame(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if f2.Method != "history" {
		t.Fatalf("frame = %q, want history", f2.Method)
	}
	if err := fc.sendFrame(map[string]any{"id": f2.ID, "result": map[string]any{"ok": true}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-resCh:
	case err := <-errCh2:
		t.Fatalf("Send after resume = %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for resumed Send")
	}
}

// TestPendingMapHygiene verifies successful replies delete their pending entry,
// so the map does not grow unbounded.
func TestPendingMapHygiene(t *testing.T) {
	f := newFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	fc, err := f.waitConn(1)
	if err != nil {
		t.Fatal(err)
	}

	const n = 5
	for i := 0; i < n; i++ {
		done := make(chan struct{})
		go func() {
			if _, err := c.Send(ctx, "history", nil); err != nil {
				t.Errorf("Send: %v", err)
			}
			close(done)
		}()
		fr, err := fc.recvFrame(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if fr.Method != "history" {
			t.Fatalf("frame = %q, want history", fr.Method)
		}
		if err := fc.sendFrame(map[string]any{"id": fr.ID, "result": map[string]any{"ok": true}}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("timed out waiting for Send")
		}
	}

	c.mu.Lock()
	nPending := len(c.pending)
	c.mu.Unlock()
	if nPending != 0 {
		t.Fatalf("len(c.pending) = %d, want 0", nPending)
	}
}

// TestRotatedToken verifies the client stores the server-issued token from the
// first auth and re-auths with it on reconnect.
func TestRotatedToken(t *testing.T) {
	f := newFakeServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		fc, err := f.waitConn(1)
		if err != nil {
			t.Error(err)
			return
		}
		// First auth uses opts.Token; the reply issues the rotated token.
		if err := fc.handshake(ctx, 30); err != nil {
			t.Error(err)
		}
	}()
	c, err := Connect(ctx, Options{URL: f.wsURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	// Force a reconnect.
	fc1, err := f.waitConn(1)
	if err != nil {
		t.Fatal(err)
	}
	fc1.close()
	if err := fc1.waitForClose(); err != nil {
		t.Fatal(err)
	}

	// The replacement connection must auth with the rotated token.
	fc2, err := f.waitConn(2)
	if err != nil {
		t.Fatal(err)
	}
	fc2.wantToken = "tok-2"
	authed := make(chan struct{})
	go func() {
		if err := fc2.handshake(ctx, 30); err != nil {
			t.Error(err)
			return
		}
		close(authed)
	}()
	select {
	case <-authed:
	case <-ctx.Done():
		t.Fatal("timed out waiting for re-auth with the rotated token")
	}
}

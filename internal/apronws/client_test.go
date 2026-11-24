package apronws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sdougbrown/whinny/internal/apronws/apronfake"
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
			f.ReportError(err)
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
			f.ReportError(err)
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
			f.ReportError(err)
			return
		}
		frame, err := fc2.RecvFrame(ctx)
		if err != nil {
			f.ReportError(err)
			return
		}
		if frame.Method != "history" {
			f.ReportError(fmt.Errorf("first post-auth frame = %q, want history", frame.Method))
			return
		}
		if err := fc2.SendFrame(map[string]any{"id": frame.ID, "result": map[string]any{"ok": true}}); err != nil {
			f.ReportError(err)
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
			f.ReportError(err)
			return
		}
		if err := fc.Handshake(ctx, 30); err != nil {
			f.ReportError(err)
			return
		}

		fr, err := fc.RecvFrame(ctx)
		if err != nil {
			f.ReportError(err)
			return
		}
		if fr.Method != "message" {
			f.ReportError(fmt.Errorf("first request = %q, want message", fr.Method))
			return
		}
		var p struct {
			RoomID string `json:"room_id"`
			Body   struct {
				Text string `json:"text"`
			} `json:"body"`
		}
		if err := json.Unmarshal(fr.Params, &p); err != nil {
			f.ReportError(err)
			return
		}
		if p.RoomID != "r1" || p.Body.Text != "hi" {
			f.ReportError(fmt.Errorf("params = %+v, want room r1 body hi", p))
			return
		}
		if err := fc.SendFrame(map[string]any{"id": fr.ID, "result": map[string]any{"message_id": "m1"}}); err != nil {
			f.ReportError(err)
			return
		}

		fr, err = fc.RecvFrame(ctx)
		if err != nil {
			f.ReportError(err)
			return
		}
		if fr.Method != "history" {
			f.ReportError(fmt.Errorf("second request = %q, want history", fr.Method))
			return
		}
		err = fc.SendFrame(map[string]any{
			"id": fr.ID,
			"error": map[string]any{
				"code":    429,
				"message": "slow down",
				"data":    map[string]any{"retry_after": 1.5},
			},
		})
		if err != nil {
			f.ReportError(err)
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
			f.ReportError(err)
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

func TestSetReactions(t *testing.T) {
	f := apronfake.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		fc, err := f.WaitConn(1)
		if err != nil {
			f.ReportError(err)
			return
		}
		if err := fc.Handshake(ctx, 30); err != nil {
			f.ReportError(err)
			return
		}
		fr, err := fc.RecvFrame(ctx)
		if err != nil {
			f.ReportError(err)
			return
		}
		if fr.Method != "reactions" {
			f.ReportError(fmt.Errorf("request = %q, want reactions", fr.Method))
			return
		}
		var p struct {
			MessageID string   `json:"message_id"`
			Emojis    []string `json:"emojis"`
		}
		if err := json.Unmarshal(fr.Params, &p); err != nil {
			f.ReportError(err)
			return
		}
		if p.MessageID != "m1" {
			f.ReportError(fmt.Errorf("message_id = %q, want m1", p.MessageID))
			return
		}
		want := []string{"👍", ":party:"}
		if len(p.Emojis) != len(want) || p.Emojis[0] != want[0] || p.Emojis[1] != want[1] {
			f.ReportError(fmt.Errorf("emojis = %v, want %v", p.Emojis, want))
			return
		}
		if err := fc.SendFrame(map[string]any{"id": fr.ID, "result": map[string]any{}}); err != nil {
			f.ReportError(err)
		}
	}()

	c, err := Connect(ctx, Options{URL: f.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	if err := c.SetReactions(ctx, "m1", []string{"👍", ":party:"}); err != nil {
		t.Fatalf("SetReactions: %v", err)
	}
}

func TestCapabilitiesAndEmojiCatalog(t *testing.T) {
	// The catalog is served over HTTP; point catalog_url at a local
	// test server and let the client fetch it.
	catalog := map[string]any{
		":party:": map[string]any{"url": "http://emoji.test/party.png", "width": 32, "height": 32},
		":owl:":   map[string]any{"url": "http://emoji.test/owl.png", "alt": "an owl"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(catalog)
	}))
	defer srv.Close()

	f := apronfake.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		fc, err := f.WaitConn(1)
		if err != nil {
			f.ReportError(err)
			return
		}
		fc.ServerCapabilities = []string{"history", "reactions"}
		fc.ServerExt = map[string]any{
			"emoji": map[string]any{"catalog_url": srv.URL},
		}
		if err := fc.Handshake(ctx, 30); err != nil {
			f.ReportError(err)
		}
	}()

	c, err := Connect(ctx, Options{URL: f.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	if !c.HasCapability("reactions") {
		t.Error("HasCapability(reactions) = false, want true")
	}
	if c.HasCapability("ext:emoji") {
		t.Error("HasCapability(ext:emoji) = true, want false (not advertised)")
	}

	got, err := c.EmojiCatalog(ctx)
	if err != nil {
		t.Fatalf("EmojiCatalog: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("catalog = %v, want 2 entries", got)
	}
	if got[":party:"].URL != "http://emoji.test/party.png" || got[":party:"].Width != 32 {
		t.Errorf(":party: entry = %+v", got[":party:"])
	}
	if got[":owl:"].Alt != "an owl" {
		t.Errorf(":owl: entry = %+v", got[":owl:"])
	}
}

func TestEmojiCatalogWithoutExt(t *testing.T) {
	f := apronfake.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		fc, err := f.WaitConn(1)
		if err != nil {
			f.ReportError(err)
			return
		}
		if err := fc.Handshake(ctx, 30); err != nil {
			f.ReportError(err)
		}
	}()

	c, err := Connect(ctx, Options{URL: f.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()

	got, err := c.EmojiCatalog(ctx)
	if err != nil {
		t.Fatalf("EmojiCatalog: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("catalog = %v, want empty", got)
	}
	if c.HasCapability("reactions") {
		t.Error("HasCapability(reactions) = true, want false")
	}
}

func TestNextIDUniqueAcrossClients(t *testing.T) {
	// Two clients sharing an apron account must never reuse a request
	// id (§1.2 dedup is per user): the ids differ across instances.
	// The fake's accepted connections accumulate, so a per-connect
	// handshake goroutine that waits for the *next* slot (one past the
	// connections already accepted) and a connect loop in lockstep
	// keeps the pairing deterministic.
	f := apronfake.New(t)
	var first *Client
	prefixes := make(map[uint64]bool)
	for i := 0; i < 3; i++ {
		next := i + 1 // the slot this round's dial lands in
		handshakeDone := make(chan error, 1)
		go func() {
			fc, err := f.WaitConn(next)
			if err != nil {
				handshakeDone <- err
				return
			}
			hctx, hcancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer hcancel()
			handshakeDone <- fc.Handshake(hctx, 30)
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		c, err := Connect(ctx, Options{URL: f.WSURL(), Token: "test-token"})
		cancel()
		if err != nil {
			t.Fatalf("Connect %d: %v", i+1, err)
		}
		if err := <-handshakeDone; err != nil {
			f.ReportError(err)
		}
		if c.idPrefix == 0 {
			t.Errorf("client %d idPrefix = 0, want seeded nonzero", i+1)
		}
		if prefixes[c.idPrefix] {
			t.Errorf("clients share id prefix %d", c.idPrefix)
		}
		prefixes[c.idPrefix] = true
		// Close before the next round so the fake's connection slots
		// do not accumulate read loops.
		c.Close()
		if first == nil {
			first = c
		}
	}
	if first.nextID() == first.nextID() {
		t.Error("one client repeated a request id")
	}
}

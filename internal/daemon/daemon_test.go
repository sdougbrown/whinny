package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"
	"testing"
	"time"

	"github.com/sdougbrown/whinny/internal/apronws"
	"github.com/sdougbrown/whinny/internal/apronws/apronfake"
	"github.com/sdougbrown/whinny/internal/store"
	"github.com/sdougbrown/whinny/internal/wake"
)

// --- fakes for the daemon's dependencies ---

type delivered struct {
	agent string
	env   Envelope
}

// fakeDeliverer records every delivery and signals each one. When
// fail is set, every delivery returns it (and is still recorded).
type fakeDeliverer struct {
	mu   sync.Mutex
	got  []delivered
	nowc chan struct{}
	fail error
}

func newFakeDeliverer() *fakeDeliverer {
	return &fakeDeliverer{nowc: make(chan struct{}, 64)}
}

func (f *fakeDeliverer) setFail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = err
}

func (f *fakeDeliverer) Deliver(_ context.Context, agent string, env Envelope) error {
	f.mu.Lock()
	f.got = append(f.got, delivered{agent: agent, env: env})
	fail := f.fail
	f.mu.Unlock()
	f.nowc <- struct{}{}
	return fail
}

func (f *fakeDeliverer) wait(t *testing.T) delivered {
	t.Helper()
	select {
	case <-f.nowc:
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.got[len(f.got)-1]
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delivery")
		return delivered{}
	}
}

func (f *fakeDeliverer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

func (f *fakeDeliverer) last(t *testing.T) delivered {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.got) == 0 {
		t.Fatal("no deliveries recorded")
	}
	return f.got[len(f.got)-1]
}

// blockingDeliverer blocks every delivery until block is closed and
// signals entered (the first delivery started) and second (a second
// delivery was attempted, while the first is still blocked):
// a deterministic slow-delivery simulation (no sleeps).
type blockingDeliverer struct {
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	block   chan struct{}
	second  chan struct{}
}

func (b *blockingDeliverer) Deliver(_ context.Context, _ string, _ Envelope) error {
	b.mu.Lock()
	n := b.calls
	b.calls++
	b.mu.Unlock()
	if n == 0 {
		b.entered <- struct{}{}
	} else {
		b.second <- struct{}{}
	}
	<-b.block
	return nil
}

// waitFor blocks until c is signaled or the test deadline passes.
func waitFor(t *testing.T, c <-chan struct{}, why string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", why)
	}
}

// fakeClock is an injected time source.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// testTicker is an injected tick source the test fires manually.
type testTicker struct{ c chan time.Time }

func newTestTicker() *testTicker { return &testTicker{c: make(chan time.Time, 8)} }

func (t *testTicker) C() <-chan time.Time { return t.c }
func (t *testTicker) Stop()               {}
func (t *testTicker) fire()               { t.c <- time.Now() }

// serverHandshake plays the server half of the apron handshake on fc
// (you is u9, matching baseCfg's Self), bounded at 10s.
func serverHandshake(t *testing.T, fc *apronfake.Conn) error {
	t.Helper()
	fc.YouUserID = "u9"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return fc.Handshake(ctx, 30)
}

// --- harness ---

type harness struct {
	srv    *apronfake.Server
	fc     *apronfake.Conn
	dlv    *fakeDeliverer
	clock  *fakeClock
	ticker *testTicker
	st     *store.Store
	cancel context.CancelFunc
	done   chan error
}

// startHarness brings up a fake apron server, a connected real
// apronws.Client, a store, and a running daemon.
func startHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	return startHarnessWithDeliverer(t, cfg, nil)
}

// startHarnessWithDeliverer is like startHarness but uses the given
// deliverer (the harness's fakeDeliverer is not wired in).
func startHarnessWithDeliverer(t *testing.T, cfg Config, custom Deliverer) *harness {
	t.Helper()
	srv := apronfake.New(t)
	srv.AutoRespond = true
	h := &harness{
		srv:    srv,
		dlv:    newFakeDeliverer(),
		clock:  newFakeClock(),
		ticker: newTestTicker(),
		done:   make(chan error, 1),
	}
	errC := make(chan error, 1)
	go func() {
		fc, err := h.srv.WaitConn(1)
		if err != nil {
			h.srv.ReportError(err)
			errC <- err
			return
		}
		errC <- serverHandshake(t, fc)
	}()
	client, err := apronws.Connect(context.Background(), apronws.Options{URL: h.srv.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(client.Close)
	if err := <-errC; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	h.fc = h.srv.ConnAt(t, 1)

	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	h.st = st
	t.Cleanup(func() { h.st.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	var deliver Deliverer = h.dlv
	if custom != nil {
		deliver = custom
	}
	deps := Deps{
		Client:  client,
		Store:   h.st,
		Deliver: deliver,
		Clock:   h.clock,
		Now:     h.clock.Now,
		Ticker:  func(time.Duration) Ticker { return h.ticker },
		Wait:    func(time.Duration) {},
	}
	go func() { h.done <- Run(ctx, cfg, deps) }()
	t.Cleanup(h.cancel)
	return h
}

// stop cancels the daemon and asserts a clean shutdown.
func (h *harness) stop(t *testing.T) {
	t.Helper()
	h.cancel()
	select {
	case err := <-h.done:
		if err != nil {
			t.Errorf("Run = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not shut down")
	}
}

func baseCfg() Config {
	return Config{
		Agent: "squire",
		Self:  wake.Self{UserID: "u9", Name: "squire", Aliases: []string{"sq"}},
		Peers: wake.Peers{"b1": {Name: "umpire", IsBot: true}},
		// The harness injects its own deliverer, but the config must
		// still pass Run's backend validation.
		ExecCmd: []string{"true"},
	}
}

// waitCursor waits until the room cursor reaches logID.
// waitNoWatches polls until the store holds no watches (the deletion
// happens right after the delivery the caller already observed).
func waitNoWatches(t *testing.T, h *harness) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ws, err := h.st.ListWatches(context.Background())
		if err == nil && len(ws) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("watches never drained: %+v (err %v)", ws, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitCursor waits until the room cursor reaches logID.
func waitCursor(t *testing.T, h *harness, logID int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		cursor, err := h.st.GetCursor(context.Background(), "r1")
		if err == nil && cursor >= logID {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cursor never reached %d (at %d)", logID, cursor)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitSettled pushes a plain (never-delivering) message and waits until
// the daemon's room cursor proves it was consumed, then asserts exactly
// want deliveries have happened. Deterministic negative assertions
// without sleeps.
func waitSettled(t *testing.T, h *harness, want int, why string) {
	t.Helper()
	const flushLog int64 = 990001
	h.fc.PushMessageAt("flush", "r1", "doug", "room noise", nil, flushLog)
	deadline := time.Now().Add(10 * time.Second)
	for {
		cursor, err := h.st.GetCursor(context.Background(), "r1")
		if err == nil && cursor >= flushLog {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: flush never consumed (cursor %d)", why, cursor)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := h.dlv.count(); got != want {
		t.Errorf("%s: deliveries = %d, want %d", why, got, want)
	}
}

// --- policy wiring (resident) ---

func TestRunMentionWake(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	h.fc.PushMessage("m1", "r1", "doug", "squire can you look at the roster", []string{"u9"})
	d := h.dlv.wait(t)
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire", d.agent)
	}
	want := `[wake kind=mention room=r1 from=doug message_id=m1]
squire can you look at the roster`
	if d.env.Text != want {
		t.Errorf("envelope = %q, want %q", d.env.Text, want)
	}
	if d.env.From != "doug" {
		t.Errorf("from = %q, want doug", d.env.From)
	}
	// The queue drained: durability was honored, delivery immediate.
	wakes, err := h.st.DequeueWakes(context.Background(), "squire")
	if err != nil || len(wakes) != 0 {
		t.Errorf("queued wakes after delivery = %v, err %v; want none", wakes, err)
	}
	// The room cursor advanced.
	logID, err := h.st.GetCursor(context.Background(), "r1")
	if err != nil || logID == 0 {
		t.Errorf("cursor = %d, err %v; want advanced", logID, err)
	}
	h.stop(t)
}

func TestRunWatchHitFiresAndOnceWatchDeleted(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	bTrue := true
	if _, err := h.st.AddWatch(context.Background(), store.Watch{
		RoomID: "r1", Owner: "squire", Kind: "pattern",
		Pattern: "deploy", Note: "deploy happened", Once: &bTrue,
	}, 3600, h.clock.Now()); err != nil {
		t.Fatalf("add watch: %v", err)
	}

	h.fc.PushMessage("m1", "r1", "doug", "time to deploy", nil)
	d := h.dlv.wait(t)
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire", d.agent)
	}
	if !strings.Contains(d.env.Text, "kind=watch") || !strings.Contains(d.env.Text, `note="deploy happened"`) {
		t.Errorf("envelope = %q, want kind=watch with note", d.env.Text)
	}
	if !strings.Contains(d.env.Text, "time to deploy") {
		t.Errorf("envelope text missing body: %q", d.env.Text)
	}
	// The once-watch is gone.
	waitNoWatches(t, h)
	h.stop(t)
}

func TestRunBotSenderNonMentionIgnored(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	h.fc.PushMessage("m1", "r1", "b1", "umpire noise, no mention", nil)
	waitSettled(t, h, 0, "bot message must not wake")
	h.stop(t)
}

// --- ambient gate ---

func TestRunAmbientValves(t *testing.T) {
	classifier := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"directed":true}`))
	}))
	defer classifier.Close()

	cfg := baseCfg()
	cfg.Ambient = AmbientConfig{
		Endpoint:   classifier.URL,
		MinSpacing: 10 * time.Minute,
		MaxPerHour: 2,
	}
	h := startHarness(t, cfg)

	h.fc.PushMessage("a1", "r1", "doug", "anyone around?", nil)
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=ambient") {
		t.Errorf("first ambient = %q", d.env.Text)
	}

	// Inside the min spacing: no ambient wake.
	h.clock.Advance(time.Minute)
	h.fc.PushMessage("a2", "r1", "doug", "still there?", nil)
	waitSettled(t, h, 1, "spacing valve")

	// Outside spacing (2nd wake of the hour, cap 2): fires.
	h.clock.Advance(20 * time.Minute)
	h.fc.PushMessage("a3", "r1", "doug", "third ask", nil)
	d = h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=ambient") {
		t.Errorf("second ambient = %q", d.env.Text)
	}

	// Both valves' wakes are inside the trailing hour now: the per-hour
	// cap blocks the next one.
	h.clock.Advance(10 * time.Minute)
	h.fc.PushMessage("a4", "r1", "doug", "fourth ask", nil)
	waitSettled(t, h, 2, "per-hour valve")

	// An hour later the cap window has passed: ambient fires again.
	h.clock.Advance(time.Hour)
	h.fc.PushMessage("a5", "r1", "doug", "much later", nil)
	d = h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=ambient") {
		t.Errorf("post-window ambient = %q", d.env.Text)
	}
	h.stop(t)
}

// --- expiry ---

func TestExpiryDeliversWatchExpiredAndDeletes(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	bFalse := false
	if _, err := h.st.AddWatch(context.Background(), store.Watch{
		RoomID: "r1", Owner: "squire", Kind: "pattern",
		Pattern: "deploy", Note: "waiting for deploy", Once: &bFalse,
	}, 1, h.clock.Now()); err != nil {
		t.Fatalf("add watch: %v", err)
	}

	h.clock.Advance(2 * time.Minute)
	h.ticker.fire()
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=watch-expired") || !strings.Contains(d.env.Text, `note="waiting for deploy"`) ||
		!strings.Contains(d.env.Text, "no match within ttl") {
		t.Errorf("expiry envelope = %q", d.env.Text)
	}
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire", d.agent)
	}
	waitNoWatches(t, h)
	h.stop(t)
}

// --- self-post subscription ---

func TestSelfPostSubscription(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	h.fc.Push("message", map[string]any{
		"message_id": "m1", "log_id": 7, "room_id": "r1",
		"from":     map[string]any{"user_id": "u9", "name": "squire"},
		"body":     map[string]any{"text": "starting the release thread"},
		"reply_to": map[string]any{"message_id": "root1"},
	})

	// Deterministic wait: a mention wake proves the record was consumed;
	// the subscription watch must exist by then.
	h.fc.PushMessage("m2", "r1", "doug", "ping", []string{"u9"})
	h.dlv.wait(t)

	ws, err := h.st.ListWatches(context.Background())
	if err != nil || len(ws) != 1 {
		t.Fatalf("watches = %+v, err %v; want one subscription", ws, err)
	}
	w := ws[0]
	if w.Kind != "subscription" || w.RoomID != "r1" || w.ThreadID != "root1" || w.Owner != "squire" {
		t.Errorf("watch = %+v, want subscription on root1 owned by squire", w)
	}
	if ttl := w.ExpiresAt - w.CreatedAt; ttl != 86400 {
		t.Errorf("ttl = %d, want 86400", ttl)
	}
	h.stop(t)
}

func TestSelfPostSubscriptionDisabledByMeta(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)
	if err := h.st.SetMeta(context.Background(), MetaAutoSubscribe, MetaAutoSubscribeOff); err != nil {
		t.Fatalf("set meta: %v", err)
	}

	h.fc.Push("message", map[string]any{
		"message_id": "m1", "log_id": 7, "room_id": "r1",
		"from":     map[string]any{"user_id": "u9", "name": "squire"},
		"body":     map[string]any{"text": "thread post"},
		"reply_to": map[string]any{"message_id": "root1"},
	})
	h.fc.PushMessageAt("m2", "r1", "doug", "ping", []string{"u9"}, 990002)
	waitCursor(t, h, 990002)

	ws, _ := h.st.ListWatches(context.Background())
	if len(ws) != 0 {
		t.Errorf("watches = %+v; want none with auto_subscribe=off", ws)
	}
	h.stop(t)
}

// --- shared mode ---

func TestSharedModeRoutesWatchToOwnerCallback(t *testing.T) {
	cfg := baseCfg()
	cfg.Mode = ModeShared
	cfg.Agent = "relay"

	sink := filepath.Join(t.TempDir(), "alice.out")
	script := filepath.Join(t.TempDir(), "callback.sh")
	writeScript(t, script, "#!/bin/sh\ncat >> "+sink+"\n")

	h := startHarness(t, cfg)
	if err := h.st.SetMeta(context.Background(), MetaDeliveryPrefix+"alice", script); err != nil {
		t.Fatalf("set meta: %v", err)
	}
	if _, err := h.st.AddWatch(context.Background(), store.Watch{
		RoomID: "r1", Owner: "alice", Kind: "pattern",
		Pattern: "deploy", Note: "deploy happened",
	}, 3600, h.clock.Now()); err != nil {
		t.Fatalf("add watch: %v", err)
	}

	// A bot's message: watches still apply in shared mode.
	h.fc.PushMessage("m1", "r1", "b1", "time to deploy", nil)

	deadline := time.Now().Add(10 * time.Second)
	var got []byte
	for {
		b, err := os.ReadFile(sink)
		if err == nil && len(b) > 0 {
			got = b
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("callback never wrote; sink err = %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(string(got), "kind=watch") || !strings.Contains(string(got), `note="deploy happened"`) {
		t.Errorf("callback got %q, want watch envelope with note", got)
	}
	if h.dlv.count() != 0 {
		t.Errorf("shared deliverer called %d times; the callback owns delivery", h.dlv.count())
	}
	// The owner's once-watch was removed.
	ws, _ := h.st.ListWatches(context.Background())
	if len(ws) != 0 {
		t.Errorf("watches after fire = %+v; want none", ws)
	}
	h.stop(t)
}

func TestSharedModeSkipsOwnerWithoutCallback(t *testing.T) {
	cfg := baseCfg()
	cfg.Mode = ModeShared
	h := startHarness(t, cfg)
	bTrue := true
	if _, err := h.st.AddWatch(context.Background(), store.Watch{
		RoomID: "r1", Owner: "bob", Kind: "pattern",
		Pattern: "release", Note: "release shipped", Once: &bTrue,
	}, 3600, h.clock.Now()); err != nil {
		t.Fatalf("add watch: %v", err)
	}

	h.fc.PushMessage("m1", "r1", "doug", "the release is out", nil)
	waitSettled(t, h, 0, "owner without a callback must not fire in shared mode")
	// The once-watch is not consumed.
	ws, _ := h.st.ListWatches(context.Background())
	if len(ws) != 1 || ws[0].Owner != "bob" {
		t.Errorf("watches = %+v, want the watch still present (not consumed)", ws)
	}
	h.stop(t)
}

func TestSharedExpiryDeliversToCallback(t *testing.T) {
	cfg := baseCfg()
	cfg.Mode = ModeShared
	cfg.Agent = "relay"

	sink := filepath.Join(t.TempDir(), "alice.out")
	script := filepath.Join(t.TempDir(), "callback.sh")
	writeScript(t, script, "#!/bin/sh\ncat >> "+sink+"\n")

	h := startHarness(t, cfg)
	if err := h.st.SetMeta(context.Background(), MetaDeliveryPrefix+"alice", script); err != nil {
		t.Fatalf("set meta: %v", err)
	}
	bFalse := false
	if _, err := h.st.AddWatch(context.Background(), store.Watch{
		RoomID: "r1", Owner: "alice", Kind: "pattern",
		Pattern: "deploy", Note: "waiting for deploy", Once: &bFalse,
	}, 1, h.clock.Now()); err != nil {
		t.Fatalf("add watch: %v", err)
	}

	h.clock.Advance(2 * time.Minute)
	h.ticker.fire()

	deadline := time.Now().Add(10 * time.Second)
	var got []byte
	for {
		b, err := os.ReadFile(sink)
		if err == nil && len(b) > 0 {
			got = b
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("callback never wrote; sink err = %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(string(got), "kind=watch-expired") || !strings.Contains(string(got), `note="waiting for deploy"`) {
		t.Errorf("callback got %q, want an expiry envelope with the note", got)
	}
	waitNoWatches(t, h)
	h.stop(t)
}

func TestSharedExpirySkipsOwnerWithoutCallback(t *testing.T) {
	cfg := baseCfg()
	cfg.Mode = ModeShared
	h := startHarness(t, cfg)
	bFalse := false
	if _, err := h.st.AddWatch(context.Background(), store.Watch{
		RoomID: "r1", Owner: "bob", Kind: "pattern",
		Pattern: "release", Note: "waiting", Once: &bFalse,
	}, 1, h.clock.Now()); err != nil {
		t.Fatalf("add watch: %v", err)
	}

	h.clock.Advance(2 * time.Minute)
	h.ticker.fire()

	// Bounded window for the sweep to (not) act: if the watch is still
	// present and undelivered at the end, the skip held.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ws, _ := h.st.ListWatches(context.Background())
		if len(ws) == 0 {
			t.Fatal("watch deleted; the shared daemon must skip owners without a callback")
		}
		if h.dlv.count() > 0 {
			t.Fatal("delivery happened; the shared daemon must skip owners without a callback")
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.stop(t)
}

// TestSharedExpiryKeepsWatchWhenDeliveryFails: a failed expiry
// delivery keeps the watch so it re-fires on the next sweep instead of
// being lost. (A successful deliverer deletes the watch: see
// TestSharedExpiryDeliversToCallback.)
func TestSharedExpiryKeepsWatchWhenDeliveryFails(t *testing.T) {
	cfg := baseCfg()
	cfg.Mode = ModeShared
	cfg.Agent = "relay"

	ran := filepath.Join(t.TempDir(), "ran.out")
	script := filepath.Join(t.TempDir(), "callback.sh")
	writeScript(t, script, "#!/bin/sh\necho fired >> "+ran+"\nexit 1\n")

	h := startHarness(t, cfg)
	if err := h.st.SetMeta(context.Background(), MetaDeliveryPrefix+"squire", script); err != nil {
		t.Fatalf("set meta: %v", err)
	}
	bFalse := false
	if _, err := h.st.AddWatch(context.Background(), store.Watch{
		RoomID: "r1", Owner: "squire", Kind: "pattern",
		Pattern: "deploy", Note: "waiting for deploy", Once: &bFalse,
	}, 1, h.clock.Now()); err != nil {
		t.Fatalf("add watch: %v", err)
	}

	h.clock.Advance(2 * time.Minute)
	h.ticker.fire()

	// The script's side effect proves the sweep delivered (and failed);
	// the watch must survive for the next sweep.
	deadline := time.Now().Add(10 * time.Second)
	for {
		b, err := os.ReadFile(ran)
		if err == nil && len(b) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("failing callback never ran; ran err = %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	ws, err := h.st.ListWatches(context.Background())
	if err != nil {
		t.Fatalf("list watches: %v", err)
	}
	if len(ws) != 1 {
		t.Fatalf("watches after failed delivery = %d, want 1 (kept for the next sweep)", len(ws))
	}
	h.stop(t)
}

// --- restart drain ---

func TestRestartDrainsQueuedWake(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)
	h.stop(t)

	// A wake enqueued before the "crash".
	stale := BuildEnvelope(wake.Decision{Wake: true, Kind: wake.KindMention}, wake.Msg{
		MessageID: "m0", RoomID: "r1", From: wake.From{UserID: "doug"}, Text: "pre-crash mention",
	})
	payload, err := json.Marshal(stale)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.st.EnqueueWake(context.Background(), store.Wake{Agent: "squire", Payload: string(payload)}, h.clock.Now()); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Start a second daemon run over the same store.
	h2 := &harness{srv: h.srv, dlv: newFakeDeliverer(), clock: h.clock, ticker: newTestTicker(), st: h.st, done: make(chan error, 1)}
	errC2 := make(chan error, 1)
	go func() {
		fc, err := h2.srv.WaitConn(2)
		if err != nil {
			h2.srv.ReportError(err)
			errC2 <- err
			return
		}
		errC2 <- serverHandshake(t, fc)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := apronws.Connect(ctx, apronws.Options{URL: h.srv.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer client.Close()
	if err := <-errC2; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	h2.fc = h2.srv.ConnAt(t, 2)
	go func() {
		h2.done <- Run(ctx, cfg, Deps{Client: client, Store: h2.st, Deliver: h2.dlv, Clock: h2.clock, Now: h2.clock.Now,
			Ticker: func(time.Duration) Ticker { return h2.ticker }})
	}()

	d := h2.dlv.wait(t)
	if !strings.Contains(d.env.Text, "pre-crash mention") {
		t.Errorf("drained envelope = %q, want the stale wake", d.env.Text)
	}
	if d.env.From != "doug" {
		t.Errorf("drained envelope from = %q, want doug (payload kept its routing metadata)", d.env.From)
	}
	cancel()
	select {
	case err := <-h2.done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("second Run = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("second daemon did not shut down")
	}
}

// --- config validation ---

func TestRunValidatesConfig(t *testing.T) {
	deps := Deps{Store: func() *store.Store { st, _ := store.Open(filepath.Join(t.TempDir(), "s.db")); return st }(), Deliver: newFakeDeliverer()}
	defer deps.Store.Close()
	if err := Run(context.Background(), Config{Mode: "bogus"}, deps); err == nil {
		t.Error("unknown mode: Run = nil, want error")
	}
	if err := Run(context.Background(), Config{Deliver: "bogus"}, deps); err == nil {
		t.Error("unknown deliver: Run = nil, want error")
	}
	// exec without ExecCmd: Run rejects the backend before it dials,
	// so the error names the missing exec command (no server needed).
	if err := Run(context.Background(), Config{URL: "ws://apron.test/ws", Token: "tok", Deliver: "exec", Self: wake.Self{UserID: "u9"}}, deps); err == nil || !strings.Contains(err.Error(), "exec command") {
		t.Errorf("exec without ExecCmd: Run = %v, want the ValidateBackend error naming the exec command", err)
	}
	if err := Run(context.Background(), baseCfg(), Deps{Deliver: newFakeDeliverer()}); err == nil {
		t.Error("nil store: Run = nil, want error")
	}
	if err := Run(context.Background(), baseCfg(), Deps{Store: deps.Store}); err == nil {
		t.Error("nil deliverer: Run = nil, want error")
	}
}

// TestRunValidatesBackendBeforeConnect: a backend Run cannot serve is
// rejected before the apron connection is attempted: no server is
// needed, and the error names the missing piece.
func TestRunValidatesBackendBeforeConnect(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	deps := Deps{Store: st, Deliver: newFakeDeliverer()}
	if err := Run(context.Background(), Config{URL: "ws://apron.test/ws", Token: "tok", Deliver: "exec", Self: wake.Self{UserID: "u9"}}, deps); err == nil || !strings.Contains(err.Error(), "exec command") {
		t.Errorf("exec without ExecCmd: Run = %v, want the ValidateBackend error naming the exec command", err)
	}
	if err := Run(context.Background(), Config{URL: "ws://apron.test/ws", Token: "tok", Deliver: "socket", Self: wake.Self{UserID: "u9"}}, deps); err == nil || !strings.Contains(err.Error(), "socket URL") {
		t.Errorf("socket without SocketURL: Run = %v, want the ValidateBackend error naming the socket URL", err)
	}
}

// TestRunRejectsDepsWithBothLinkAndClient: Deps carrying both an
// injected Link and an apron Client is rejected before any dial: the
// two transport-injection paths are mutually exclusive, and the error
// names both fields.
func TestRunRejectsDepsWithBothLinkAndClient(t *testing.T) {
	srv := apronfake.New(t)
	srv.AutoRespond = true
	errC := make(chan error, 1)
	go func() {
		fc, err := srv.WaitConn(1)
		if err != nil {
			srv.ReportError(err)
			errC <- err
			return
		}
		errC <- serverHandshake(t, fc)
	}()
	client, err := apronws.Connect(context.Background(), apronws.Options{URL: srv.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(client.Close)
	if err := <-errC; err != nil {
		t.Fatalf("server handshake: %v", err)
	}

	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	deps := Deps{Client: client, Link: newFakeLink(), Store: st, Deliver: newFakeDeliverer()}
	if err := Run(context.Background(), baseCfg(), deps); err == nil || !strings.Contains(err.Error(), "Link") || !strings.Contains(err.Error(), "Client") {
		t.Errorf("Run = %v, want the error naming both Link and Client", err)
	}
}

// startHarnessWithSetup is like startHarness but calls setup (to
// configure the fake server) after the client connects and before Run
// starts, so the startup catch-up sees the configured responses.
func startHarnessWithSetup(t *testing.T, cfg Config, setup func(*harness)) *harness {
	t.Helper()
	srv := apronfake.New(t)
	srv.AutoRespond = true
	h := &harness{
		srv:    srv,
		dlv:    newFakeDeliverer(),
		clock:  newFakeClock(),
		ticker: newTestTicker(),
		done:   make(chan error, 1),
	}
	errC := make(chan error, 1)
	go func() {
		fc, err := h.srv.WaitConn(1)
		if err != nil {
			h.srv.ReportError(err)
			errC <- err
			return
		}
		errC <- serverHandshake(t, fc)
	}()
	client, err := apronws.Connect(context.Background(), apronws.Options{URL: h.srv.WSURL(), Token: "test-token"})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(client.Close)
	if err := <-errC; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	h.fc = h.srv.ConnAt(t, 1)

	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	h.st = st
	t.Cleanup(func() { h.st.Close() })

	if setup != nil {
		setup(h)
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	deps := Deps{
		Client:  client,
		Store:   h.st,
		Deliver: h.dlv,
		Clock:   h.clock,
		Now:     h.clock.Now,
		Ticker:  func(time.Duration) Ticker { return h.ticker },
		Wait:    func(time.Duration) {},
	}
	go func() { h.done <- Run(ctx, cfg, deps) }()
	t.Cleanup(h.cancel)
	return h
}

// --- Fix 4: resident expiry routes through the queue under cfg.Agent ---

func TestExpiryRoutesThroughQueueUnderAgent(t *testing.T) {
	cfg := baseCfg() // Agent="squire"
	h := startHarness(t, cfg)

	// A watch owned by "resident" (the default) expires; the resident
	// daemon (agent "squire") delivers the expiry under cfg.Agent.
	bFalse := false
	if _, err := h.st.AddWatch(context.Background(), store.Watch{
		RoomID: "r1", Owner: "resident", Kind: "pattern",
		Pattern: "deploy", Note: "waiting", Once: &bFalse,
	}, 1, h.clock.Now()); err != nil {
		t.Fatalf("add watch: %v", err)
	}
	h.clock.Advance(2 * time.Minute)
	h.ticker.fire()
	d := h.dlv.wait(t)
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire (cfg.Agent, not the watch owner)", d.agent)
	}
	if !strings.Contains(d.env.Text, "kind=watch-expired") {
		t.Errorf("envelope = %q, want watch-expired", d.env.Text)
	}
	// The queue drained: the expiry was enqueued and delivered.
	wakes, err := h.st.DequeueWakes(context.Background(), "squire")
	if err != nil || len(wakes) != 0 {
		t.Errorf("queued wakes after delivery = %v, err %v; want none", wakes, err)
	}
	h.stop(t)
}

// --- Fix 5: resident mode skips foreign-owner watches ---

func TestResidentSkipsForeignOwnerWatch(t *testing.T) {
	cfg := baseCfg() // Agent="squire"
	h := startHarness(t, cfg)

	// A watch owned by "alice" (a shared-mode transient) is in the
	// store; the resident daemon must not consume it.
	bTrue := true
	if _, err := h.st.AddWatch(context.Background(), store.Watch{
		RoomID: "r1", Owner: "alice", Kind: "pattern",
		Pattern: "deploy", Note: "foreign", Once: &bTrue,
	}, 3600, h.clock.Now()); err != nil {
		t.Fatalf("add watch: %v", err)
	}

	// A message that would match the foreign watch: no wake (the watch
	// is skipped).
	h.fc.PushMessage("m1", "r1", "doug", "time to deploy", nil)
	waitSettled(t, h, 0, "foreign-owner watch must not fire in resident mode")
	// The foreign watch is still in the store (not consumed).
	ws, _ := h.st.ListWatches(context.Background())
	if len(ws) != 1 || ws[0].Owner != "alice" {
		t.Errorf("watches = %+v, want the foreign watch still present", ws)
	}
	h.stop(t)
}

// --- Fix 6: cursor catch-up on start and on reconnect ---

func TestCatchUpOnStartReplaysMissedMessages(t *testing.T) {
	cfg := baseCfg()
	h := startHarnessWithSetup(t, cfg, func(h *harness) {
		// Pre-seed the cursor: the daemon was "down" since log 5.
		if err := h.st.SetCursor(context.Background(), "r1", 5); err != nil {
			t.Fatalf("set cursor: %v", err)
		}
		// Room r1 is joined; the history has a missed mention at log 6.
		h.srv.SetCatchUp(
			[]map[string]any{{"room_id": "r1"}},
			map[string][]map[string]any{
				"r1": {{
					"message_id": "m6", "log_id": 6, "room_id": "r1",
					"from":     map[string]any{"user_id": "doug", "name": "doug"},
					"body":     map[string]any{"text": "squire can you look"},
					"mentions": []string{"u9"},
				}},
			},
		)
	})

	// The startup catch-up replays the missed mention.
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=mention") {
		t.Errorf("catch-up envelope = %q, want a mention wake", d.env.Text)
	}
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire", d.agent)
	}
	// The cursor advanced past the replayed message.
	if v, _ := h.st.GetCursor(context.Background(), "r1"); v < 6 {
		t.Errorf("cursor = %d, want >= 6 (replayed)", v)
	}
	h.stop(t)
}

func TestCatchUpOnReconnect(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	// Pre-seed the cursor and configure the history for the reconnect.
	if err := h.st.SetCursor(context.Background(), "r1", 5); err != nil {
		t.Fatalf("set cursor: %v", err)
	}
	h.srv.SetCatchUp(
		[]map[string]any{{"room_id": "r1"}},
		map[string][]map[string]any{
			"r1": {{
				"message_id": "m6", "log_id": 6, "room_id": "r1",
				"from":     map[string]any{"user_id": "doug", "name": "doug"},
				"body":     map[string]any{"text": "squire can you look"},
				"mentions": []string{"u9"},
			}},
		},
	)

	// Force a reconnect: the client's OnReconnect fires the catch-up.
	fc1 := h.srv.ConnAt(t, 1)
	fc1.Close()
	if err := fc1.WaitForClose(); err != nil {
		t.Fatal(err)
	}
	// The replacement connection handshakes.
	go func() {
		fc2, err := h.srv.WaitConn(2)
		if err != nil {
			h.srv.ReportError(err)
			return
		}
		fc2.WantToken = "tok-2" // the client re-auths with the rotated token
		if err := serverHandshake(t, fc2); err != nil {
			h.srv.ReportError(err)
		}
	}()

	// The reconnect catch-up replays the missed mention.
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=mention") {
		t.Errorf("reconnect catch-up envelope = %q, want a mention wake", d.env.Text)
	}
	h.stop(t)
}

// --- cursor dedupe (handleRecord skips LogID <= cursor) ---

func TestDedupeSkipsLiveMessageAtCursor(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	// Pre-seed the cursor at the message's log id: the live push is a
	// duplicate and must not wake.
	if err := h.st.SetCursor(context.Background(), "r1", 5); err != nil {
		t.Fatalf("set cursor: %v", err)
	}
	h.fc.PushMessageAt("m5", "r1", "doug", "squire can you look", []string{"u9"}, 5)
	waitSettled(t, h, 0, "live message at the cursor must not wake")
	h.stop(t)
}

func TestCatchUpDedupesReplayedMessages(t *testing.T) {
	cfg := baseCfg()
	h := startHarnessWithSetup(t, cfg, func(h *harness) {
		// Pre-seed the cursor: the daemon was "down" since log 5.
		if err := h.st.SetCursor(context.Background(), "r1", 5); err != nil {
			t.Fatalf("set cursor: %v", err)
		}
		// The history returns older (log 3, 5) and newer (log 6) messages;
		// only the newer one is evaluated.
		h.srv.SetCatchUp(
			[]map[string]any{{"room_id": "r1"}},
			map[string][]map[string]any{
				"r1": {
					{
						"message_id": "m3", "log_id": 3, "room_id": "r1",
						"from": map[string]any{"user_id": "doug", "name": "doug"},
						"body": map[string]any{"text": "old noise"},
					},
					{
						"message_id": "m5", "log_id": 5, "room_id": "r1",
						"from": map[string]any{"user_id": "doug", "name": "doug"},
						"body": map[string]any{"text": "at-cursor noise"},
					},
					{
						"message_id": "m6", "log_id": 6, "room_id": "r1",
						"from":     map[string]any{"user_id": "doug", "name": "doug"},
						"body":     map[string]any{"text": "squire can you look"},
						"mentions": []string{"u9"},
					},
				},
			},
		)
	})

	// Exactly one wake: only the newer-than-cursor message.
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=mention") {
		t.Errorf("catch-up envelope = %q, want the mention wake", d.env.Text)
	}
	waitSettled(t, h, 1, "only newer-than-cursor messages evaluated")
	h.stop(t)
}

func TestReplayAndLiveOverlapDeliversOnce(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	// Pre-seed the cursor and hold the history response: the live push
	// lands between the reconnect and the replay.
	if err := h.st.SetCursor(context.Background(), "r1", 5); err != nil {
		t.Fatalf("set cursor: %v", err)
	}
	h.srv.HistoryGate = make(chan struct{})
	h.srv.HistoryGateHit = make(chan struct{}, 1)
	var gateOnce sync.Once
	closeGate := func() { gateOnce.Do(func() { close(h.srv.HistoryGate) }) }
	t.Cleanup(closeGate)
	h.srv.SetCatchUp(
		[]map[string]any{{"room_id": "r1"}},
		map[string][]map[string]any{
			"r1": {{
				"message_id": "m6", "log_id": 6, "room_id": "r1",
				"from":     map[string]any{"user_id": "doug", "name": "doug"},
				"body":     map[string]any{"text": "squire can you look"},
				"mentions": []string{"u9"},
			}},
		},
	)

	// Force a reconnect: the catch-up goroutine blocks on the history
	// response.
	fc1 := h.srv.ConnAt(t, 1)
	fc1.Close()
	if err := fc1.WaitForClose(); err != nil {
		t.Fatal(err)
	}
	fc2 := h.srv.ConnAt(t, 2)
	fc2.WantToken = "tok-2" // the client re-auths with the rotated token
	errC := make(chan error, 1)
	go func() { errC <- serverHandshake(t, fc2) }()
	if err := <-errC; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
	h.fc = fc2

	// The live push of the same message (log 6) is evaluated and
	// advances the cursor.
	fc2.PushMessageAt("m6", "r1", "doug", "squire can you look", []string{"u9"}, 6)
	waitCursor(t, h, 6)

	// Wait for the catch-up to reach the history fetch (the replay
	// path is live), then release it: the catch-up's copy of m6 is at
	// the cursor and is skipped.
	select {
	case <-h.srv.HistoryGateHit:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the catch-up history fetch")
	}
	closeGate()
	waitSettled(t, h, 1, "replay + live overlap delivers once")
	h.stop(t)
}

// --- Fix 7: reply-author cache from the record stream ---

func TestHistoryCacheFedByStream(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	// The bot posts a message (m2, log 2): its author is cached by the
	// stream.
	h.fc.Push("message", map[string]any{
		"message_id": "m2", "log_id": 2, "room_id": "r1",
		"from": map[string]any{"user_id": "u9", "name": "squire"},
		"body": map[string]any{"text": "bot post"},
	})
	waitCursor(t, h, 2)

	// A reply to the bot's post (m3, log 3): the author (u9) resolves
	// from the cache (m2 was fed by the stream), so the reply wake
	// fires without a History call.
	h.fc.Push("message", map[string]any{
		"message_id": "m3", "log_id": 3, "room_id": "r1",
		"from":     map[string]any{"user_id": "doug", "name": "doug"},
		"body":     map[string]any{"text": "reply to bot"},
		"reply_to": map[string]any{"message_id": "m2"},
	})
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=reply") {
		t.Errorf("reply envelope = %q, want a reply wake (author from the cache)", d.env.Text)
	}
	if n := h.srv.HistoryCallCount(); n != 0 {
		t.Errorf("history lookups = %d, want 0 (the stream cache resolved the author)", n)
	}
	h.stop(t)
}

// TestHistoryCacheMissFallsBackToHistory: a reply to a message not in
// the stream falls back to the client's History.
func TestHistoryCacheMissFallsBackToHistory(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)
	// Configure the fake server's history to include the bot's message
	// (so the History fallback resolves the author).
	h.srv.SetCatchUp(
		nil,
		map[string][]map[string]any{
			"r1": {{
				"message_id": "m100", "log_id": 100, "room_id": "r1",
				"from": map[string]any{"user_id": "u9", "name": "squire"},
				"body": map[string]any{"text": "bot post from history"},
			}},
		},
	)
	// A reply to m100 (not in the stream): the author resolves via the
	// History fallback.
	h.fc.Push("message", map[string]any{
		"message_id": "m101", "log_id": 101, "room_id": "r1",
		"from":     map[string]any{"user_id": "doug", "name": "doug"},
		"body":     map[string]any{"text": "reply to history"},
		"reply_to": map[string]any{"message_id": "m100"},
	})
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=reply") {
		t.Errorf("reply envelope = %q, want a reply wake (author from the History fallback)", d.env.Text)
	}
	if n := h.srv.HistoryCallCount(); n == 0 {
		t.Error("history lookups = 0, want >= 1 (the fallback ran)")
	}
	h.stop(t)
}

// TestReplyWakeRetriesFailedHistoryLookup: a reply-to-my-message wake
// whose History lookup fails twice is still delivered: the lookup is
// retried (the injected pause is a no-op), and the third attempt
// resolves the author.
func TestReplyWakeRetriesFailedHistoryLookup(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)
	h.srv.SetCatchUp(
		nil,
		map[string][]map[string]any{
			"r1": {{
				"message_id": "m100", "log_id": 100, "room_id": "r1",
				"from": map[string]any{"user_id": "u9", "name": "squire"},
				"body": map[string]any{"text": "bot post from history"},
			}},
		},
	)
	h.srv.SetHistoryFailures(2)
	// A reply to m100 (not in the stream): the History fallback fails
	// twice, then succeeds on the retry.
	h.fc.Push("message", map[string]any{
		"message_id": "m101", "log_id": 101, "room_id": "r1",
		"from":     map[string]any{"user_id": "doug", "name": "doug"},
		"body":     map[string]any{"text": "reply to history"},
		"reply_to": map[string]any{"message_id": "m100"},
	})
	d := h.dlv.wait(t)
	if !strings.Contains(d.env.Text, "kind=reply") {
		t.Errorf("reply envelope = %q, want a reply wake (lookup retried past two failures)", d.env.Text)
	}
	h.stop(t)
}

// TestSlowDelivererDoesNotBlockRecordProcessing: a blocked first
// delivery must not hold procMu: while the first delivery is still
// blocked, the reconnect catch-up evaluates and enqueues a second
// message, whose delivery is attempted concurrently (delivery runs
// after the lock is released).
func TestSlowDelivererDoesNotBlockRecordProcessing(t *testing.T) {
	cfg := baseCfg()
	bd := &blockingDeliverer{
		entered: make(chan struct{}),
		block:   make(chan struct{}),
		second:  make(chan struct{}),
	}
	h := startHarnessWithDeliverer(t, cfg, bd)

	// m1 (log 1) is a live mention: the daemon enqueues it and the
	// flush blocks in the deliverer.
	h.fc.PushMessageAt("m1", "r1", "doug", "squire can you look", []string{"u9"}, 1)
	waitFor(t, bd.entered, "the first delivery to start")

	// m2 (log 2) is replayed by the reconnect catch-up while the first
	// delivery is still blocked. The cursor is already at 1 (m1), so
	// only m2 is replayed.
	h.srv.SetCatchUp(
		[]map[string]any{{"room_id": "r1"}},
		map[string][]map[string]any{
			"r1": {{
				"message_id": "m2", "log_id": 2, "room_id": "r1",
				"from":     map[string]any{"user_id": "doug", "name": "doug"},
				"body":     map[string]any{"text": "squire can you look"},
				"mentions": []string{"u9"},
			}},
		},
	)
	fc1 := h.srv.ConnAt(t, 1)
	fc1.Close()
	if err := fc1.WaitForClose(); err != nil {
		t.Fatal(err)
	}
	fc2 := h.srv.ConnAt(t, 2)
	fc2.WantToken = "tok-2" // the client re-auths with the rotated token
	errC := make(chan error, 1)
	go func() { errC <- serverHandshake(t, fc2) }()
	if err := <-errC; err != nil {
		t.Fatalf("server handshake: %v", err)
	}

	// The catch-up evaluates m2 and its flush delivers it while the
	// first delivery is still blocked: a slow delivery does not hold
	// procMu.
	waitFor(t, bd.second, "the second delivery to start while the first is blocked")

	// Release the blocked deliveries and shut down.
	close(bd.block)
	h.stop(t)
}

// --- Fix 8: the reply pre-resolve is gated (resident + non-self, not a duplicate) ---

// TestSharedModeReplyNoHistoryLookup: a shared daemon never wakes on
// replies, so a reply_to message must not trigger the History lookup.
func TestSharedModeReplyNoHistoryLookup(t *testing.T) {
	cfg := baseCfg()
	cfg.Mode = ModeShared
	h := startHarness(t, cfg)

	h.fc.Push("message", map[string]any{
		"message_id": "m1", "log_id": 1, "room_id": "r1",
		"from":     map[string]any{"user_id": "doug", "name": "doug"},
		"body":     map[string]any{"text": "reply"},
		"reply_to": map[string]any{"message_id": "m0"},
	})
	waitSettled(t, h, 0, "shared mode never wakes on replies")
	if n := h.srv.HistoryCallCount(); n != 0 {
		t.Errorf("history lookups = %d, want 0 (shared mode)", n)
	}
	h.stop(t)
}

// TestSelfPostReplyNoHistoryLookup: a self-post never wakes via reply,
// so its reply_to must not trigger the History lookup (the thread
// subscription still arms: no lookup is needed for it).
func TestSelfPostReplyNoHistoryLookup(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	h.fc.Push("message", map[string]any{
		"message_id": "m1", "log_id": 1, "room_id": "r1",
		"from":     map[string]any{"user_id": "u9", "name": "squire"},
		"body":     map[string]any{"text": "self reply"},
		"reply_to": map[string]any{"message_id": "m0"},
	})
	waitSettled(t, h, 0, "a self-post never wakes via reply")
	if n := h.srv.HistoryCallCount(); n != 0 {
		t.Errorf("history lookups = %d, want 0 (self-post)", n)
	}
	h.stop(t)
}

func TestSlackSelfIDMismatchWins(t *testing.T) {
	// Fake Web API: auth.test resolves the real identity,
	// apps.connections.open hands out the fake socket URL.
	wsURL := &atomic.Value{}
	apiMux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth.test":
			w.Write([]byte(`{"ok":true,"user_id":"real-self","bot_id":"B1"}`))
		case "/api/apps.connections.open":
			w.Write([]byte(`{"ok":true,"url":"` + wsURL.Load().(string) + `"}`))
		default:
			w.Write([]byte(`{"ok":false}`))
		}
	})
	api := httptest.NewServer(apiMux)
	defer api.Close()

	events := make(chan string, 4)
	ws := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		for {
			// A self post, pushed by the "server" side.
			select {
			case payload := <-events:
				frame := `{"type":"events_api","envelope_id":"e1","payload":{"type":"event_callback","event":` + payload + `}}`
				if err := conn.Write(context.Background(), websocket.MessageText, []byte(frame)); err != nil {
					return
				}
			case <-r.Context().Done():
				return
			}
			// Read the ack so the client does not resend.
			if _, _, err := conn.Read(context.Background()); err != nil {
				return
			}
		}
	}))
	defer ws.Close()
	wsURL.Store("ws://" + strings.TrimPrefix(ws.URL, "http://"))

	cfg := baseCfg()
	cfg.Transport = TransportSlack
	cfg.BotToken, cfg.AppToken = "xoxb-test", "xapp-test"
	// A stale configured self id must not bypass the firebreak: the
	// resolved Slack identity wins.
	cfg.Self.UserID = "stale-self"

	h := &harness{dlv: newFakeDeliverer(), clock: newFakeClock(), ticker: newTestTicker(), done: make(chan error, 1)}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	h.st = st
	t.Cleanup(func() { h.st.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	t.Cleanup(h.cancel)
	go func() {
		h.done <- Run(ctx, cfg, Deps{
			Store:        h.st,
			Deliver:      h.dlv,
			Clock:        h.clock,
			Now:          h.clock.Now,
			Ticker:       func(time.Duration) Ticker { return h.ticker },
			Wait:         func(time.Duration) {},
			SlackBaseURL: api.URL + "/api",
		})
	}()

	// A self post with the resolved id: the firebreak must drop it even
	// though the configured self id says otherwise.
	events <- `{"type":"message","channel":"C1","user":"real-self","text":"my own post","ts":"1700000000.000100"}`
	waitCursorAt(t, h.st, "C1", 1700000000000100)
	if got := h.dlv.count(); got != 0 {
		t.Errorf("deliveries = %d, want 0 (self-post firebreak uses the resolved id)", got)
	}
	h.stop(t)
	select {
	case err := <-h.done:
		t.Logf("run error: %v", err)
	default:
	}
}

func TestClientWithSlackTransportRejected(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	cfg := baseCfg()
	cfg.Transport = TransportSlack
	cfg.BotToken, cfg.AppToken = "xoxb-test", "xapp-test"
	deps := Deps{Client: &apronws.Client{}, Link: nil, Store: st, Deliver: newFakeDeliverer()}
	if err := Run(context.Background(), cfg, deps); err == nil || !strings.Contains(err.Error(), "slack transport") {
		t.Errorf("Run = %v, want the client-with-slack rejection", err)
	}
}

func TestRunReactionWake(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	// The bot posts; its own post arms the thread subscription and
	// advances the cursor without waking.
	h.fc.PushMessageAt("m1", "r1", "u9", "bot says the thing", nil, 100)
	waitSettled(t, h, 0, "bot's own post must not wake")

	// A human reacts to the bot's message: kind=reaction wakes. The
	// reaction's log id sits above waitSettled's flush line (990001).
	h.fc.Push("reactions", map[string]any{
		"log_id":     "990101",
		"message_id": "m1",
		"room_id":    "r1",
		"reactions": []any{
			map[string]any{"from": map[string]any{"user_id": "doug", "name": "Doug"}, "emojis": []string{"👍"}},
		},
	})
	d := h.dlv.wait(t)
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire", d.agent)
	}
	want := `[wake kind=reaction room=r1 from=doug message_id=m1]` + "\n👍 from doug"
	if d.env.Text != want {
		t.Errorf("envelope = %q, want %q", d.env.Text, want)
	}
	h.stop(t)
}

// TestRunReactionDedupedByCursor pushes the same reaction record
// twice: the first wakes, the second dedupes on the cursor before the
// author lookup, so a replay cannot push a History fetch onto the
// live loop (the umpire's design finding).
func TestRunReactionDedupedByCursor(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	h.fc.PushMessageAt("m1", "r1", "u9", "bot says the thing", nil, 100)
	waitSettled(t, h, 0, "bot post consumed")

	payload := map[string]any{
		"log_id":     "990101",
		"message_id": "m1",
		"room_id":    "r1",
		"reactions": []any{
			map[string]any{"from": map[string]any{"user_id": "doug"}, "emojis": []string{"👍"}},
		},
	}
	h.fc.Push("reactions", payload)
	d := h.dlv.wait(t)
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire", d.agent)
	}
	// The replay: same record, already below the cursor. waitSettled
	// proves the loop consumed it; the assertion is that the daemon
	// stays healthy and delivers nothing further.
	h.fc.Push("reactions", payload)
	waitSettled(t, h, 1, "replayed reaction must dedupe before the lookup")
	h.stop(t)
}

func TestRunReactionSelfAndPeerIgnored(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	h.fc.PushMessageAt("m1", "r1", "u9", "bot says the thing", nil, 100)
	waitSettled(t, h, 0, "bot's own post must not wake")

	// The bot's own reaction set on its own message is never a wake.
	h.fc.Push("reactions", map[string]any{
		"log_id": "990101", "message_id": "m1", "room_id": "r1",
		"reactions": []any{
			map[string]any{"from": map[string]any{"user_id": "u9"}, "emojis": []string{"👀"}},
		},
	})
	// A peer bot's reaction set is neither (the ack-storm firebreak).
	h.fc.Push("reactions", map[string]any{
		"log_id": "990102", "message_id": "m1", "room_id": "r1",
		"reactions": []any{
			map[string]any{"from": map[string]any{"user_id": "b1"}, "emojis": []string{"🤖"}},
		},
	})
	waitSettled(t, h, 0, "self and peer-bot reactions must not wake")
	h.stop(t)
}

func TestRunReactionMixedAuthorsFiltersSelfAndPeer(t *testing.T) {
	cfg := baseCfg()
	h := startHarness(t, cfg)

	h.fc.PushMessageAt("m1", "r1", "u9", "bot says the thing", nil, 100)
	waitSettled(t, h, 0, "bot's own post must not wake")

	// A record carrying the bot's set, a peer bot's set, and a human's
	// set wakes once, with the firebroken sets absent from the body.
	h.fc.Push("reactions", map[string]any{
		"log_id": "990101", "message_id": "m1", "room_id": "r1",
		"reactions": []any{
			map[string]any{"from": map[string]any{"user_id": "u9"}, "emojis": []string{"👀"}},
			map[string]any{"from": map[string]any{"user_id": "b1"}, "emojis": []string{"🤖"}},
			map[string]any{"from": map[string]any{"user_id": "doug"}, "emojis": []string{"👍"}},
		},
	})
	d := h.dlv.wait(t)
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire", d.agent)
	}
	if strings.Contains(d.env.Text, "👀") || strings.Contains(d.env.Text, "🤖") {
		t.Errorf("envelope carries a firebroken set: %q", d.env.Text)
	}
	if !strings.Contains(d.env.Text, "👍 from doug") {
		t.Errorf("envelope missing the human's set: %q", d.env.Text)
	}
	if strings.Contains(d.env.Text, "from=u9") || strings.Contains(d.env.Text, "from=b1") {
		t.Errorf("envelope header names a firebroken author: %q", d.env.Text)
	}
	h.stop(t)
}

func TestRunReactionDoesNotSuppressEarlierMessage(t *testing.T) {
	// The review's cursor-race finding: a reaction must not advance the
	// cursor past an unprocessed message. The reaction at log 101 lands
	// before the mention at log 100 was processed; the mention is older
	// than the cursor the reaction set, and it must still wake.
	cfg := baseCfg()
	h := startHarness(t, cfg)

	h.fc.Push("reactions", map[string]any{
		"log_id": "101", "message_id": "m1", "room_id": "r1",
		"reactions": []any{
			map[string]any{"from": map[string]any{"user_id": "doug"}, "emojis": []string{"👍"}},
		},
	})
	// The mention at log 100 arrives after the reaction at 101 was
	// pumped; it is older than the reaction but must still wake.
	h.fc.PushMessageAt("m0", "r1", "doug", "squire ping", []string{"u9"}, 100)
	d := h.dlv.wait(t)
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire", d.agent)
	}
	if !strings.Contains(d.env.Text, "kind=mention") || !strings.Contains(d.env.Text, "squire ping") {
		t.Errorf("envelope = %q, want the mention wake", d.env.Text)
	}
	h.stop(t)
}

func TestRunReactionDuplicateDoesNotReplayLookup(t *testing.T) {
	// The umpire's design finding: a duplicate reaction must dedupe on
	// the cursor before the author lookup, so a dead-target replay
	// cannot push a History fetch onto the live loop. The record
	// arrives twice; the author lookup must run for the first only.
	cfg := baseCfg()
	h := startHarness(t, cfg)

	h.fc.PushMessageAt("m1", "r1", "u9", "bot says the thing", nil, 100)
	waitSettled(t, h, 0, "bot post consumed")

	payload := map[string]any{
		"log_id":     "990101",
		"message_id": "m1",
		"room_id":    "r1",
		"reactions": []any{
			map[string]any{"from": map[string]any{"user_id": "doug"}, "emojis": []string{"👍"}},
		},
	}
	h.fc.Push("reactions", payload)
	d := h.dlv.wait(t)
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire", d.agent)
	}
	// The replay: same record, already below the cursor. waitSettled
	// proves the loop consumed it; the assertion is that the daemon
	// stays healthy and delivers nothing further.
	h.fc.Push("reactions", payload)
	waitSettled(t, h, 1, "replayed reaction must dedupe before the lookup")
	h.stop(t)
}

func TestCatchUpReplaysReactionDuringDowntime(t *testing.T) {
	// The umpire's catch-up finding: a reaction on the bot's message
	// that lands while the daemon is down must wake, exactly as a
	// mention in the same gap replays. The cursor is pre-seeded below
	// both records; the history response carries the reaction record.
	cfg := baseCfg()
	h := startHarnessWithSetup(t, cfg, func(h *harness) {
		if err := h.st.SetCursor(context.Background(), "r1", 100); err != nil {
			t.Fatalf("set cursor: %v", err)
		}
		h.srv.SetCatchUp(
			[]map[string]any{{"room_id": "r1"}},
			map[string][]map[string]any{
				"r1": {{
					"message_id": "m1", "log_id": 100, "room_id": "r1",
					"from": map[string]any{"user_id": "u9", "name": "squire"},
					"body": map[string]any{"text": "bot says the thing"},
				}},
			},
		)
		h.srv.ReactionsByRoom = map[string][]map[string]any{
			"r1": {{
				"log_id": 101, "message_id": "m1", "room_id": "r1",
				"reactions": []any{
					map[string]any{"from": map[string]any{"user_id": "doug"}, "emojis": []string{"👍"}},
				},
			}},
		}
	})

	d := h.dlv.wait(t)
	if d.agent != "squire" {
		t.Errorf("agent = %q, want squire", d.agent)
	}
	if !strings.Contains(d.env.Text, "kind=reaction") || !strings.Contains(d.env.Text, "👍 from doug") {
		t.Errorf("envelope = %q, want the replayed reaction wake", d.env.Text)
	}
	h.stop(t)
}

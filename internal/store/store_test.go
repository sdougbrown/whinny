package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/whinny/internal/watch"
)

var testCtx = context.Background()

var testNow = time.Unix(1_000_000, 0)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func tableNames(t *testing.T, s *Store) []string {
	t.Helper()
	rows, err := s.db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		names = append(names, n)
	}
	return names
}

func TestMigrateIdempotentAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for _, want := range []string{"watches", "wake_queue", "breaker", "cursors", "meta"} {
		found := false
		for _, n := range tableNames(t, s) {
			if n == want {
				found = true
			}
		}
		if !found {
			t.Errorf("table %s missing after Open", want)
		}
	}

	var mode string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Errorf("journal_mode = %q, want wal", mode)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: migrations re-run on the existing database without error.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if _, err := s2.GetBreaker(testCtx); err != nil {
		t.Fatalf("GetBreaker after reopen: %v", err)
	}
}

func TestOpenMergesExistingQueryParams(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db") + "?foo=bar"
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	var fk int
	if err := s.db.QueryRow(`PRAGMA foreign_keys`).Scan(&fk); err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	if fk != 1 {
		t.Errorf("foreign_keys = %d, want 1", fk)
	}
	var mode string
	if err := s.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Errorf("journal_mode = %q, want wal", mode)
	}
}

func TestWatchCRUD(t *testing.T) {
	s := openTestStore(t)

	w := Watch{RoomID: "room1", Kind: watch.KindPattern, Pattern: "hello"}
	added, err := s.AddWatch(testCtx, w, 86400, testNow)
	if err != nil {
		t.Fatalf("AddWatch: %v", err)
	}
	if added.ID == "" {
		t.Fatal("AddWatch did not assign an id")
	}
	if added.CreatedAt != testNow.Unix() {
		t.Errorf("CreatedAt = %d, want %d", added.CreatedAt, testNow.Unix())
	}
	if added.ExpiresAt != testNow.Unix()+86400 {
		t.Errorf("ExpiresAt = %d, want %d", added.ExpiresAt, testNow.Unix()+86400)
	}
	if added.Owner != "resident" {
		t.Errorf("Owner = %q, want resident (default)", added.Owner)
	}

	got, err := s.GetWatch(testCtx, added.ID)
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got.ID != added.ID || got.RoomID != added.RoomID || got.Owner != added.Owner ||
		got.Kind != added.Kind || got.Pattern != added.Pattern ||
		got.CreatedAt != added.CreatedAt || got.ExpiresAt != added.ExpiresAt {
		t.Errorf("GetWatch = %+v, want %+v", got, added)
	}
	if got.Once == nil || !*got.Once {
		t.Errorf("GetWatch Once = %v, want true (nil stored as true)", got.Once)
	}

	if _, err := s.AddWatch(testCtx, Watch{ID: "second", RoomID: "room2", Kind: watch.KindEdge, ReplyTo: "m1"}, 100, testNow); err != nil {
		t.Fatalf("AddWatch second: %v", err)
	}
	list, err := s.ListWatches(testCtx)
	if err != nil {
		t.Fatalf("ListWatches: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("ListWatches len = %d, want 2", len(list))
	}

	if err := s.DeleteWatch(testCtx, added.ID); err != nil {
		t.Fatalf("DeleteWatch: %v", err)
	}
	if _, err := s.GetWatch(testCtx, added.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetWatch after delete: err = %v, want ErrNotFound", err)
	}
}

func TestWatchOwnerColumn(t *testing.T) {
	s := openTestStore(t)

	// A custom owner round-trips through AddWatch/GetWatch.
	added, err := s.AddWatch(testCtx, Watch{ID: "owned", RoomID: "room2", Kind: watch.KindEdge, ReplyTo: "m1", Owner: "agent-x"}, 100, testNow)
	if err != nil {
		t.Fatalf("AddWatch: %v", err)
	}
	if added.Owner != "agent-x" {
		t.Errorf("AddWatch Owner = %q, want agent-x", added.Owner)
	}
	got, err := s.GetWatch(testCtx, "owned")
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got.Owner != "agent-x" {
		t.Errorf("GetWatch Owner = %q, want agent-x", got.Owner)
	}
	list, err := s.ListWatches(testCtx)
	if err != nil {
		t.Fatalf("ListWatches: %v", err)
	}
	for _, w := range list {
		if w.ID == "owned" && w.Owner != "agent-x" {
			t.Errorf("ListWatches Owner = %q, want agent-x", w.Owner)
		}
	}
}

func TestOwnerColumnMigratesLegacyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	// Create a database with the pre-owner watches schema and one row.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	_, err = raw.Exec(`CREATE TABLE watches (
		id TEXT PRIMARY KEY,
		room_id TEXT NOT NULL,
		kind TEXT NOT NULL,
		from_user TEXT,
		pattern TEXT,
		thread_id TEXT,
		reply_to TEXT,
		note TEXT,
		once INTEGER NOT NULL DEFAULT 1,
		ttl_s INTEGER NOT NULL,
		created_at INTEGER NOT NULL,
		expires_at INTEGER NOT NULL
	)`)
	if err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	_, err = raw.Exec(`INSERT INTO watches (id, room_id, kind, from_user, pattern, thread_id, reply_to, note, once, ttl_s, created_at, expires_at)
		VALUES ('legacy', 'room1', 'edge', '', '', '', 'm1', '', 1, 100, 1000000, 1000100)`)
	if err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	// Open migrates the owner column in; the legacy row takes the default.
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	w, err := s.GetWatch(testCtx, "legacy")
	if err != nil {
		t.Fatalf("GetWatch legacy: %v", err)
	}
	if w.Owner != "resident" {
		t.Errorf("legacy Owner = %q, want resident", w.Owner)
	}
	// New writes work on the migrated table.
	if _, err := s.AddWatch(testCtx, Watch{ID: "new", RoomID: "room1", Kind: watch.KindEdge, ReplyTo: "m2"}, 100, testNow); err != nil {
		t.Fatalf("AddWatch on migrated table: %v", err)
	}
}

func TestDueWatchesBoundary(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.AddWatch(testCtx, Watch{RoomID: "r", Kind: watch.KindEdge, ReplyTo: "m1"}, 100, testNow); err != nil {
		t.Fatalf("AddWatch: %v", err)
	}

	due, err := s.DueWatches(testCtx, testNow.Add(99*time.Second))
	if err != nil {
		t.Fatalf("DueWatches: %v", err)
	}
	if len(due) != 0 {
		t.Errorf("DueWatches 99s in: len = %d, want 0", len(due))
	}

	// Boundary: expires_at <= now, so exactly at the TTL the watch is due.
	due, err = s.DueWatches(testCtx, testNow.Add(100*time.Second))
	if err != nil {
		t.Fatalf("DueWatches: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("DueWatches 100s in: len = %d, want 1", len(due))
	}
	if due[0].ExpiresAt != testNow.Unix()+100 {
		t.Errorf("ExpiresAt = %d, want %d", due[0].ExpiresAt, testNow.Unix()+100)
	}
}

func TestWakeQueueBatch(t *testing.T) {
	s := openTestStore(t)
	// Distinct CreatedAt per enqueue so the FIFO assertion is a real
	// ordering check, not an autoincrement artifact.
	wakes := []struct {
		agent     string
		payload   string
		createdAt int64
	}{
		{"a", "p1", testNow.Unix()},
		{"a", "p2", testNow.Unix() + 1},
		{"b", "p3", testNow.Unix() + 2},
		{"a", "p4", testNow.Unix() + 3},
	}
	var ids []int64
	for _, w := range wakes {
		id, err := s.EnqueueWake(testCtx, Wake{Agent: w.agent, Payload: w.payload, CreatedAt: w.createdAt}, time.Unix(w.createdAt, 0))
		if err != nil {
			t.Fatalf("EnqueueWake %s: %v", w.payload, err)
		}
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("row ids not increasing: %v", ids)
		}
	}

	got, err := s.DequeueWakes(testCtx, "a")
	if err != nil {
		t.Fatalf("DequeueWakes: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("DequeueWakes len = %d, want 3", len(got))
	}
	for i, want := range []string{"p1", "p2", "p4"} {
		if got[i].Payload != want {
			t.Errorf("got[%d].Payload = %q, want %q (FIFO order)", i, got[i].Payload, want)
		}
	}
	for i, want := range []int64{testNow.Unix(), testNow.Unix() + 1, testNow.Unix() + 3} {
		if got[i].CreatedAt != want {
			t.Errorf("got[%d].CreatedAt = %d, want %d", i, got[i].CreatedAt, want)
		}
	}

	// The batch was deleted: a second dequeue is empty.
	got, err = s.DequeueWakes(testCtx, "a")
	if err != nil {
		t.Fatalf("DequeueWakes again: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("second DequeueWakes len = %d, want 0", len(got))
	}

	// Other agents' wakes are untouched.
	got, err = s.DequeueWakes(testCtx, "b")
	if err != nil {
		t.Fatalf("DequeueWakes b: %v", err)
	}
	if len(got) != 1 || got[0].Payload != "p3" {
		t.Errorf("DequeueWakes b = %+v, want one p3", got)
	}
}

func TestBreakerPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	b := Breaker{Latched: true, LatchedAt: 42, Reason: "server rate limit"}
	if err := s.SetBreaker(testCtx, b); err != nil {
		t.Fatalf("SetBreaker: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	got, err := s2.GetBreaker(testCtx)
	if err != nil {
		t.Fatalf("GetBreaker after reopen: %v", err)
	}
	if got != b {
		t.Errorf("GetBreaker = %+v, want %+v", got, b)
	}

	// Clearing goes through the same ON CONFLICT UPDATE path.
	if err := s2.SetBreaker(testCtx, Breaker{}); err != nil {
		t.Fatalf("SetBreaker clear: %v", err)
	}
	got, err = s2.GetBreaker(testCtx)
	if err != nil {
		t.Fatalf("GetBreaker after clear: %v", err)
	}
	if got != (Breaker{}) {
		t.Errorf("GetBreaker after clear = %+v, want zero (not latched)", got)
	}

	// A fresh database reports a not-latched breaker.
	fresh := openTestStore(t)
	got, err = fresh.GetBreaker(testCtx)
	if err != nil {
		t.Fatalf("GetBreaker fresh: %v", err)
	}
	if got.Latched {
		t.Errorf("fresh breaker latched = true, want false")
	}
}

func TestCursorUpsert(t *testing.T) {
	s := openTestStore(t)

	if v, err := s.GetCursor(testCtx, "room1"); err != nil || v != 0 {
		t.Fatalf("GetCursor before set = (%d, %v), want (0, nil)", v, err)
	}
	if err := s.SetCursor(testCtx, "room1", 10); err != nil {
		t.Fatalf("SetCursor: %v", err)
	}
	if v, _ := s.GetCursor(testCtx, "room1"); v != 10 {
		t.Errorf("GetCursor = %d, want 10", v)
	}
	// Upsert: same room, new value.
	if err := s.SetCursor(testCtx, "room1", 20); err != nil {
		t.Fatalf("SetCursor again: %v", err)
	}
	if v, _ := s.GetCursor(testCtx, "room1"); v != 20 {
		t.Errorf("GetCursor after upsert = %d, want 20", v)
	}
}

// TestRateStateRoundTrip: the rate-bucket state round-trips through
// SetRateState/RateState, including across a close and reopen.
func TestRateStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// A never-set store reports (0, zero, nil).
	tok, last, err := s.RateState(testCtx)
	if err != nil {
		t.Fatalf("RateState before set: %v", err)
	}
	if tok != 0 || !last.IsZero() {
		t.Fatalf("RateState before set = (%v, %v), want (0, zero)", tok, last)
	}

	last = testNow
	if err := s.SetRateState(testCtx, 1.5, last); err != nil {
		t.Fatalf("SetRateState: %v", err)
	}
	tok, gotLast, err := s.RateState(testCtx)
	if err != nil {
		t.Fatalf("RateState: %v", err)
	}
	if tok != 1.5 {
		t.Errorf("tokens = %v, want 1.5", tok)
	}
	if !gotLast.Equal(last) {
		t.Errorf("last = %v, want %v", gotLast, last)
	}

	// Upsert: same keys, new values.
	if err := s.SetRateState(testCtx, 0, testNow.Add(time.Minute)); err != nil {
		t.Fatalf("SetRateState again: %v", err)
	}
	tok, gotLast, err = s.RateState(testCtx)
	if err != nil {
		t.Fatalf("RateState after upsert: %v", err)
	}
	if tok != 0 || !gotLast.Equal(testNow.Add(time.Minute)) {
		t.Errorf("RateState after upsert = (%v, %v), want (0, +1m)", tok, gotLast)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: the state persists.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	tok, gotLast, err = s2.RateState(testCtx)
	if err != nil {
		t.Fatalf("RateState after reopen: %v", err)
	}
	if tok != 0 || !gotLast.Equal(testNow.Add(time.Minute)) {
		t.Errorf("RateState after reopen = (%v, %v), want (0, +1m)", tok, gotLast)
	}
}

// TestRateStatePartialWriteIsAtomic: a torn (one key set, the other
// not) state is never observable: both keys are written in one
// transaction.
func TestRateStatePartialWriteIsAtomic(t *testing.T) {
	s := openTestStore(t)
	if err := s.SetRateState(testCtx, 3, testNow); err != nil {
		t.Fatalf("SetRateState: %v", err)
	}
	tokStr, err := s.GetMeta(testCtx, MetaRateTokens)
	if err != nil {
		t.Fatalf("GetMeta tokens: %v", err)
	}
	lastStr, err := s.GetMeta(testCtx, MetaRateLast)
	if err != nil {
		t.Fatalf("GetMeta last: %v", err)
	}
	if tokStr == "" || lastStr == "" {
		t.Fatalf("torn state: tokens=%q last=%q, want both set", tokStr, lastStr)
	}
}

func TestMeta(t *testing.T) {
	s := openTestStore(t)

	if v, err := s.GetMeta(testCtx, "k"); err != nil || v != "" {
		t.Fatalf("GetMeta before set = (%q, %v), want (\"\", nil)", v, err)
	}
	if err := s.SetMeta(testCtx, "k", "v"); err != nil {
		t.Fatalf("SetMeta: %v", err)
	}
	if v, _ := s.GetMeta(testCtx, "k"); v != "v" {
		t.Errorf("GetMeta = %q, want v", v)
	}
	if err := s.SetMeta(testCtx, "k", "v2"); err != nil {
		t.Fatalf("SetMeta again: %v", err)
	}
	if v, _ := s.GetMeta(testCtx, "k"); v != "v2" {
		t.Errorf("GetMeta after upsert = %q, want v2", v)
	}
}

func TestWatchValidation(t *testing.T) {
	s := openTestStore(t)
	cases := []struct {
		name string
		w    Watch
		ttl  int
	}{
		{"bad kind", Watch{RoomID: "r", Kind: "nope"}, 100},
		{"missing room", Watch{Kind: watch.KindEdge, ReplyTo: "m1"}, 100},
		{"edge missing reply", Watch{RoomID: "r", Kind: watch.KindEdge}, 100},
		{"pattern empty", Watch{RoomID: "r", Kind: watch.KindPattern}, 100},
		{"bad pattern", Watch{RoomID: "r", Kind: watch.KindPattern, Pattern: "("}, 100},
		{"thread missing thread id", Watch{RoomID: "r", Kind: watch.KindThread}, 100},
		{"subscription missing thread id", Watch{RoomID: "r", Kind: watch.KindSubscription}, 100},
		{"ttl zero", Watch{RoomID: "r", Kind: watch.KindEdge, ReplyTo: "m1"}, 0},
		{"ttl negative", Watch{RoomID: "r", Kind: watch.KindEdge, ReplyTo: "m1"}, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.AddWatch(testCtx, tc.w, tc.ttl, testNow); !errors.Is(err, ErrInvalidWatch) {
				t.Errorf("AddWatch err = %v, want ErrInvalidWatch", err)
			}
		})
	}

	// A ttl beyond the cap is accepted; AddWatch caps the expiry instead.
	if _, err := s.AddWatch(testCtx, Watch{RoomID: "r", Kind: watch.KindEdge, ReplyTo: "m1"}, MaxTTLSeconds+1, testNow); err != nil {
		t.Errorf("AddWatch over cap: %v", err)
	}
}

// TestExpiresAtCappedAtMaxTTL: a future CreatedAt is honored but the
// expiry never exceeds now + MaxTTLSeconds.
func TestExpiresAtCappedAtMaxTTL(t *testing.T) {
	s := openTestStore(t)
	future := testNow.Unix() + 2*MaxTTLSeconds
	added, err := s.AddWatch(testCtx, Watch{ID: "capped", RoomID: "r", Kind: watch.KindEdge, ReplyTo: "m1", CreatedAt: future}, 86400, testNow)
	if err != nil {
		t.Fatalf("AddWatch: %v", err)
	}
	if added.CreatedAt != future {
		t.Errorf("CreatedAt = %d, want %d (honored)", added.CreatedAt, future)
	}
	if added.ExpiresAt != testNow.Unix()+MaxTTLSeconds {
		t.Errorf("ExpiresAt = %d, want cap %d", added.ExpiresAt, testNow.Unix()+MaxTTLSeconds)
	}
	got, err := s.GetWatch(testCtx, "capped")
	if err != nil {
		t.Fatalf("GetWatch: %v", err)
	}
	if got.ExpiresAt != added.ExpiresAt {
		t.Errorf("stored ExpiresAt = %d, want %d", got.ExpiresAt, added.ExpiresAt)
	}
}

// TestOnceRoundTrip: Once *bool round-trips through every read path;
// nil stores as true, an explicit false stays false.
func TestOnceRoundTrip(t *testing.T) {
	s := openTestStore(t)
	f := false
	if _, err := s.AddWatch(testCtx, Watch{ID: "once-nil", RoomID: "r", Kind: watch.KindEdge, ReplyTo: "m1"}, 100, testNow); err != nil {
		t.Fatalf("AddWatch once-nil: %v", err)
	}
	if _, err := s.AddWatch(testCtx, Watch{ID: "once-false", RoomID: "r", Kind: watch.KindEdge, ReplyTo: "m2", Once: &f}, 100, testNow); err != nil {
		t.Fatalf("AddWatch once-false: %v", err)
	}

	check := func(name string, w Watch) {
		t.Helper()
		if w.Once == nil {
			t.Errorf("%s Once = nil, want non-nil", name)
			return
		}
		want := w.ID == "once-nil"
		if *w.Once != want {
			t.Errorf("%s Once = %v, want %v", name, *w.Once, want)
		}
	}

	got, err := s.GetWatch(testCtx, "once-nil")
	if err != nil {
		t.Fatalf("GetWatch once-nil: %v", err)
	}
	check("GetWatch once-nil", got)
	got, err = s.GetWatch(testCtx, "once-false")
	if err != nil {
		t.Fatalf("GetWatch once-false: %v", err)
	}
	check("GetWatch once-false", got)

	list, err := s.ListWatches(testCtx)
	if err != nil {
		t.Fatalf("ListWatches: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("ListWatches len = %d, want 2", len(list))
	}
	for _, w := range list {
		check("ListWatches "+w.ID, w)
	}

	due, err := s.DueWatches(testCtx, testNow.Add(100*time.Second))
	if err != nil {
		t.Fatalf("DueWatches: %v", err)
	}
	if len(due) != 2 {
		t.Fatalf("DueWatches len = %d, want 2", len(due))
	}
	for _, w := range due {
		check("DueWatches "+w.ID, w)
	}
}

// TestTakeRateTokenParallel: N parallel TakeRateToken calls against one
// store with perMinute=2 admit exactly 2 (the atomic take serializes
// the read-modify-write, so the others see an empty bucket).
func TestTakeRateTokenParallel(t *testing.T) {
	s := openTestStore(t)
	const n = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	okCount := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, err := s.TakeRateToken(testCtx, 2, testNow)
			if err != nil {
				t.Errorf("TakeRateToken: %v", err)
				return
			}
			if ok {
				mu.Lock()
				okCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if okCount != 2 {
		t.Errorf("okCount = %d, want 2 (perMinute=2)", okCount)
	}
	// A later call (after a refill period) admits again: the state was
	// persisted.
	later := testNow.Add(time.Minute)
	ok, _, err := s.TakeRateToken(testCtx, 2, later)
	if err != nil {
		t.Fatalf("TakeRateToken later: %v", err)
	}
	if !ok {
		t.Error("TakeRateToken later = false, want true (refilled)")
	}
}

// TestTakeRateTokenRefill: a drained bucket refills over time and the
// retry-after reflects the wait to the next token.
func TestTakeRateTokenRefill(t *testing.T) {
	s := openTestStore(t)
	// Drain the 2/min bucket.
	for i := 0; i < 2; i++ {
		if ok, _, err := s.TakeRateToken(testCtx, 2, testNow); err != nil || !ok {
			t.Fatalf("TakeRateToken %d: (%v, %v), want (true, nil)", i+1, ok, err)
		}
	}
	// Empty: denied, with a 30s retry-after (1 token per 30s).
	if ok, wait, err := s.TakeRateToken(testCtx, 2, testNow); err != nil || ok || wait != 30*time.Second {
		t.Errorf("TakeRateToken empty = (%v, %v, %v), want (false, 30s, nil)", ok, wait, err)
	}
	// Half a refill period in: still denied, 15s to the next.
	if ok, wait, _ := s.TakeRateToken(testCtx, 2, testNow.Add(15*time.Second)); ok || wait != 15*time.Second {
		t.Errorf("TakeRateToken +15s = (%v, %v), want (false, 15s)", ok, wait)
	}
	// A full refill period in: admitted again.
	if ok, _, err := s.TakeRateToken(testCtx, 2, testNow.Add(time.Minute)); err != nil || !ok {
		t.Errorf("TakeRateToken +1m = (%v, %v), want (true, nil)", ok, err)
	}
}

// TestTakeRateTokenPersistenceError: a persistence error denies (fail
// closed) with a 1s retry-after; a send is never admitted on a storage
// failure.
func TestTakeRateTokenPersistenceError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ok, retryAfter, err := s.TakeRateToken(testCtx, 2, testNow)
	if err == nil {
		t.Error("TakeRateToken with a closed DB: err = nil, want an error (fail closed)")
	}
	if ok {
		t.Error("TakeRateToken with a closed DB: ok = true, want false (fail closed)")
	}
	if retryAfter != time.Second {
		t.Errorf("retryAfter = %v, want 1s", retryAfter)
	}
}

// TestTakeRateTokenNonPositivePerMinute: a non-positive perMinute denies
// (fail closed).
func TestTakeRateTokenNonPositivePerMinute(t *testing.T) {
	s := openTestStore(t)
	for _, pm := range []int{0, -1} {
		if ok, _, err := s.TakeRateToken(testCtx, pm, testNow); err != nil || ok {
			t.Errorf("TakeRateToken perMinute=%d = (%v, %v), want (false, nil)", pm, ok, err)
		}
	}
}

// TestBusyTimeoutSet: the DSN sets a 5s busy timeout, so a concurrent
// writer waits for the lock instead of failing with SQLITE_BUSY.
func TestBusyTimeoutSet(t *testing.T) {
	s := openTestStore(t)
	var timeout int
	if err := s.db.QueryRow("PRAGMA busy_timeout").Scan(&timeout); err != nil {
		t.Fatalf("PRAGMA busy_timeout: %v", err)
	}
	if timeout != 5000 {
		t.Errorf("busy_timeout = %d, want 5000", timeout)
	}
}

// TestBusyTimeoutConcurrentWrites: two stores on one DB do concurrent
// writes; the busy_timeout makes the second wait (not fail) for the
// lock, and both succeed.
func TestBusyTimeoutConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open s1: %v", err)
	}
	defer s1.Close()
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open s2: %v", err)
	}
	defer s2.Close()

	// s1 holds a write transaction (the write lock); s2's write waits
	// (busy_timeout) instead of failing with SQLITE_BUSY.
	release := make(chan struct{})
	held := make(chan struct{})
	go func() {
		tx, err := s1.db.Begin()
		if err != nil {
			t.Errorf("s1 Begin: %v", err)
			return
		}
		if _, err := tx.Exec(`INSERT INTO meta (key, value) VALUES ('k1', 'v1')`); err != nil {
			t.Errorf("s1 write: %v", err)
		}
		close(held)
		<-release
		if err := tx.Commit(); err != nil {
			t.Errorf("s1 Commit: %v", err)
		}
	}()
	<-held
	errCh := make(chan error, 1)
	go func() {
		_, err := s2.db.Exec(`INSERT INTO meta (key, value) VALUES ('k2', 'v2')
			ON CONFLICT(key) DO UPDATE SET value = excluded.value`)
		errCh <- err
	}()
	// Give s2's write a moment to block on the lock, then release s1.
	time.Sleep(50 * time.Millisecond)
	close(release)
	if err := <-errCh; err != nil {
		t.Errorf("s2 concurrent write = %v, want nil (waited, not SQLITE_BUSY)", err)
	}
}

// TestRateStatePreservesSubSecondPrecision: the last-refill instant is
// persisted with nanosecond precision (the legacy unix-seconds shape
// refunded up to ~1s of refill per process).
func TestRateStatePreservesSubSecondPrecision(t *testing.T) {
	s := openTestStore(t)
	last := time.Unix(1_000_000, 123456789) // 123ms into the second
	if err := s.SetRateState(testCtx, 1.5, last); err != nil {
		t.Fatalf("SetRateState: %v", err)
	}
	_, gotLast, err := s.RateState(testCtx)
	if err != nil {
		t.Fatalf("RateState: %v", err)
	}
	if !gotLast.Equal(last) {
		t.Errorf("last = %v, want %v (sub-second precision lost)", gotLast, last)
	}
}

// TestRateStateLegacySeconds: a legacy unix-seconds last-refill instant
// is read back correctly (the magnitude heuristic).
func TestRateStateLegacySeconds(t *testing.T) {
	s := openTestStore(t)
	// Write a legacy unix-seconds value directly.
	if err := s.SetMeta(testCtx, MetaRateTokens, "1"); err != nil {
		t.Fatalf("SetMeta tokens: %v", err)
	}
	if err := s.SetMeta(testCtx, MetaRateLast, "1000000"); err != nil {
		t.Fatalf("SetMeta last: %v", err)
	}
	_, gotLast, err := s.RateState(testCtx)
	if err != nil {
		t.Fatalf("RateState: %v", err)
	}
	want := time.Unix(1_000_000, 0)
	if !gotLast.Equal(want) {
		t.Errorf("last = %v, want %v (legacy seconds)", gotLast, want)
	}
}

// TestWatchesForRoom: the room-filtered, unexpired watch list excludes
// other rooms and already-expired watches.
func TestWatchesForRoom(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.AddWatch(testCtx, Watch{ID: "in-room", RoomID: "room1", Kind: watch.KindPattern, Pattern: "x"}, 3600, testNow); err != nil {
		t.Fatalf("AddWatch in-room: %v", err)
	}
	if _, err := s.AddWatch(testCtx, Watch{ID: "other-room", RoomID: "room2", Kind: watch.KindPattern, Pattern: "y"}, 3600, testNow); err != nil {
		t.Fatalf("AddWatch other-room: %v", err)
	}
	// An expired watch in room1: created 200s ago with a 100s ttl, so
	// it expired 100s ago.
	expiredCreated := testNow.Add(-200 * time.Second)
	if _, err := s.AddWatch(testCtx, Watch{ID: "expired", RoomID: "room1", Kind: watch.KindPattern, Pattern: "z", CreatedAt: expiredCreated.Unix()}, 100, testNow); err != nil {
		t.Fatalf("AddWatch expired: %v", err)
	}

	ws, err := s.WatchesForRoom(testCtx, "room1", testNow)
	if err != nil {
		t.Fatalf("WatchesForRoom: %v", err)
	}
	if len(ws) != 1 {
		t.Fatalf("WatchesForRoom len = %d, want 1; got %+v", len(ws), ws)
	}
	if ws[0].ID != "in-room" {
		t.Errorf("WatchesForRoom[0].ID = %q, want in-room (other-room and expired excluded)", ws[0].ID)
	}
}

// TestSetCursorMaxUpsert: the cursor moves only forward; a lower id is
// ignored, a higher id advances.
func TestSetCursorMaxUpsert(t *testing.T) {
	s := openTestStore(t)
	if err := s.SetCursor(testCtx, "room1", 10); err != nil {
		t.Fatalf("SetCursor 10: %v", err)
	}
	if v, _ := s.GetCursor(testCtx, "room1"); v != 10 {
		t.Fatalf("GetCursor = %d, want 10", v)
	}
	// A lower id is ignored (never moves backward).
	if err := s.SetCursor(testCtx, "room1", 5); err != nil {
		t.Fatalf("SetCursor 5: %v", err)
	}
	if v, _ := s.GetCursor(testCtx, "room1"); v != 10 {
		t.Errorf("GetCursor after lower set = %d, want 10 (never moves backward)", v)
	}
	// A higher id advances.
	if err := s.SetCursor(testCtx, "room1", 20); err != nil {
		t.Fatalf("SetCursor 20: %v", err)
	}
	if v, _ := s.GetCursor(testCtx, "room1"); v != 20 {
		t.Errorf("GetCursor after higher set = %d, want 20", v)
	}
}

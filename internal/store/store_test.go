package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
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

func TestWatchCRUD(t *testing.T) {
	s := openTestStore(t)

	w := Watch{RoomID: "room1", Kind: WatchKindPattern, Pattern: "hello", TTLS: 86400}
	added, err := s.AddWatch(testCtx, w, testNow)
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
	if got != added {
		t.Errorf("GetWatch = %+v, want %+v", got, added)
	}

	if _, err := s.AddWatch(testCtx, Watch{ID: "second", RoomID: "room2", Kind: WatchKindEdge, ReplyTo: "m1", TTLS: 100}, testNow); err != nil {
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
	added, err := s.AddWatch(testCtx, Watch{ID: "owned", RoomID: "room2", Kind: WatchKindEdge, ReplyTo: "m1", Owner: "agent-x", TTLS: 100}, testNow)
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
	if _, err := s.AddWatch(testCtx, Watch{ID: "new", RoomID: "room1", Kind: WatchKindEdge, ReplyTo: "m2", TTLS: 100}, testNow); err != nil {
		t.Fatalf("AddWatch on migrated table: %v", err)
	}
}

func TestDueWatchesBoundary(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.AddWatch(testCtx, Watch{RoomID: "r", Kind: WatchKindEdge, ReplyTo: "m1", TTLS: 100}, testNow); err != nil {
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
	for _, w := range []Wake{
		{Agent: "a", Payload: "p1"},
		{Agent: "a", Payload: "p2"},
		{Agent: "b", Payload: "p3"},
		{Agent: "a", Payload: "p4"},
	} {
		if err := s.EnqueueWake(testCtx, w, testNow); err != nil {
			t.Fatalf("EnqueueWake %s: %v", w.Payload, err)
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
	for _, w := range got {
		if w.CreatedAt != testNow.Unix() {
			t.Errorf("CreatedAt = %d, want %d", w.CreatedAt, testNow.Unix())
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
		want error
	}{
		{"bad kind", Watch{Kind: "nope", TTLS: 100}, ErrInvalidKind},
		{"ttl zero", Watch{Kind: WatchKindEdge, TTLS: 0}, ErrInvalidTTL},
		{"ttl negative", Watch{Kind: WatchKindEdge, TTLS: -1}, ErrInvalidTTL},
		{"ttl over cap", Watch{Kind: WatchKindEdge, TTLS: MaxTTLSeconds + 1}, ErrInvalidTTL},
		{"bad pattern", Watch{Kind: WatchKindPattern, Pattern: "(", TTLS: 100}, ErrInvalidPattern},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.AddWatch(testCtx, tc.w, testNow); !errors.Is(err, tc.want) {
				t.Errorf("AddWatch err = %v, want %v", err, tc.want)
			}
		})
	}

	// Boundary: exactly the cap is accepted.
	if _, err := s.AddWatch(testCtx, Watch{Kind: WatchKindEdge, TTLS: MaxTTLSeconds}, testNow); err != nil {
		t.Errorf("AddWatch at cap: %v", err)
	}
}

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/sdougbrown/whinny/internal/watch"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned by GetWatch when the id does not exist. GetCursor
// and GetMeta instead return a zero value with a nil error for missing keys.
var ErrNotFound = errors.New("store: not found")

// Store is an open whinny state database.
//
// The daemon is a single writer: all write methods are serialized by one
// mutex, so writes never contend even if called from multiple goroutines.
// Reads may run concurrently.
type Store struct {
	db *sql.DB
	mu sync.Mutex
}

// Open opens (creating if needed) the SQLite database at path, runs the
// schema migrations, and enables WAL mode, foreign keys, and a busy
// timeout on every connection. The busy timeout (5s) makes a concurrent
// writer wait for the database lock instead of failing with SQLITE_BUSY,
// which the atomic rate take and the daemon's single-writer model rely
// on. The default parameters are merged into any query string the path
// already carries.
func Open(path string) (*Store, error) {
	dsn := path
	if strings.Contains(path, "?") {
		dsn = path + "&_foreign_keys=on&_journal_mode=WAL&_busy_timeout=5000"
	} else {
		dsn = path + "?_foreign_keys=on&_journal_mode=WAL&_busy_timeout=5000"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// schema is the full migration set. Every statement is idempotent, so
// re-running it on an existing database is a no-op.
const schema = `
CREATE TABLE IF NOT EXISTS watches (
  id TEXT PRIMARY KEY,
  room_id TEXT NOT NULL,
  owner TEXT NOT NULL DEFAULT '` + watch.DefaultOwner + `',
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
);
CREATE INDEX IF NOT EXISTS idx_watches_expires_at ON watches (expires_at);
CREATE TABLE IF NOT EXISTS wake_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  agent TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wake_queue_agent ON wake_queue (agent);
CREATE TABLE IF NOT EXISTS breaker (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  latched INTEGER NOT NULL DEFAULT 0,
  latched_at INTEGER,
  reason TEXT
);
CREATE TABLE IF NOT EXISTS cursors (
  room_id TEXT PRIMARY KEY,
  last_log_id INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return err
	}
	return s.ensureWatchOwner(ctx)
}

// ensureWatchOwner adds the owner column to the watches table when it is
// missing (databases created before the column existed). The column is
// TEXT NOT NULL DEFAULT watch.DefaultOwner.
func (s *Store) ensureWatchOwner(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(watches)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	hasOwner := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == "owner" {
			hasOwner = true
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if hasOwner {
		return nil
	}
	_, err = s.db.ExecContext(ctx, `ALTER TABLE watches ADD COLUMN owner TEXT NOT NULL DEFAULT '`+watch.DefaultOwner+`'`)
	return err
}

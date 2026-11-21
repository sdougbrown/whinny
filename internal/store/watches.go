package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Watch kinds.
const (
	WatchKindEdge         = "edge"
	WatchKindPattern      = "pattern"
	WatchKindThread       = "thread"
	WatchKindSubscription = "subscription"
)

// TTL bounds, in seconds.
const (
	// DefaultTTLSeconds is the default watch TTL (24h).
	DefaultTTLSeconds = 86400
	// MaxTTLSeconds is the hard cap on a watch TTL (7d).
	MaxTTLSeconds = 604800
)

// Sentinel errors for watch validation at the store boundary.
var (
	// ErrInvalidKind is returned when the watch kind is not one of the
	// known kinds.
	ErrInvalidKind = errors.New("store: invalid watch kind")
	// ErrInvalidTTL is returned when ttl_s is not in (0, MaxTTLSeconds].
	ErrInvalidTTL = errors.New("store: invalid ttl_s")
	// ErrInvalidPattern is returned when the pattern does not compile as a
	// Go regexp.
	ErrInvalidPattern = errors.New("store: invalid pattern")
)

// Watch is one row of the watches table.
type Watch struct {
	ID        string
	RoomID    string
	Kind      string
	FromUser  string
	Pattern   string
	ThreadID  string
	ReplyTo   string
	Note      string
	Once      bool
	TTLS      int
	CreatedAt int64 // unix seconds
	ExpiresAt int64 // unix seconds
}

// Validate checks the watch's kind, TTL, and pattern.
func (w Watch) Validate() error {
	switch w.Kind {
	case WatchKindEdge, WatchKindPattern, WatchKindThread, WatchKindSubscription:
	default:
		return fmt.Errorf("%w: %q", ErrInvalidKind, w.Kind)
	}
	if w.TTLS <= 0 || w.TTLS > MaxTTLSeconds {
		return fmt.Errorf("%w: %d", ErrInvalidTTL, w.TTLS)
	}
	if _, err := regexp.Compile(w.Pattern); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPattern, err)
	}
	return nil
}

// AddWatch inserts a watch, validating it at the boundary. If w.ID is empty
// a daemon-generated id (crypto/rand hex) is assigned; if w.CreatedAt is
// zero it is set from now. ExpiresAt is always derived as CreatedAt + TTLS.
// The stored watch is returned.
func (s *Store) AddWatch(ctx context.Context, w Watch, now time.Time) (Watch, error) {
	if err := w.Validate(); err != nil {
		return Watch{}, err
	}
	if w.ID == "" {
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			return Watch{}, fmt.Errorf("store: generate watch id: %w", err)
		}
		w.ID = hex.EncodeToString(b)
	}
	if w.CreatedAt == 0 {
		w.CreatedAt = now.Unix()
	}
	w.ExpiresAt = w.CreatedAt + int64(w.TTLS)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO watches (id, room_id, kind, from_user, pattern, thread_id, reply_to, note, once, ttl_s, created_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		w.ID, w.RoomID, w.Kind, w.FromUser, w.Pattern, w.ThreadID, w.ReplyTo, w.Note, w.Once, w.TTLS, w.CreatedAt, w.ExpiresAt)
	return w, err
}

// GetWatch returns the watch with id, or ErrNotFound.
func (s *Store) GetWatch(ctx context.Context, id string) (Watch, error) {
	var w Watch
	var once int
	err := s.db.QueryRowContext(ctx,
		`SELECT id, room_id, kind, from_user, pattern, thread_id, reply_to, note, once, ttl_s, created_at, expires_at
		 FROM watches WHERE id = ?`, id).
		Scan(&w.ID, &w.RoomID, &w.Kind, &w.FromUser, &w.Pattern, &w.ThreadID, &w.ReplyTo, &w.Note, &once, &w.TTLS, &w.CreatedAt, &w.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Watch{}, ErrNotFound
	}
	if err != nil {
		return Watch{}, err
	}
	w.Once = once == 1
	return w, nil
}

// DeleteWatch removes the watch with id. Deleting a missing id is a no-op.
func (s *Store) DeleteWatch(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `DELETE FROM watches WHERE id = ?`, id)
	return err
}

// ListWatches returns all watches, oldest first.
func (s *Store) ListWatches(ctx context.Context) ([]Watch, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, room_id, kind, from_user, pattern, thread_id, reply_to, note, once, ttl_s, created_at, expires_at
		 FROM watches ORDER BY created_at, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Watch
	for rows.Next() {
		w, err := scanWatch(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// DueWatches returns all watches with expires_at <= now.Unix(), oldest
// first.
func (s *Store) DueWatches(ctx context.Context, now time.Time) ([]Watch, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, room_id, kind, from_user, pattern, thread_id, reply_to, note, once, ttl_s, created_at, expires_at
		 FROM watches WHERE expires_at <= ? ORDER BY expires_at, id`, now.Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Watch
	for rows.Next() {
		w, err := scanWatch(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanWatch(row scanner) (Watch, error) {
	var w Watch
	var once int
	err := row.Scan(&w.ID, &w.RoomID, &w.Kind, &w.FromUser, &w.Pattern, &w.ThreadID, &w.ReplyTo, &w.Note, &once, &w.TTLS, &w.CreatedAt, &w.ExpiresAt)
	if err != nil {
		return Watch{}, err
	}
	w.Once = once == 1
	return w, nil
}

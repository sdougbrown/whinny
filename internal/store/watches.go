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

	"github.com/sdougbrown/whinny/internal/watch"
)

// Watch is the canonical watch row, shared with the wake package.
type Watch = watch.Watch

// TTL bounds, in seconds.
const (
	// DefaultTTLSeconds is the default watch TTL (24h).
	DefaultTTLSeconds = 86400
	// MaxTTLSeconds is the hard cap on a watch TTL (7d).
	MaxTTLSeconds = 604800
)

// ErrInvalidWatch is returned by Validate and AddWatch when the watch is
// not well-formed; the error carries a detail naming the problem.
var ErrInvalidWatch = errors.New("store: invalid watch")

// Validate checks the watch's room, kind, and kind-specific fields:
// edge requires ReplyTo, pattern requires a non-empty compiling
// Pattern, thread and subscription require ThreadID.
func Validate(w Watch) error {
	if w.RoomID == "" {
		return fmt.Errorf("%w: room_id required", ErrInvalidWatch)
	}
	switch w.Kind {
	case watch.KindEdge:
		if w.ReplyTo == "" {
			return fmt.Errorf("%w: edge requires reply_to", ErrInvalidWatch)
		}
	case watch.KindPattern:
		if w.Pattern == "" {
			return fmt.Errorf("%w: pattern requires a non-empty pattern", ErrInvalidWatch)
		}
		if _, err := regexp.Compile(w.Pattern); err != nil {
			return fmt.Errorf("%w: pattern does not compile: %v", ErrInvalidWatch, err)
		}
	case watch.KindThread, watch.KindSubscription:
		if w.ThreadID == "" {
			return fmt.Errorf("%w: %s requires thread_id", ErrInvalidWatch, w.Kind)
		}
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInvalidWatch, w.Kind)
	}
	return nil
}

// AddWatch inserts a watch, validating it at the boundary. If w.ID is
// empty a daemon-generated id (crypto/rand hex) is assigned; if
// w.CreatedAt is zero it is set from now. ExpiresAt is CreatedAt + ttl,
// capped at now + MaxTTLSeconds: a future CreatedAt is honored but never
// yields an expiry beyond the cap. The stored watch is returned.
func (s *Store) AddWatch(ctx context.Context, w Watch, ttl int, now time.Time) (Watch, error) {
	if ttl <= 0 {
		return Watch{}, fmt.Errorf("%w: ttl_s must be > 0", ErrInvalidWatch)
	}
	if err := Validate(w); err != nil {
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
	if w.Owner == "" {
		w.Owner = "resident"
	}
	w.ExpiresAt = w.CreatedAt + int64(ttl)
	if cap := now.Unix() + MaxTTLSeconds; w.ExpiresAt > cap {
		w.ExpiresAt = cap
	}
	once := 0
	if w.Once == nil || *w.Once {
		once = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO watches (id, room_id, owner, kind, from_user, pattern, thread_id, reply_to, note, once, ttl_s, created_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		w.ID, w.RoomID, w.Owner, w.Kind, w.FromUser, w.Pattern, w.ThreadID, w.ReplyTo, w.Note, once, ttl, w.CreatedAt, w.ExpiresAt)
	return w, err
}

// GetWatch returns the watch with id, or ErrNotFound.
func (s *Store) GetWatch(ctx context.Context, id string) (Watch, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, room_id, owner, kind, from_user, pattern, thread_id, reply_to, note, once, ttl_s, created_at, expires_at
		 FROM watches WHERE id = ?`, id)
	w, err := scanWatch(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Watch{}, ErrNotFound
	}
	if err != nil {
		return Watch{}, err
	}
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
		`SELECT id, room_id, owner, kind, from_user, pattern, thread_id, reply_to, note, once, ttl_s, created_at, expires_at
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
		`SELECT id, room_id, owner, kind, from_user, pattern, thread_id, reply_to, note, once, ttl_s, created_at, expires_at
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

// WatchesForRoom returns the room's unexpired watches (expires_at >
// now.Unix()), oldest first. Already-expired watches are excluded so a
// stale watch cannot fire on a message that arrives after its TTL.
func (s *Store) WatchesForRoom(ctx context.Context, roomID string, now time.Time) ([]Watch, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, room_id, owner, kind, from_user, pattern, thread_id, reply_to, note, once, ttl_s, created_at, expires_at
		 FROM watches WHERE room_id = ? AND expires_at > ? ORDER BY created_at, id`, roomID, now.Unix())
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

// scanWatch reads one row. The ttl_s column is read but not retained:
// the shared Watch type carries no TTL field.
func scanWatch(row scanner) (Watch, error) {
	var w Watch
	var once, ttl int
	err := row.Scan(&w.ID, &w.RoomID, &w.Owner, &w.Kind, &w.FromUser, &w.Pattern, &w.ThreadID, &w.ReplyTo, &w.Note, &once, &ttl, &w.CreatedAt, &w.ExpiresAt)
	if err != nil {
		return Watch{}, err
	}
	b := once == 1
	w.Once = &b
	return w, nil
}

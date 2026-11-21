package store

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
)

// Breaker is the send-path breaker latch.
type Breaker struct {
	Latched   bool
	LatchedAt int64 // unix seconds; 0 when not latched
	Reason    string
}

// MetaPostFailures is the meta key for the consecutive post-failure
// count: the bad send outcomes (rate-limit denials and failed posts)
// that trip the send-path breaker.
const MetaPostFailures = "post_failures"

// SetBreaker upserts the breaker row (id = 1).
func (s *Store) SetBreaker(ctx context.Context, b Breaker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	latched := 0
	if b.Latched {
		latched = 1
	}
	var latchedAt sql.NullInt64
	if b.LatchedAt != 0 {
		latchedAt = sql.NullInt64{Int64: b.LatchedAt, Valid: true}
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO breaker (id, latched, latched_at, reason) VALUES (1, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET latched = excluded.latched, latched_at = excluded.latched_at, reason = excluded.reason`,
		latched, latchedAt, b.Reason)
	return err
}

// GetBreaker returns the breaker state. A database that has never had the
// breaker set returns a zero (not-latched) Breaker with a nil error.
func (s *Store) GetBreaker(ctx context.Context) (Breaker, error) {
	var (
		b       Breaker
		latched int
		at      sql.NullInt64
		reason  sql.NullString
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT latched, latched_at, reason FROM breaker WHERE id = 1`).
		Scan(&latched, &at, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return Breaker{}, nil
	}
	if err != nil {
		return Breaker{}, err
	}
	b.Latched = latched == 1
	b.LatchedAt = at.Int64
	b.Reason = reason.String
	return b, nil
}

// BumpPostFailures increments the consecutive post-failure count by
// one and returns the new count. The increment is one upsert
// (`value = value + 1`), so concurrent bumpers under the store's
// write mutex — and cross-process writers under the database lock —
// never lose an increment.
func (s *Store) BumpPostFailures(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO meta (key, value) VALUES (?, '1')
		 ON CONFLICT(key) DO UPDATE SET value = CAST(value AS INTEGER) + 1`,
		MetaPostFailures); err != nil {
		return 0, err
	}
	var v string
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, MetaPostFailures).Scan(&v); err != nil {
		return 0, err
	}
	return strconv.Atoi(v)
}

// ResetPostFailures sets the consecutive post-failure count to 0.
func (s *Store) ResetPostFailures(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO meta (key, value) VALUES (?, '0')
		 ON CONFLICT(key) DO UPDATE SET value = '0'`,
		MetaPostFailures)
	return err
}

// ClearBreaker releases the breaker latch and resets the consecutive
// post-failure counter in one transaction: the two writes are atomic,
// so a failure between them can never leave a cleared latch with a
// live counter (or a latched breaker with a stale count). A retried
// clear is idempotent.
func (s *Store) ClearBreaker(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO breaker (id, latched, latched_at, reason) VALUES (1, 0, NULL, '')
		 ON CONFLICT(id) DO UPDATE SET latched = 0, latched_at = NULL, reason = ''`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO meta (key, value) VALUES (?, '0')
		 ON CONFLICT(key) DO UPDATE SET value = '0'`,
		MetaPostFailures); err != nil {
		return err
	}
	return tx.Commit()
}

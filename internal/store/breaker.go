package store

import (
	"context"
	"database/sql"
	"errors"
)

// Breaker is the send-path breaker latch.
type Breaker struct {
	Latched   bool
	LatchedAt int64 // unix seconds; 0 when not latched
	Reason    string
}

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

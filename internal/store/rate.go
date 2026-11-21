package store

import (
	"context"
	"strconv"
	"time"
)

// Rate-bucket meta keys. The token count and the last-refill instant
// are stored as separate meta rows so the pair persists across the
// separate post and daemon processes.
const (
	// MetaRateTokens is the meta key for the bucket's token count.
	MetaRateTokens = "rate_tokens"
	// MetaRateLast is the meta key for the bucket's last-refill
	// instant (unix seconds).
	MetaRateLast = "rate_last"
)

// RateState returns the persisted rate-bucket state: the token count
// and the last-refill instant. A store that has never had the state
// set returns (0, a zero time, nil).
func (s *Store) RateState(ctx context.Context) (float64, time.Time, error) {
	tokStr, err := s.GetMeta(ctx, MetaRateTokens)
	if err != nil {
		return 0, time.Time{}, err
	}
	lastStr, err := s.GetMeta(ctx, MetaRateLast)
	if err != nil {
		return 0, time.Time{}, err
	}
	var tokens float64
	var last time.Time
	if tokStr != "" {
		tokens, err = strconv.ParseFloat(tokStr, 64)
		if err != nil {
			return 0, time.Time{}, err
		}
	}
	if lastStr != "" {
		sec, err := strconv.ParseInt(lastStr, 10, 64)
		if err != nil {
			return 0, time.Time{}, err
		}
		last = time.Unix(sec, 0)
	}
	return tokens, last, nil
}

// SetRateState persists the rate-bucket state atomically: both meta
// rows are written in one transaction so a torn (one set, one not)
// state can never be observed by a reader.
func (s *Store) SetRateState(ctx context.Context, tokens float64, last time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	const upsert = `INSERT INTO meta (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`
	if _, err := tx.ExecContext(ctx, upsert, MetaRateTokens, strconv.FormatFloat(tokens, 'f', -1, 64)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, upsert, MetaRateLast, strconv.FormatInt(last.Unix(), 10)); err != nil {
		return err
	}
	return tx.Commit()
}

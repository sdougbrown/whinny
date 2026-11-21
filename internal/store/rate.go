package store

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/sdougbrown/whinny/internal/rate"
)

// Rate-bucket meta keys. The token count and the last-refill instant
// are stored as separate meta rows so the pair persists across the
// separate post and daemon processes.
const (
	// MetaRateTokens is the meta key for the bucket's token count.
	MetaRateTokens = "rate_tokens"
	// MetaRateLast is the meta key for the bucket's last-refill
	// instant (unix nanos; legacy values below the boundary are unix
	// seconds).
	MetaRateLast = "rate_last"
)

// rateLastNanoBoundary separates a legacy unix-seconds last-refill
// instant from a unix-nanos one: current dates are ~1.8e9 seconds and
// ~1.8e18 nanos, so 1e15 sits safely between them.
const rateLastNanoBoundary = int64(1e15)

// parseRateLast decodes a persisted last-refill instant. Values below
// the boundary are legacy unix seconds; the rest are unix nanos.
func parseRateLast(v int64) time.Time {
	if v < rateLastNanoBoundary {
		return time.Unix(v, 0)
	}
	return time.Unix(0, v)
}

// decodeRateState decodes the persisted rate-bucket pair: the token
// count and the last-refill instant. An empty string is an unset
// value (zero).
func decodeRateState(tokStr, lastStr string) (rate.State, error) {
	var state rate.State
	if tokStr != "" {
		tokens, err := strconv.ParseFloat(tokStr, 64)
		if err != nil {
			return rate.State{}, err
		}
		state.Tokens = tokens
	}
	if lastStr != "" {
		nano, err := strconv.ParseInt(lastStr, 10, 64)
		if err != nil {
			return rate.State{}, err
		}
		state.Last = parseRateLast(nano)
	}
	return state, nil
}

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
	state, err := decodeRateState(tokStr, lastStr)
	if err != nil {
		return 0, time.Time{}, err
	}
	return state.Tokens, state.Last, nil
}

// SetRateState persists the rate-bucket state atomically: both meta
// rows are written in one transaction so a torn (one set, one not)
// state can never be observed by a reader. The last-refill instant is
// stored as unix nanos.
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
	if _, err := tx.ExecContext(ctx, upsert, MetaRateLast, strconv.FormatInt(last.UnixNano(), 10)); err != nil {
		return err
	}
	return tx.Commit()
}

// TakeRateToken refills and (if a token is available) takes one token
// from the persisted rate bucket, in a single BEGIN IMMEDIATE
// transaction. The read, the refill, the take, and the write-back are
// one transaction, so N concurrent callers against one store can never
// all pass a perMinute=1 limit: exactly perMinute of them succeed per
// refill period. On a denial nothing is consumed and nothing is written
// back (the persisted last-refill instant stays, so the next call
// refills from it). A non-positive perMinute denies (fail closed). A
// persistence error denies with a 1s retry-after (fail closed): a
// storage failure must never admit a send it could not record.
func (s *Store) TakeRateToken(ctx context.Context, perMinute int, now time.Time) (bool, time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if perMinute <= 0 {
		return false, 0, nil
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, time.Second, err
	}
	defer conn.Close()
	// BEGIN IMMEDIATE takes the write lock up front, so the whole
	// read-modify-write is serialized against concurrent takes (in- and
	// cross-process); a deferred BEGIN would let two takes read the same
	// state before either writes.
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return false, time.Second, err
	}
	done := false
	defer func() {
		if !done {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	var tokStr, lastStr string
	if err := conn.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, MetaRateTokens).Scan(&tokStr); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, time.Second, err
	}
	if err := conn.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, MetaRateLast).Scan(&lastStr); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, time.Second, err
	}
	state, err := decodeRateState(tokStr, lastStr)
	if err != nil {
		return false, time.Second, err
	}
	state, ok := rate.Refill(state, perMinute, now)
	if !ok {
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return false, time.Second, err
		}
		done = true
		return false, 0, nil
	}
	admitted, wait := rate.Available(state, perMinute)
	if !admitted {
		// Denial: nothing is consumed, nothing is written back (the
		// persisted last-refill instant stays, so the next call refills
		// from it).
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return false, time.Second, err
		}
		done = true
		return false, wait, nil
	}
	state = rate.Take(state)
	const upsert = `INSERT INTO meta (key, value) VALUES (?, ?)
			 ON CONFLICT(key) DO UPDATE SET value = excluded.value`
	if _, err := conn.ExecContext(ctx, upsert, MetaRateTokens, strconv.FormatFloat(state.Tokens, 'f', -1, 64)); err != nil {
		return false, time.Second, err
	}
	if _, err := conn.ExecContext(ctx, upsert, MetaRateLast, strconv.FormatInt(state.Last.UnixNano(), 10)); err != nil {
		return false, time.Second, err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return false, time.Second, err
	}
	done = true
	return true, 0, nil
}

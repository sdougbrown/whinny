package store

import (
	"context"
	"strings"
	"time"
)

// Wake is one row of the wake_queue table.
type Wake struct {
	ID        int64
	Agent     string
	Payload   string
	CreatedAt int64 // unix seconds
}

// EnqueueWake appends a wake to agent's queue. If w.CreatedAt is zero
// it is set from now. It returns the new row's id.
func (s *Store) EnqueueWake(ctx context.Context, w Wake, now time.Time) (int64, error) {
	if w.CreatedAt == 0 {
		w.CreatedAt = now.Unix()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO wake_queue (agent, payload, created_at) VALUES (?, ?, ?)`,
		w.Agent, w.Payload, w.CreatedAt)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// DequeueWakes fetches and deletes all queued wakes for agent, in FIFO
// (insertion) order.
func (s *Store) DequeueWakes(ctx context.Context, agent string) ([]Wake, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx,
		`SELECT id, agent, payload, created_at FROM wake_queue WHERE agent = ? ORDER BY id`, agent)
	if err != nil {
		return nil, err
	}
	var out []Wake
	for rows.Next() {
		var w Wake
		if err := rows.Scan(&w.ID, &w.Agent, &w.Payload, &w.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, w)
	}
	// A fetch-level error after the last Next yields a partial list;
	// check it before the delete so a partial fetch does not silently
	// drop the remaining wakes.
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	err = rows.Close()
	if err != nil {
		return nil, err
	}
	if len(out) > 0 {
		ids := make([]any, len(out))
		for i, w := range out {
			ids[i] = w.ID
		}
		query := `DELETE FROM wake_queue WHERE id IN (` + placeholders(len(ids)) + `)`
		if _, err := tx.ExecContext(ctx, query, ids...); err != nil {
			return nil, err
		}
	}
	return out, tx.Commit()
}

func placeholders(n int) string {
	out := make([]string, n)
	for i := range out {
		out[i] = "?"
	}
	return strings.Join(out, ",")
}

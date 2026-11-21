package store

import (
	"context"
	"database/sql"
	"errors"
)

// SetCursor upserts the last-log-id cursor for roomID, moving it only
// forward: an incoming id below the stored cursor is ignored, so a
// replayed or out-of-order message can never rewind the cursor.
func (s *Store) SetCursor(ctx context.Context, roomID string, lastLogID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO cursors (room_id, last_log_id) VALUES (?, ?)
		 ON CONFLICT(room_id) DO UPDATE SET last_log_id = MAX(last_log_id, excluded.last_log_id)`,
		roomID, lastLogID)
	return err
}

// GetCursor returns the last log id recorded for roomID, or 0 when no
// cursor has been set.
func (s *Store) GetCursor(ctx context.Context, roomID string) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx,
		`SELECT last_log_id FROM cursors WHERE room_id = ?`, roomID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

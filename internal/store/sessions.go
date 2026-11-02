package store

import (
	"database/sql"
	"errors"
	"fmt"
)

// Session is the stored lock and state of one message session.
type Session struct {
	EntityID    int64
	SessionID   string
	State       []byte // nil when no state is set
	LockToken   []byte // nil when unlocked
	LockedUntil int64
	UpdatedAt   int64
}

// Session returns the session row, or ErrNotFound.
func (t *Tx) Session(entityID int64, sessionID string) (*Session, error) {
	s := &Session{EntityID: entityID, SessionID: sessionID}
	err := t.queryRow(`SELECT state, lock_token, locked_until, updated_at FROM sessions
		WHERE entity_id = ? AND session_id = ?`, entityID, sessionID).
		Scan(&s.State, &s.LockToken, &s.LockedUntil, &s.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}

// PutSession inserts or replaces a session row.
func (t *Tx) PutSession(s *Session) error {
	_, err := t.exec(`INSERT INTO sessions (entity_id, session_id, state, lock_token, locked_until, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (entity_id, session_id) DO UPDATE SET
			state = excluded.state, lock_token = excluded.lock_token,
			locked_until = excluded.locked_until, updated_at = excluded.updated_at`,
		s.EntityID, s.SessionID, nullBytes(s.State), nullBytes(s.LockToken), s.LockedUntil, s.UpdatedAt)
	if err != nil {
		return fmt.Errorf("store: put session %s: %w", s.SessionID, err)
	}
	return nil
}

// NextAvailableSession returns the session of the oldest deliverable
// message whose session is not locked, if any.
func (t *Tx) NextAvailableSession(entityID int64, subqueue int, now int64) (string, bool, error) {
	var id string
	err := t.queryRow(`SELECT m.session_id FROM messages m
		LEFT JOIN sessions s ON s.entity_id = m.entity_id AND s.session_id = m.session_id
		WHERE m.entity_id = +? AND m.subqueue = +? AND m.state = 0 AND m.visible_at <= ?
			AND m.locked_until <= ? AND m.session_id IS NOT NULL
			AND (s.locked_until IS NULL OR s.locked_until <= ?)
		ORDER BY m.seq LIMIT 1`, entityID, subqueue, now, now, now).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

// SetSessionMessageLocks sets the lock expiry of every message of a session
// that is currently locked, so message locks follow the session lock.
func (t *Tx) SetSessionMessageLocks(entityID int64, sessionID string, lockedUntil, now int64) error {
	_, err := t.exec(`UPDATE messages SET locked_until = ?
		WHERE entity_id = ? AND subqueue IN (0, 1, 2) AND session_id = ? AND lock_token IS NOT NULL AND locked_until > 0 AND locked_until > ?`,
		lockedUntil, entityID, sessionID, now)
	return err
}

// UnlockSessionMessages releases the message locks held through a session.
func (t *Tx) UnlockSessionMessages(entityID int64, sessionID string) error {
	_, err := t.exec(`UPDATE messages SET lock_token = NULL, locked_until = 0
		WHERE entity_id = ? AND subqueue IN (0, 1, 2) AND session_id = ? AND lock_token IS NOT NULL`, entityID, sessionID)
	return err
}

// ListSessions lists session ids of an entity, ordered by id. With
// updatedAfter 0 it lists sessions that have active messages or state;
// otherwise sessions whose state changed after updatedAfter.
func (t *Tx) ListSessions(entityID int64, updatedAfter int64, skip, top int) ([]string, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if updatedAfter == 0 {
		rows, err = t.query(`SELECT session_id FROM messages
				WHERE entity_id = ? AND subqueue = 0 AND session_id IS NOT NULL
			UNION
			SELECT session_id FROM sessions WHERE entity_id = ? AND state IS NOT NULL
			ORDER BY 1 LIMIT ? OFFSET ?`, entityID, entityID, top, skip)
	} else {
		rows, err = t.query(`SELECT session_id FROM sessions
			WHERE entity_id = ? AND updated_at > ? AND state IS NOT NULL
			ORDER BY 1 LIMIT ? OFFSET ?`, entityID, updatedAfter, top, skip)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

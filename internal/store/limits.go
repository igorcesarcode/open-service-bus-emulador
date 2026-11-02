package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrEntityGone reports a write to an entity deleted concurrently.
var ErrEntityGone = errors.New("store: entity no longer exists")

// isForeignKeyError reports a write that referenced a deleted entity.
func isForeignKeyError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "FOREIGN KEY constraint failed")
}

// EntitySize returns the bytes stored for an entity, in every sub-queue.
func (t *Tx) EntitySize(entityID int64) (int64, error) {
	var n int64
	err := t.queryRow(`SELECT size_bytes FROM entity_sizes WHERE entity_id = ?`, entityID).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return n, err
}

// RecordMessageID records a message id for duplicate detection. It
// reports whether the id was recorded after since, making the message a
// duplicate; duplicates leave the original record unchanged.
func (t *Tx) RecordMessageID(entityID int64, messageID string, now, since int64) (bool, error) {
	// One statement: the update of an id seen after since is skipped, and
	// then no row changes.
	res, err := t.exec(`INSERT INTO duplicates (entity_id, message_id, seen_at) VALUES (?, ?, ?)
		ON CONFLICT (entity_id, message_id) DO UPDATE SET seen_at = excluded.seen_at
		WHERE duplicates.seen_at <= ?`, entityID, messageID, now, since)
	if isForeignKeyError(err) {
		return false, fmt.Errorf("%w: %d", ErrEntityGone, entityID)
	}
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 0, err
}

// PurgeMessageIDs forgets up to limit message ids of an entity recorded at
// or before until, oldest first, and returns how many it forgot. Callers
// repeat it until it returns less than limit, a batch per transaction, so
// that the writer is not held for long.
func (t *Tx) PurgeMessageIDs(entityID, until int64, limit int) (int, error) {
	res, err := t.exec(`DELETE FROM duplicates WHERE entity_id = ? AND message_id IN (
			SELECT message_id FROM duplicates WHERE entity_id = +? AND seen_at <= ?
			ORDER BY seen_at, message_id LIMIT `+strconv.Itoa(limit)+`)`, entityID, entityID, until)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// EntityActivity holds when an entity was last used.
type EntityActivity struct {
	ID         int64
	AccessedAt int64
}

// Activity returns when each entity was last used.
func (t *Tx) Activity() ([]EntityActivity, error) {
	rows, err := t.query(`SELECT id, accessed_at FROM entities`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EntityActivity
	for rows.Next() {
		var a EntityActivity
		if err := rows.Scan(&a.ID, &a.AccessedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// AccessedAt returns when an entity was last used.
func (t *Tx) AccessedAt(entityID int64) (int64, error) {
	var at int64
	err := t.queryRow(`SELECT accessed_at FROM entities WHERE id = ?`, entityID).Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: %d", ErrEntityGone, entityID)
	}
	return at, err
}

// TouchAll records activity on every entity.
func (t *Tx) TouchAll(at int64) error {
	_, err := t.exec(`UPDATE entities SET accessed_at = ?`, at)
	return err
}

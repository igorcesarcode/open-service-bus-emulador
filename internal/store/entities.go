package store

import (
	"database/sql"
	"errors"
	"fmt"
)

// Entity is a stored queue, topic or subscription.
type Entity struct {
	ID         int64
	Kind       string
	Path       string
	Name       string
	ParentID   int64 // 0 for top-level entities
	Props      []byte
	NextSeq    int64
	CreatedAt  int64
	UpdatedAt  int64
	AccessedAt int64
}

const entityColumns = `id, kind, path, name, COALESCE(parent_id, 0), props, next_seq, created_at, updated_at, accessed_at`

func scanEntity(row interface{ Scan(...any) error }) (*Entity, error) {
	e := &Entity{}
	err := row.Scan(&e.ID, &e.Kind, &e.Path, &e.Name, &e.ParentID, &e.Props, &e.NextSeq,
		&e.CreatedAt, &e.UpdatedAt, &e.AccessedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return e, err
}

func nullInt(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

// InsertEntity stores e and sets its ID.
func (t *Tx) InsertEntity(e *Entity) error {
	if e.NextSeq == 0 {
		e.NextSeq = 1
	}
	res, err := t.exec(`INSERT INTO entities (kind, path, name, parent_id, props, next_seq, created_at, updated_at, accessed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Kind, e.Path, e.Name, nullInt(e.ParentID), e.Props, e.NextSeq, e.CreatedAt, e.UpdatedAt, e.AccessedAt)
	if err != nil {
		return fmt.Errorf("store: insert entity %s: %w", e.Path, err)
	}
	e.ID, err = res.LastInsertId()
	return err
}

// UpdateEntity stores the properties and timestamps of e.
func (t *Tx) UpdateEntity(e *Entity) error {
	_, err := t.exec(`UPDATE entities SET props = ?, updated_at = ?, accessed_at = ? WHERE id = ?`,
		e.Props, e.UpdatedAt, e.AccessedAt, e.ID)
	if err != nil {
		return fmt.Errorf("store: update entity %s: %w", e.Path, err)
	}
	return nil
}

// TouchEntity records activity on an entity.
func (t *Tx) TouchEntity(id, at int64) error {
	_, err := t.exec(`UPDATE entities SET accessed_at = ? WHERE id = ?`, at, id)
	return err
}

// DeleteEntity deletes an entity with its children, rules and messages.
func (t *Tx) DeleteEntity(id int64) error {
	_, err := t.exec(`DELETE FROM entities WHERE id = ?`, id)
	return err
}

// Entities returns every entity, parents before children.
func (t *Tx) Entities() ([]*Entity, error) {
	rows, err := t.query(`SELECT ` + entityColumns + ` FROM entities ORDER BY COALESCE(parent_id, 0), id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Entity
	for rows.Next() {
		e, err := scanEntity(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AllocSequence reserves n consecutive sequence numbers on an entity and
// returns the first.
func (t *Tx) AllocSequence(entityID int64, n int) (int64, error) {
	var next int64
	const update = `UPDATE entities SET next_seq = next_seq + ? WHERE id = ? RETURNING next_seq`
	err := t.queryRow(update, n, entityID).Scan(&next)
	if err == nil && t.rec != nil {
		*t.rec = append(*t.rec, Stmt{Query: update, Args: []any{n, entityID}})
	}
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: %d", ErrEntityGone, entityID)
	}
	if err != nil {
		return 0, fmt.Errorf("store: allocating sequence numbers: %w", err)
	}
	return next - int64(n), nil
}

// Rule is a stored subscription rule.
type Rule struct {
	ID             int64
	SubscriptionID int64
	Name           string
	Props          []byte
	CreatedAt      int64
}

// InsertRule stores r and sets its ID.
func (t *Tx) InsertRule(r *Rule) error {
	res, err := t.exec(`INSERT INTO rules (subscription_id, name, props, created_at) VALUES (?, ?, ?, ?)`,
		r.SubscriptionID, r.Name, r.Props, r.CreatedAt)
	if err != nil {
		return fmt.Errorf("store: insert rule %s: %w", r.Name, err)
	}
	r.ID, err = res.LastInsertId()
	return err
}

// UpdateRule stores the properties of r.
func (t *Tx) UpdateRule(r *Rule) error {
	_, err := t.exec(`UPDATE rules SET props = ? WHERE id = ?`, r.Props, r.ID)
	return err
}

// DeleteRule deletes a rule.
func (t *Tx) DeleteRule(id int64) error {
	_, err := t.exec(`DELETE FROM rules WHERE id = ?`, id)
	return err
}

// Rules returns every rule, ordered by subscription and creation.
func (t *Tx) Rules() ([]*Rule, error) {
	rows, err := t.query(`SELECT id, subscription_id, name, props, created_at FROM rules ORDER BY subscription_id, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Rule
	for rows.Next() {
		r := &Rule{}
		if err := rows.Scan(&r.ID, &r.SubscriptionID, &r.Name, &r.Props, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AllocSequenceNext returns the next sequence number an entity would hand
// out, without reserving it.
func (t *Tx) AllocSequenceNext(entityID int64) (int64, error) {
	var next int64
	err := t.queryRow(`SELECT next_seq FROM entities WHERE id = ?`, entityID).Scan(&next)
	return next, err
}

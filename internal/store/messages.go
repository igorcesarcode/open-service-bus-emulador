package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Sub-queues of an entity.
const (
	SubqueueActive      = 0
	SubqueueDeadLetter  = 1
	SubqueueTransferDLQ = 2
)

// Message states.
const (
	StateActive    = 0
	StateDeferred  = 1
	StateScheduled = 2
)

// Message is a stored message. Times are Unix milliseconds.
type Message struct {
	ID               int64
	EntityID         int64
	Subqueue         int
	Seq              int64
	State            int
	SessionID        string // "" when the message has no session
	MessageID        string
	EnqueuedAt       int64
	VisibleAt        int64
	ExpiresAt        int64 // 0 means never
	DeliveryCount    int
	LockToken        []byte // nil when unlocked
	LockedUntil      int64
	DeadLetterSource string
	Payload          []byte
	// OriginSeq is set on copies made by rules with actions, to the
	// sequence number the message had on the topic. It is stored but not
	// read back with the message.
	OriginSeq int64
	// Partition is the partition a message of a partitioned entity was
	// assigned to; 0 on other entities.
	Partition int
}

const messageColumns = `id, entity_id, subqueue, seq, state, COALESCE(session_id, ''), COALESCE(message_id, ''),
	enqueued_at, visible_at, expires_at, delivery_count, lock_token, locked_until,
	COALESCE(dead_letter_source, ''), payload, partition`

// messageHeaderColumns are messageColumns with a NULL payload.
const messageHeaderColumns = `id, entity_id, subqueue, seq, state, COALESCE(session_id, ''), COALESCE(message_id, ''),
	enqueued_at, visible_at, expires_at, delivery_count, lock_token, locked_until,
	COALESCE(dead_letter_source, ''), NULL, partition`

func scanMessages(rows *sql.Rows) ([]*Message, error) {
	defer rows.Close()
	var out []*Message
	for rows.Next() {
		m := &Message{}
		if err := rows.Scan(&m.ID, &m.EntityID, &m.Subqueue, &m.Seq, &m.State, &m.SessionID, &m.MessageID,
			&m.EnqueuedAt, &m.VisibleAt, &m.ExpiresAt, &m.DeliveryCount, &m.LockToken, &m.LockedUntil,
			&m.DeadLetterSource, &m.Payload, &m.Partition); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullBytes(b []byte) any {
	if b == nil {
		return nil
	}
	return b
}

// InsertMessage stores m and sets its ID.
func (t *Tx) InsertMessage(m *Message) error {
	res, err := t.exec(`INSERT INTO messages (entity_id, subqueue, seq, state, session_id, message_id,
		enqueued_at, visible_at, expires_at, delivery_count, lock_token, locked_until, dead_letter_source, payload, origin_seq, partition)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.EntityID, m.Subqueue, m.Seq, m.State, nullString(m.SessionID), nullString(m.MessageID),
		m.EnqueuedAt, m.VisibleAt, m.ExpiresAt, m.DeliveryCount, nullBytes(m.LockToken), m.LockedUntil,
		nullString(m.DeadLetterSource), m.Payload, m.OriginSeq, m.Partition)
	if isForeignKeyError(err) {
		return fmt.Errorf("%w: %d", ErrEntityGone, m.EntityID)
	}
	if err != nil {
		return fmt.Errorf("store: insert message: %w", err)
	}
	m.ID, err = res.LastInsertId()
	return err
}

// UpdateMessage stores the mutable fields of m.
func (t *Tx) UpdateMessage(m *Message) error {
	_, err := t.exec(`UPDATE messages SET entity_id = ?, subqueue = ?, state = ?, visible_at = ?, expires_at = ?,
		delivery_count = ?, lock_token = ?, locked_until = ?, dead_letter_source = ?, payload = ? WHERE id = ?`,
		m.EntityID, m.Subqueue, m.State, m.VisibleAt, m.ExpiresAt, m.DeliveryCount, nullBytes(m.LockToken),
		m.LockedUntil, nullString(m.DeadLetterSource), m.Payload, m.ID)
	if err != nil {
		return fmt.Errorf("store: update message: %w", err)
	}
	return nil
}

// The updates below change only the columns an operation touches. The full
// UpdateMessage rewrites the entry of every index that holds one of its
// columns, which locking and unlocking a message never need.

// LockMessage records a delivery: it locks m until lockedUntil with token
// and stores the delivery count.
func (t *Tx) LockMessage(m *Message) error {
	_, err := t.exec(`UPDATE messages SET delivery_count = ?, lock_token = ?, locked_until = ? WHERE id = ?`,
		m.DeliveryCount, nullBytes(m.LockToken), m.LockedUntil, m.ID)
	if err != nil {
		return fmt.Errorf("store: lock message: %w", err)
	}
	return nil
}

// RenewMessageLock stores the new lock expiry of m.
func (t *Tx) RenewMessageLock(m *Message) error {
	_, err := t.exec(`UPDATE messages SET locked_until = ? WHERE id = ?`, m.LockedUntil, m.ID)
	if err != nil {
		return fmt.Errorf("store: renew message lock: %w", err)
	}
	return nil
}

// UnlockMessage releases the lock of m and stores its delivery count, which
// abandoning keeps and releasing lowers.
func (t *Tx) UnlockMessage(m *Message) error {
	_, err := t.exec(`UPDATE messages SET delivery_count = ?, lock_token = NULL, locked_until = 0 WHERE id = ?`,
		m.DeliveryCount, m.ID)
	if err != nil {
		return fmt.Errorf("store: unlock message: %w", err)
	}
	return nil
}

// ActivateMessage stores the state of m, as when a scheduled message
// becomes active.
func (t *Tx) ActivateMessage(m *Message) error {
	_, err := t.exec(`UPDATE messages SET state = ? WHERE id = ?`, m.State, m.ID)
	if err != nil {
		return fmt.Errorf("store: activate message: %w", err)
	}
	return nil
}

// DeleteMessage deletes a message.
func (t *Tx) DeleteMessage(id int64) error {
	_, err := t.exec(`DELETE FROM messages WHERE id = ?`, id)
	return err
}

// MessageByLock returns the message holding lockToken.
func (t *Tx) MessageByLock(lockToken []byte) (*Message, error) {
	return t.messageByLock(messageColumns, lockToken)
}

// MessageHeaderByLock is MessageByLock without the payload, which a
// settlement that deletes the message or only moves its lock does not need.
func (t *Tx) MessageHeaderByLock(lockToken []byte) (*Message, error) {
	return t.messageByLock(messageHeaderColumns, lockToken)
}

func (t *Tx) messageByLock(columns string, lockToken []byte) (*Message, error) {
	rows, err := t.query(`SELECT `+columns+` FROM messages WHERE lock_token = ?`, lockToken)
	if err != nil {
		return nil, err
	}
	ms, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	if len(ms) == 0 {
		return nil, ErrNotFound
	}
	return ms[0], nil
}

// AvailableQuery selects messages that can be delivered now.
type AvailableQuery struct {
	EntityID int64
	Subqueue int
	Now      int64
	Limit    int
	// Session restricts the result to one session when HasSession is set.
	Session    string
	HasSession bool
}

// Available returns active, visible and unlocked (or lock-expired)
// messages in sequence order.
func (t *Tx) Available(q AvailableQuery) ([]*Message, error) {
	// Two reads, merged by sequence number: the messages with no lock, and
	// the ones whose lock lapsed. Looking in messages_order for both would
	// walk past every message locked at the head of the queue.
	build := func(cond string) (string, []any) {
		query := `SELECT ` + messageColumns + ` FROM messages
			WHERE entity_id = +? AND subqueue = +? AND state = 0 AND ` + cond
		args := []any{q.EntityID, q.Subqueue}
		return query, args
	}
	limit := ` ORDER BY seq LIMIT ` + strconv.Itoa(int(q.Limit))
	session := ""
	if q.HasSession {
		session = ` AND session_id = +?`
	}
	readyQ, args := build(`lock_token IS NULL AND visible_at <= ? AND locked_until <= ?`)
	args = append(args, q.Now, q.Now)
	if q.HasSession {
		args = append(args, q.Session)
	}
	// The limit is part of the text: SQLite plans a statement again on every
	// run when its LIMIT is a parameter, which costs more than the query.
	rows, err := t.query(readyQ+session+limit, args...)
	if err != nil {
		return nil, err
	}
	ready, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	// Most of the time no lock has lapsed, and a probe of the index costs
	// less than the query that fetches the messages.
	if lapsed, err := t.HasLapsedLocks(q.Now); err != nil || !lapsed {
		return ready, err
	}
	lapsedQ, args := build(`locked_until > 0 AND locked_until <= ? AND visible_at <= ?`)
	args = append(args, q.Now, q.Now)
	if q.HasSession {
		args = append(args, q.Session)
	}
	rows, err = t.query(strings.Replace(lapsedQ, "FROM messages", "FROM messages INDEXED BY messages_locked", 1)+session+limit, args...)
	if err != nil {
		return nil, err
	}
	lapsed, err := scanMessages(rows)
	if err != nil || len(lapsed) == 0 {
		return ready, err
	}
	out := make([]*Message, 0, len(ready)+len(lapsed))
	i, j := 0, 0
	for len(out) < q.Limit && (i < len(ready) || j < len(lapsed)) {
		if j == len(lapsed) || i < len(ready) && ready[i].Seq < lapsed[j].Seq {
			out = append(out, ready[i])
			i++
		} else {
			out = append(out, lapsed[j])
			j++
		}
	}
	return out, nil
}

// HasLapsedLocks reports whether a message lock expired at or before now and
// is still recorded.
func (t *Tx) HasLapsedLocks(now int64) (bool, error) {
	var one int
	err := t.queryRow(`SELECT 1 FROM messages INDEXED BY messages_locked WHERE locked_until > 0 AND locked_until <= ? LIMIT 1`, now).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// ReleaseLapsedLocks clears the locks that expired at or before now, up to
// limit of them, oldest first, and returns the entities they belong to. The
// lock token goes, so the message is a candidate of messages_ready again.
func (t *Tx) ReleaseLapsedLocks(now int64, limit int) (entities []int64, n int, err error) {
	rows, err := t.query(`SELECT id, entity_id FROM messages INDEXED BY messages_locked
		WHERE locked_until > 0 AND locked_until <= ? ORDER BY locked_until, id LIMIT `+strconv.Itoa(limit), now)
	if err != nil {
		return nil, 0, err
	}
	var ids []int64
	seen := map[int64]bool{}
	for rows.Next() {
		var id, entity int64
		if err := rows.Scan(&id, &entity); err != nil {
			rows.Close()
			return nil, 0, err
		}
		ids = append(ids, id)
		if !seen[entity] {
			seen[entity] = true
			entities = append(entities, entity)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, 0, err
	}
	// By id, not by the condition, so a replica clears the same rows.
	for _, id := range ids {
		if _, err := t.exec(`UPDATE messages SET lock_token = NULL, locked_until = 0 WHERE id = ?`, id); err != nil {
			return nil, 0, err
		}
	}
	return entities, len(ids), nil
}

// MessagesBySeq returns the messages of a sub-queue with the given
// sequence numbers, in sequence order.
func (t *Tx) MessagesBySeq(entityID int64, subqueue int, seqs []int64) ([]*Message, error) {
	if len(seqs) == 0 {
		return nil, nil
	}
	args := []any{entityID, subqueue}
	for _, s := range seqs {
		args = append(args, s)
	}
	rows, err := t.query(`SELECT `+messageColumns+` FROM messages
		WHERE entity_id = +? AND subqueue = +? AND seq IN (`+placeholders(len(seqs))+`) ORDER BY seq`, args...)
	if err != nil {
		return nil, err
	}
	return scanMessages(rows)
}

// ScheduledBySeq returns the scheduled messages of an entity's active queue
// whose sequence number, or the one they had on the topic, is in seqs.
func (t *Tx) ScheduledBySeq(entityID int64, seqs []int64) ([]*Message, error) {
	if len(seqs) == 0 {
		return nil, nil
	}
	args := []any{entityID, SubqueueActive, StateScheduled}
	for range 2 {
		for _, s := range seqs {
			args = append(args, s)
		}
	}
	rows, err := t.query(`SELECT `+messageColumns+` FROM messages
		WHERE entity_id = +? AND subqueue = +? AND state = +?
		AND (seq IN (`+placeholders(len(seqs))+`) OR origin_seq IN (`+placeholders(len(seqs))+`)) ORDER BY seq`, args...)
	if err != nil {
		return nil, err
	}
	return scanMessages(rows)
}

// PeekQuery selects messages to peek.
type PeekQuery struct {
	EntityID int64
	Subqueue int
	FromSeq  int64
	Limit    int
	// Session restricts the result to one session when HasSession is set.
	Session    string
	HasSession bool
}

// Peek returns messages of a sub-queue in any state, starting at FromSeq.
func (t *Tx) Peek(q PeekQuery) ([]*Message, error) {
	query := `SELECT ` + messageColumns + ` FROM messages WHERE entity_id = +? AND subqueue = +? AND seq >= ?`
	args := []any{q.EntityID, q.Subqueue, q.FromSeq}
	if q.HasSession {
		query += ` AND session_id = +?`
		args = append(args, q.Session)
	}
	// The limit is part of the text: SQLite plans a statement again on every
	// run when its LIMIT is a parameter, which costs more than the query.
	query += ` ORDER BY seq LIMIT ` + strconv.Itoa(int(q.Limit))
	rows, err := t.query(query, args...)
	if err != nil {
		return nil, err
	}
	return scanMessages(rows)
}

// DeleteOldest deletes up to limit of the oldest messages of a sub-queue that
// were enqueued before the cutoff (Unix milliseconds), in sequence order. It
// leaves alone messages that are locked at now, and scheduled ones, which
// are not enqueued yet. A non-empty session restricts it to that session.
// It returns how many it deleted.
func (t *Tx) DeleteOldest(entityID int64, subqueue int, before, now int64, limit int, session string) (int64, error) {
	query := `DELETE FROM messages WHERE id IN (SELECT id FROM messages
		WHERE entity_id = +? AND subqueue = +? AND state != ? AND enqueued_at < ?
		AND (lock_token IS NULL OR locked_until <= ?)`
	args := []any{entityID, subqueue, StateScheduled, before, now}
	if session != "" {
		query += ` AND session_id = +?`
		args = append(args, session)
	}
	query += ` ORDER BY seq LIMIT ` + strconv.Itoa(int(limit)) + `)`
	res, err := t.exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DueScheduled returns scheduled messages whose enqueue time has come.
func (t *Tx) DueScheduled(now int64, limit int) ([]*Message, error) {
	rows, err := t.query(`SELECT `+messageColumns+` FROM messages
		WHERE state = 2 AND visible_at <= ? ORDER BY visible_at LIMIT `+strconv.Itoa(limit), now)
	if err != nil {
		return nil, err
	}
	return scanMessages(rows)
}

// Expired returns unlocked messages of active sub-queues whose time to live
// has passed. Locked messages expire once their lock is released.
func (t *Tx) Expired(now int64, limit int) ([]*Message, error) {
	rows, err := t.query(`SELECT `+messageColumns+` FROM messages
		WHERE expires_at > 0 AND subqueue = 0 AND expires_at <= ? AND locked_until <= ? AND state <> 2
		ORDER BY expires_at LIMIT `+strconv.Itoa(limit), now, now)
	if err != nil {
		return nil, err
	}
	return scanMessages(rows)
}

// NextTimer returns the earliest time after now at which a scheduled
// message becomes due, a message expires or a lock lapses, or 0 if there is
// none.
func (t *Tx) NextTimer(now int64) (int64, error) {
	var next sql.NullInt64
	err := t.queryRow(`SELECT MIN(v) FROM (
		SELECT MIN(visible_at) AS v FROM messages WHERE state = 2 AND visible_at > ?
		UNION ALL SELECT MIN(expires_at) FROM messages WHERE expires_at > 0 AND expires_at > ? AND subqueue = 0
		UNION ALL SELECT MIN(locked_until) FROM messages WHERE locked_until > 0 AND locked_until > ?
		UNION ALL SELECT MIN(locked_until) FROM sessions WHERE locked_until > 0 AND locked_until > ?
	)`, now, now, now, now).Scan(&next)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	return next.Int64, nil
}

// Counts holds message counts of an entity.
type Counts struct {
	Active, DeadLetter, TransferDeadLetter, Scheduled, Deferred int64
	SizeBytes                                                   int64
}

// CountMessages counts the messages of an entity by sub-queue and state.
func (t *Tx) CountMessages(entityID int64) (Counts, error) {
	var c Counts
	// The counts and the size are kept by triggers, so no message is read.
	rows, err := t.query(`SELECT subqueue, state, n FROM entity_counts WHERE entity_id = ? AND n > 0`, entityID)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var subqueue, state, n int64
		if err := rows.Scan(&subqueue, &state, &n); err != nil {
			return c, err
		}
		switch {
		case subqueue == SubqueueDeadLetter:
			c.DeadLetter += n
		case subqueue == SubqueueTransferDLQ:
			c.TransferDeadLetter += n
		case subqueue != SubqueueActive:
		case state == StateActive:
			c.Active += n
		case state == StateScheduled:
			c.Scheduled += n
		case state == StateDeferred:
			c.Deferred += n
		}
	}
	if err := rows.Err(); err != nil {
		return c, err
	}
	c.SizeBytes, err = t.EntitySize(entityID)
	return c, err
}

// DeleteAllMessages deletes every message of an entity's sub-queue.
func (t *Tx) DeleteAllMessages(entityID int64, subqueue int) error {
	_, err := t.exec(`DELETE FROM messages WHERE entity_id = ? AND subqueue = ?`, entityID, subqueue)
	return err
}

// LockLapsedEntities returns the entities with message locks that lapsed in
// the interval (from, to].
func (t *Tx) LockLapsedEntities(from, to int64) ([]int64, error) {
	rows, err := t.query(`SELECT entity_id FROM messages WHERE locked_until > 0 AND locked_until > ? AND locked_until <= ?
		UNION SELECT entity_id FROM sessions WHERE locked_until > 0 AND locked_until > ? AND locked_until <= ?`, from, to, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

package store

import "strings"

// Metric is one value of a one-minute metrics bucket.
type Metric struct {
	Entity string
	Minute int64
	Name   string
	Value  int64
}

// AddMetric adds m.Value to the stored value of the bucket, so that two
// flushes within the same minute, such as around a restart, add up.
func (t *Tx) AddMetric(m Metric) error {
	_, err := t.exec(`INSERT INTO metrics (entity, minute, name, value) VALUES (?, ?, ?, ?)
		ON CONFLICT (entity, minute, name) DO UPDATE SET value = value + excluded.value`,
		m.Entity, m.Minute, m.Name, m.Value)
	return err
}

// SetMetric stores m, replacing the bucket.
func (t *Tx) SetMetric(m Metric) error {
	_, err := t.exec(`INSERT OR REPLACE INTO metrics (entity, minute, name, value) VALUES (?, ?, ?, ?)`,
		m.Entity, m.Minute, m.Name, m.Value)
	return err
}

// Metrics returns the buckets of entity in the minutes from..to, inclusive,
// ordered by minute.
func (t *Tx) Metrics(entity string, from, to int64) ([]Metric, error) {
	rows, err := t.query(`SELECT entity, minute, name, value FROM metrics
		WHERE entity = +? AND minute >= ? AND minute <= ? ORDER BY minute`, entity, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Metric
	for rows.Next() {
		var m Metric
		if err := rows.Scan(&m.Entity, &m.Minute, &m.Name, &m.Value); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// PruneMetrics deletes the buckets of minutes before minute.
func (t *Tx) PruneMetrics(minute int64) error {
	_, err := t.exec(`DELETE FROM metrics WHERE minute < ?`, minute)
	return err
}

// ActivityEntry is a row of the activity log.
type ActivityEntry struct {
	ID        int64
	Time      int64 // Unix milliseconds
	Actor     string
	Operation string
	Entity    string
	Status    string
	Detail    string
}

// InsertActivityEntry appends a row to the activity log.
func (t *Tx) InsertActivityEntry(a *ActivityEntry) error {
	res, err := t.exec(`INSERT INTO activity_log (time, actor, operation, entity, status, detail)
		VALUES (?, ?, ?, ?, ?, ?)`, a.Time, a.Actor, a.Operation, a.Entity, a.Status, a.Detail)
	if err != nil {
		return err
	}
	a.ID, err = res.LastInsertId()
	return err
}

// ActivityQuery selects rows of the activity log. Zero fields match all.
type ActivityQuery struct {
	From, To  int64 // Unix milliseconds; To 0 means no upper bound
	Actor     string
	Operation string
	Entity    string // matches the entity and the paths below it
	Status    string
	Limit     int
}

// ActivityEntries returns the rows matching q, newest first.
func (t *Tx) ActivityEntries(q ActivityQuery) ([]ActivityEntry, error) {
	var (
		where []string
		args  []any
	)
	add := func(cond string, arg any) {
		where = append(where, cond)
		args = append(args, arg)
	}
	add("time >= ?", q.From)
	if q.To > 0 {
		add("time <= ?", q.To)
	}
	if q.Actor != "" {
		add("actor = ?", q.Actor)
	}
	if q.Operation != "" {
		add("operation = ?", q.Operation)
	}
	if q.Entity != "" {
		where = append(where, "(entity = ? OR entity LIKE ? ESCAPE '\\')")
		esc := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(q.Entity)
		args = append(args, q.Entity, esc+"/%")
	}
	if q.Status != "" {
		add("status = ?", q.Status)
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit)
	rows, err := t.query(`SELECT id, time, actor, operation, entity, status, detail FROM activity_log
		WHERE `+strings.Join(where, " AND ")+` ORDER BY id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ActivityEntry
	for rows.Next() {
		var a ActivityEntry
		if err := rows.Scan(&a.ID, &a.Time, &a.Actor, &a.Operation, &a.Entity, &a.Status, &a.Detail); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// PruneActivity deletes the rows older than before, in Unix milliseconds.
func (t *Tx) PruneActivity(before int64) error {
	_, err := t.exec(`DELETE FROM activity_log WHERE time < ?`, before)
	return err
}

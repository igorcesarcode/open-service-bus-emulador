package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
)

// Stmt is a statement that changed the database, with its arguments, as a
// write transaction ran it. Replaying the statements of a transaction, in
// order, on a database that began identical makes it identical again.
type Stmt struct {
	Query string
	Args  []any
}

// Replicator receives the statements of every committed write transaction,
// in commit order, while the store still orders its writes: the next write
// does not start until it returns. An error is returned to the writer, whose
// transaction has already committed.
type Replicator func(ctx context.Context, stmts []Stmt) error

// ErrThrottled is returned by a write that a gate refused.
var ErrThrottled = errors.New("store: writes are throttled")

// SetReplicator sets, or with nil removes, the function that receives the
// statements of committed writes.
func (s *Store) SetReplicator(r Replicator) {
	s.hookMu.Lock()
	s.repl = r
	s.hookMu.Unlock()
}

// SetGate sets, or with nil removes, a function consulted before every
// write; an error it returns is the write's error and nothing is written.
func (s *Store) SetGate(g func() error) {
	s.hookMu.Lock()
	s.gate = g
	s.hookMu.Unlock()
}

// ApplyBatch runs the statements of one replicated transaction, in one
// transaction of this store.
func (s *Store) ApplyBatch(ctx context.Context, stmts []Stmt) error {
	s.repMu.Lock()
	defer s.repMu.Unlock()
	return s.run(ctx, s.write, false, func(tx *Tx) error {
		for _, st := range stmts {
			if _, err := tx.execRaw(st.Query, st.Args...); err != nil {
				return fmt.Errorf("store: replaying %q: %w", firstLine(st.Query), err)
			}
		}
		return nil
	})
}

func firstLine(q string) string {
	q = strings.TrimSpace(q)
	if i := strings.IndexByte(q, '\n'); i >= 0 {
		q = q[:i]
	}
	return q
}

// tables lists the tables a copy has to carry.
func tables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// derived tables are rebuilt from the others after a copy.
var derived = map[string]bool{"entity_sizes": true, "entity_counts": true}

// Reset deletes every row of the store, to be filled again by a copy.
func (s *Store) Reset(ctx context.Context) error {
	s.repMu.Lock()
	defer s.repMu.Unlock()
	return s.resetLocked(ctx)
}

func (s *Store) resetLocked(ctx context.Context) error {
	names, err := tables(ctx, s.write)
	if err != nil {
		return err
	}
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, n := range names {
		if _, err := tx.Exec(`DELETE FROM "` + n + `"`); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CopyTo replaces the contents of dst with those of s, and runs after, if it
// is not nil, while s accepts no writes, so that nothing committed after the
// copy and before after is missed.
func (s *Store) CopyTo(ctx context.Context, dst *Store, after func()) error {
	s.repMu.Lock()
	defer s.repMu.Unlock()
	dst.repMu.Lock()
	defer dst.repMu.Unlock()
	if err := dst.resetLocked(ctx); err != nil {
		return err
	}
	names, err := tables(ctx, s.write)
	if err != nil {
		return err
	}
	out, err := dst.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer out.Rollback()
	// Rows reference their entities, which may be copied later.
	if _, err := out.Exec(`PRAGMA defer_foreign_keys = ON`); err != nil {
		return err
	}
	for _, name := range names {
		if derived[name] {
			continue
		}
		if err := copyTable(ctx, s.write, out, name); err != nil {
			return fmt.Errorf("store: copying %s: %w", name, err)
		}
	}
	// The totals are recomputed, as at every start.
	if _, err := out.Exec(`DELETE FROM entity_sizes`); err != nil {
		return err
	}
	if _, err := out.Exec(`INSERT INTO entity_sizes (entity_id, size_bytes)
		SELECT entity_id, SUM(LENGTH(payload)) FROM messages GROUP BY entity_id`); err != nil {
		return err
	}
	if _, err := out.Exec(`DELETE FROM entity_counts`); err != nil {
		return err
	}
	if _, err := out.Exec(`INSERT INTO entity_counts (entity_id, subqueue, state, n)
		SELECT entity_id, subqueue, state, COUNT(*) FROM messages GROUP BY entity_id, subqueue, state`); err != nil {
		return err
	}
	if err := out.Commit(); err != nil {
		return err
	}
	if after != nil {
		after()
	}
	return nil
}

func copyTable(ctx context.Context, src *sql.DB, dst *sql.Tx, name string) error {
	rows, err := src.QueryContext(ctx, `SELECT * FROM "`+name+`"`)
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = `"` + c + `"`
	}
	insert := `INSERT INTO "` + name + `" (` + strings.Join(quoted, ", ") + `) VALUES (` + placeholders(len(cols)) + `)`
	stmt, err := dst.PrepareContext(ctx, insert)
	if err != nil {
		return err
	}
	defer stmt.Close()
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, vals...); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Ref is a store that can be replaced while others use it: a Geo-Replication
// promotion swaps the store of a running namespace for its replica. Its
// Read and Write run on the current store.
type Ref struct {
	p atomic.Pointer[Store]
}

// NewRef returns a Ref to s.
func NewRef(s *Store) *Ref {
	r := &Ref{}
	r.p.Store(s)
	return r
}

// Get returns the current store.
func (r *Ref) Get() *Store { return r.p.Load() }

// Set makes s the current store.
func (r *Ref) Set(s *Store) { r.p.Store(s) }

// Read runs fn in a read-only transaction of the current store.
func (r *Ref) Read(ctx context.Context, fn func(*Tx) error) error { return r.p.Load().Read(ctx, fn) }

// Write runs fn in a write transaction of the current store.
func (r *Ref) Write(ctx context.Context, fn func(*Tx) error) error { return r.p.Load().Write(ctx, fn) }

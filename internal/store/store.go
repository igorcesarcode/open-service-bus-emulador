// Package store persists namespaces, entities and messages in SQLite.
//
// All writes go through a single connection, which serializes them without
// lock contention; reads use a separate pool so peeks and admin queries
// run concurrently with writes (WAL mode). An in-memory database uses one
// connection for both, since separate connections would see separate
// databases.
package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

//go:embed schema.sql
var schema string

// ErrNotFound reports a missing row.
var ErrNotFound = errors.New("store: not found")

// Store is an open database.
type Store struct {
	write *sql.DB
	read  *sql.DB

	// repMu orders the writes with what follows each of them: the gate
	// before it and the replicator after it.
	repMu sync.Mutex

	// qmu guards the queue of writes waiting for the leader, which is the
	// caller that is committing.
	qmu     sync.Mutex
	queue   []*writeReq
	leading bool
	closed  bool
	leaders sync.WaitGroup

	// stmts caches prepared statements by database and query text, so that
	// SQLite parses each statement once per connection instead of on every
	// call.
	stmts    sync.Map // stmtKey -> *sql.Stmt
	stmtSize atomic.Int32
	writes   atomic.Int64 // calls to Write, for tests
	// repl and gate are set by SetReplicator and SetGate.
	hookMu sync.RWMutex
	repl   Replicator
	gate   func() error
}

// Memory is the path that selects an in-memory database.
const Memory = ":memory:"

// pragmas tune SQLite for a single-process server: WAL lets readers run
// alongside the writer, synchronous=NORMAL drops the per-commit fsync
// (still crash-safe for the database file in WAL mode), and a larger page
// cache and mmap keep hot pages out of read(2) calls.
var pragmas = []string{
	"journal_mode(WAL)",
	"synchronous(NORMAL)",
	"busy_timeout(10000)",
	"foreign_keys(ON)",
	"temp_store(MEMORY)",
	"cache_size(-65536)",
	"mmap_size(1073741824)",
}

// Open opens or creates the database at path and applies the schema.
// Use Memory for a database that lives only as long as the Store.
func Open(ctx context.Context, path string) (*Store, error) {
	s := &Store{}
	var err error
	if path == Memory {
		if s.write, err = openDB(":memory:", 1); err != nil {
			return nil, err
		}
		s.read = s.write
	} else {
		if s.write, err = openDB(path, 1); err != nil {
			return nil, err
		}
		if s.read, err = openDB(path, max(4, runtime.NumCPU())); err != nil {
			s.write.Close()
			return nil, err
		}
	}
	if _, err := s.write.ExecContext(ctx, schema); err != nil {
		s.Close()
		return nil, fmt.Errorf("store: applying schema: %w", err)
	}
	if err := s.migrate(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// migrate adds the columns that databases created by earlier versions
// lack. CREATE TABLE IF NOT EXISTS leaves such tables as they are.
func (s *Store) migrate(ctx context.Context) error {
	var n int
	err := s.write.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name = 'origin_seq'`).Scan(&n)
	if err != nil {
		return fmt.Errorf("store: checking the schema: %w", err)
	}
	if n == 0 {
		if _, err := s.write.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN origin_seq INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("store: migrating the schema: %w", err)
		}
	}
	err = s.write.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name = 'partition'`).Scan(&n)
	if err != nil {
		return fmt.Errorf("store: checking the schema: %w", err)
	}
	if n == 0 {
		if _, err := s.write.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN partition INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("store: migrating the schema: %w", err)
		}
	}
	return nil
}

func openDB(path string, conns int) (*sql.DB, error) {
	q := url.Values{}
	for _, p := range pragmas {
		q.Add("_pragma", p)
	}
	// Write transactions take the write lock up front, so two of them can
	// never deadlock upgrading from a read lock.
	q.Set("_txlock", "immediate")
	// The path is part of a file: URI, so characters with a meaning in
	// URIs must be escaped.
	escaped := strings.NewReplacer("%", "%25", "?", "%3f", "#", "%23").Replace(path)
	dsn := "file:" + escaped + "?" + q.Encode()
	if path == ":memory:" {
		dsn = "file::memory:?" + q.Encode()
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	// Connections must never be closed: an in-memory database disappears
	// with its last connection.
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	return db, nil
}

// Close closes the database.
func (s *Store) Close() error {
	s.qmu.Lock()
	s.closed = true
	s.qmu.Unlock()
	s.leaders.Wait()
	err := s.write.Close()
	if s.read != s.write {
		err = errors.Join(err, s.read.Close())
	}
	return err
}

// maxGroup is the most writes committed together.
const maxGroup = 64

type soloKey struct{}

// WithSolo marks a context whose writes commit alone, never grouped with
// others. It is how an entity with batched operations disabled keeps each of
// its operations in a transaction of its own.
func WithSolo(ctx context.Context) context.Context {
	return context.WithValue(ctx, soloKey{}, true)
}

func solo(ctx context.Context) bool { return ctx.Value(soloKey{}) != nil }

// writeReq is a write waiting for the writer goroutine.
type writeReq struct {
	ctx  context.Context
	fn   func(*Tx) error
	done chan error
	// enq is when the write was queued, for the time it waits for a commit.
	enq time.Time
}

// ErrClosed reports a write to a closed store.
var ErrClosed = errors.New("store: closed")

// Writes returns how many times Write was called.
func (s *Store) Writes() int64 { return s.writes.Load() }

// Write runs fn in a write transaction, committing if fn returns nil.
//
// Writes that arrive while another is being committed are grouped: the
// caller that finds no writer running becomes the leader and commits the
// queue in one transaction per group, each write inside a savepoint that a
// failure rolls back alone. A write returns after its group has committed
// (and replicated), so callers see the same guarantees as with one
// transaction each, and a lone write pays nothing for the grouping.
func (s *Store) Write(ctx context.Context, fn func(*Tx) error) error {
	s.writes.Add(1)
	req := &writeReq{ctx: ctx, fn: fn, done: make(chan error, 1)}
	if telemetry.Enabled() {
		req.enq = time.Now()
	}
	s.qmu.Lock()
	if s.closed {
		s.qmu.Unlock()
		return ErrClosed
	}
	s.queue = append(s.queue, req)
	if s.leading {
		s.qmu.Unlock()
		return <-req.done
	}
	s.leading = true
	s.leaders.Add(1)
	// Lead until this write is done, then leave the rest of the queue to
	// another goroutine so that this caller returns.
	for {
		group := s.takeGroupLocked()
		s.qmu.Unlock()
		s.commitGroup(group)
		s.qmu.Lock()
		if slices.Contains(group, req) {
			return s.handOff(req)
		}
	}
}

// takeGroupLocked removes up to maxGroup writes from the queue.
func (s *Store) takeGroupLocked() []*writeReq {
	n := 0
	for n < len(s.queue) && n < maxGroup {
		if solo(s.queue[n].ctx) {
			if n == 0 {
				n = 1 // A solo write goes alone.
			}
			break
		}
		n++
	}
	group := append([]*writeReq(nil), s.queue[:n]...)
	s.queue = s.queue[n:]
	return group
}

// handOff ends the leadership of the caller, whose write is done. The queue
// lock is held. If writes are waiting, a new goroutine leads on.
func (s *Store) handOff(req *writeReq) error {
	err := <-req.done
	if len(s.queue) == 0 {
		s.leading = false
		s.qmu.Unlock()
		s.leaders.Done()
		return err
	}
	s.qmu.Unlock()
	go func() {
		defer s.leaders.Done()
		s.qmu.Lock()
		for len(s.queue) > 0 {
			group := s.takeGroupLocked()
			s.qmu.Unlock()
			s.commitGroup(group)
			s.qmu.Lock()
		}
		s.leading = false
		s.qmu.Unlock()
	}()
	return err
}

func (s *Store) commitGroup(group []*writeReq) {
	s.repMu.Lock()
	defer s.repMu.Unlock()
	s.hookMu.RLock()
	gate, repl := s.gate, s.repl
	s.hookMu.RUnlock()

	results := make([]error, len(group))
	var live []int // indexes of the writes that run
	for i, req := range group {
		switch {
		case req.ctx.Err() != nil:
			results[i] = req.ctx.Err()
		case gate != nil:
			if err := gate(); err != nil {
				results[i] = err
				continue
			}
			live = append(live, i)
		default:
			live = append(live, i)
		}
	}
	defer func() {
		for i, req := range group {
			req.done <- results[i]
		}
	}()
	if len(live) == 0 {
		return
	}
	var stats commitStats
	if telemetry.Enabled() {
		stats = startCommit(group, live)
		defer func() { stats.finish(results, live) }()
	}
	tx, err := s.write.BeginTx(context.Background(), nil)
	if err != nil {
		for _, i := range live {
			results[i] = fmt.Errorf("store: begin: %w", err)
		}
		return
	}
	var all []Stmt
	var ran, okTx []*Tx
	var okIdx []int
	bound := map[string]*sql.Stmt{}
	for _, i := range live {
		t := &Tx{tx: tx, s: s, db: s.write, bound: bound}
		var stmts []Stmt
		if repl != nil {
			t.rec = &stmts
		}
		if len(live) > 1 {
			if _, err := t.execRaw("SAVEPOINT w"); err != nil {
				results[i] = fmt.Errorf("store: savepoint: %w", err)
				continue
			}
		}
		fnStart := time.Now()
		err := s.callWrite(group[i].fn, t)
		stats.fn += time.Since(fnStart)
		if len(live) > 1 {
			if err != nil {
				if _, rerr := t.execRaw("ROLLBACK TO w"); rerr != nil {
					err = errors.Join(err, rerr)
				}
			}
			if _, rerr := t.execRaw("RELEASE w"); rerr != nil && err == nil {
				err = rerr
			}
		}
		if err != nil {
			results[i] = err
			ran = append(ran, t)
			continue
		}
		okIdx = append(okIdx, i)
		okTx = append(okTx, t)
		ran = append(ran, t)
		all = append(all, stmts...)
	}
	if len(okIdx) == 0 {
		tx.Rollback()
		s.cacheAll(ran)
		return
	}
	stats.stmts = 0
	for _, t := range ran {
		stats.stmts += t.stmtCount
	}
	commitStart := time.Now()
	err = tx.Commit()
	stats.commit = time.Since(commitStart)
	if err != nil {
		for _, i := range okIdx {
			results[i] = fmt.Errorf("store: commit: %w", err)
		}
		s.cacheAll(ran)
		return
	}
	s.cacheAll(ran)
	for _, t := range okTx {
		for _, fn := range t.after {
			fn()
		}
	}
	if repl == nil || len(all) == 0 {
		return
	}
	if err := repl(context.Background(), all); err != nil {
		for _, i := range okIdx {
			results[i] = err
		}
	}
}

// callWrite runs fn, turning a panic into an error so that one bad write
// does not take the writer goroutine down.
func (s *Store) callWrite(fn func(*Tx) error, t *Tx) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("store: write panicked: %v", r)
		}
	}()
	return fn(t)
}

func (s *Store) cacheAll(txs []*Tx) {
	for _, t := range txs {
		t.cacheMisses()
	}
}

// Read runs fn in a read-only transaction.
func (s *Store) Read(ctx context.Context, fn func(*Tx) error) error {
	return s.run(ctx, s.read, true, fn)
}

func (s *Store) run(ctx context.Context, db *sql.DB, readOnly bool, fn func(*Tx) error) error {
	opts := &sql.TxOptions{ReadOnly: readOnly && db != s.write}
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	t := &Tx{tx: tx, s: s, db: db, bound: map[string]*sql.Stmt{}}
	if err := fn(t); err != nil {
		tx.Rollback()
		t.cacheMisses()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	t.cacheMisses()
	for _, fn := range t.after {
		fn()
	}
	return nil
}

// Tx is a database transaction.
type Tx struct {
	tx *sql.Tx
	s  *Store
	db *sql.DB
	// rec, when not nil, collects the statements that change the database,
	// for replication.
	rec *[]Stmt
	// bound caches the statements bound to the transaction, which every Tx
	// of a group shares: binding a prepared statement allocates.
	bound map[string]*sql.Stmt
	// misses are the queries run without a cached statement.
	misses []string
	// stmtCount is the number of statements that changed the database.
	stmtCount int
	// after are run once the transaction has committed.
	after []func()
}

// AfterCommit schedules fn to run after the transaction commits. It does
// not run if the transaction fails or is rolled back, and it runs on the
// goroutine that commits, so it must be quick and must not write.
func (t *Tx) AfterCommit(fn func()) { t.after = append(t.after, fn) }

// AfterCommitFirst is AfterCommit for a function that must run before the
// ones already scheduled.
func (t *Tx) AfterCommitFirst(fn func()) { t.after = append([]func(){fn}, t.after...) }

// exec runs a statement that changes the database, and records it when the
// store replicates.
func (t *Tx) exec(query string, args ...any) (sql.Result, error) {
	res, err := t.execRaw(query, args...)
	if err == nil {
		t.stmtCount++
	}
	if err == nil && t.rec != nil {
		*t.rec = append(*t.rec, Stmt{Query: query, Args: args})
	}
	return res, err
}

type stmtKey struct {
	db    *sql.DB
	query string
}

// maxCachedStmts bounds the statement cache; queries built with a varying
// number of placeholders would otherwise fill it.
const maxCachedStmts = 512

// stmt returns the prepared form of query bound to the transaction, or nil
// when it cannot be prepared or the cache is full.
func (t *Tx) stmt(query string) *sql.Stmt {
	if st, ok := t.bound[query]; ok {
		return st
	}
	key := stmtKey{t.db, query}
	if v, ok := t.s.stmts.Load(key); ok {
		st := t.tx.Stmt(v.(*sql.Stmt))
		if t.bound != nil {
			t.bound[query] = st
		}
		return st
	}
	if t.s.stmtSize.Load() >= maxCachedStmts {
		return nil
	}
	// Preparing needs a connection, and a one-connection pool has none
	// while the transaction holds it: prepare once the transaction ends.
	if !slices.Contains(t.misses, query) {
		t.misses = append(t.misses, query)
	}
	return nil
}

// cacheMisses prepares the statements the transaction ran uncached, so the
// next one finds them.
func (t *Tx) cacheMisses() {
	for _, query := range t.misses {
		if t.s.stmtSize.Load() >= maxCachedStmts {
			return
		}
		prepared, err := t.db.Prepare(query)
		if err != nil {
			continue
		}
		if _, loaded := t.s.stmts.LoadOrStore(stmtKey{t.db, query}, prepared); loaded {
			prepared.Close()
			continue
		}
		t.s.stmtSize.Add(1)
	}
}

func (t *Tx) execRaw(query string, args ...any) (sql.Result, error) {
	if st := t.stmt(query); st != nil {
		return st.Exec(args...)
	}
	return t.tx.Exec(query, args...)
}

func (t *Tx) query(query string, args ...any) (*sql.Rows, error) {
	if st := t.stmt(query); st != nil {
		// The statement bound to the transaction is released with the
		// rows by the transaction; closing it here would close them.
		return st.Query(args...)
	}
	return t.tx.Query(query, args...)
}

func (t *Tx) queryRow(query string, args ...any) *sql.Row {
	if st := t.stmt(query); st != nil {
		return st.QueryRow(args...)
	}
	return t.tx.QueryRow(query, args...)
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

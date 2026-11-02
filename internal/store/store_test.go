package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func openStores(t *testing.T) map[string]*Store {
	t.Helper()
	ctx := context.Background()
	out := map[string]*Store{}
	for name, path := range map[string]string{
		"memory": Memory,
		"file":   filepath.Join(t.TempDir(), "osb.db"),
	} {
		s, err := Open(ctx, path)
		if err != nil {
			t.Fatalf("Open(%s) error: %v", name, err)
		}
		t.Cleanup(func() { s.Close() })
		out[name] = s
	}
	return out
}

func TestMessageLifecycle(t *testing.T) {
	for name, s := range openStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			q := &Entity{Kind: "queue", Path: "orders", Name: "Orders", Props: []byte("{}")}
			err := s.Write(ctx, func(tx *Tx) error {
				if err := tx.InsertEntity(q); err != nil {
					return err
				}
				first, err := tx.AllocSequence(q.ID, 3)
				if err != nil {
					return err
				}
				if first != 1 {
					t.Errorf("AllocSequence() first = %d, want 1", first)
				}
				for i := range 3 {
					m := &Message{EntityID: q.ID, Seq: first + int64(i), EnqueuedAt: 10, VisibleAt: 10, Payload: []byte{byte(i)}}
					if i == 1 {
						m.SessionID = "s1"
					}
					if i == 2 {
						m.State, m.VisibleAt = StateScheduled, 500
					}
					if err := tx.InsertMessage(m); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatalf("Write() error: %v", err)
			}

			err = s.Write(ctx, func(tx *Tx) error {
				avail, err := tx.Available(AvailableQuery{EntityID: q.ID, Now: 100, Limit: 10})
				if err != nil {
					return err
				}
				if len(avail) != 2 || avail[0].Seq != 1 || avail[1].Seq != 2 {
					t.Errorf("Available() = %d messages, want seq 1 and 2", len(avail))
				}
				avail[0].LockToken = []byte("token-0123456789")
				avail[0].LockedUntil = 200
				if err := tx.UpdateMessage(avail[0]); err != nil {
					return err
				}
				avail, err = tx.Available(AvailableQuery{EntityID: q.ID, Now: 100, Limit: 10})
				if err != nil {
					return err
				}
				if len(avail) != 1 || avail[0].SessionID != "s1" {
					t.Errorf("Available() after lock = %v, want only the session message", avail)
				}
				session, err := tx.Available(AvailableQuery{EntityID: q.ID, Now: 100, Limit: 10, Session: "s1", HasSession: true})
				if err != nil {
					return err
				}
				if len(session) != 1 {
					t.Errorf("Available(session s1) = %d messages, want 1", len(session))
				}
				next, err := tx.NextTimer(100)
				if err != nil {
					return err
				}
				if next != 200 {
					t.Errorf("NextTimer(100) = %d, want 200 (the lock)", next)
				}
				m, err := tx.MessageByLock([]byte("token-0123456789"))
				if err != nil {
					return err
				}
				if m.Seq != 1 {
					t.Errorf("MessageByLock() seq = %d, want 1", m.Seq)
				}
				if _, err := tx.MessageByLock([]byte("missing")); !errors.Is(err, ErrNotFound) {
					t.Errorf("MessageByLock(missing) error = %v, want ErrNotFound", err)
				}
				due, err := tx.DueScheduled(600, 10)
				if err != nil {
					return err
				}
				if len(due) != 1 || due[0].Seq != 3 {
					t.Errorf("DueScheduled(600) = %v, want seq 3", due)
				}
				c, err := tx.CountMessages(q.ID)
				if err != nil {
					return err
				}
				if c.Active != 2 || c.Scheduled != 1 {
					t.Errorf("CountMessages() = %+v, want 2 active and 1 scheduled", c)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("Write() error: %v", err)
			}
		})
	}
}

func TestDeleteEntityCascades(t *testing.T) {
	s := openStores(t)["file"]
	ctx := context.Background()
	err := s.Write(ctx, func(tx *Tx) error {
		topic := &Entity{Kind: "topic", Path: "t", Name: "t", Props: []byte("{}")}
		if err := tx.InsertEntity(topic); err != nil {
			return err
		}
		sub := &Entity{Kind: "subscription", Path: "t/subscriptions/s", Name: "s", ParentID: topic.ID, Props: []byte("{}")}
		if err := tx.InsertEntity(sub); err != nil {
			return err
		}
		if err := tx.InsertMessage(&Message{EntityID: sub.ID, Seq: 1, Payload: []byte{1}}); err != nil {
			return err
		}
		if err := tx.InsertRule(&Rule{SubscriptionID: sub.ID, Name: "$Default", Props: []byte("{}")}); err != nil {
			return err
		}
		return tx.DeleteEntity(topic.ID)
	})
	if err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	err = s.Read(ctx, func(tx *Tx) error {
		es, err := tx.Entities()
		if err != nil {
			return err
		}
		rules, err := tx.Rules()
		if err != nil {
			return err
		}
		if len(es) != 0 || len(rules) != 0 {
			t.Errorf("after deleting the topic: %d entities, %d rules remain, want none", len(es), len(rules))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}
}

func TestReopenKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "osb.db")
	ctx := context.Background()
	s, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	if err := s.Write(ctx, func(tx *Tx) error {
		return tx.InsertEntity(&Entity{Kind: "queue", Path: "q", Name: "q", Props: []byte("{}")})
	}); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	s.Close()

	s, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen error: %v", err)
	}
	defer s.Close()
	err = s.Read(ctx, func(tx *Tx) error {
		es, err := tx.Entities()
		if len(es) != 1 {
			t.Errorf("Entities() after reopen = %d, want 1", len(es))
		}
		return err
	})
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}
}

func TestOpenPathWithURICharacters(t *testing.T) {
	name := "odd ?#% name.db"
	if runtime.GOOS == "windows" {
		name = "odd #% name.db" // a file name cannot hold "?" there
	}
	path := filepath.Join(t.TempDir(), name)
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open(%q) error: %v", path, err)
	}
	defer s.Close()
	if err := s.Write(context.Background(), func(tx *Tx) error {
		return tx.InsertEntity(&Entity{Kind: "queue", Path: "q", Name: "q", Props: []byte("{}")})
	}); err != nil {
		t.Fatalf("Write() error: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("database not created at %q: %v", path, err)
	}
}

func TestEntitySizes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sizes.db")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var (
		e  = &Entity{Kind: "queue", Path: "q", Name: "q", Props: []byte("{}")}
		m1 = &Message{Seq: 1, Payload: make([]byte, 100)}
		m2 = &Message{Seq: 2, Payload: make([]byte, 50)}
	)
	size := func(st *Store) int64 {
		var n int64
		if err := st.Read(ctx, func(tx *Tx) error {
			var err error
			n, err = tx.EntitySize(e.ID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	err = st.Write(ctx, func(tx *Tx) error {
		if err := tx.InsertEntity(e); err != nil {
			return err
		}
		m1.EntityID, m2.EntityID = e.ID, e.ID
		if err := tx.InsertMessage(m1); err != nil {
			return err
		}
		return tx.InsertMessage(m2)
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := size(st); got != 150 {
		t.Errorf("size after inserts = %d, want 150", got)
	}
	err = st.Write(ctx, func(tx *Tx) error {
		m1.Payload = make([]byte, 30)
		m1.LockedUntil = 5 // An update that only locks keeps the total.
		if err := tx.UpdateMessage(m1); err != nil {
			return err
		}
		return tx.DeleteMessage(m2.ID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := size(st); got != 30 {
		t.Errorf("size after update and delete = %d, want 30", got)
	}
	// Totals are recomputed when the store opens.
	st.Write(ctx, func(tx *Tx) error {
		_, err := tx.tx.Exec(`UPDATE entity_sizes SET size_bytes = 999`)
		return err
	})
	st.Close()
	st, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if got := size(st); got != 30 {
		t.Errorf("size after reopening = %d, want the recomputed 30", got)
	}
	if err := st.Write(ctx, func(tx *Tx) error { return tx.InsertMessage(&Message{EntityID: 9999, Payload: []byte("x")}) }); !errors.Is(err, ErrEntityGone) {
		t.Errorf("InsertMessage(deleted entity) error = %v, want ErrEntityGone", err)
	}
}

func TestRecordMessageID(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, Memory)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	e := &Entity{Kind: "queue", Path: "q", Name: "q", Props: []byte("{}")}
	err = st.Write(ctx, func(tx *Tx) error {
		if err := tx.InsertEntity(e); err != nil {
			return err
		}
		for _, tt := range []struct {
			now, since int64
			want       bool
		}{
			{now: 100, since: 0, want: false},   // First sight.
			{now: 150, since: 50, want: true},   // Within the window.
			{now: 250, since: 150, want: false}, // Window passed: recorded again.
			{now: 300, since: 200, want: true},
		} {
			dup, err := tx.RecordMessageID(e.ID, "m", tt.now, tt.since)
			if err != nil {
				return err
			}
			if dup != tt.want {
				t.Errorf("RecordMessageID(now %d) = %v, want %v", tt.now, dup, tt.want)
			}
		}
		_, err = tx.PurgeMessageIDs(e.ID, 300, 10)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestOpenMigratesOldMessagesTable checks that a database created before
// messages had origin_seq gets the column.
func TestOpenMigratesOldMessagesTable(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/old.db"
	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = old.Exec(`CREATE TABLE entities (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, path TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
			parent_id INTEGER, props TEXT NOT NULL, next_seq INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, accessed_at INTEGER NOT NULL);
		CREATE TABLE messages (id INTEGER PRIMARY KEY, entity_id INTEGER NOT NULL, subqueue INTEGER NOT NULL DEFAULT 0, seq INTEGER NOT NULL,
			state INTEGER NOT NULL DEFAULT 0, session_id TEXT, message_id TEXT, enqueued_at INTEGER NOT NULL, visible_at INTEGER NOT NULL,
			expires_at INTEGER NOT NULL DEFAULT 0, delivery_count INTEGER NOT NULL DEFAULT 0, lock_token BLOB, locked_until INTEGER NOT NULL DEFAULT 0,
			dead_letter_source TEXT, payload BLOB NOT NULL);`)
	if err != nil {
		t.Fatal(err)
	}
	old.Close()

	st, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open() on an old database error: %v", err)
	}
	defer st.Close()
	// Opening again finds the column and changes nothing.
	st2, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("second Open() error: %v", err)
	}
	st2.Close()
	err = st.Write(ctx, func(tx *Tx) error {
		e := &Entity{Kind: "queue", Path: "q", Name: "q", Props: []byte("{}")}
		if err := tx.InsertEntity(e); err != nil {
			return err
		}
		return tx.InsertMessage(&Message{EntityID: e.ID, Seq: 1, State: StateScheduled, OriginSeq: 7, Payload: []byte("x")})
	})
	if err != nil {
		t.Fatalf("writing after the migration: %v", err)
	}
	st.Read(ctx, func(tx *Tx) error {
		rows, err := tx.ScheduledBySeq(1, []int64{7})
		if err != nil || len(rows) != 1 {
			t.Errorf("ScheduledBySeq(origin 7) = %d, %v; want the copy", len(rows), err)
		}
		return nil
	})
}

func TestReplicationReplaysAndCopies(t *testing.T) {
	ctx := context.Background()
	primary, _ := Open(ctx, Memory)
	defer primary.Close()
	replica, _ := Open(ctx, Memory)
	defer replica.Close()

	// A copy carries what the primary has, then statements keep it equal.
	var q *Entity
	err := primary.Write(ctx, func(tx *Tx) error {
		q = &Entity{Kind: "queue", Path: "q", Name: "q", Props: []byte("{}")}
		if err := tx.InsertEntity(q); err != nil {
			return err
		}
		return tx.InsertMessage(&Message{EntityID: q.ID, Seq: 1, Payload: []byte("first")})
	})
	if err != nil {
		t.Fatal(err)
	}
	var batches [][]Stmt
	if err := primary.CopyTo(ctx, replica, func() {
		primary.SetReplicator(func(_ context.Context, stmts []Stmt) error {
			batches = append(batches, stmts)
			return replica.ApplyBatch(ctx, stmts)
		})
	}); err != nil {
		t.Fatalf("CopyTo() error: %v", err)
	}
	size := func(s *Store) (n int64) {
		s.Read(ctx, func(tx *Tx) error { n, _ = tx.EntitySize(q.ID); return nil })
		return
	}
	if size(replica) != 5 {
		t.Fatalf("the copy's entity size = %d, want 5 (recomputed)", size(replica))
	}
	for i := 0; i < 3; i++ {
		err := primary.Write(ctx, func(tx *Tx) error {
			seq, err := tx.AllocSequence(q.ID, 1)
			if err != nil {
				return err
			}
			return tx.InsertMessage(&Message{EntityID: q.ID, Seq: seq, Payload: []byte("more")})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// A rolled back transaction replicates nothing.
	primary.Write(ctx, func(tx *Tx) error {
		tx.InsertMessage(&Message{EntityID: q.ID, Seq: 99, Payload: []byte("rolled back")})
		return context.Canceled
	})
	if len(batches) != 3 {
		t.Errorf("batches = %d, want 3", len(batches))
	}
	same := func(s *Store) (counts Counts, next int64) {
		s.Read(ctx, func(tx *Tx) error {
			counts, _ = tx.CountMessages(q.ID)
			next, _ = tx.AllocSequenceNext(q.ID)
			return nil
		})
		return
	}
	pc, pn := same(primary)
	rc, rn := same(replica)
	if pc != rc || pn != rn || pc.Active != 4 {
		t.Errorf("primary %+v next %d, replica %+v next %d; want equal, with 4 active", pc, pn, rc, rn)
	}
	if size(replica) != size(primary) {
		t.Errorf("sizes: primary %d, replica %d", size(primary), size(replica))
	}

	// A gate refuses writes, and nothing is written.
	primary.SetGate(func() error { return ErrThrottled })
	if err := primary.Write(ctx, func(tx *Tx) error { t.Error("a gated write ran"); return nil }); !errors.Is(err, ErrThrottled) {
		t.Errorf("gated Write() error = %v, want ErrThrottled", err)
	}
	primary.SetGate(nil)
}

// TestSoloWritesCommitAlone checks that writes marked WithSolo are never
// grouped, while the others may be: the replicator sees one call per commit.
func TestSoloWritesCommitAlone(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(t.TempDir(), "osb.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var mu sync.Mutex
	commits := 0
	s.SetReplicator(func(_ context.Context, stmts []Stmt) error {
		mu.Lock()
		commits++
		mu.Unlock()
		return nil
	})
	run := func(ctx context.Context, n int) int {
		mu.Lock()
		commits = 0
		mu.Unlock()
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := s.Write(ctx, func(tx *Tx) error { return tx.SetSetting("k", "v") })
				if err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		return commits
	}
	if got := run(WithSolo(ctx), 40); got != 40 {
		t.Errorf("40 solo writes made %d commits, want 40", got)
	}
	if got := run(ctx, 40); got > 40 || got < 1 {
		t.Errorf("40 writes made %d commits", got)
	}
}

func TestNarrowUpdatesAndCounts(t *testing.T) {
	for name, s := range openStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			q := &Entity{Kind: "queue", Path: "q", Name: "Q", Props: []byte("{}")}
			var ms []*Message
			err := s.Write(ctx, func(tx *Tx) error {
				if err := tx.InsertEntity(q); err != nil {
					return err
				}
				for i, state := range []int{StateActive, StateActive, StateDeferred, StateScheduled} {
					m := &Message{EntityID: q.ID, Seq: int64(i + 1), State: state, EnqueuedAt: 1, VisibleAt: 1, Payload: []byte("abcd")}
					if err := tx.InsertMessage(m); err != nil {
						return err
					}
					ms = append(ms, m)
				}
				dl := &Message{EntityID: q.ID, Subqueue: SubqueueDeadLetter, Seq: 9, EnqueuedAt: 1, VisibleAt: 1, Payload: []byte("ab")}
				return tx.InsertMessage(dl)
			})
			if err != nil {
				t.Fatal(err)
			}
			err = s.Write(ctx, func(tx *Tx) error {
				c, err := tx.CountMessages(q.ID)
				if err != nil {
					return err
				}
				if want := (Counts{Active: 2, DeadLetter: 1, Scheduled: 1, Deferred: 1, SizeBytes: 18}); c != want {
					t.Errorf("CountMessages() = %+v, want %+v", c, want)
				}
				m := ms[0]
				m.DeliveryCount, m.LockToken, m.LockedUntil = 1, []byte("tok"), 500
				if err := tx.LockMessage(m); err != nil {
					return err
				}
				got, err := tx.MessageByLock([]byte("tok"))
				if err != nil || got.ID != m.ID || got.DeliveryCount != 1 || got.LockedUntil != 500 || string(got.Payload) != "abcd" {
					t.Errorf("after LockMessage: %+v, %v", got, err)
				}
				got.LockedUntil = 900
				if err := tx.RenewMessageLock(got); err != nil {
					return err
				}
				got.LockToken, got.LockedUntil, got.DeliveryCount = nil, 0, 0
				if err := tx.UnlockMessage(got); err != nil {
					return err
				}
				if _, err := tx.MessageByLock([]byte("tok")); !errors.Is(err, ErrNotFound) {
					t.Errorf("MessageByLock after UnlockMessage = %v, want ErrNotFound", err)
				}
				ms[3].State = StateActive
				if err := tx.ActivateMessage(ms[3]); err != nil {
					return err
				}
				avail, err := tx.Available(AvailableQuery{EntityID: q.ID, Now: 10, Limit: 10})
				if err != nil {
					return err
				}
				if len(avail) != 3 || avail[0].LockedUntil != 0 || avail[0].DeliveryCount != 0 {
					t.Errorf("Available() = %d messages, first %+v; want 3 unlocked", len(avail), avail[0])
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestHotQueriesUseIndexes guards the plans of the queries that run per
// message or per timer run, so a change to them does not bring back scans.
func TestHotQueriesUseIndexes(t *testing.T) {
	s := openStores(t)["memory"]
	ctx := context.Background()
	for _, tc := range []struct{ name, query, want string }{
		{"peek", `SELECT id FROM messages WHERE entity_id = +1 AND subqueue = +1 AND seq >= 0 ORDER BY seq`, "messages_entity_subqueue_seq"},
		{"available", `SELECT id FROM messages WHERE entity_id = +1 AND subqueue = +0 AND state = 0 AND visible_at <= 5 AND locked_until <= 5 ORDER BY seq`, "messages_order"},
		{"session locks", `UPDATE messages SET lock_token = NULL, locked_until = 0 WHERE entity_id = 1 AND subqueue IN (0, 1, 2) AND session_id = 'x' AND lock_token IS NOT NULL`, "messages_by_session"},
		{"ready", `SELECT id FROM messages WHERE entity_id = +1 AND subqueue = +0 AND state = 0 AND lock_token IS NULL AND visible_at <= 5 AND locked_until <= 5 ORDER BY seq LIMIT 10`, "messages_ready"},
		{"ready in a session", `SELECT id FROM messages WHERE entity_id = +1 AND subqueue = +0 AND state = 0 AND lock_token IS NULL AND visible_at <= 5 AND locked_until <= 5 AND session_id = +'x' ORDER BY seq LIMIT 10`, "messages_by_session"},
		{"lapsed locks", `SELECT id FROM messages INDEXED BY messages_locked WHERE entity_id = +1 AND subqueue = +0 AND state = 0 AND locked_until > 0 AND locked_until <= 5 AND visible_at <= 5 ORDER BY seq LIMIT 10`, "messages_locked"},
		{"purge duplicates", `SELECT message_id FROM duplicates WHERE entity_id = +1 AND seen_at <= 5 ORDER BY seen_at, message_id LIMIT 10`, "duplicates_seen"},
		{"session timers", `SELECT entity_id FROM sessions WHERE locked_until > 0 AND locked_until > 1 AND locked_until <= 9`, "sessions_locked"},
	} {
		rows, err := s.write.QueryContext(ctx, "EXPLAIN QUERY PLAN "+tc.query)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		var plan string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan += detail + "\n"
		}
		rows.Close()
		if !strings.Contains(plan, tc.want) || strings.Contains(plan, "SCAN") {
			t.Errorf("%s plan:\n%s\nwant a search on %s and no scan", tc.name, plan, tc.want)
		}
	}
	var n int
	if err := s.write.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE name = 'messages_seq'`).Scan(&n); err != nil || n != 0 {
		t.Errorf("messages_seq exists (%d, %v); it was replaced", n, err)
	}
}

func TestAvailableSkipsLockedHeadAndFindsLapsedLocks(t *testing.T) {
	for name, s := range openStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			q := &Entity{Kind: "queue", Path: "q", Name: "Q", Props: []byte("{}")}
			err := s.Write(ctx, func(tx *Tx) error {
				if err := tx.InsertEntity(q); err != nil {
					return err
				}
				// seq 1-3 locked (1 lapsed at now=100), 4-6 free.
				for i := int64(1); i <= 6; i++ {
					m := &Message{EntityID: q.ID, Seq: i, EnqueuedAt: 1, VisibleAt: 1, Payload: []byte("x")}
					if i <= 3 {
						m.LockToken = []byte{byte(i)}
						m.LockedUntil = 1000
					}
					if i == 1 {
						m.LockedUntil = 50
					}
					if err := tx.InsertMessage(m); err != nil {
						return err
					}
				}
				got, err := tx.Available(AvailableQuery{EntityID: q.ID, Now: 100, Limit: 3})
				if err != nil {
					return err
				}
				if seqs := seqsOf(got); len(got) != 3 || seqs[0] != 1 || seqs[1] != 4 || seqs[2] != 5 {
					t.Errorf("Available() sequences = %v, want [1 4 5]", seqs)
				}
				has, err := tx.HasLapsedLocks(100)
				if err != nil || !has {
					t.Errorf("HasLapsedLocks() = %v, %v; want true", has, err)
				}
				ents, n, err := tx.ReleaseLapsedLocks(100, 10)
				if err != nil || n != 1 || len(ents) != 1 || ents[0] != q.ID {
					t.Errorf("ReleaseLapsedLocks() = %v, %d, %v; want [%d], 1", ents, n, err, q.ID)
				}
				if has, _ := tx.HasLapsedLocks(100); has {
					t.Error("a lapsed lock is left after ReleaseLapsedLocks")
				}
				got, err = tx.Available(AvailableQuery{EntityID: q.ID, Now: 100, Limit: 10})
				if err != nil {
					return err
				}
				if seqs := seqsOf(got); len(got) != 4 || seqs[0] != 1 || seqs[1] != 4 {
					t.Errorf("Available() after release = %v, want [1 4 5 6]", seqs)
				}
				if got[0].LockToken != nil || got[0].LockedUntil != 0 {
					t.Errorf("released message still has a lock: %+v", got[0])
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func seqsOf(ms []*Message) []int64 {
	var out []int64
	for _, m := range ms {
		out = append(out, m.Seq)
	}
	return out
}

// BenchmarkAvailableLockedHead shows that the cost of Available does not
// grow with the messages locked at the head of the queue.
func BenchmarkAvailableLockedHead(b *testing.B) {
	for _, locked := range []int{0, 2000, 20000} {
		b.Run(fmt.Sprint(locked), func(b *testing.B) {
			ctx := context.Background()
			s, err := Open(ctx, Memory)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			q := &Entity{Kind: "queue", Path: "q", Name: "Q", Props: []byte("{}")}
			err = s.Write(ctx, func(tx *Tx) error {
				if err := tx.InsertEntity(q); err != nil {
					return err
				}
				for i := 1; i <= locked+100; i++ {
					m := &Message{EntityID: q.ID, Seq: int64(i), EnqueuedAt: 1, VisibleAt: 1, Payload: []byte("x")}
					if i <= locked {
						m.LockToken = []byte(fmt.Sprint(i))
						m.LockedUntil = 1 << 40
					}
					if err := tx.InsertMessage(m); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for range b.N {
				err := s.Read(ctx, func(tx *Tx) error {
					_, err := tx.Available(AvailableQuery{EntityID: q.ID, Now: 100, Limit: 10})
					return err
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestMessageHeaderByLockSkipsPayload(t *testing.T) {
	s := openStores(t)["memory"]
	ctx := context.Background()
	q := &Entity{Kind: "queue", Path: "q", Name: "Q", Props: []byte("{}")}
	err := s.Write(ctx, func(tx *Tx) error {
		if err := tx.InsertEntity(q); err != nil {
			return err
		}
		m := &Message{EntityID: q.ID, Seq: 7, EnqueuedAt: 1, VisibleAt: 1, LockToken: []byte("tok"), LockedUntil: 9, DeliveryCount: 2, Payload: []byte("body")}
		if err := tx.InsertMessage(m); err != nil {
			return err
		}
		full, err := tx.MessageByLock([]byte("tok"))
		if err != nil {
			return err
		}
		head, err := tx.MessageHeaderByLock([]byte("tok"))
		if err != nil {
			return err
		}
		if head.Payload != nil || string(full.Payload) != "body" {
			t.Errorf("payloads: header %q, full %q", head.Payload, full.Payload)
		}
		head.Payload = full.Payload
		if diff := cmp.Diff(full, head); diff != "" {
			t.Errorf("header differs from the full message (-full +header):\n%s", diff)
		}
		if _, err := tx.MessageHeaderByLock([]byte("none")); !errors.Is(err, ErrNotFound) {
			t.Errorf("MessageHeaderByLock(unknown) = %v, want ErrNotFound", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPurgeMessageIDsInBatches(t *testing.T) {
	s := openStores(t)["memory"]
	ctx := context.Background()
	e := &Entity{Kind: "queue", Path: "q", Name: "Q", Props: []byte("{}")}
	err := s.Write(ctx, func(tx *Tx) error {
		if err := tx.InsertEntity(e); err != nil {
			return err
		}
		for i := range 25 {
			if _, err := tx.RecordMessageID(e.ID, fmt.Sprint("id", i), int64(i), -1); err != nil {
				return err
			}
		}
		var sizes []int
		for {
			n, err := tx.PurgeMessageIDs(e.ID, 19, 8) // ids 0 to 19 are old
			if err != nil {
				return err
			}
			sizes = append(sizes, n)
			if n < 8 {
				break
			}
		}
		if fmt.Sprint(sizes) != "[8 8 4]" {
			t.Errorf("batches = %v, want [8 8 4]", sizes)
		}
		// The newest ids are still known.
		if dup, err := tx.RecordMessageID(e.ID, "id24", 100, 0); err != nil || !dup {
			t.Errorf("RecordMessageID(id24) = %v, %v; want a duplicate", dup, err)
		}
		if dup, err := tx.RecordMessageID(e.ID, "id3", 100, 0); err != nil || dup {
			t.Errorf("RecordMessageID(purged id3) = %v, %v; want a new id", dup, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// scanCounts counts by reading every message, which is what CountMessages did
// before the counts were kept by triggers.
func scanCounts(tx *Tx, entityID int64) (map[[2]int64]int64, error) {
	rows, err := tx.query(`SELECT subqueue, state, COUNT(*) FROM messages WHERE entity_id = ? GROUP BY subqueue, state`, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[[2]int64]int64{}
	for rows.Next() {
		var sq, st, n int64
		if err := rows.Scan(&sq, &st, &n); err != nil {
			return nil, err
		}
		out[[2]int64{sq, st}] = n
	}
	return out, rows.Err()
}

func TestEntityCountsFollowMessages(t *testing.T) {
	for name, s := range openStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			a := &Entity{Kind: "queue", Path: "a", Name: "A", Props: []byte("{}")}
			b := &Entity{Kind: "queue", Path: "b", Name: "B", Props: []byte("{}")}
			var ms []*Message
			check := func(step string) {
				t.Helper()
				err := s.Read(ctx, func(tx *Tx) error {
					for _, e := range []*Entity{a, b} {
						want, err := scanCounts(tx, e.ID)
						if err != nil {
							return err
						}
						c, err := tx.CountMessages(e.ID)
						if err != nil {
							return err
						}
						got := Counts{}
						got.Active, got.DeadLetter = want[[2]int64{SubqueueActive, StateActive}], want[[2]int64{SubqueueDeadLetter, StateActive}]
						got.Scheduled, got.Deferred = want[[2]int64{SubqueueActive, StateScheduled}], want[[2]int64{SubqueueActive, StateDeferred}]
						got.TransferDeadLetter = want[[2]int64{SubqueueTransferDLQ, StateActive}]
						got.SizeBytes = c.SizeBytes
						if c != got {
							t.Errorf("%s: %s: CountMessages() = %+v, scan says %+v", step, e.Path, c, got)
						}
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			write := func(f func(tx *Tx) error) {
				t.Helper()
				if err := s.Write(ctx, f); err != nil {
					t.Fatal(err)
				}
			}
			write(func(tx *Tx) error {
				if err := tx.InsertEntity(a); err != nil {
					return err
				}
				if err := tx.InsertEntity(b); err != nil {
					return err
				}
				for i := 1; i <= 12; i++ {
					m := &Message{EntityID: a.ID, Seq: int64(i), EnqueuedAt: 1, VisibleAt: 1, Payload: []byte("xy")}
					if err := tx.InsertMessage(m); err != nil {
						return err
					}
					ms = append(ms, m)
				}
				return nil
			})
			check("insert")
			write(func(tx *Tx) error { // state changes, a dead letter, a move to another entity
				ms[0].State = StateDeferred
				ms[1].State = StateScheduled
				ms[2].Subqueue = SubqueueDeadLetter
				ms[3].EntityID = b.ID
				for _, m := range ms[:4] {
					if err := tx.UpdateMessage(m); err != nil {
						return err
					}
				}
				return nil
			})
			check("update")
			write(func(tx *Tx) error { // updates that touch neither group nor entity
				for _, m := range ms[4:8] {
					m.DeliveryCount++
					m.Payload = []byte("longer payload")
					if err := tx.UpdateMessage(m); err != nil {
						return err
					}
				}
				return nil
			})
			check("same group")
			write(func(tx *Tx) error {
				if err := tx.DeleteMessage(ms[5].ID); err != nil {
					return err
				}
				return tx.DeleteAllMessages(a.ID, SubqueueDeadLetter)
			})
			check("delete")
			// A rolled back write changes nothing.
			s.Write(ctx, func(tx *Tx) error {
				tx.DeleteAllMessages(a.ID, SubqueueActive)
				return context.Canceled
			})
			check("rollback")
		})
	}
}

// BenchmarkCountMessages shows that counting does not grow with the messages
// in the queue.
func BenchmarkCountMessages(b *testing.B) {
	for _, n := range []int{1000, 20000, 200000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			ctx := context.Background()
			s, err := Open(ctx, Memory)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			q := &Entity{Kind: "queue", Path: "q", Name: "Q", Props: []byte("{}")}
			err = s.Write(ctx, func(tx *Tx) error {
				if err := tx.InsertEntity(q); err != nil {
					return err
				}
				for i := 1; i <= n; i++ {
					if err := tx.InsertMessage(&Message{EntityID: q.ID, Seq: int64(i), EnqueuedAt: 1, VisibleAt: 1, Payload: []byte("x")}); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for range b.N {
				if err := s.Read(ctx, func(tx *Tx) error { _, err := tx.CountMessages(q.ID); return err }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkInsertDeleteMessage is the write cost of the triggers that keep
// sizes and counts: one insert and one delete per iteration.
func BenchmarkInsertDeleteMessage(b *testing.B) {
	ctx := context.Background()
	s, err := Open(ctx, Memory)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	q := &Entity{Kind: "queue", Path: "q", Name: "Q", Props: []byte("{}")}
	if err := s.Write(ctx, func(tx *Tx) error { return tx.InsertEntity(q) }); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := range b.N {
		err := s.Write(ctx, func(tx *Tx) error {
			m := &Message{EntityID: q.ID, Seq: int64(i + 1), EnqueuedAt: 1, VisibleAt: 1, Payload: []byte("payload")}
			if err := tx.InsertMessage(m); err != nil {
				return err
			}
			return tx.DeleteMessage(m.ID)
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestAfterCommitRunsOnlyOnCommit(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Memory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	var ran []string
	if err := s.Write(ctx, func(tx *Tx) error {
		tx.AfterCommit(func() { ran = append(ran, "committed") })
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	failed := errors.New("rolled back")
	if err := s.Write(ctx, func(tx *Tx) error {
		tx.AfterCommit(func() { ran = append(ran, "rolled back") })
		return failed
	}); !errors.Is(err, failed) {
		t.Fatalf("Write() error = %v", err)
	}
	if len(ran) != 1 || ran[0] != "committed" {
		t.Errorf("AfterCommit ran %v, want only [committed]", ran)
	}
}

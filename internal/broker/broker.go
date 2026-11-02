// Package broker implements Service Bus semantics independently of the
// wire protocol: entities, sequencing, locks, settlement, dead-lettering,
// time to live, scheduling and deferral. State is persisted in a
// store.Store; the broker keeps an in-memory index of entities and wakes
// waiting receivers when messages become available.
package broker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// Options configures a Broker.
type Options struct {
	// Now returns the current time. Nil uses time.Now.
	Now func() time.Time
	// Logger receives diagnostics. Nil disables logging.
	Logger *slog.Logger
	// Events, if set, receives one record for each thing that happens to a
	// message: sent, routed, received, settled, dead-lettered, expired. See
	// events.go.
	Events *slog.Logger
	// OnDelete, if set, is called with the path of each queue or topic
	// after it is deleted, for state kept elsewhere such as its shared
	// access policies.
	OnDelete func(ctx context.Context, path string)
	// Partitions is how many partitions a partitioned entity has. Zero
	// means DefaultPartitions.
	Partitions int
	// MaxBatchDelete is the most messages one DeleteMessages call removes.
	// Zero means DefaultMaxBatchDelete.
	MaxBatchDelete int
	// MetricsRetention is how long metrics and the activity log are kept.
	// Zero means DefaultMetricsRetention.
	MetricsRetention time.Duration
}

// Broker holds the entities of one namespace.
type Broker struct {
	store *store.Ref
	now   func() time.Time
	log   *slog.Logger
	// events is nil unless Options.Events is set.
	events *slog.Logger

	// adminMu serializes entity creation, updates and deletion. It is
	// held across the store write, so it must never be taken inside a
	// store transaction.
	adminMu sync.Mutex

	// mu guards the in-memory indexes. It is never held across a store
	// write, and never taken inside a store transaction: the store has a
	// single writer connection, so either would deadlock against a
	// transaction waiting for mu.
	mu       sync.RWMutex
	entities map[string]*Entity // by normalized path
	byID     map[int64]*Entity
	rules    map[int64][]*compiledRule // by subscription id
	waiters  map[int64]chan struct{}

	// routes is republished by publishLocked after every change of
	// entities or rules.
	routes atomic.Pointer[routes]

	// act holds the use of entities by receive and peek, which write nothing.
	act activity

	// lastPurge is when the timer loop last forgot old message ids.
	lastPurge time.Time

	// rec counts events in memory until flushMetrics writes them. obsMu
	// keeps a flush from hiding counters from a concurrent Metrics read.
	rec       *metrics.Recorder
	obsMu     sync.RWMutex
	sampled   int64 // minute of the last gauge sample
	retention time.Duration
	conns     atomic.Int64
	onDelete  func(ctx context.Context, path string)
	maxBatch  int
	readOnly  atomic.Bool

	metaMu sync.RWMutex
	meta   Meta

	hookMu       sync.Mutex
	metadataHook func()
	partitions   int

	poke chan struct{}
	// nextWake is the deadline the timer loop sleeps until, in Unix
	// milliseconds, or 0 while it runs or has nothing to wait for.
	nextWake atomic.Int64
	cancel   context.CancelFunc
	done     chan struct{}
}

// New loads the entities in st and starts the background timers. Close
// stops them; it does not close st.
func New(ctx context.Context, st *store.Store, opts Options) (*Broker, error) {
	b := &Broker{
		store:    store.NewRef(st),
		now:      opts.Now,
		log:      opts.Logger,
		events:   opts.Events,
		entities: make(map[string]*Entity),
		byID:     make(map[int64]*Entity),
		rules:    make(map[int64][]*compiledRule),
		waiters:  make(map[int64]chan struct{}),
		poke:     make(chan struct{}, 1),
		done:     make(chan struct{}),

		rec:        metrics.NewRecorder(),
		retention:  opts.MetricsRetention,
		onDelete:   opts.OnDelete,
		maxBatch:   opts.MaxBatchDelete,
		partitions: opts.Partitions,
	}
	if b.now == nil {
		b.now = time.Now
	}
	if b.log == nil {
		b.log = slog.New(slog.DiscardHandler)
	}
	if b.partitions <= 0 {
		b.partitions = DefaultPartitions
	}
	if b.maxBatch <= 0 {
		b.maxBatch = DefaultMaxBatchDelete
	}
	if b.retention <= 0 {
		b.retention = DefaultMetricsRetention
	}
	if err := b.load(ctx); err != nil {
		return nil, err
	}
	if err := b.loadMeta(ctx); err != nil {
		return nil, err
	}
	// Time the emulator was stopped does not count toward
	// auto-delete-on-idle.
	if err := b.store.Write(ctx, func(tx *store.Tx) error { return tx.TouchAll(b.nowMillis()) }); err != nil {
		return nil, err
	}
	tctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	go b.runTimers(tctx)
	return b, nil
}

// Close stops the background timers.
func (b *Broker) Close() error {
	b.cancel()
	<-b.done
	return b.flushMetrics(context.Background(), b.now(), true)
}

func (b *Broker) load(ctx context.Context) error {
	entities, byID, rules, err := b.readState(ctx)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.entities, b.byID, b.rules = entities, byID, rules
	b.publishLocked()
	b.mu.Unlock()
	return nil
}

// readState reads the entities and rules of the current store.
func (b *Broker) readState(ctx context.Context) (map[string]*Entity, map[int64]*Entity, map[int64][]*compiledRule, error) {
	entities := make(map[string]*Entity)
	byID := make(map[int64]*Entity)
	rules := make(map[int64][]*compiledRule)
	err := b.store.Read(ctx, func(tx *store.Tx) error {
		rows, err := tx.Entities()
		if err != nil {
			return err
		}
		for _, r := range rows {
			e := &Entity{
				id:         r.ID,
				Kind:       EntityKind(r.Kind),
				Path:       r.Path,
				Name:       r.Name,
				CreatedAt:  time.UnixMilli(r.CreatedAt).UTC(),
				UpdatedAt:  time.UnixMilli(r.UpdatedAt).UTC(),
				AccessedAt: time.UnixMilli(r.AccessedAt).UTC(),
			}
			if err := json.Unmarshal(r.Props, &e.Properties); err != nil {
				return fmt.Errorf("broker: entity %s: decoding properties: %w", r.Path, err)
			}
			if r.ParentID != 0 {
				e.Topic = byID[r.ParentID]
			}
			entities[e.Path] = e
			byID[e.id] = e
		}
		rows2, err := tx.Rules()
		if err != nil {
			return err
		}
		for _, r := range rows2 {
			c, err := ruleFromRow(r)
			if err != nil {
				return err
			}
			rules[r.SubscriptionID] = append(rules[r.SubscriptionID], c)
		}
		return nil
	})
	return entities, byID, rules, err
}

// SwapStore makes st, a copy of the current store, the broker's store, and
// reloads the entities and rules from it. Locks held on messages stay valid,
// since they are in the copy. Waiting receivers are woken to look again.
func (b *Broker) SwapStore(ctx context.Context, st *store.Store) error {
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	b.store.Set(st)
	if err := b.load(ctx); err != nil {
		return err
	}
	b.mu.Lock()
	for id, ch := range b.waiters {
		close(ch)
		delete(b.waiters, id)
	}
	b.mu.Unlock()
	b.wakeTimers()
	return nil
}

func (b *Broker) nowMillis() int64 { return b.now().UnixMilli() }

// CreateQueue creates a queue. It returns ErrEntityExists if an entity
// with the same path exists.
func (b *Broker) CreateQueue(ctx context.Context, name string, props Properties) (_ *Entity, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.create_queue")
	defer done(&err)
	if err := validateName(name, true); err != nil {
		return nil, err
	}
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	return b.create(ctx, &Entity{Kind: KindQueue, Path: NormalizePath(name), Name: name, Properties: props})
}

// CreateTopic creates a topic.
func (b *Broker) CreateTopic(ctx context.Context, name string, props Properties) (_ *Entity, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.create_topic")
	defer done(&err)
	if err := validateName(name, true); err != nil {
		return nil, err
	}
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	return b.create(ctx, &Entity{Kind: KindTopic, Path: NormalizePath(name), Name: name, Properties: props})
}

// CreateSubscription creates a subscription on topic.
func (b *Broker) CreateSubscription(ctx context.Context, topic, name string, props Properties) (_ *Entity, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.create_subscription")
	defer done(&err)
	if err := validateName(name, false); err != nil {
		return nil, err
	}
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	b.mu.RLock()
	t := b.entities[NormalizePath(topic)]
	b.mu.RUnlock()
	if t == nil || t.Kind != KindTopic {
		return nil, fmt.Errorf("%w: topic %s", ErrEntityNotFound, topic)
	}
	return b.create(ctx, &Entity{Kind: KindSubscription, Path: SubscriptionPath(topic, name), Name: name, Topic: t, Properties: props})
}

// create stores a new entity and logs it. The caller holds adminMu.
func (b *Broker) create(ctx context.Context, e *Entity) (*Entity, error) {
	if err := b.checkWritable(ctx); err != nil {
		return nil, err
	}
	parent := ""
	if e.Topic != nil {
		parent = e.Topic.Path
	}
	if err := b.checkLocks(ctx, opWrite, parent); err != nil {
		b.audit(ctx, "Create"+kindName(e.Kind), e.Path, err, "")
		return nil, err
	}
	out, err := b.insertEntity(ctx, e)
	b.audit(ctx, "Create"+kindName(e.Kind), e.Path, err, "")
	return out, err
}

func (b *Broker) insertEntity(ctx context.Context, e *Entity) (*Entity, error) {
	if err := e.Properties.Validate(e.Kind); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrInvalidOperation, e.Name, err)
	}
	props, err := json.Marshal(e.Properties)
	if err != nil {
		return nil, err
	}
	now := b.now().UTC().Truncate(time.Millisecond)
	e.CreatedAt, e.UpdatedAt, e.AccessedAt = now, now, now

	b.mu.RLock()
	_, exists := b.entities[e.Path]
	b.mu.RUnlock()
	if exists {
		return nil, fmt.Errorf("%w: %s", ErrEntityExists, e.DisplayPath())
	}
	row := &store.Entity{
		Kind:       string(e.Kind),
		Path:       e.Path,
		Name:       e.Name,
		Props:      props,
		CreatedAt:  now.UnixMilli(),
		UpdatedAt:  now.UnixMilli(),
		AccessedAt: now.UnixMilli(),
	}
	if e.Topic != nil {
		row.ParentID = e.Topic.id
	}
	var rule *compiledRule
	if e.Kind == KindSubscription {
		r := TrueRule(DefaultRuleName)
		r.CreatedAt = now
		rule = &compiledRule{Rule: r}
	}
	err = b.store.Write(ctx, func(tx *store.Tx) error {
		if err := tx.InsertEntity(row); err != nil {
			return err
		}
		if rule == nil {
			return nil
		}
		rr, err := rule.row(row.ID)
		if err != nil {
			return err
		}
		if err := tx.InsertRule(rr); err != nil {
			return err
		}
		rule.id = rr.ID
		return nil
	})
	if err != nil {
		return nil, err
	}
	e.id = row.ID
	b.mu.Lock()
	b.entities[e.Path] = e
	b.byID[e.id] = e
	if rule != nil {
		b.rules[e.id] = []*compiledRule{rule}
	}
	b.publishLocked()
	b.mu.Unlock()
	return e.clone(), nil
}

// UpdateEntity replaces the properties of the entity at path.
func (b *Broker) UpdateEntity(ctx context.Context, path string, props Properties) (_ *Entity, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.update_entity", telemetry.Entity(path))
	defer done(&err)
	if err := b.checkWritable(ctx); err != nil {
		return nil, err
	}
	if err := b.checkLocks(ctx, opWrite, path); err != nil {
		b.audit(ctx, "UpdateEntity", path, err, "")
		return nil, err
	}
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	e, err := b.updateEntity(ctx, path, props)
	op := "UpdateEntity" // Unknown when the entity does not exist.
	if cur, cerr := b.Entity(path); cerr == nil {
		op = "Update" + kindName(cur.Kind)
	}
	b.audit(ctx, op, path, err, "")
	return e, err
}

func (b *Broker) updateEntity(ctx context.Context, path string, props Properties) (*Entity, error) {
	b.mu.RLock()
	e := b.entities[NormalizePath(path)]
	b.mu.RUnlock()
	if e == nil {
		return nil, fmt.Errorf("%w: %s", ErrEntityNotFound, path)
	}
	if err := props.Validate(e.Kind); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrInvalidOperation, e.Name, err)
	}
	raw, err := json.Marshal(props)
	if err != nil {
		return nil, err
	}
	now := b.now().UTC().Truncate(time.Millisecond)
	err = b.store.Write(ctx, func(tx *store.Tx) error {
		// An update is activity for auto-delete-on-idle.
		if err := tx.UpdateEntity(&store.Entity{ID: e.id, Path: e.Path, Props: raw, UpdatedAt: now.UnixMilli(), AccessedAt: now.UnixMilli()}); err != nil {
			return err
		}
		return touchTx(tx, e, now.UnixMilli())
	})
	if err != nil {
		return nil, err
	}
	// Entities are shared with in-flight operations, so replace rather
	// than mutate.
	updated := e.clone()
	updated.Properties = props
	updated.UpdatedAt = now
	updated.AccessedAt = now
	b.mu.Lock()
	b.replaceLocked(e, updated)
	b.notifyLocked(e.id)
	b.publishLocked()
	b.mu.Unlock()
	return updated.clone(), nil
}

func (b *Broker) replaceLocked(old, updated *Entity) {
	b.entities[old.Path] = updated
	b.byID[old.id] = updated
	if old.Kind == KindTopic {
		for _, s := range b.entities {
			if s.Topic != nil && s.Topic.id == old.id {
				ns := s.clone()
				ns.Topic = updated
				b.entities[s.Path] = ns
				b.byID[s.id] = ns
			}
		}
	}
}

// DeleteEntity deletes the entity at path with its messages; deleting a
// topic deletes its subscriptions.
func (b *Broker) DeleteEntity(ctx context.Context, path string) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.delete_entity", telemetry.Entity(path))
	defer done(&err)
	if err := b.checkWritable(ctx); err != nil {
		return err
	}
	if err := b.checkLocks(ctx, opDelete, path); err != nil {
		b.audit(ctx, "DeleteEntity", path, err, "")
		return err
	}
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	return b.deleteLocked(ctx, path, "")
}

// deleteLocked deletes an entity and logs it, with detail. The caller
// holds adminMu.
func (b *Broker) deleteLocked(ctx context.Context, path, detail string) error {
	b.mu.RLock()
	e := b.entities[NormalizePath(path)]
	b.mu.RUnlock()
	if e == nil {
		err := fmt.Errorf("%w: %s", ErrEntityNotFound, path)
		b.audit(ctx, "DeleteEntity", path, err, detail)
		return err
	}
	err := b.store.Write(ctx, func(tx *store.Tx) error { return tx.DeleteEntity(e.id) })
	b.audit(ctx, "Delete"+kindName(e.Kind), e.Path, err, detail)
	if err != nil {
		return err
	}
	if b.onDelete != nil && e.Kind != KindSubscription {
		b.onDelete(ctx, e.Path)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for p, x := range b.entities {
		if x.id == e.id || x.Topic != nil && x.Topic.id == e.id {
			delete(b.entities, p)
			delete(b.byID, x.id)
			delete(b.rules, x.id)
			b.act.forget(x.id)
			b.notifyLocked(x.id)
		}
	}
	b.publishLocked()
	return nil
}

// entitySnapshot returns a copy of the entity index by ID, for use inside
// store transactions where mu must not be taken.
func (b *Broker) entitySnapshot() map[int64]*Entity {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[int64]*Entity, len(b.byID))
	for id, e := range b.byID {
		out[id] = e
	}
	return out
}

// Entity returns the entity at path.
func (b *Broker) Entity(path string) (*Entity, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	e := b.entities[NormalizePath(path)]
	if e == nil {
		return nil, fmt.Errorf("%w: %s", ErrEntityNotFound, path)
	}
	return e.clone(), nil
}

// Entities returns the entities of one kind sorted by path. For
// subscriptions, topic restricts the result to one topic.
func (b *Broker) Entities(kind EntityKind, topic string) []*Entity {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var out []*Entity
	for _, e := range b.entities {
		if e.Kind != kind {
			continue
		}
		if kind == KindSubscription && topic != "" && e.Topic.Path != NormalizePath(topic) {
			continue
		}
		out = append(out, e.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// subscriptionsLocked returns the current subscriptions of topic.
func (b *Broker) subscriptionsLocked(topic *Entity) []*Entity {
	var out []*Entity
	for _, e := range b.entities {
		if e.Kind == KindSubscription && e.Topic.id == topic.id {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// Resolve resolves a link address such as "orders",
// "orders/$DeadLetterQueue" or "events/Subscriptions/audit".
func (b *Broker) Resolve(address string) (Address, error) {
	p, sq := splitAddress(address)
	b.mu.RLock()
	defer b.mu.RUnlock()
	e := b.entities[p]
	if e == nil {
		return Address{}, fmt.Errorf("%w: %s", ErrEntityNotFound, address)
	}
	return Address{Entity: e, Subqueue: sq}, nil
}

// Exists reports whether address resolves to an entity.
func (b *Broker) Exists(address string) bool {
	_, err := b.Resolve(address)
	return err == nil
}

// current returns the latest version of e, or nil if it was deleted.
func (b *Broker) current(e *Entity) *Entity {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.byID[e.id]
}

// Changed returns a channel that is closed the next time messages of the
// entity may have become available. Callers must obtain the channel before
// checking for messages, so no wakeup is missed.
func (b *Broker) Changed(e *Entity) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch, ok := b.waiters[e.id]
	if !ok {
		ch = make(chan struct{})
		b.waiters[e.id] = ch
	}
	return ch
}

func (b *Broker) notify(ids ...int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, id := range ids {
		b.notifyLocked(id)
	}
}

func (b *Broker) notifyLocked(id int64) {
	if ch, ok := b.waiters[id]; ok {
		close(ch)
		delete(b.waiters, id)
	}
}

// wakeForLocks wakes the timer loop for the locks of deliveries, if one
// expires before the deadline it waits for. A receive that takes a lock no
// sooner than the loop's next wake leaves it asleep: the loop computes the
// lock into the deadline when it runs, and each wake reads the whole table.
func (b *Broker) wakeForLocks(ds []*Delivery) {
	if len(ds) == 0 {
		return
	}
	until := ds[0].LockedUntil.UnixMilli()
	for _, d := range ds[1:] {
		until = min(until, d.LockedUntil.UnixMilli())
	}
	next := b.nextWake.Load()
	if next == 0 || next <= b.nowMillis() || until < next {
		b.wakeTimers()
	}
}

// wakeTimers makes the timer loop recompute its next deadline.
func (b *Broker) wakeTimers() {
	select {
	case b.poke <- struct{}{}:
	default:
	}
}

// LockToken identifies a message lock. It is a random (version 4) UUID in
// RFC 4122 byte order.
type LockToken [16]byte

func newLockToken() LockToken {
	var t LockToken
	rand.Read(t[:])
	t[6] = t[6]&0x0f | 0x40
	t[8] = t[8]&0x3f | 0x80
	return t
}

func (t LockToken) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", t[0:4], t[4:6], t[6:8], t[8:10], t[10:])
}

func checkStatus(e *Entity, sending bool) error {
	switch e.Properties.Status {
	case StatusDisabled:
		return fmt.Errorf("%w: %s", ErrEntityDisabled, e.DisplayPath())
	case StatusSendDisabled:
		if sending {
			return fmt.Errorf("%w: sending to %s is disabled", ErrEntityDisabled, e.DisplayPath())
		}
	case StatusReceiveDisabled:
		if !sending {
			return fmt.Errorf("%w: receiving from %s is disabled", ErrEntityDisabled, e.DisplayPath())
		}
	}
	return nil
}

// Counts are the message counts of an entity and when it was last used.
type Counts struct {
	store.Counts
	AccessedAt time.Time
}

// Counts returns the message counts of the entity at path. A topic holds no
// messages itself: its counts add up the scheduled messages and sizes of
// its subscriptions.
func (b *Broker) Counts(ctx context.Context, path string) (Counts, error) {
	e, err := b.Entity(path)
	if err != nil {
		return Counts{}, err
	}
	ids := []int64{e.id}
	if e.Kind == KindTopic {
		ids = nil
		for _, s := range b.Entities(KindSubscription, e.Path) {
			ids = append(ids, s.id)
		}
	}
	var total Counts
	err = b.store.Read(ctx, func(tx *store.Tx) error {
		at, err := tx.AccessedAt(e.id)
		if err != nil {
			return err
		}
		total.AccessedAt = time.UnixMilli(b.act.latest(e.id, at)).UTC()
		for _, id := range ids {
			c, err := tx.CountMessages(id)
			if err != nil {
				return err
			}
			if e.Kind == KindTopic {
				c = store.Counts{Scheduled: c.Scheduled, SizeBytes: c.SizeBytes}
			}
			total.Active += c.Active
			total.DeadLetter += c.DeadLetter
			total.TransferDeadLetter += c.TransferDeadLetter
			total.Scheduled += c.Scheduled
			total.Deferred += c.Deferred
			total.SizeBytes += c.SizeBytes
		}
		return nil
	})
	return total, err
}

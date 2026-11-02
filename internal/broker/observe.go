package broker

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// DefaultMetricsRetention is how long metrics and the activity log are
// kept, enough for the 30-day interval of the portal.
const DefaultMetricsRetention = 30 * 24 * time.Hour

// scope lists the metric entities an event on e counts for: the entity, and
// the topic of a subscription. The namespace is always added by the
// recorder.
func scope(e *Entity) []string {
	if e == nil {
		return nil
	}
	if e.Topic != nil {
		return []string{e.Path, e.Topic.Path}
	}
	return []string{e.Path}
}

// request records one request on e, and whether it succeeded. A request
// that failed because the caller gave up is not counted.
func (b *Broker) request(e *Entity, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	at, entities := b.now(), scope(e)
	b.rec.Add(at, metrics.IncomingRequests, 1, entities...)
	if telemetry.Enabled() {
		telemetry.Add(context.Background(), telemetry.Requests, 1, requestAttrs(e, err)...)
	}
	switch {
	case err == nil:
		b.rec.Add(at, metrics.SuccessfulRequests, 1, entities...)
	case errors.Is(err, ErrQuotaExceeded):
		b.rec.Add(at, metrics.ThrottledRequests, 1, entities...)
	case isUserError(err):
		b.rec.Add(at, metrics.UserErrors, 1, entities...)
	default:
		b.rec.Add(at, metrics.ServerErrors, 1, entities...)
	}
}

func init() {
	telemetry.AddClassifier(func(err error) string {
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return "canceled"
		case errors.Is(err, ErrQuotaExceeded):
			return "throttled"
		case isUserError(err):
			return "user_error"
		}
		return ""
	})
}

// requestAttrs describes the outcome of a request for telemetry.
func requestAttrs(e *Entity, err error) []attribute.KeyValue {
	outcome := "ok"
	switch {
	case err == nil:
	case errors.Is(err, ErrQuotaExceeded):
		outcome = "throttled"
	case isUserError(err):
		outcome = "user_error"
	default:
		outcome = "server_error"
	}
	attrs := []attribute.KeyValue{attribute.String("outcome", outcome)}
	if e != nil {
		attrs = append(attrs, attribute.String("osb.entity", e.DisplayPath()), attribute.String("osb.entity.kind", string(e.Kind)))
	}
	return attrs
}

// isUserError reports whether err is the caller's fault, as opposed to a
// failure of the emulator.
func isUserError(err error) bool {
	for _, target := range []error{
		ErrEntityNotFound, ErrEntityExists, ErrEntityDisabled, ErrInvalidOperation, ErrLockLost,
		ErrMessageNotFound, ErrSessionRequired, ErrMessageTooLarge, ErrRuleNotFound,
		ErrSessionCannotBeLocked, ErrSessionLockLost, ErrNoSessionAvailable,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// messages records n messages sent to or received from e.
func (b *Broker) messages(name metrics.Name, n int, e *Entity) {
	b.rec.Add(b.now(), name, int64(n), scope(e)...)
	if telemetry.Enabled() && e != nil {
		switch name {
		case metrics.IncomingMessages:
			telemetry.Add(context.Background(), telemetry.MessagesSent, int64(n), telemetry.Entity(e.DisplayPath()))
		case metrics.OutgoingMessages:
			telemetry.Add(context.Background(), telemetry.MessagesReceived, int64(n), telemetry.Entity(e.DisplayPath()))
		}
	}
}

// ConnectionOpened and ConnectionClosed record AMQP connections.
func (b *Broker) ConnectionOpened() {
	b.conns.Add(1)
	b.rec.Add(b.now(), metrics.ConnectionsOpened, 1)
}

// ConnectionClosed records the end of a connection counted by
// ConnectionOpened.
func (b *Broker) ConnectionClosed() {
	b.conns.Add(-1)
	b.rec.Add(b.now(), metrics.ConnectionsClosed, 1)
}

// gaugeSamples reads the message counts of the entities that count for only:
// all of them when only is empty. A topic adds up its subscriptions, and the
// namespace adds up everything.
func (b *Broker) gaugeSamples(ctx context.Context, minute int64, only string) ([]metrics.Sample, error) {
	entities := b.entitySnapshot()
	sums := map[string]*store.Counts{metrics.Namespace: {}}
	add := func(path string, c store.Counts) {
		s := sums[path]
		if s == nil {
			s = &store.Counts{}
			sums[path] = s
		}
		s.Active += c.Active
		s.DeadLetter += c.DeadLetter
		s.TransferDeadLetter += c.TransferDeadLetter
		s.Scheduled += c.Scheduled
		s.SizeBytes += c.SizeBytes
	}
	err := b.store.Read(ctx, func(tx *store.Tx) error {
		for _, e := range entities {
			if e.Kind == KindTopic {
				add(e.Path, store.Counts{})
				continue
			}
			if only != metrics.Namespace && !slices.Contains(scope(e), only) {
				continue
			}
			c, err := tx.CountMessages(e.id)
			if err != nil {
				return err
			}
			for _, p := range append(scope(e), metrics.Namespace) {
				add(p, c)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var out []metrics.Sample
	for path, c := range sums {
		for _, v := range []struct {
			name  metrics.Name
			value int64
		}{
			{metrics.ActiveMessages, c.Active},
			{metrics.DeadletteredMessages, c.DeadLetter},
			{metrics.TransferDeadletteredMessages, c.TransferDeadLetter},
			{metrics.ScheduledMessages, c.Scheduled},
			{metrics.Size, c.SizeBytes},
		} {
			out = append(out, metrics.Sample{Entity: path, Minute: minute, Name: v.name, Value: v.value})
		}
	}
	out = append(out, metrics.Sample{Entity: metrics.Namespace, Minute: minute, Name: metrics.ActiveConnections, Value: b.conns.Load()})
	return out, nil
}

// flushMetrics writes the closed minutes to the store, samples the gauges
// once per minute and removes data past the retention. With final, the open
// minute is written too, for shutdown.
func (b *Broker) flushMetrics(ctx context.Context, at time.Time, final bool) error {
	cur := metrics.Minute(at)
	b.obsMu.Lock()
	defer b.obsMu.Unlock()
	sampleGauges := cur > b.sampled
	var gauges []metrics.Sample
	if sampleGauges {
		var err error
		if gauges, err = b.gaugeSamples(ctx, cur, metrics.Namespace); err != nil {
			return err
		}
	}
	counters := b.rec.Drain(at, final)
	if len(counters) == 0 && !sampleGauges {
		return nil
	}
	err := b.store.Write(ctx, func(tx *store.Tx) error {
		for _, s := range counters {
			if err := tx.AddMetric(store.Metric{Entity: s.Entity, Minute: s.Minute, Name: string(s.Name), Value: s.Value}); err != nil {
				return err
			}
		}
		for _, s := range gauges {
			if err := tx.SetMetric(store.Metric{Entity: s.Entity, Minute: s.Minute, Name: string(s.Name), Value: s.Value}); err != nil {
				return err
			}
		}
		if sampleGauges {
			cutoff := at.Add(-b.retention)
			if err := tx.PruneMetrics(metrics.Minute(cutoff)); err != nil {
				return err
			}
			return tx.PruneActivity(cutoff.UnixMilli())
		}
		return nil
	})
	if err != nil {
		// Keep the counters for the next attempt.
		b.rec.Restore(counters)
		return err
	}
	if sampleGauges {
		b.sampled = cur
	}
	return nil
}

// Metrics returns, for each metric, its values between from and to in
// intervals of step (at least one minute). entity is the path of a queue,
// topic or subscription, or "" for the namespace. The result includes the
// minute still open.
func (b *Broker) Metrics(ctx context.Context, entity string, from, to time.Time, step time.Duration) (map[metrics.Name][]metrics.Point, error) {
	entity = NormalizePath(entity)
	b.obsMu.RLock()
	defer b.obsMu.RUnlock()
	var samples []metrics.Sample
	err := b.store.Read(ctx, func(tx *store.Tx) error {
		rows, err := tx.Metrics(entity, metrics.Minute(from), metrics.Minute(to))
		for _, r := range rows {
			samples = append(samples, metrics.Sample{Entity: r.Entity, Minute: r.Minute, Name: metrics.Name(r.Name), Value: r.Value})
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	samples = append(samples, b.rec.Pending(entity)...)
	if now := b.now(); !to.Before(now.Truncate(time.Minute)) {
		live, err := b.gaugeSamples(ctx, metrics.Minute(now), entity)
		if err != nil {
			return nil, err
		}
		for _, s := range live {
			if s.Entity == entity {
				samples = append(samples, s)
			}
		}
	}
	out := make(map[metrics.Name][]metrics.Point)
	for _, names := range [][]metrics.Name{metrics.Counters, metrics.Gauges} {
		for _, n := range names {
			out[n] = metrics.Aggregate(samples, n, from, to, step)
		}
	}
	return out, nil
}

// MetricTotals returns the counters since the emulator started and the
// current gauges, for the Prometheus endpoint.
func (b *Broker) MetricTotals(ctx context.Context) (counters, gauges []metrics.Total, err error) {
	counters = b.rec.Totals()
	samples, err := b.gaugeSamples(ctx, metrics.Minute(b.now()), metrics.Namespace)
	if err != nil {
		return nil, nil, err
	}
	for _, s := range samples {
		gauges = append(gauges, metrics.Total{Entity: s.Entity, Name: s.Name, Value: s.Value})
	}
	sort.Slice(gauges, func(i, j int) bool {
		if gauges[i].Entity != gauges[j].Entity {
			return gauges[i].Entity < gauges[j].Entity
		}
		return gauges[i].Name < gauges[j].Name
	})
	return counters, gauges, nil
}

type actorKey struct{}

// DefaultActor is the actor of operations whose context names none.
const DefaultActor = "api"

// WithActor returns a context whose administrative operations are logged
// as done by actor, such as "atom", "management", "config" or "portal".
func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorKey{}, actor)
}

func actorOf(ctx context.Context) string {
	if a, ok := ctx.Value(actorKey{}).(string); ok && a != "" {
		return a
	}
	return DefaultActor
}

// Activity statuses.
const (
	StatusSucceeded = "Succeeded"
	StatusFailed    = "Failed"
)

// ActivityEntry is an administrative operation in the activity log.
type ActivityEntry struct {
	ID        int64
	Time      time.Time
	Actor     string
	Operation string
	Entity    string
	Status    string
	Detail    string
}

// ActivityQuery selects entries of the activity log; zero fields match all.
// Entity matches the entity and the paths below it.
type ActivityQuery struct {
	From, To  time.Time
	Actor     string
	Operation string
	Entity    string
	Status    string
	Limit     int
}

// audit records an administrative operation: the outcome of err on entity.
// It runs after the operation, outside its transaction, and never fails it.
func (b *Broker) audit(ctx context.Context, operation, entity string, err error, detail string) {
	entry := store.ActivityEntry{
		Time: b.nowMillis(), Actor: actorOf(ctx), Operation: operation,
		Entity: NormalizePath(entity), Status: StatusSucceeded, Detail: detail,
	}
	if err != nil {
		entry.Status, entry.Detail = StatusFailed, err.Error()
	}
	werr := b.store.Write(context.WithoutCancel(ctx), func(tx *store.Tx) error { return tx.InsertActivityEntry(&entry) })
	if werr != nil {
		b.log.Error("writing the activity log", "operation", operation, "err", werr)
	}
	if err == nil && isMetadataOperation(operation) {
		b.metadataChanged()
	}
}

// isMetadataOperation reports whether an activity log operation changes
// entities or rules, the metadata that Geo-DR replicates.
func isMetadataOperation(op string) bool {
	if op == "DeleteMessages" {
		return false
	}
	return strings.HasPrefix(op, "Create") || strings.HasPrefix(op, "Update") || strings.HasPrefix(op, "Delete") || op == "SetRules"
}

// RecordFailure logs a failed administrative operation that a protocol layer
// rejected before it reached the broker, such as creating an entity that
// exists. verb is "Create", "Update" or "Delete".
func (b *Broker) RecordFailure(ctx context.Context, verb string, kind EntityKind, path string, err error) {
	b.audit(ctx, verb+kindName(kind), path, err, "")
}

// Record logs an administrative operation done outside the broker, such as
// a change of shared access policies, with the outcome of err.
func (b *Broker) Record(ctx context.Context, operation, entity string, err error, detail string) {
	b.audit(ctx, operation, entity, err, detail)
}

// kindName names an entity kind in the activity log.
func kindName(k EntityKind) string {
	s := string(k)
	return strings.ToUpper(s[:1]) + s[1:]
}

// Activity returns the activity log entries matching q, newest first.
func (b *Broker) Activity(ctx context.Context, q ActivityQuery) ([]ActivityEntry, error) {
	sq := store.ActivityQuery{Actor: q.Actor, Operation: q.Operation, Entity: NormalizePath(q.Entity), Status: q.Status, Limit: q.Limit}
	if !q.From.IsZero() {
		sq.From = q.From.UnixMilli()
	}
	if !q.To.IsZero() {
		sq.To = q.To.UnixMilli()
	}
	var out []ActivityEntry
	err := b.store.Read(ctx, func(tx *store.Tx) error {
		rows, err := tx.ActivityEntries(sq)
		for _, r := range rows {
			out = append(out, ActivityEntry{
				ID: r.ID, Time: time.UnixMilli(r.Time).UTC(), Actor: r.Actor, Operation: r.Operation,
				Entity: r.Entity, Status: r.Status, Detail: r.Detail,
			})
		}
		return err
	})
	return out, err
}

func ruleEntity(sub, rule string) string { return fmt.Sprintf("%s/rules/%s", sub, rule) }

// ErrReadOnly reports a change of entities or rules in a namespace that is
// the secondary of a Geo-Disaster Recovery pairing: its metadata is a copy
// of the primary's.
var ErrReadOnly = errors.New("broker: the namespace is a Geo-DR secondary; change its primary instead")

type replicationKey struct{}

// WithReplication marks a context whose changes replicate the primary's
// metadata, which a read-only namespace accepts.
func WithReplication(ctx context.Context) context.Context {
	return context.WithValue(ctx, replicationKey{}, true)
}

// SetReadOnly makes the broker refuse changes of entities and rules, except
// those of WithReplication contexts.
func (b *Broker) SetReadOnly(on bool) { b.readOnly.Store(on) }

// ReadOnly reports whether changes of entities and rules are refused.
func (b *Broker) ReadOnly() bool { return b.readOnly.Load() }

func (b *Broker) checkWritable(ctx context.Context) error {
	if b.readOnly.Load() && ctx.Value(replicationKey{}) == nil {
		return ErrReadOnly
	}
	return nil
}

// OnMetadataChange sets a function called, without blocking, after every
// successful change of entities or rules. There is one: a later call
// replaces it.
func (b *Broker) OnMetadataChange(fn func()) {
	b.hookMu.Lock()
	b.metadataHook = fn
	b.hookMu.Unlock()
}

func (b *Broker) metadataChanged() {
	b.hookMu.Lock()
	fn := b.metadataHook
	b.hookMu.Unlock()
	if fn != nil {
		fn()
	}
}

package broker

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sort"
	"strings"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/filter"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// IncomingMessage is a message to enqueue. Payload is the encoded AMQP
// message; the other fields are the system properties the broker needs,
// extracted by the protocol layer.
type IncomingMessage struct {
	Payload   []byte
	MessageID string
	SessionID string
	// PartitionKey is the PartitionKey property, which pins the message to
	// a partition of a partitioned entity.
	PartitionKey string
	// TimeToLive is the message's own time to live; zero means unset, so
	// the entity default applies.
	TimeToLive time.Duration
	// ScheduledEnqueueTime delays the message when it is in the future.
	ScheduledEnqueueTime time.Time
}

// MaxForwardHops is how many times a message can be auto-forwarded before
// it is dead-lettered, as in Service Bus.
const MaxForwardHops = 4

// Dead-letter reasons for messages the broker cannot route.
const (
	ReasonForwardFailed          = "MessageForwardingFailed"
	ReasonFilterEvaluation       = "FilterEvaluationException"
	ReasonRuleAction             = "RuleActionException"
	ReasonMaxTransferHopExceeded = "MaxTransferHopCountExceeded"
	// ReasonSessionIDNull is the reason of a message without a session id
	// that a session-enabled entity refuses.
	ReasonSessionIDNull = "Session ID is null"
)

// routes is an immutable snapshot of the entities and rules, published
// after every change. Code inside a store transaction reads it instead of
// taking mu.
type routes struct {
	byPath map[string]*Entity
	// subs lists the subscriptions of each topic, by topic id.
	subs map[int64][]*subRoute
}

type subRoute struct {
	e     *Entity
	rules []*compiledRule
	// all reports whether the rules select every message unchanged, so
	// the message need not be decoded.
	all bool
}

// publishLocked publishes a new routing snapshot. The caller holds mu for
// writing.
func (b *Broker) publishLocked() {
	rt := &routes{byPath: make(map[string]*Entity, len(b.entities)), subs: map[int64][]*subRoute{}}
	for p, e := range b.entities {
		rt.byPath[p] = e
		if e.Kind != KindSubscription {
			continue
		}
		sr := &subRoute{e: e, rules: b.rules[e.id]}
		for _, r := range sr.rules {
			if r.matchesAll() {
				sr.all = true
			}
		}
		// A copy per action rule is still needed when another rule
		// matches everything.
		for _, r := range sr.rules {
			if r.action != nil {
				sr.all = false
			}
		}
		rt.subs[e.Topic.id] = append(rt.subs[e.Topic.id], sr)
	}
	for _, s := range rt.subs {
		sort.Slice(s, func(i, j int) bool { return s[i].e.id < s[j].e.id })
	}
	b.routes.Store(rt)
}

// Send enqueues messages on a queue or topic and returns their sequence
// numbers. On a topic, the subscriptions whose rules select a message get a
// copy; queues and subscriptions with ForwardTo pass messages on.
func (b *Broker) Send(ctx context.Context, address string, msgs []IncomingMessage) (seqs []int64, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.send", telemetry.Entity(address))
	defer done(&err)
	var e *Entity
	defer func() {
		b.request(e, err)
		if err == nil {
			b.messages(metrics.IncomingMessages, len(msgs), e)
		}
	}()
	if e, err = b.sendTarget(address, msgs); err != nil {
		return nil, err
	}
	var ec *enqueueCtx
	err = b.store.Write(b.batching(ctx, e), func(tx *store.Tx) error {
		ec = b.newEnqueueCtx(tx)
		var err error
		seqs, err = b.enqueueAll(ec, e, msgs)
		return err
	})
	if err != nil {
		return nil, entityGone(err, e)
	}
	ec.finish()
	if telemetry.Enabled() {
		span := trace.SpanFromContext(ctx)
		size := 0
		for _, m := range msgs {
			size += len(m.Payload)
			telemetry.Record(ctx, telemetry.MessageSize, float64(len(m.Payload)))
		}
		span.SetAttributes(attribute.Int("osb.count", len(msgs)), attribute.Int("osb.bytes", size))
		if e.Kind == KindTopic {
			span.SetAttributes(
				attribute.Int("osb.route.subscriptions", ec.subsEvaluated), attribute.Int("osb.route.matched", ec.subsMatched),
				attribute.Int("osb.route.filter_errors", ec.filterErrors))
		}
		if ec.duplicates > 0 {
			span.SetAttributes(attribute.Int("osb.duplicates", ec.duplicates))
			telemetry.Add(ctx, telemetry.DuplicatesDropped, int64(ec.duplicates), telemetry.Entity(e.DisplayPath()))
		}
		for _, m := range msgs {
			if m.ScheduledEnqueueTime.After(time.Now()) {
				telemetry.Add(ctx, telemetry.MessagesScheduled, 1, attribute.String("event", "sent"))
			}
		}
	}
	return seqs, nil
}

// sendTarget resolves the queue or topic that address names and checks that
// it accepts msgs.
func (b *Broker) sendTarget(address string, msgs []IncomingMessage) (*Entity, error) {
	addr, err := b.Resolve(address)
	if err != nil {
		return nil, err
	}
	e := addr.Entity
	if addr.Subqueue != SubqueueActive || e.Kind == KindSubscription {
		return e, fmt.Errorf("%w: cannot send to %s", ErrInvalidOperation, addr)
	}
	if err := checkStatus(e, true); err != nil {
		return e, err
	}
	if err := validatePartitioned(e, msgs); err != nil {
		return e, err
	}
	if limit := e.Properties.MaxMessageSizeInKilobytes * 1024; limit > 0 {
		for _, m := range msgs {
			if int64(len(m.Payload)) > limit {
				return e, fmt.Errorf("%w: the message is %d bytes; %s accepts up to %d KB", ErrMessageTooLarge, len(m.Payload), e.DisplayPath(), e.Properties.MaxMessageSizeInKilobytes)
			}
		}
	}
	return e, nil
}

// enqueueAll enqueues msgs on e inside the transaction of ec and returns
// their sequence numbers.
func (b *Broker) enqueueAll(ec *enqueueCtx, e *Entity, msgs []IncomingMessage) ([]int64, error) {
	seqs := make([]int64, len(msgs))
	if len(msgs) > 1 && e.Kind == KindQueue && e.Properties.ForwardTo == "" && !e.Properties.RequiresDuplicateDetection {
		// One statement for the whole batch. Nothing in it is dropped
		// without failing the transaction, so no number is left unused.
		first, err := ec.tx.AllocSequence(e.id, len(msgs))
		if err != nil {
			return nil, err
		}
		ec.seqs = map[int64]*seqRange{e.id: {next: first, left: int64(len(msgs))}}
	}
	for i, m := range msgs {
		seq, err := b.enqueue(ec, e, &outgoing{IncomingMessage: m}, 0)
		if err != nil {
			return nil, err
		}
		seqs[i] = seq
	}
	return seqs, ec.tx.TouchEntity(e.id, ec.now.UnixMilli())
}

// enqueueCtx collects the effects of enqueuing inside one transaction.
type enqueueCtx struct {
	b       *Broker
	tx      *store.Tx
	rt      *routes
	now     time.Time
	changed []int64
	// seqs are sequence numbers reserved ahead for a batch.
	seqs map[int64]*seqRange
	// sizes caches the bytes each entity held when first asked, plus what
	// the transaction added since, so a batch asks the database once.
	sizes map[int64]int64
	// timed reports whether any message needs the timer loop: scheduled
	// delivery or expiry.
	timed bool
	// subsEvaluated, subsMatched, duplicates and filterErrors describe the
	// routing of the send, for telemetry.
	subsEvaluated, subsMatched, duplicates, filterErrors int
}

type seqRange struct{ next, left int64 }

// allocSequence returns the next sequence number of entity id, from the
// range reserved for the batch if there is one.
func (c *enqueueCtx) allocSequence(id int64) (int64, error) {
	if r := c.seqs[id]; r != nil && r.left > 0 {
		seq := r.next
		r.next++
		r.left--
		return seq, nil
	}
	return c.tx.AllocSequence(id, 1)
}

// size returns the bytes entity id holds.
func (c *enqueueCtx) size(id int64) (int64, error) {
	if n, ok := c.sizes[id]; ok {
		return n, nil
	}
	n, err := c.tx.EntitySize(id)
	if err != nil {
		return 0, err
	}
	if c.sizes == nil {
		c.sizes = map[int64]int64{}
	}
	c.sizes[id] = n
	return n, nil
}

// addSize records that the transaction added delta bytes to entity id.
func (c *enqueueCtx) addSize(id int64, delta int) {
	if _, ok := c.sizes[id]; ok {
		c.sizes[id] += int64(delta)
	}
}

func (b *Broker) newEnqueueCtx(tx *store.Tx) *enqueueCtx {
	return &enqueueCtx{b: b, tx: tx, rt: b.routes.Load(), now: b.now()}
}

// finish wakes receivers and timers after the transaction commits.
func (c *enqueueCtx) finish() {
	c.b.notify(c.changed...)
	if c.timed {
		c.b.wakeTimers()
	}
}

// outgoing is a message being routed, with its envelope decoded on demand
// for filters.
type outgoing struct {
	IncomingMessage
	env *frames.Envelope
	// origin is the sequence number the message had on the topic, set on
	// copies made by rule actions, which get sequence numbers of their own.
	origin int64
}

func (m *outgoing) envelope() (*frames.Envelope, error) {
	if m.env == nil {
		env, err := frames.DecodeEnvelope(m.Payload)
		if err != nil {
			return nil, err
		}
		m.env = env
	}
	return m.env, nil
}

// enqueue puts m on queue or topic e and returns its sequence number
// there, or 0 for a dropped duplicate. hops counts the forwards m went
// through.
func (b *Broker) enqueue(c *enqueueCtx, e *Entity, m *outgoing, hops int) (int64, error) {
	if dup, err := c.duplicate(e, m); err != nil || dup {
		if dup {
			c.duplicates++
		}
		return 0, err
	}
	if full, err := c.overQuota(e, len(m.Payload)); err != nil {
		return 0, err
	} else if full {
		return 0, fmt.Errorf("%w: %s is at its maximum size of %d MB", ErrQuotaExceeded, e.DisplayPath(), e.Properties.MaxSizeInMegabytes)
	}
	if e.Kind == KindQueue {
		if e.Properties.ForwardTo != "" {
			return b.forward(c, e, m, hops)
		}
		if e.Properties.RequiresSession && m.SessionID == "" {
			return 0, fmt.Errorf("%w: %s requires sessions; the message has no session id", ErrSessionRequired, e.DisplayPath())
		}
		seq, err := c.allocSequence(e.id)
		if err != nil {
			return 0, err
		}
		return b.publicSeq(b.partitionFor(e, m, seq), seq), b.insert(c, e, e, m, seq)
	}

	seq, err := c.tx.AllocSequence(e.id, 1)
	if err != nil {
		return 0, err
	}
	for _, sr := range c.rt.subs[e.id] {
		copies, err := b.route(c, sr, m, seq)
		if err != nil {
			return 0, err
		}
		c.subsEvaluated++
		if len(copies) > 0 {
			c.subsMatched++
		}
		for i, cp := range copies {
			cseq := seq
			if i > 0 {
				// Each copy has its own sequence number.
				if cseq, err = c.tx.AllocSequence(e.id, 1); err != nil {
					return 0, err
				}
			}
			if i > 0 {
				cp.origin = seq
			}
			if err := b.deliverCopy(c, e, sr.e, cp, cseq, hops); err != nil {
				return 0, err
			}
		}
	}
	return b.publicSeq(b.partitionFor(e, m, seq), seq), nil
}

// deliverCopy stores a copy routed to subscription sub, or forwards it.
func (b *Broker) deliverCopy(c *enqueueCtx, topic, sub *Entity, m *outgoing, seq int64, hops int) error {
	if sub.Properties.ForwardTo != "" {
		_, err := b.forward(c, sub, m, hops)
		return err
	}
	if sub.Properties.RequiresSession && m.SessionID == "" {
		if hops == 0 {
			return fmt.Errorf("%w: %s requires sessions; the message has no session id", ErrSessionRequired, sub.DisplayPath())
		}
		return b.deadLetterNew(c, sub, m, ReasonSessionIDNull, "Session enabled entity doesn't allow a message whose session identifier is null.")
	}
	return b.insert(c, topic, sub, m, seq)
}

// route evaluates the rules of a subscription and returns the copies it
// gets: one for the rules without actions, if any matches, and one per
// matching rule with an action. Messages whose rules fail to evaluate are
// dead-lettered.
func (b *Broker) route(c *enqueueCtx, sr *subRoute, m *outgoing, seq int64) ([]*outgoing, error) {
	if sr.all {
		return []*outgoing{m}, nil
	}
	env, err := m.envelope()
	if err != nil {
		return nil, err
	}
	view := &envMessage{env: env, seq: seq, enqueued: c.now, size: len(m.Payload)}
	var (
		plain   bool
		actions []*outgoing
		evalErr error
	)
	for _, r := range sr.rules {
		ok, err := r.match(view)
		if err != nil {
			evalErr = err
			continue
		}
		if !ok {
			continue
		}
		if r.action == nil {
			plain = true
			continue
		}
		cp, err := applyAction(m, r)
		if err != nil {
			if filter.IsEvalError(err) {
				if err := b.deadLetterNew(c, sr.e, m, ReasonRuleAction, err.Error()); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		actions = append(actions, cp)
	}
	if evalErr != nil {
		c.filterErrors++
	}
	if evalErr != nil && sr.e.Properties.DeadLetteringOnFilterEvaluationExceptions {
		if err := b.deadLetterNew(c, sr.e, m, ReasonFilterEvaluation, evalErr.Error()); err != nil {
			return nil, err
		}
	}
	if plain {
		return append([]*outgoing{m}, actions...), nil
	}
	return actions, nil
}

// applyAction returns a copy of m changed by the action of r, with a
// RuleName property naming the rule.
func applyAction(m *outgoing, r *compiledRule) (*outgoing, error) {
	var props *frames.Properties
	payload, err := frames.RewriteMessage(m.Payload, func(env *frames.Envelope) error {
		view := &envMessage{env: env, size: len(m.Payload)}
		if err := r.action.Apply(view); err != nil {
			return err
		}
		view.SetProperty(filter.User, "RuleName", r.Name)
		props = env.Properties
		return nil
	})
	if err != nil {
		return nil, err
	}
	cp := &outgoing{IncomingMessage: m.IncomingMessage}
	cp.Payload = payload
	if props != nil {
		cp.MessageID = MessageIDString(props.MessageID)
		cp.SessionID = props.GroupID
	}
	return cp, nil
}

// insert stores m on target, which is the queue or a subscription of the
// topic via.
func (b *Broker) insert(c *enqueueCtx, via, target *Entity, m *outgoing, seq int64) error {
	visible := c.now
	state := store.StateActive
	if m.ScheduledEnqueueTime.After(c.now) {
		visible = m.ScheduledEnqueueTime
		state = store.StateScheduled
		c.timed = true
	}
	row := &store.Message{
		EntityID:   target.id,
		Seq:        seq,
		State:      state,
		SessionID:  m.SessionID,
		MessageID:  m.MessageID,
		EnqueuedAt: visible.UnixMilli(),
		VisibleAt:  visible.UnixMilli(),
		ExpiresAt:  expiry(visible, effectiveTTL(m.TimeToLive, via, target)),
		Payload:    m.Payload,
		OriginSeq:  m.origin,
		Partition:  b.partitionFor(via, m, seq),
	}
	if err := c.tx.InsertMessage(row); err != nil {
		return err
	}
	c.addSize(target.id, len(m.Payload))
	c.timed = c.timed || row.ExpiresAt != 0
	c.changed = append(c.changed, target.id)
	return nil
}

// forward passes m from src to the entity in its ForwardTo and returns its
// sequence number there, which is what a sender to src is told: it is the
// number CancelScheduled takes. Messages that cannot be forwarded go to the
// dead-letter queue of src, as the documentation says, and get no number.
func (b *Broker) forward(c *enqueueCtx, src *Entity, m *outgoing, hops int) (int64, error) {
	target, code, desc := c.forwardTarget(src.Properties.ForwardTo, m, hops)
	if target != nil {
		full, err := c.overQuota(target, len(m.Payload))
		if err != nil {
			return 0, err
		}
		if full {
			target, code, desc = nil, ReasonForwardFailed, fmt.Sprintf("The forwarding destination %s is at its maximum size.", target.DisplayPath())
		}
	}
	if target == nil {
		return 0, b.deadLetterNew(c, src, m, code, desc)
	}
	return b.enqueue(c, target, m, hops+1)
}

// forwardTarget resolves a forwarding destination. When m cannot be
// forwarded there it returns nil with the dead-letter reason and
// description. Too many hops and a missing session id have the reasons the
// documentation lists; the other failures use ReasonForwardFailed.
func (c *enqueueCtx) forwardTarget(path string, m *outgoing, hops int) (*Entity, string, string) {
	if hops >= MaxForwardHops {
		return nil, ReasonMaxTransferHopExceeded, fmt.Sprintf("The maximum number of allowed hops when forwarding between queues exceeded the limit. This value is set to %d.", MaxForwardHops)
	}
	target := c.rt.byPath[NormalizePath(ForwardPath(path))]
	switch {
	case target == nil || target.Kind == KindSubscription:
		return nil, ReasonForwardFailed, fmt.Sprintf("The forwarding destination %s does not exist.", path)
	case checkStatus(target, true) != nil:
		return nil, ReasonForwardFailed, fmt.Sprintf("The forwarding destination %s is disabled.", target.DisplayPath())
	case target.Kind == KindQueue && target.Properties.RequiresSession && target.Properties.ForwardTo == "" && m.SessionID == "":
		return nil, ReasonSessionIDNull, "Session enabled entity doesn't allow a message whose session identifier is null."
	}
	return target, "", ""
}

// duplicate reports whether m repeats a message id that queue or topic e
// recorded within its duplicate detection window, and records the id
// otherwise. A duplicate is dropped and does not extend the window.
func (c *enqueueCtx) duplicate(e *Entity, m *outgoing) (bool, error) {
	if !e.Properties.RequiresDuplicateDetection || m.MessageID == "" {
		return false, nil
	}
	now := c.now.UnixMilli()
	since := now - e.Properties.DuplicateDetectionHistoryTimeWindow.Milliseconds()
	return c.tx.RecordMessageID(e.id, m.MessageID, now, since)
}

// overQuota reports whether adding size bytes takes queue or topic e past
// its MaxSizeInMegabytes. A topic holds what its subscriptions hold.
func (c *enqueueCtx) overQuota(e *Entity, size int) (bool, error) {
	limit := c.b.MaxSizeBytes(e)
	if limit <= 0 {
		return false, nil
	}
	ids := []int64{e.id}
	if e.Kind == KindTopic {
		ids = ids[:0]
		for _, sr := range c.rt.subs[e.id] {
			ids = append(ids, sr.e.id)
		}
	}
	total := int64(size)
	for _, id := range ids {
		n, err := c.size(id)
		if err != nil {
			return false, err
		}
		total += n
	}
	return total > limit, nil
}

// ForwardPath returns the entity path of a ForwardTo value, which may be a
// name or an absolute URL such as "sb://host:port/queue".
func ForwardPath(s string) string {
	_, rest, ok := strings.Cut(s, "://")
	if !ok {
		return strings.TrimPrefix(s, "/")
	}
	_, path, _ := strings.Cut(rest, "/")
	return path
}

// deadLetterNew stores m directly in the dead-letter queue of e.
func (b *Broker) deadLetterNew(c *enqueueCtx, e *Entity, m *outgoing, reason, desc string) error {
	_, err := b.parkNew(c, e, m, store.SubqueueDeadLetter, reason, desc)
	return err
}

// parkNew stores m directly in a dead-letter sub-queue of e, with the reason
// and description in its application properties.
func (b *Broker) parkNew(c *enqueueCtx, e *Entity, m *outgoing, subqueue int, reason, desc string) (int64, error) {
	if subqueue == store.SubqueueDeadLetter {
		telemetry.Add(context.Background(), telemetry.MessagesDeadLettered, 1, telemetry.Entity(e.DisplayPath()), attribute.String("reason", reason))
	}
	payload, err := frames.RewriteMessage(m.Payload, func(env *frames.Envelope) error {
		env.ApplicationProperties.Set("DeadLetterReason", reason)
		env.ApplicationProperties.Set("DeadLetterErrorDescription", desc)
		env.ApplicationPropertiesModified = true
		return nil
	})
	if err != nil {
		return 0, err
	}
	seqEntity := e
	if e.Kind == KindSubscription {
		// Subscriptions share the sequence of their topic.
		seqEntity = e.Topic
	}
	seq, err := c.tx.AllocSequence(seqEntity.id, 1)
	if err != nil {
		return 0, err
	}
	now := c.now.UnixMilli()
	c.changed = append(c.changed, e.id)
	part := b.partitionFor(seqEntity, &outgoing{}, seq)
	err = c.tx.InsertMessage(&store.Message{
		Partition:  part,
		EntityID:   e.id,
		Subqueue:   subqueue,
		Seq:        seq,
		SessionID:  m.SessionID,
		MessageID:  m.MessageID,
		EnqueuedAt: now,
		VisibleAt:  now,
		Payload:    payload,
	})
	if err == nil {
		c.addSize(e.id, len(payload))
	}
	return b.publicSeq(part, seq), err
}

// forwardDeadLetter passes a message being dead-lettered on e to the
// entity in its ForwardDeadLetteredMessagesTo. It reports false, leaving
// m alone, when there is no valid destination.
func (b *Broker) forwardDeadLetter(c *enqueueCtx, e *Entity, m *store.Message, payload []byte) (bool, error) {
	path := e.Properties.ForwardDeadLetteredMessagesTo
	if path == "" {
		return false, nil
	}
	out := &outgoing{IncomingMessage: IncomingMessage{Payload: payload, MessageID: m.MessageID, SessionID: m.SessionID}}
	if env, err := out.envelope(); err == nil && env.Header != nil && env.Header.TTL != nil {
		out.TimeToLive = *env.Header.TTL
	}
	target, _, reason := c.forwardTarget(path, out, 0)
	if target != nil {
		full, err := c.overQuota(target, len(payload))
		if err != nil {
			return false, err
		}
		if full {
			target, reason = nil, fmt.Sprintf("the destination %s is at its maximum size", target.DisplayPath())
		}
	}
	if target == nil {
		b.log.Warn("keeping dead-lettered message", "entity", e.DisplayPath(), "seq", m.Seq, "reason", reason)
		return false, nil
	}
	if err := c.tx.DeleteMessage(m.ID); err != nil {
		return false, err
	}
	c.addSize(m.EntityID, -len(m.Payload))
	_, err := b.enqueue(c, target, out, 1)
	return err == nil, err
}

// MessageIDString formats any AMQP message-id type as text, the form
// Service Bus uses for duplicate detection and filters.
func MessageIDString(id any) string {
	switch id := id.(type) {
	case nil:
		return ""
	case string:
		return id
	case encoding.UUID:
		return filter.GUID(id).String()
	case uint64:
		return fmt.Sprint(id)
	case []byte:
		return fmt.Sprintf("%x", id)
	}
	return fmt.Sprint(id)
}

// effectiveTTL applies the entity defaults: a message lives no longer than
// the default time to live of its topic and subscription (or queue).
func effectiveTTL(msgTTL time.Duration, sendEntity, target *Entity) time.Duration {
	ttl := Infinite
	if msgTTL > 0 {
		ttl = msgTTL
	}
	ttl = min(ttl, sendEntity.Properties.DefaultMessageTimeToLive)
	if target != sendEntity {
		ttl = min(ttl, target.Properties.DefaultMessageTimeToLive)
	}
	return ttl
}

// expiry returns the Unix millisecond expiry time, or 0 for never.
func expiry(from time.Time, ttl time.Duration) int64 {
	if ttl >= Infinite {
		return 0
	}
	const maxMillis = 1<<62 - 1
	ms := from.UnixMilli()
	ttlMs := ttl.Milliseconds()
	if ttlMs > maxMillis-ms {
		return 0
	}
	return ms + ttlMs
}

// TransferDeadLetter stores messages in the transfer dead-letter queue of
// the queue or subscription at path, with the reason and description in
// their application properties, and returns their sequence numbers. The
// messaging engine puts a message there when it cannot transfer it to
// another entity in a send-via transaction; this exposes the sub-queue to
// tests and to those transactions.
func (b *Broker) TransferDeadLetter(ctx context.Context, path string, msgs []IncomingMessage, reason, desc string) (_ []int64, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.transfer_dead_letter", telemetry.Entity(path))
	defer done(&err)
	b.mu.RLock()
	e := b.entities[NormalizePath(path)]
	b.mu.RUnlock()
	if e == nil || e.Kind == KindTopic {
		return nil, fmt.Errorf("%w: %s is not a queue or subscription", ErrEntityNotFound, path)
	}
	seqs := make([]int64, len(msgs))
	var ec *enqueueCtx
	err = b.store.Write(ctx, func(tx *store.Tx) error {
		ec = b.newEnqueueCtx(tx)
		for i, m := range msgs {
			seq, err := b.parkNew(ec, e, &outgoing{IncomingMessage: m}, store.SubqueueTransferDLQ, reason, desc)
			if err != nil {
				return err
			}
			seqs[i] = seq
		}
		return nil
	})
	if err != nil {
		return nil, entityGone(err, e)
	}
	ec.finish()
	return seqs, nil
}

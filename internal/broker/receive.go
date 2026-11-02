package broker

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// ReceiveMode selects how received messages are settled.
type ReceiveMode int

const (
	// PeekLock locks messages until they are settled or the lock expires.
	PeekLock ReceiveMode = iota
	// ReceiveAndDelete removes messages once delivered. Received messages
	// are locked until the caller removes them with Complete just before
	// handing them over, so the ones it does not reach can be released
	// instead of lost.
	ReceiveAndDelete
)

// Message state values reported to clients.
const (
	MessageStateActive    = store.StateActive
	MessageStateDeferred  = store.StateDeferred
	MessageStateScheduled = store.StateScheduled
)

// Dead-letter reasons set by the broker.
const (
	ReasonMaxDeliveryCount = "MaxDeliveryCountExceeded"
	ReasonTTLExpired       = "TTLExpiredException"
)

// Delivery is a message handed to a receiver, or returned by a peek.
type Delivery struct {
	Address        Address
	SequenceNumber int64
	// LockToken is zero for peeked messages.
	LockToken   LockToken
	LockedUntil time.Time
	EnqueuedAt  time.Time
	// ExpiresAt is zero when the message never expires.
	ExpiresAt time.Time
	// DeliveryCount counts deliveries including this one.
	DeliveryCount    int
	State            int
	SessionID        string
	DeadLetterSource string
	// Payload is the stored encoded message.
	Payload []byte
}

func newDelivery(addr Address, m *store.Message) *Delivery {
	d := &Delivery{
		Address:          addr,
		SequenceNumber:   publicSeqOf(m.Partition, m.Seq),
		EnqueuedAt:       time.UnixMilli(m.EnqueuedAt).UTC(),
		DeliveryCount:    m.DeliveryCount,
		State:            m.State,
		SessionID:        m.SessionID,
		DeadLetterSource: m.DeadLetterSource,
		Payload:          m.Payload,
	}
	if m.ExpiresAt != 0 {
		d.ExpiresAt = time.UnixMilli(m.ExpiresAt).UTC()
	}
	if m.LockToken != nil {
		copy(d.LockToken[:], m.LockToken)
		d.LockedUntil = time.UnixMilli(m.LockedUntil).UTC()
	}
	return d
}

// receiveLockDuration bounds the temporary lock of a receive-and-delete
// delivery, held until the receiver removes it just before the transfer.
const receiveLockDuration = time.Minute

// Receive locks and returns up to max available messages of addr in
// sequence order. It does not wait; use Changed to wait for messages.
// Messages found expired, or past their max delivery count, are expired or
// dead-lettered on the way.
func (b *Broker) Receive(ctx context.Context, addr Address, max int, mode ReceiveMode) (_ []*Delivery, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.receive", telemetry.Entity(addr.String()))
	defer done(&err)
	return b.receive(ctx, addr, max, mode, nil)
}

// receive implements Receive and ReceiveSession. With a session lock, only
// messages of that session are delivered, and their locks expire with the
// session lock.
func (b *Broker) receive(ctx context.Context, addr Address, max int, mode ReceiveMode, session *SessionLock) ([]*Delivery, error) {
	out, err := b.doReceive(ctx, addr, max, mode, session)
	b.request(addr.Entity, err)
	b.messages(metrics.OutgoingMessages, len(out), addr.Entity)
	if telemetry.Enabled() {
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int("osb.count", len(out)))
		now := time.Now()
		for _, d := range out {
			if d.DeliveryCount <= 1 {
				telemetry.Seconds(ctx, telemetry.TimeInQueue, now.Sub(d.EnqueuedAt))
			}
		}
	}
	return out, err
}

// anyAvailable reports whether a receive on the sub-queue could find a
// message, which may be one to expire or dead-letter.
func (b *Broker) anyAvailable(ctx context.Context, e *Entity, sq Subqueue) (bool, error) {
	var found bool
	err := b.store.Read(ctx, func(tx *store.Tx) error {
		ms, err := tx.Available(store.AvailableQuery{EntityID: e.id, Subqueue: int(sq), Now: b.nowMillis(), Limit: 1})
		found = len(ms) > 0
		return err
	})
	return found, err
}

func (b *Broker) doReceive(ctx context.Context, addr Address, max int, mode ReceiveMode, session *SessionLock) ([]*Delivery, error) {
	e := b.current(addr.Entity)
	if e == nil {
		return nil, fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	if e.Kind == KindTopic {
		return nil, fmt.Errorf("%w: cannot receive from topic %s", ErrInvalidOperation, e.DisplayPath())
	}
	if err := checkStatus(e, false); err != nil {
		return nil, err
	}
	if addr.Subqueue == SubqueueActive && e.Properties.ForwardTo != "" {
		return nil, fmt.Errorf("%w: cannot receive from %s, which forwards its messages to %s", ErrInvalidOperation, e.DisplayPath(), e.Properties.ForwardTo)
	}
	addr.Entity = e
	// A receive is activity even when it returns nothing. It is kept in
	// memory: a write for it would send every empty receive through the
	// single writer.
	b.act.touch(e, b.nowMillis())
	drained := b.act.flag(e, addr.Subqueue)
	if session == nil && drained.Load() {
		// The last receive found nothing: with many receivers waiting, most
		// wake up for a message another takes. Look first, since a read
		// costs a fraction of a write and takes no turn at the writer.
		any, err := b.anyAvailable(ctx, e, addr.Subqueue)
		if err != nil {
			return nil, err
		}
		if !any {
			return nil, nil
		}
	}
	var (
		out     []*Delivery
		changed []int64
	)
	err := b.store.Write(b.batching(ctx, e), func(tx *store.Tx) error {
		now := b.nowMillis()
		q := store.AvailableQuery{EntityID: e.id, Subqueue: int(addr.Subqueue), Now: now}
		lockUntil := now + e.Properties.LockDuration.Milliseconds()
		if session != nil {
			row, err := b.heldSession(tx, session, now)
			if err != nil {
				return err
			}
			q.Session, q.HasSession = session.SessionID, true
			lockUntil = row.LockedUntil
		}
		if mode == ReceiveAndDelete {
			lockUntil = now + receiveLockDuration.Milliseconds()
		}
		for len(out) < max {
			q.Limit = max - len(out)
			cands, err := tx.Available(q)
			if err != nil {
				return err
			}
			if len(cands) == 0 {
				break
			}
			for _, m := range cands {
				ok, err := b.prepareDelivery(tx, e, addr.Subqueue, m, now, &changed)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				token := newLockToken()
				m.LockToken = token[:]
				m.LockedUntil = lockUntil
				m.DeliveryCount++
				if err := tx.LockMessage(m); err != nil {
					return err
				}
				out = append(out, newDelivery(addr, m))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	drained.Store(len(out) == 0)
	b.notify(changed...)
	b.wakeForLocks(out)
	return out, nil
}

// prepareDelivery expires or dead-letters a candidate that must not be
// delivered. It reports whether m can be delivered.
func (b *Broker) prepareDelivery(tx *store.Tx, e *Entity, sq Subqueue, m *store.Message, now int64, changed *[]int64) (bool, error) {
	if sq == SubqueueActive && m.ExpiresAt != 0 && m.ExpiresAt <= now {
		return false, b.expire(tx, e, m, changed)
	}
	// A message whose last lock lapsed has used up a delivery attempt.
	if sq == SubqueueActive && int32(m.DeliveryCount) >= e.Properties.MaxDeliveryCount {
		desc := fmt.Sprintf("Message could not be consumed after %d delivery attempts.", e.Properties.MaxDeliveryCount)
		return false, b.deadLetter(tx, e, m, ReasonMaxDeliveryCount, desc, nil, changed)
	}
	return true, nil
}

// expire removes a message whose time to live has passed, dead-lettering
// it when the entity asks for that.
func (b *Broker) expire(tx *store.Tx, e *Entity, m *store.Message, changed *[]int64) error {
	telemetry.Add(context.Background(), telemetry.MessagesExpired, 1, telemetry.Entity(e.DisplayPath()))
	if e.Properties.DeadLetteringOnMessageExpiration {
		return b.deadLetter(tx, e, m, ReasonTTLExpired, "The message expired and was dead lettered.", nil, changed)
	}
	return tx.DeleteMessage(m.ID)
}

// deadLetter moves m to the dead-letter queue of e, recording the reason
// in the message's application properties as Service Bus does.
func (b *Broker) deadLetter(tx *store.Tx, e *Entity, m *store.Message, reason, desc string, props encoding.Map, changed *[]int64) error {
	telemetry.Add(context.Background(), telemetry.MessagesDeadLettered, 1, telemetry.Entity(e.DisplayPath()), attribute.String("reason", reason))
	payload, err := frames.RewriteMessage(m.Payload, func(env *frames.Envelope) error {
		applyProperties(env, props)
		if reason != "" {
			env.ApplicationProperties.Set("DeadLetterReason", reason)
		}
		if desc != "" {
			env.ApplicationProperties.Set("DeadLetterErrorDescription", desc)
		}
		env.ApplicationPropertiesModified = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("broker: dead-lettering message %d: %w", m.Seq, err)
	}
	ec := b.newEnqueueCtx(tx)
	forwarded, err := b.forwardDeadLetter(ec, e, m, payload)
	if err != nil {
		return err
	}
	if forwarded {
		*changed = append(*changed, ec.changed...)
		if ec.timed {
			b.wakeTimers()
		}
		return nil
	}
	m.Payload = payload
	m.Subqueue = store.SubqueueDeadLetter
	m.State = store.StateActive
	m.LockToken = nil
	m.LockedUntil = 0
	// Dead-lettered messages do not expire.
	m.ExpiresAt = 0
	*changed = append(*changed, e.id)
	return tx.UpdateMessage(m)
}

// applyProperties merges props into the application properties.
func applyProperties(env *frames.Envelope, props encoding.Map) {
	for _, p := range props {
		env.ApplicationProperties.Set(p.Key, p.Value)
		env.ApplicationPropertiesModified = true
	}
}

// lockedMessage returns the message locked with token on addr, failing
// with ErrLockLost if the lock is unknown or has expired.
func (b *Broker) lockedMessage(tx *store.Tx, addr Address, token LockToken, now int64) (*store.Message, error) {
	return b.lockedMessageWith(tx, addr, token, now, true)
}

// lockedMessageWith is lockedMessage, optionally without the payload.
func (b *Broker) lockedMessageWith(tx *store.Tx, addr Address, token LockToken, now int64, payload bool) (*store.Message, error) {
	byLock := tx.MessageHeaderByLock
	if payload {
		byLock = tx.MessageByLock
	}
	m, err := byLock(token[:])
	if errors.Is(err, store.ErrNotFound) || err == nil && (m.EntityID != addr.Entity.id || m.Subqueue != int(addr.Subqueue)) {
		return nil, fmt.Errorf("%w: lock %s is not held on %s", ErrLockLost, token, addr)
	}
	if err != nil {
		return nil, err
	}
	if m.LockedUntil <= now {
		return nil, fmt.Errorf("%w: lock %s on %s expired", ErrLockLost, token, addr)
	}
	return m, nil
}

// settle runs fn on the message locked with token.
func (b *Broker) settle(ctx context.Context, addr Address, token LockToken, fn func(tx *store.Tx, e *Entity, m *store.Message, changed *[]int64) error) (err error) {
	return b.settleWith(ctx, addr, token, true, fn)
}

// settleWith is settle, optionally without loading the payload of the
// message.
func (b *Broker) settleWith(ctx context.Context, addr Address, token LockToken, payload bool, fn func(tx *store.Tx, e *Entity, m *store.Message, changed *[]int64) error) (err error) {
	defer func() { b.request(addr.Entity, err) }()
	e := b.current(addr.Entity)
	if e == nil {
		return fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	addr.Entity = e
	var changed []int64
	err = b.store.Write(b.batching(ctx, e), func(tx *store.Tx) error {
		m, err := b.lockedMessageWith(tx, addr, token, b.nowMillis(), payload)
		if err != nil {
			return err
		}
		return fn(tx, e, m, &changed)
	})
	if err != nil {
		return err
	}
	b.notify(changed...)
	return nil
}

// SettleKind is how a received message is settled.
type SettleKind int

// Ways to settle a locked message.
const (
	SettleComplete SettleKind = iota
	SettleAbandon
	SettleRelease
	SettleDefer
	SettleDeadLetter
)

// Settlement describes the settlement of one locked message.
type Settlement struct {
	Kind SettleKind
	// Props are applied to the message's application properties by Abandon,
	// Defer and DeadLetter.
	Props encoding.Map
	// Reason and Description go with a dead-lettered message.
	Reason, Description string
}

// settleFn returns the change a settlement makes to a locked message, run
// inside a transaction.
func (b *Broker) settleFn(addr Address, s Settlement) func(tx *store.Tx, e *Entity, m *store.Message, changed *[]int64) error {
	return func(tx *store.Tx, e *Entity, m *store.Message, changed *[]int64) error {
		if telemetry.Enabled() && s.Kind != SettleRelease {
			ctx := context.Background()
			telemetry.Record(ctx, telemetry.DeliveryCount, float64(m.DeliveryCount), attribute.Int("kind", int(s.Kind)))
			if m.LockedUntil > 0 && e.Properties.LockDuration > 0 {
				// The lock started LockDuration before it ends (renewals
				// restart it), so this is the time since the last (re)lock.
				held := time.Duration(b.nowMillis()-(m.LockedUntil-e.Properties.LockDuration.Milliseconds())) * time.Millisecond
				telemetry.Seconds(ctx, telemetry.LockHeld, held)
			}
		}
		switch s.Kind {
		case SettleComplete:
			return tx.DeleteMessage(m.ID)
		case SettleAbandon:
			if err := rewriteProperties(m, s.Props); err != nil {
				return err
			}
			if addr.Subqueue == SubqueueActive && m.State == store.StateActive &&
				int32(m.DeliveryCount) >= e.Properties.MaxDeliveryCount {
				desc := fmt.Sprintf("Message could not be consumed after %d delivery attempts.", e.Properties.MaxDeliveryCount)
				return b.deadLetter(tx, e, m, ReasonMaxDeliveryCount, desc, nil, changed)
			}
			m.LockToken = nil
			m.LockedUntil = 0
			*changed = append(*changed, e.id)
			if len(s.Props) > 0 {
				return tx.UpdateMessage(m)
			}
			return tx.UnlockMessage(m)
		case SettleRelease:
			m.LockToken = nil
			m.LockedUntil = 0
			m.DeliveryCount = max(0, m.DeliveryCount-1)
			*changed = append(*changed, e.id)
			return tx.UnlockMessage(m)
		case SettleDefer:
			if err := rewriteProperties(m, s.Props); err != nil {
				return err
			}
			m.State = store.StateDeferred
			m.LockToken = nil
			m.LockedUntil = 0
			return tx.UpdateMessage(m)
		case SettleDeadLetter:
			return b.deadLetter(tx, e, m, s.Reason, s.Description, s.Props, changed)
		}
		return fmt.Errorf("%w: unknown settlement %d", ErrInvalidOperation, s.Kind)
	}
}

// Complete removes a locked message.
func (b *Broker) Complete(ctx context.Context, addr Address, token LockToken) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.complete", telemetry.Entity(addr.String()))
	defer done(&err)
	err = b.settleWith(ctx, addr, token, false, b.settleFn(addr, Settlement{Kind: SettleComplete}))
	if err == nil {
		telemetry.Add(ctx, telemetry.MessagesCompleted, 1, telemetry.Entity(addr.String()))
	}
	return err
}

// Abandon unlocks a message so it can be delivered again, applying props
// to its application properties. A message that reached its max delivery
// count is dead-lettered instead.
func (b *Broker) Abandon(ctx context.Context, addr Address, token LockToken, props encoding.Map) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.abandon", telemetry.Entity(addr.String()))
	defer done(&err)
	err = b.settle(ctx, addr, token, b.settleFn(addr, Settlement{Kind: SettleAbandon, Props: props}))
	if err == nil {
		telemetry.Add(ctx, telemetry.MessagesAbandoned, 1, telemetry.Entity(addr.String()))
	}
	return err
}

// Release unlocks a message without counting the delivery, for messages
// the client received but never handed to the application.
func (b *Broker) Release(ctx context.Context, addr Address, token LockToken) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.release", telemetry.Entity(addr.String()))
	defer done(&err)
	return b.settleWith(ctx, addr, token, false, b.settleFn(addr, Settlement{Kind: SettleRelease}))
}

// Defer sets a message aside; it can then only be received by sequence
// number.
func (b *Broker) Defer(ctx context.Context, addr Address, token LockToken, props encoding.Map) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.defer", telemetry.Entity(addr.String()))
	defer done(&err)
	err = b.settle(ctx, addr, token, b.settleFn(addr, Settlement{Kind: SettleDefer, Props: props}))
	if err == nil {
		telemetry.Add(ctx, telemetry.MessagesDeferred, 1, telemetry.Entity(addr.String()))
	}
	return err
}

// DeadLetter moves a locked message to the dead-letter queue.
func (b *Broker) DeadLetter(ctx context.Context, addr Address, token LockToken, reason, description string, props encoding.Map) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.dead_letter", telemetry.Entity(addr.String()))
	defer done(&err)
	err = b.settle(ctx, addr, token, b.settleFn(addr, Settlement{Kind: SettleDeadLetter, Reason: reason, Description: description, Props: props}))
	return err
}

func rewriteProperties(m *store.Message, props encoding.Map) error {
	if len(props) == 0 {
		return nil
	}
	payload, err := frames.RewriteMessage(m.Payload, func(env *frames.Envelope) error {
		applyProperties(env, props)
		return nil
	})
	if err != nil {
		return fmt.Errorf("broker: modifying message %d: %w", m.Seq, err)
	}
	m.Payload = payload
	return nil
}

// RenewLocks extends message locks by the entity's lock duration and
// returns the new expiry times.
func (b *Broker) RenewLocks(ctx context.Context, addr Address, tokens []LockToken) (_ []time.Time, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.renew_locks", telemetry.Entity(addr.String()))
	defer done(&err)
	out, err := b.renewLocks(ctx, addr, tokens)
	b.request(addr.Entity, err)
	return out, err
}

func (b *Broker) renewLocks(ctx context.Context, addr Address, tokens []LockToken) ([]time.Time, error) {
	e := b.current(addr.Entity)
	if e == nil {
		return nil, fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	addr.Entity = e
	out := make([]time.Time, len(tokens))
	err := b.store.Write(ctx, func(tx *store.Tx) error {
		now := b.nowMillis()
		for i, tok := range tokens {
			m, err := b.lockedMessageWith(tx, addr, tok, now, false)
			if err != nil {
				return err
			}
			m.LockedUntil = now + e.Properties.LockDuration.Milliseconds()
			if err := tx.RenewMessageLock(m); err != nil {
				return err
			}
			out[i] = time.UnixMilli(m.LockedUntil).UTC()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// A renewed lock expires later than the deadline the timer loop waits
	// for, so there is nothing to recompute.
	return out, nil
}

package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// Peek returns up to max messages of addr in any state, starting at
// sequence number fromSeq, without locking them. A non-empty sessionID
// restricts the result to that session.
func (b *Broker) Peek(ctx context.Context, addr Address, fromSeq int64, max int, sessionID string) (_ []*Delivery, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.peek", telemetry.Entity(addr.String()))
	defer done(&err)
	out, err := b.peek(ctx, addr, fromSeq, max, sessionID)
	b.request(addr.Entity, err)
	return out, err
}

func (b *Broker) peek(ctx context.Context, addr Address, fromSeq int64, max int, sessionID string) ([]*Delivery, error) {
	e := b.current(addr.Entity)
	if e == nil {
		return nil, fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	if e.Kind == KindTopic {
		return nil, fmt.Errorf("%w: cannot peek topic %s", ErrInvalidOperation, e.DisplayPath())
	}
	addr.Entity = e
	fromSeq = internalSeq(fromSeq)
	var out []*Delivery
	err := b.store.Read(ctx, func(tx *store.Tx) error {
		rows, err := tx.Peek(store.PeekQuery{
			EntityID: e.id, Subqueue: int(addr.Subqueue), FromSeq: fromSeq, Limit: max,
			Session: sessionID, HasSession: sessionID != "",
		})
		if err != nil {
			return err
		}
		for _, m := range rows {
			d := newDelivery(addr, m)
			// Peeked messages carry no lock.
			d.LockToken = LockToken{}
			d.LockedUntil = time.Time{}
			out = append(out, d)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Peeking is activity for auto-delete-on-idle.
	b.act.touch(e, b.nowMillis())
	return out, nil
}

// CancelScheduled deletes scheduled messages by sequence number. On a
// topic, the copies of every subscription are deleted, including the extra
// copies made by rules with actions, which have sequence numbers of their
// own. On an entity that forwards its messages, the number is the one the
// destination gave, as Send returned it, so the cancellation follows the
// forwarding.
func (b *Broker) CancelScheduled(ctx context.Context, addr Address, seqs []int64) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.cancel_scheduled", telemetry.Entity(addr.String()))
	defer done(&err)
	err = b.cancelScheduled(ctx, addr, seqs)
	b.request(addr.Entity, err)
	return err
}

func (b *Broker) cancelScheduled(ctx context.Context, addr Address, seqs []int64) error {
	seqs = internalSeqs(seqs)
	e := b.current(addr.Entity)
	if e == nil {
		return fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	b.mu.RLock()
	for hops := 0; e.Kind != KindTopic && e.Properties.ForwardTo != "" && hops < MaxForwardHops; hops++ {
		next := b.entities[NormalizePath(ForwardPath(e.Properties.ForwardTo))]
		if next == nil || next.Kind == KindSubscription {
			break
		}
		e = next
	}
	targets := []*Entity{e}
	if e.Kind == KindTopic {
		targets = b.subscriptionsLocked(e)
	}
	b.mu.RUnlock()
	return b.store.Write(ctx, func(tx *store.Tx) error {
		for _, t := range targets {
			rows, err := tx.ScheduledBySeq(t.id, seqs)
			if err != nil {
				return err
			}
			for _, m := range rows {
				if err := tx.DeleteMessage(m.ID); err != nil {
					return err
				}
				b.emit(tx, "cancel-scheduled", storedAttrs(t, m)...)
			}
		}
		return nil
	})
}

// DefaultMaxBatchDelete is the most messages a batch delete removes in one
// request, the limit of the Premium tier.
const DefaultMaxBatchDelete = 4000

// MaxBatchDelete returns the most messages DeleteMessages removes per call.
func (b *Broker) MaxBatchDelete() int { return b.maxBatch }

// DeleteMessages deletes up to max of the oldest messages of the queue or
// sub-queue at addr that were enqueued before the cutoff, and returns how
// many it deleted, which is less than max when fewer qualify. Locked
// messages and scheduled ones are left alone. A non-empty sessionID
// restricts it to that session. A max above MaxBatchDelete is an error, as
// the service refuses it; callers that purge delete in rounds.
func (b *Broker) DeleteMessages(ctx context.Context, addr Address, max int, before time.Time, sessionID string) (_ int, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.delete_messages", telemetry.Entity(addr.String()))
	defer done(&err)
	n, err := b.deleteMessages(ctx, addr, max, before, sessionID)
	b.request(addr.Entity, err)
	if err == nil {
		b.audit(ctx, "DeleteMessages", addr.Entity.Path+subqueueSuffix(addr.Subqueue), nil, fmt.Sprintf("%d messages", n))
	}
	return n, err
}

func subqueueSuffix(sq Subqueue) string {
	switch sq {
	case SubqueueDeadLetter:
		return dlqSuffix
	case SubqueueTransferDLQ:
		return transferDLQSuffix
	}
	return ""
}

func (b *Broker) deleteMessages(ctx context.Context, addr Address, max int, before time.Time, sessionID string) (int, error) {
	e := b.current(addr.Entity)
	if e == nil {
		return 0, fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	if e.Kind == KindTopic {
		return 0, fmt.Errorf("%w: cannot delete messages of topic %s; name a subscription", ErrInvalidOperation, e.DisplayPath())
	}
	if max < 1 || max > b.maxBatch {
		return 0, fmt.Errorf("%w: the number of messages to delete must be between 1 and %d, got %d", ErrInvalidOperation, b.maxBatch, max)
	}
	if err := checkStatus(e, false); err != nil {
		return 0, err
	}
	var n int64
	err := b.store.Write(ctx, func(tx *store.Tx) error {
		var err error
		n, err = tx.DeleteOldest(e.id, int(addr.Subqueue), before.UnixMilli(), b.nowMillis(), max, sessionID)
		if err != nil {
			return err
		}
		return touchTx(tx, e, b.nowMillis())
	})
	return int(n), entityGone(err, e)
}

// ReceiveDeferred locks deferred messages by sequence number. In
// ReceiveAndDelete mode they are removed instead. A non-empty sessionID
// restricts it to that session. It fails with ErrMessageNotFound if any
// requested message is not deferred on addr (or the session).
func (b *Broker) ReceiveDeferred(ctx context.Context, addr Address, seqs []int64, mode ReceiveMode, sessionID string) (_ []*Delivery, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.receive_deferred", telemetry.Entity(addr.String()))
	defer done(&err)
	out, err := b.receiveDeferred(ctx, addr, seqs, mode, sessionID)
	b.request(addr.Entity, err)
	b.messages(metrics.OutgoingMessages, len(out), addr.Entity)
	return out, err
}

func (b *Broker) receiveDeferred(ctx context.Context, addr Address, seqs []int64, mode ReceiveMode, sessionID string) ([]*Delivery, error) {
	seqs = internalSeqs(seqs)
	e := b.current(addr.Entity)
	if e == nil {
		return nil, fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	if err := checkStatus(e, false); err != nil {
		return nil, err
	}
	addr.Entity = e
	var out []*Delivery
	err := b.store.Write(ctx, func(tx *store.Tx) error {
		now := b.nowMillis()
		rows, err := tx.MessagesBySeq(e.id, int(addr.Subqueue), seqs)
		if err != nil {
			return err
		}
		var deferred []*store.Message
		for _, m := range rows {
			if m.State == store.StateDeferred && (sessionID == "" || m.SessionID == sessionID) {
				deferred = append(deferred, m)
			}
		}
		if len(deferred) != len(uniqueSeqs(seqs)) {
			return fmt.Errorf("%w: one or more deferred messages were not found on %s", ErrMessageNotFound, addr)
		}
		for _, m := range deferred {
			if m.LockedUntil > now {
				return fmt.Errorf("%w: deferred message %d is locked", ErrInvalidOperation, m.Seq)
			}
			m.DeliveryCount++
			if mode == ReceiveAndDelete {
				if err := tx.DeleteMessage(m.ID); err != nil {
					return err
				}
				out = append(out, newDelivery(addr, m))
				continue
			}
			token := newLockToken()
			m.LockToken = token[:]
			m.LockedUntil = now + e.Properties.LockDuration.Milliseconds()
			if err := tx.LockMessage(m); err != nil {
				return err
			}
			out = append(out, newDelivery(addr, m))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	b.wakeForLocks(out)
	return out, nil
}

func uniqueSeqs(seqs []int64) map[int64]struct{} {
	m := make(map[int64]struct{}, len(seqs))
	for _, s := range seqs {
		m[s] = struct{}{}
	}
	return m
}

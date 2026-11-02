package broker

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// maxTimerSleep bounds how long the timer loop sleeps without checking, as
// a safety net against missed wakeups.
const maxTimerSleep = 5 * time.Second

// timerBatch bounds the work done per transaction so that the writer is
// not held for long.
const timerBatch = 500

// runTimers activates scheduled messages, expires messages whose time to
// live passed and wakes receivers when locks lapse.
func (b *Broker) runTimers(ctx context.Context) {
	defer close(b.done)
	last := b.nowMillis()
	for {
		b.nextWake.Store(0)
		now := b.nowMillis()
		next, err := b.processTimers(ctx, last, now)
		if err != nil && ctx.Err() == nil {
			b.log.Error("processing message timers", "err", err)
		}
		idle, err := b.sweepIdle(ctx, now)
		if err != nil && ctx.Err() == nil {
			b.log.Error("deleting idle entities", "err", err)
		}
		if err := b.flushMetrics(ctx, b.now(), false); err != nil && ctx.Err() == nil {
			b.log.Error("writing metrics", "err", err)
		}
		if idle > 0 && (next == 0 || idle < next) {
			next = idle
		}
		last = now
		b.nextWake.Store(next)

		wait := maxTimerSleep
		if next > 0 {
			wait = min(wait, time.Duration(next-b.nowMillis())*time.Millisecond)
		}
		t := time.NewTimer(max(wait, time.Millisecond))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-b.poke:
		case <-t.C:
		}
		t.Stop()
	}
}

// processTimers handles every timer due at now and returns the next
// deadline (0 if none). Locks that lapsed after last wake their entity's
// receivers.
func (b *Broker) processTimers(ctx context.Context, last, now int64) (int64, error) {
	var (
		changed []int64
		next    int64
	)
	// Most wakes find nothing due: look in a read transaction first, so
	// that they do not take the writer.
	var idleNext int64
	var lapsed []int64
	nothing := false
	err := b.store.Read(ctx, func(tx *store.Tx) error {
		due, err := tx.DueScheduled(now, 1)
		if err != nil || len(due) > 0 {
			return err
		}
		expired, err := tx.Expired(now, 1)
		if err != nil || len(expired) > 0 {
			return err
		}
		if lapsed, err = tx.LockLapsedEntities(last, now); err != nil {
			return err
		}
		if idleNext, err = tx.NextTimer(now); err != nil {
			return err
		}
		// A lapsed lock is cleared in a write, so that the message is a
		// candidate of the index of unlocked messages again.
		hasLapsed, err := tx.HasLapsedLocks(now)
		nothing = !hasLapsed
		return err
	})
	if err != nil {
		return 0, err
	}
	if nothing {
		b.notify(lapsed...)
		return idleNext, nil
	}
	// Only a sweep that has work gets a span, so an idle emulator is quiet.
	ctx, finish := telemetry.Operation(ctx, "broker.timers.sweep")
	var sweepErr error
	defer func() { finish(&sweepErr) }()
	var nDue, nExpired, nUnlocked int
	defer func() {
		telemetry.SpanFromContext(ctx).SetAttributes(
			attribute.Int("osb.timers.activated", nDue), attribute.Int("osb.timers.expired", nExpired), attribute.Int("osb.timers.locks_released", nUnlocked))
		telemetry.Add(ctx, telemetry.MessagesScheduled, int64(nDue), attribute.String("event", "activated"))
		telemetry.Add(ctx, telemetry.LocksExpired, int64(nUnlocked))
	}()
	for {
		more := false
		entities := b.entitySnapshot()
		err := b.store.Write(ctx, func(tx *store.Tx) error {
			due, err := tx.DueScheduled(now, timerBatch)
			if err != nil {
				return err
			}
			for _, m := range due {
				m.State = store.StateActive
				if err := tx.ActivateMessage(m); err != nil {
					return err
				}
				changed = append(changed, m.EntityID)
			}
			expired, err := tx.Expired(now, timerBatch)
			if err != nil {
				return err
			}
			for _, m := range expired {
				e := entities[m.EntityID]
				if e == nil {
					// The entity was deleted concurrently.
					if err := tx.DeleteMessage(m.ID); err != nil {
						return err
					}
					continue
				}
				if err := b.expire(tx, e, m, &changed); err != nil {
					return err
				}
			}
			lapsed, err := tx.LockLapsedEntities(last, now)
			if err != nil {
				return err
			}
			changed = append(changed, lapsed...)
			// After LockLapsedEntities, which looks at the locks this clears.
			released, n, err := tx.ReleaseLapsedLocks(now, timerBatch)
			if err != nil {
				return err
			}
			changed = append(changed, released...)
			nDue += len(due)
			nExpired += len(expired)
			nUnlocked += n
			more = len(due) == timerBatch || len(expired) == timerBatch || n == timerBatch
			if !more {
				next, err = tx.NextTimer(now)
			}
			return err
		})
		if err != nil {
			sweepErr = err
			return 0, err
		}
		b.notify(changed...)
		changed = changed[:0]
		if !more {
			return next, nil
		}
	}
}

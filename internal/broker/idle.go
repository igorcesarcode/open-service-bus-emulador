package broker

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// purgeInterval is how often message ids past their duplicate detection
// window are forgotten.
const purgeInterval = time.Minute

// purgeBatch is how many message ids one transaction forgets.
const purgeBatch = 500

// autoDeletes reports whether e is deleted after AutoDeleteOnIdle. Entities
// that forward their messages are never idle, as in Service Bus.
func autoDeletes(e *Entity) bool {
	d := e.Properties.AutoDeleteOnIdle
	return d > 0 && d < Infinite && e.Properties.ForwardTo == ""
}

// idleDeadline is when e becomes idle after its last activity at, in Unix
// milliseconds.
func idleDeadline(e *Entity, at int64) int64 {
	ms := e.Properties.AutoDeleteOnIdle.Milliseconds()
	if ms > math.MaxInt64-at {
		return math.MaxInt64
	}
	return at + ms
}

// sweepIdle deletes the entities idle for their AutoDeleteOnIdle and
// forgets expired message ids. It returns the next idle deadline, or 0.
func (b *Broker) sweepIdle(ctx context.Context, now int64) (int64, error) {
	var candidates, dedup []*Entity
	for _, e := range b.entitySnapshot() {
		if autoDeletes(e) {
			candidates = append(candidates, e)
		}
		if e.Properties.RequiresDuplicateDetection && e.Kind != KindSubscription {
			dedup = append(dedup, e)
		}
	}
	if len(dedup) > 0 && time.UnixMilli(now).Sub(b.lastPurge) >= purgeInterval {
		b.lastPurge = time.UnixMilli(now)
		for _, e := range dedup {
			until := now - e.Properties.DuplicateDetectionHistoryTimeWindow.Milliseconds()
			for n := purgeBatch; n == purgeBatch; {
				err := b.store.Write(ctx, func(tx *store.Tx) (err error) {
					n, err = tx.PurgeMessageIDs(e.id, until, purgeBatch)
					return err
				})
				if err != nil {
					return 0, err
				}
			}
		}
	}
	if len(candidates) == 0 {
		return 0, nil
	}
	accessed := map[int64]int64{}
	err := b.store.Read(ctx, func(tx *store.Tx) error {
		rows, err := tx.Activity()
		for _, r := range rows {
			accessed[r.ID] = r.AccessedAt
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	var next int64
	for _, e := range candidates {
		at, ok := accessed[e.id]
		if !ok {
			continue
		}
		at = b.act.latest(e.id, at)
		deadline := idleDeadline(e, at)
		if deadline > now {
			if next == 0 || deadline < next {
				next = deadline
			}
			continue
		}
		if err := b.deleteIfIdle(ctx, e, now); err != nil && !errors.Is(err, ErrEntityNotFound) {
			return 0, err
		}
	}
	return next, nil
}

// deleteIfIdle deletes e if it is still idle at now, checking its last
// activity again under adminMu.
func (b *Broker) deleteIfIdle(ctx context.Context, e *Entity, now int64) error {
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	current := b.current(e)
	if current == nil || !autoDeletes(current) || b.lockedAgainstDelete(current.Path) {
		return nil
	}
	var at int64
	err := b.store.Read(ctx, func(tx *store.Tx) error {
		var err error
		at, err = tx.AccessedAt(e.id)
		return err
	})
	if errors.Is(err, store.ErrEntityGone) {
		return nil
	}
	if err != nil {
		return err
	}
	at = b.act.latest(e.id, at)
	if idleDeadline(current, at) > now {
		return nil
	}
	b.log.Info("deleting idle entity", "entity", current.DisplayPath(), "auto_delete_on_idle", current.Properties.AutoDeleteOnIdle)
	return b.deleteLocked(WithActor(ctx, "system"), current.Path, "auto-delete on idle")
}

// RestartIdleTimers counts every entity as used now, so that the time to
// auto-delete on idle starts over, as it does after a Geo-Replication
// promotion.
func (b *Broker) RestartIdleTimers(ctx context.Context) error {
	err := b.store.Write(ctx, func(tx *store.Tx) error { return tx.TouchAll(b.nowMillis()) })
	b.wakeTimers()
	return err
}

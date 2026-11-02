package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// Session errors.
var (
	// ErrSessionCannotBeLocked reports a session held by another receiver.
	ErrSessionCannotBeLocked = errors.New("broker: session cannot be locked")
	// ErrSessionLockLost reports a session lock that expired or was released.
	ErrSessionLockLost = errors.New("broker: session lock lost")
	// ErrNoSessionAvailable reports that no unlocked session has messages.
	ErrNoSessionAvailable = errors.New("broker: no session available")
)

// SessionLock is a receiver's exclusive lock on one message session.
type SessionLock struct {
	Address     Address
	SessionID   string
	Token       LockToken
	LockedUntil time.Time
}

func requireSessions(addr Address) (*Entity, error) {
	e := addr.Entity
	if e.Kind == KindTopic {
		return nil, fmt.Errorf("%w: cannot receive from topic %s", ErrInvalidOperation, e.DisplayPath())
	}
	if !e.Properties.RequiresSession {
		return nil, fmt.Errorf("%w: %s does not require sessions", ErrInvalidOperation, addr)
	}
	return e, nil
}

// AcceptSession locks the session sessionID. A session that does not exist
// yet can be locked; messages sent to it later are delivered to the lock
// holder.
func (b *Broker) AcceptSession(ctx context.Context, addr Address, sessionID string) (_ *SessionLock, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.accept_session", telemetry.Entity(addr.String()))
	defer done(&err)
	return b.acceptSession(ctx, addr, sessionID)
}

// AcceptNextSession locks the session of the oldest available message
// whose session is not locked. It fails with ErrNoSessionAvailable without
// waiting; use Changed to wait.
func (b *Broker) AcceptNextSession(ctx context.Context, addr Address) (_ *SessionLock, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.accept_next_session", telemetry.Entity(addr.String()))
	defer done(&err)
	return b.acceptSession(ctx, addr, "")
}

func (b *Broker) acceptSession(ctx context.Context, addr Address, sessionID string) (*SessionLock, error) {
	e := b.current(addr.Entity)
	if e == nil {
		return nil, fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	addr.Entity = e
	if _, err := requireSessions(addr); err != nil {
		return nil, err
	}
	if err := checkStatus(e, false); err != nil {
		return nil, err
	}
	var lock *SessionLock
	err := b.store.Write(ctx, func(tx *store.Tx) error {
		now := b.nowMillis()
		id := sessionID
		if id == "" {
			next, ok, err := tx.NextAvailableSession(e.id, int(addr.Subqueue), now)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w on %s", ErrNoSessionAvailable, addr)
			}
			id = next
		}
		row, err := tx.Session(e.id, id)
		if errors.Is(err, store.ErrNotFound) {
			row = &store.Session{EntityID: e.id, SessionID: id, UpdatedAt: now}
		} else if err != nil {
			return err
		}
		if row.LockToken != nil && row.LockedUntil > now {
			return fmt.Errorf("%w: session %q on %s is locked by another receiver", ErrSessionCannotBeLocked, id, addr)
		}
		// Messages still locked through a lapsed session lock become
		// available to the new holder.
		if row.LockToken != nil {
			if err := tx.UnlockSessionMessages(e.id, id); err != nil {
				return err
			}
		}
		token := newLockToken()
		row.LockToken = token[:]
		row.LockedUntil = now + e.Properties.LockDuration.Milliseconds()
		if err := tx.PutSession(row); err != nil {
			return err
		}
		lock = &SessionLock{
			Address:     addr,
			SessionID:   id,
			Token:       token,
			LockedUntil: time.UnixMilli(row.LockedUntil).UTC(),
		}
		return touchTx(tx, e, now)
	})
	if err != nil {
		return nil, err
	}
	b.wakeTimers()
	return lock, nil
}

// heldSession returns the session row if lock still holds it.
func (b *Broker) heldSession(tx *store.Tx, lock *SessionLock, now int64) (*store.Session, error) {
	row, err := tx.Session(lock.Address.Entity.id, lock.SessionID)
	if errors.Is(err, store.ErrNotFound) || err == nil && (!bytes.Equal(row.LockToken, lock.Token[:]) || row.LockedUntil <= now) {
		return nil, fmt.Errorf("%w: session %q on %s", ErrSessionLockLost, lock.SessionID, lock.Address)
	}
	return row, err
}

// lockedSession returns the session row of sessionID if it is currently
// locked by any receiver. Session management operations identify the
// session by id only.
func (b *Broker) lockedSession(tx *store.Tx, addr Address, sessionID string, now int64) (*store.Session, error) {
	row, err := tx.Session(addr.Entity.id, sessionID)
	if errors.Is(err, store.ErrNotFound) || err == nil && (row.LockToken == nil || row.LockedUntil <= now) {
		return nil, fmt.Errorf("%w: session %q on %s is not locked", ErrSessionLockLost, sessionID, addr)
	}
	return row, err
}

// ReceiveSession is like Receive for the session held by lock.
func (b *Broker) ReceiveSession(ctx context.Context, lock *SessionLock, max int, mode ReceiveMode) (_ []*Delivery, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.receive_session")
	defer done(&err)
	return b.receive(ctx, lock.Address, max, mode, lock)
}

// RenewSessionLock extends a session lock, and the locks of the messages
// received through it, by the entity's lock duration.
func (b *Broker) RenewSessionLock(ctx context.Context, addr Address, sessionID string) (_ time.Time, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.renew_session_lock", telemetry.Entity(addr.String()))
	defer done(&err)
	e := b.current(addr.Entity)
	if e == nil {
		return time.Time{}, fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	addr.Entity = e
	var until int64
	err = b.store.Write(ctx, func(tx *store.Tx) error {
		now := b.nowMillis()
		row, err := b.lockedSession(tx, addr, sessionID, now)
		if err != nil {
			return err
		}
		until = now + e.Properties.LockDuration.Milliseconds()
		row.LockedUntil = until
		if err := tx.PutSession(row); err != nil {
			return err
		}
		return tx.SetSessionMessageLocks(e.id, sessionID, until, now)
	})
	if err != nil {
		return time.Time{}, err
	}
	b.wakeTimers()
	return time.UnixMilli(until).UTC(), nil
}

// ReleaseSession gives up a session lock. Messages received through it and
// not settled become available again; their delivery attempt is counted.
func (b *Broker) ReleaseSession(ctx context.Context, lock *SessionLock) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.release_session")
	defer done(&err)
	e := b.current(lock.Address.Entity)
	if e == nil {
		return nil
	}
	err = b.store.Write(ctx, func(tx *store.Tx) error {
		row, err := tx.Session(e.id, lock.SessionID)
		if errors.Is(err, store.ErrNotFound) || err == nil && !bytes.Equal(row.LockToken, lock.Token[:]) {
			return nil // already lost to another receiver
		}
		if err != nil {
			return err
		}
		row.LockToken = nil
		row.LockedUntil = 0
		if err := tx.PutSession(row); err != nil {
			return err
		}
		return tx.UnlockSessionMessages(e.id, lock.SessionID)
	})
	if err != nil {
		return err
	}
	b.notify(e.id)
	return nil
}

// SessionState returns the state stored for a locked session, or nil.
func (b *Broker) SessionState(ctx context.Context, addr Address, sessionID string) (_ []byte, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.session_state", telemetry.Entity(addr.String()))
	defer done(&err)
	e := b.current(addr.Entity)
	if e == nil {
		return nil, fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	addr.Entity = e
	var state []byte
	err = b.store.Read(ctx, func(tx *store.Tx) error {
		row, err := b.lockedSession(tx, addr, sessionID, b.nowMillis())
		if err != nil {
			return err
		}
		state = row.State
		return nil
	})
	return state, err
}

// SetSessionState stores state for a locked session; nil clears it.
func (b *Broker) SetSessionState(ctx context.Context, addr Address, sessionID string, state []byte) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.set_session_state", telemetry.Entity(addr.String()))
	defer done(&err)
	e := b.current(addr.Entity)
	if e == nil {
		return fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	addr.Entity = e
	return b.store.Write(ctx, func(tx *store.Tx) error {
		now := b.nowMillis()
		row, err := b.lockedSession(tx, addr, sessionID, now)
		if err != nil {
			return err
		}
		row.State = state
		row.UpdatedAt = now
		return tx.PutSession(row)
	})
}

// ListSessions lists session ids of addr. With a zero updatedAfter it lists
// sessions that have messages or state; otherwise sessions whose state
// changed after updatedAfter.
func (b *Broker) ListSessions(ctx context.Context, addr Address, updatedAfter time.Time, skip, top int) (_ []string, err error) {
	ctx, done := telemetry.Operation(ctx, "broker.list_sessions", telemetry.Entity(addr.String()))
	defer done(&err)
	e := b.current(addr.Entity)
	if e == nil {
		return nil, fmt.Errorf("%w: %s", ErrEntityNotFound, addr)
	}
	var after int64
	if !updatedAfter.IsZero() {
		after = updatedAfter.UnixMilli()
	}
	var out []string
	err = b.store.Read(ctx, func(tx *store.Tx) error {
		var err error
		out, err = tx.ListSessions(e.id, after, skip, top)
		return err
	})
	return out, err
}

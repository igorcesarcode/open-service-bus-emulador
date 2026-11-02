package amqpsb

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// settleTimeout bounds a settlement started by a peer disposition. It does
// not depend on the link, which may detach right after settling.
const settleTimeout = 30 * time.Second

// receiver moves messages from the broker to a client's receiving link
// whenever the client has credit.
type receiver struct {
	h    *Handler
	link *amqp.SenderLink
	addr broker.Address
	mode broker.ReceiveMode
	// session is the held session lock of a session receiver.
	session *broker.SessionLock
}

// epochTicks is the Unix epoch in .NET ticks (100ns since 0001-01-01).
const epochTicks = 621355968000000000

func dotnetTicks(t time.Time) int64 {
	return epochTicks + t.UnixMilli()*10000
}

// acceptSession locks the requested session, or waits up to timeout for
// any session to become available, then completes the pending attach.
func (r *receiver) acceptSession(sessionID string, timeout time.Duration) {
	ctx := r.link.Context()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var (
		lock *broker.SessionLock
		err  error
	)
	for {
		changed := r.h.Broker.Changed(r.addr.Entity)
		if sessionID != "" {
			lock, err = r.h.Broker.AcceptSession(ctx, r.addr, sessionID)
		} else {
			lock, err = r.h.Broker.AcceptNextSession(ctx, r.addr)
		}
		if !errors.Is(err, broker.ErrNoSessionAvailable) {
			break
		}
		select {
		case <-changed:
			continue
		case <-deadline.C:
		case <-ctx.Done():
			return
		}
		break
	}
	if err != nil {
		r.link.CompleteAttach(amqpError(err))
		return
	}
	setSessionFilter(r.link.Source(), lock.SessionID)
	r.link.Properties = encoding.Map{
		{Key: encoding.Symbol("com.microsoft:locked-until-utc"), Value: dotnetTicks(lock.LockedUntil)},
	}
	if err := r.link.CompleteAttach(nil); err != nil {
		// The client gave up waiting.
		r.releaseSession(lock)
		return
	}
	r.session = lock
	r.h.linkSessions.Store(r.link.Name(), lock.SessionID)
	defer r.h.linkSessions.Delete(r.link.Name())
	r.run(ctx)
}

func (r *receiver) releaseSession(lock *broker.SessionLock) {
	ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	if err := r.h.Broker.ReleaseSession(ctx, lock); err != nil {
		r.h.logger().Error("releasing session", "entity", r.addr.String(), "session", lock.SessionID, "err", err)
	}
}

func (r *receiver) fetch(ctx context.Context, max int) ([]*broker.Delivery, error) {
	if r.session != nil {
		return r.h.Broker.ReceiveSession(ctx, r.session, max, r.mode)
	}
	return r.h.Broker.Receive(ctx, r.addr, max, r.mode)
}

func (r *receiver) run(ctx context.Context) {
	log := r.h.logger().With("link", r.link.Name(), "entity", r.addr.String())
	if r.session != nil {
		defer r.releaseSession(r.session)
	}
	for {
		flow := r.link.FlowChanged()
		credit, drain, err := r.link.Credit()
		if err != nil {
			return
		}
		if credit == 0 {
			if drain {
				if err := r.link.CompleteDrain(); err != nil {
					return
				}
			}
			select {
			case <-flow:
				continue
			case <-ctx.Done():
				return
			}
		}
		changed := r.h.Broker.Changed(r.addr.Entity)
		ds, err := r.fetch(ctx, int(credit))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, broker.ErrEntityNotFound) || errors.Is(err, broker.ErrSessionLockLost) {
				// Clients recover a session receiver by re-accepting its
				// session, which fails fast if another receiver took it.
				log.Debug("stopping receiver", "err", err)
				r.link.Detach(amqpError(err))
				return
			}
			log.Error("receiving messages", "err", err)
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}
		if len(ds) == 0 {
			if drain {
				if err := r.link.CompleteDrain(); err != nil {
					return
				}
			}
			select {
			case <-changed:
			case <-flow:
			case <-ctx.Done():
				return
			}
			continue
		}
		for i, d := range ds {
			if err := r.deliver(ctx, d); err != nil {
				// The link went away: make the remaining messages
				// available again.
				for _, rest := range ds[i:] {
					r.release(rest)
				}
				return
			}
		}
	}
}

func (r *receiver) deliver(ctx context.Context, d *broker.Delivery) (err error) {
	ctx, done := telemetry.Operation(ctx, "amqp.deliver", telemetry.Entity(r.addr.String()),
		attribute.Int("osb.delivery_count", d.DeliveryCount), attribute.Int("osb.bytes", len(d.Payload)), attribute.Bool("osb.peek_lock", r.mode == broker.PeekLock))
	defer done(&err)
	locked := r.mode == broker.PeekLock
	payload, err := deliveryPayload(d, locked)
	if err != nil {
		r.h.logger().Error("encoding delivery", "entity", r.addr.String(), "seq", d.SequenceNumber, "err", err)
		// A stored message that cannot be re-encoded would fail forever.
		dctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
		defer cancel()
		return r.h.Broker.DeadLetter(dctx, r.addr, d.LockToken, "MessageEncodingError", err.Error(), nil)
	}
	opts := amqp.SendOptions{Tag: deliveryTag(d.LockToken), Settled: !locked}
	if locked {
		addr, token := r.addr, d.LockToken
		opts.OnSettle = func(state frames.DeliveryState, settle func(frames.DeliveryState)) {
			go func() {
				sctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
				defer cancel()
				if ts, ok := state.(*frames.TransactionalState); ok {
					settle(r.h.enlistSettle(r.link.Context(), addr, token, ts))
					return
				}
				settle(r.h.settle(sctx, addr, token, state))
			}()
		}
	}
	if !locked {
		// Receive-and-delete is at most once: the message is removed before
		// its transfer, so a client never sees it again, even through a
		// peek right after receiving it.
		cctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
		defer cancel()
		if err := r.h.Broker.Complete(cctx, r.addr, d.LockToken); err != nil {
			r.h.logger().Error("removing received message", "entity", r.addr.String(), "seq", d.SequenceNumber, "err", err)
			r.release(d)
			return nil
		}
	}
	return r.link.Send(ctx, payload, opts)
}

func (r *receiver) release(d *broker.Delivery) {
	ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	// A message fetched but never sent, because the link went away or the
	// transfer failed: a span of its own so it can be told from a peer's release.
	ctx, done := telemetry.Operation(ctx, "amqp.release_undelivered", telemetry.Entity(r.addr.String()))
	var err error
	defer func() { done(&err) }()
	if err = r.h.Broker.Release(ctx, r.addr, d.LockToken); err != nil && !errors.Is(err, broker.ErrLockLost) {
		r.h.logger().Error("releasing message", "entity", r.addr.String(), "seq", d.SequenceNumber, "err", err)
	}
}

// settle applies a peer's delivery outcome to a locked message and returns
// the final outcome to report: the peer's own outcome on success, or a
// rejection carrying the failure.
func (h *Handler) settle(ctx context.Context, addr broker.Address, token broker.LockToken, state frames.DeliveryState) frames.DeliveryState {
	ctx, done := telemetry.Operation(ctx, "amqp.disposition", telemetry.Entity(addr.String()), attribute.String("amqp.outcome", fmt.Sprintf("%T", state)))
	var err error
	defer func() { done(&err) }()
	switch s := state.(type) {
	case *frames.Accepted:
		err = h.Broker.Complete(ctx, addr, token)
	case *frames.Released:
		err = h.Broker.Release(ctx, addr, token)
	case *frames.Modified:
		props := stringKeyed(s.MessageAnnotations)
		if s.UndeliverableHere {
			err = h.Broker.Defer(ctx, addr, token, props)
		} else {
			err = h.Broker.Abandon(ctx, addr, token, props)
		}
	case *frames.Rejected:
		reason, desc, props := deadLetterInfo(s.Error)
		err = h.Broker.DeadLetter(ctx, addr, token, reason, desc, props)
	default:
		// Non-terminal states such as received do not settle.
		return state
	}
	if err != nil {
		return &frames.Rejected{Error: amqpError(err)}
	}
	if _, ok := state.(*frames.Rejected); ok {
		// A rejected outcome reports failure to clients, so a successful
		// dead-letter settles as accepted.
		return &frames.Accepted{}
	}
	return state
}

// deadLetterInfo splits the info of a dead-letter rejection into the
// reason, the description and the properties to modify.
func deadLetterInfo(e *frames.Error) (reason, desc string, props encoding.Map) {
	if e == nil {
		return "", "", nil
	}
	for _, entry := range stringKeyed(e.Info) {
		switch entry.Key {
		case "DeadLetterReason":
			reason, _ = entry.Value.(string)
		case "DeadLetterErrorDescription":
			desc, _ = entry.Value.(string)
		default:
			props = append(props, entry)
		}
	}
	if reason == "" && e.Condition != condDeadLetter {
		reason = string(e.Condition)
		desc = e.Description
	}
	return reason, desc, props
}

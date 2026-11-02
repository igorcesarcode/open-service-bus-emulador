package broker

import (
	"context"
	"errors"
	"fmt"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// ReasonTransferFailed is the dead-letter reason of a message that a
// send-via transaction could not transfer to its destination.
const ReasonTransferFailed = "TransferFailed"

// TxOp is an operation of a transaction: sends and settlements that take
// effect together when the transaction commits.
type TxOp struct {
	settle bool

	// Send.
	to   string
	via  string
	msgs []IncomingMessage

	// Settle.
	addr  Address
	token LockToken
	s     Settlement
}

// TxSend sends msgs to the queue or topic at to. With a non-empty via, which
// is the entity the transaction received from, the send is a transfer: a
// destination that is disabled or full does not fail the commit, and the
// messages go to the transfer dead-letter queue of via instead.
func TxSend(to, via string, msgs []IncomingMessage) TxOp {
	return TxOp{to: to, via: via, msgs: msgs}
}

// TxSettle settles the message locked with token on addr.
func TxSettle(addr Address, token LockToken, s Settlement) TxOp {
	return TxOp{settle: true, addr: addr, token: token, s: s}
}

// Commit applies ops in order in one database transaction: either all take
// effect, or none do and the error says why. A settlement fails when its
// lock is lost, and a send when its destination refuses the messages, except
// for the transfers TxSend describes.
func (b *Broker) Commit(ctx context.Context, ops []TxOp) (err error) {
	ctx, done := telemetry.Operation(ctx, "broker.commit")
	defer done(&err)
	var (
		ec      *enqueueCtx
		changed []int64
		sent    = map[*Entity]int{}
	)
	err = b.store.Write(ctx, func(tx *store.Tx) error {
		ec = b.newEnqueueCtx(tx)
		changed = changed[:0]
		clear(sent)
		for _, op := range ops {
			var err error
			if op.settle {
				err = b.commitSettle(tx, op, &changed)
			} else {
				err = b.commitSend(ec, op, sent)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	b.notify(changed...)
	ec.finish()
	for _, op := range ops {
		if op.settle {
			b.request(op.addr.Entity, nil)
		}
	}
	for e, n := range sent {
		b.request(e, nil)
		b.messages(metrics.IncomingMessages, n, e)
	}
	return nil
}

func (b *Broker) commitSettle(tx *store.Tx, op TxOp, changed *[]int64) error {
	e := b.current(op.addr.Entity)
	if e == nil {
		return fmt.Errorf("%w: %s", ErrEntityNotFound, op.addr)
	}
	addr := op.addr
	addr.Entity = e
	m, err := b.lockedMessage(tx, addr, op.token, b.nowMillis())
	if err != nil {
		return err
	}
	return b.settleFn(addr, op.s)(tx, e, m, changed)
}

func (b *Broker) commitSend(ec *enqueueCtx, op TxOp, sent map[*Entity]int) error {
	e, err := b.sendTarget(op.to, op.msgs)
	if err != nil {
		if op.via == "" || !transferFailure(err) {
			return err
		}
		return b.parkTransfers(ec, op, err)
	}
	for _, m := range op.msgs {
		_, err := b.enqueue(ec, e, &outgoing{IncomingMessage: m}, 0)
		if err != nil {
			if op.via != "" && transferFailure(err) {
				if perr := b.parkTransfers(ec, TxOp{via: op.via, msgs: []IncomingMessage{m}}, err); perr != nil {
					return perr
				}
				continue
			}
			return err
		}
		sent[e]++
	}
	return ec.tx.TouchEntity(e.id, ec.now.UnixMilli())
}

// transferFailure reports whether err is a refusal by the destination of a
// transfer, which the transfer dead-letter queue absorbs: the destination is
// disabled, full or does not take the message.
func transferFailure(err error) bool {
	return errors.Is(err, ErrEntityDisabled) || errors.Is(err, ErrQuotaExceeded) ||
		errors.Is(err, ErrMessageTooLarge) || errors.Is(err, ErrEntityNotFound) || errors.Is(err, ErrSessionRequired)
}

// parkTransfers puts the messages of op in the transfer dead-letter queue
// of its via entity.
func (b *Broker) parkTransfers(ec *enqueueCtx, op TxOp, cause error) error {
	b.mu.RLock()
	via := b.entities[NormalizePath(op.via)]
	b.mu.RUnlock()
	if via == nil {
		return fmt.Errorf("%w: %s", ErrEntityNotFound, op.via)
	}
	for _, m := range op.msgs {
		if _, err := b.parkNew(ec, via, &outgoing{IncomingMessage: m}, store.SubqueueTransferDLQ, ReasonTransferFailed, cause.Error()); err != nil {
			return err
		}
	}
	return nil
}

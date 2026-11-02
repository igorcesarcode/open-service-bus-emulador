package amqpsb

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
)

// DefaultTxnTimeout is how long a transaction may stay open, as in Service
// Bus.
const DefaultTxnTimeout = 2 * time.Minute

// Error conditions of AMQP transactions (part 4 of the specification).
const (
	condTxnUnknownID = encoding.Symbol("amqp:transaction:unknown-id")
	condTxnRollback  = encoding.Symbol("amqp:transaction:rollback")
	condTxnTimeout   = encoding.Symbol("amqp:transaction:timeout")
)

// transferDestination is the link property of a send-via link that names
// the entity the messages are meant for. The link's own address is the
// entity the transaction receives from, the via entity.
const transferDestination = encoding.Symbol("com.microsoft:transfer-destination-address")

// txnKey is where a connection keeps its transactions in amqp.ConnData.
type txnKey struct{}

// txn is a transaction in progress: the operations wait here until the
// coordinator commits them together.
type txn struct {
	id       []byte
	deadline time.Time

	mu  sync.Mutex
	ops []broker.TxOp
	// scope is the entity the transaction is bound to, as in Service Bus:
	// the entity it received from or, when it only sends, the one it sends
	// to. A transaction cannot span entities except by sending via the one
	// it received from.
	scope string
	// received reports that scope was set by a receive, which makes it the
	// via entity of sends to other entities.
	received bool
	// partKey is the partition key the messages sent to partitioned
	// entities share.
	partKey string
}

// txnTable holds the open transactions of a connection.
type txnTable struct {
	mu   sync.Mutex
	txns map[string]*txn
}

func (h *Handler) txnTable(ctx context.Context) *txnTable {
	data := amqp.ConnData(ctx)
	if data == nil {
		return &txnTable{}
	}
	v, _ := data.LoadOrStore(txnKey{}, &txnTable{})
	return v.(*txnTable)
}

func (h *Handler) txnTimeout() time.Duration {
	if h.TxnTimeout > 0 {
		return h.TxnTimeout
	}
	return DefaultTxnTimeout
}

func (t *txnTable) declare(timeout time.Duration) *txn {
	id := make([]byte, 16)
	rand.Read(id)
	tx := &txn{id: id, deadline: time.Now().Add(timeout)}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.txns == nil {
		t.txns = map[string]*txn{}
	}
	// Forget the transactions nobody discharged.
	for k, old := range t.txns {
		if time.Now().After(old.deadline.Add(timeout)) {
			delete(t.txns, k)
		}
	}
	t.txns[string(id)] = tx
	return tx
}

func (t *txnTable) get(id []byte) *txn {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.txns[string(id)]
}

func (t *txnTable) take(id []byte) *txn {
	t.mu.Lock()
	defer t.mu.Unlock()
	tx := t.txns[string(id)]
	delete(t.txns, string(id))
	return tx
}

func txnError(cond encoding.Symbol, format string, args ...any) *frames.Rejected {
	return &frames.Rejected{Error: amqp.NewError(cond, fmt.Sprintf(format, args...))}
}

// coordinator serves a link to the transaction coordinator.
type coordinator struct {
	h *Handler
}

// HandleMessage implements amqp.MessageHandler: it declares and discharges
// transactions.
func (c *coordinator) HandleMessage(ctx context.Context, d *amqp.Delivery) frames.DeliveryState {
	msg, err := d.Message()
	if err != nil {
		return &frames.Rejected{Error: amqp.NewError(amqp.CondDecodeError, err.Error())}
	}
	body, err := frames.DecodeTransactionBody(msg.Value)
	if err != nil {
		return &frames.Rejected{Error: amqp.NewError(amqp.CondDecodeError, err.Error())}
	}
	table := c.h.txnTable(ctx)
	switch b := body.(type) {
	case *frames.Declare:
		tx := table.declare(c.h.txnTimeout())
		return &frames.Declared{TxnID: tx.id}
	case *frames.Discharge:
		tx := table.take(b.TxnID)
		if tx == nil {
			return txnError(condTxnUnknownID, "the transaction is unknown or already discharged")
		}
		if b.Fail {
			return &frames.Accepted{} // Rolled back: the operations are dropped.
		}
		if time.Now().After(tx.deadline) {
			return txnError(condTxnTimeout, "the transaction was open for more than %v", c.h.txnTimeout())
		}
		tx.mu.Lock()
		ops := tx.ops
		tx.mu.Unlock()
		if err := c.h.Broker.Commit(ctx, ops); err != nil {
			c.h.logger().Debug("transaction rolled back", "err", err)
			return txnError(condTxnRollback, "%s", err.Error())
		}
		return &frames.Accepted{}
	}
	return &frames.Rejected{Error: amqp.NewError(amqp.CondNotImplemented, "unsupported coordinator message")}
}

// bind records the entity an operation touches and checks the transaction
// stays within one entity. A send to another entity is allowed after a
// receive, and goes via the entity the transaction received from.
func (t *txn) bind(entity string, receive bool) (via string, err error) {
	switch {
	case t.scope == "":
		t.scope, t.received = entity, receive
		return "", nil
	case t.scope == entity:
		t.received = t.received || receive
		return "", nil
	case !receive && t.received:
		return t.scope, nil
	}
	return "", fmt.Errorf("a transaction cannot span %s and %s: operations on another entity are allowed only as sends via the entity it received from", t.scope, entity)
}

// enlistSend adds a send to the transaction of the delivery's state.
// target is the entity the messages are for and via, when not empty, the
// entity a send-via link goes through.
func (h *Handler) enlistSend(ctx context.Context, state *frames.TransactionalState, entity *broker.Entity, via string, msgs []broker.IncomingMessage) frames.DeliveryState {
	target := entity.Path
	tx := h.txnTable(ctx).get(state.TxnID)
	if tx == nil {
		return txnError(condTxnUnknownID, "the transaction is unknown or already discharged")
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if via != "" {
		if _, err := tx.bind(via, true); err != nil || tx.scope != via {
			return &frames.Rejected{Error: amqp.NewError(amqp.CondNotAllowed, fmt.Sprintf("send via %s: the transaction is bound to %s", via, tx.scope))}
		}
	} else {
		implied, err := tx.bind(target, false)
		if err != nil {
			return &frames.Rejected{Error: amqp.NewError(amqp.CondNotAllowed, err.Error())}
		}
		via = implied
	}
	key, err := broker.ValidateTransactionSend(entity, msgs, tx.partKey)
	if err != nil {
		return &frames.Rejected{Error: amqpError(err)}
	}
	tx.partKey = key
	tx.ops = append(tx.ops, broker.TxSend(target, via, msgs))
	return &frames.TransactionalState{TxnID: state.TxnID, Outcome: &frames.Accepted{}}
}

// enlistSettle adds the settlement of a received message to the transaction
// of state, which is the disposition the client sent.
func (h *Handler) enlistSettle(ctx context.Context, addr broker.Address, token broker.LockToken, state *frames.TransactionalState) frames.DeliveryState {
	tx := h.txnTable(ctx).get(state.TxnID)
	if tx == nil {
		return txnError(condTxnUnknownID, "the transaction is unknown or already discharged")
	}
	s, ok := settlementOf(state.Outcome)
	if !ok {
		return state // Not terminal, such as received: nothing to do yet.
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if _, err := tx.bind(addr.Entity.Path, true); err != nil || tx.scope != addr.Entity.Path {
		return &frames.Rejected{Error: amqp.NewError(amqp.CondNotAllowed, fmt.Sprintf("the transaction is bound to %s, not %s", tx.scope, addr.Entity.DisplayPath()))}
	}
	tx.ops = append(tx.ops, broker.TxSettle(addr, token, s))
	return state
}

// settlementOf maps the outcome of a transactional disposition to a
// settlement; false means the outcome is not terminal.
func settlementOf(outcome frames.DeliveryState) (broker.Settlement, bool) {
	switch s := outcome.(type) {
	case *frames.Accepted:
		return broker.Settlement{Kind: broker.SettleComplete}, true
	case *frames.Released:
		return broker.Settlement{Kind: broker.SettleRelease}, true
	case *frames.Modified:
		props := stringKeyed(s.MessageAnnotations)
		if s.UndeliverableHere {
			return broker.Settlement{Kind: broker.SettleDefer, Props: props}, true
		}
		return broker.Settlement{Kind: broker.SettleAbandon, Props: props}, true
	case *frames.Rejected:
		reason, desc, props := deadLetterInfo(s.Error)
		return broker.Settlement{Kind: broker.SettleDeadLetter, Reason: reason, Description: desc, Props: props}, true
	}
	return broker.Settlement{}, false
}

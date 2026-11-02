package frames

import (
	"fmt"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
)

// Descriptor codes of the transactions part of AMQP 1.0 (part 4).
const (
	codeCoordinator        = 0x30
	codeDeclare            = 0x31
	codeDischarge          = 0x32
	codeDeclared           = 0x33
	codeTransactionalState = 0x34
)

func init() {
	for name, code := range map[encoding.Symbol]uint64{
		"amqp:coordinator:list":         codeCoordinator,
		"amqp:declare:list":             codeDeclare,
		"amqp:discharge:list":           codeDischarge,
		"amqp:declared:list":            codeDeclared,
		"amqp:transactional-state:list": codeTransactionalState,
	} {
		symbolicCodes[name] = code
	}
}

// LocalTransactions is the capability of a coordinator that controls local
// transactions.
const LocalTransactions encoding.Symbol = "amqp:local-transactions"

// Coordinator is the target of a link to a transaction coordinator. On such
// a link the peer sends Declare and Discharge messages.
type Coordinator struct {
	Capabilities []encoding.Symbol
}

// MarshalAMQP implements encoding.Marshaler.
func (c *Coordinator) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeCoordinator, c.Capabilities))
}

func decodeCoordinator(d encoding.Described) (*Coordinator, error) {
	f, err := newFields("coordinator", d.Value)
	if err != nil {
		return nil, err
	}
	return &Coordinator{Capabilities: f.symbols(0)}, f.err
}

// isCoordinator reports whether a described terminus is a coordinator.
func isCoordinator(v any) (encoding.Described, bool) {
	d, ok := v.(encoding.Described)
	if !ok {
		return d, false
	}
	code, _ := DescriptorCode(d.Descriptor)
	return d, code == codeCoordinator
}

// Declare asks the coordinator to start a transaction. It is the body of a
// message sent on a coordinator link, as an amqp-value.
type Declare struct {
	GlobalID []byte
}

// MarshalAMQP implements encoding.Marshaler.
func (d *Declare) MarshalAMQP(w *encoding.Writer) error {
	var id any
	if d.GlobalID != nil {
		id = d.GlobalID
	}
	return w.Write(composite(codeDeclare, id))
}

// Discharge asks the coordinator to end a transaction: commit it, or roll
// it back when Fail is set.
type Discharge struct {
	TxnID []byte
	Fail  bool
}

// MarshalAMQP implements encoding.Marshaler.
func (d *Discharge) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeDischarge, d.TxnID, falseAsNull(d.Fail)))
}

// DecodeTransactionBody decodes the amqp-value body of a message sent to a
// coordinator: a *Declare or a *Discharge.
func DecodeTransactionBody(v any) (any, error) {
	d, ok := v.(encoding.Described)
	if !ok {
		return nil, fmt.Errorf("coordinator message: body is %T, want declare or discharge", v)
	}
	code, _ := DescriptorCode(d.Descriptor)
	switch code {
	case codeDeclare:
		f, err := newFields("declare", d.Value)
		if err != nil {
			return nil, err
		}
		out := &Declare{}
		if b, ok := f.at(0).([]byte); ok {
			out.GlobalID = b
		}
		return out, f.err
	case codeDischarge:
		f, err := newFields("discharge", d.Value)
		if err != nil {
			return nil, err
		}
		id, _ := f.at(0).([]byte)
		return &Discharge{TxnID: id, Fail: f.bool(1)}, f.err
	}
	return nil, fmt.Errorf("coordinator message: unknown body descriptor %v", d.Descriptor)
}

// Declared is the outcome of a Declare: the id of the new transaction.
type Declared struct {
	TxnID []byte
}

// TransactionalState is the state of a delivery that belongs to a
// transaction. On a transfer it enlists the delivery in the transaction; on
// a disposition, Outcome is the outcome that takes effect when the
// transaction commits.
type TransactionalState struct {
	TxnID   []byte
	Outcome DeliveryState
}

func (*Declared) isDeliveryState()           {}
func (*TransactionalState) isDeliveryState() {}

// MarshalAMQP implements encoding.Marshaler.
func (s *Declared) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeDeclared, s.TxnID))
}

// MarshalAMQP implements encoding.Marshaler.
func (s *TransactionalState) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeTransactionalState, s.TxnID, stateValue(s.Outcome)))
}

func decodeTransactionalState(f *fields) (*TransactionalState, error) {
	id, _ := f.at(0).([]byte)
	s := &TransactionalState{TxnID: id}
	if f.present(1) {
		out, err := DecodeDeliveryState(f.at(1))
		if err != nil {
			return nil, err
		}
		s.Outcome = out
	}
	return s, f.err
}

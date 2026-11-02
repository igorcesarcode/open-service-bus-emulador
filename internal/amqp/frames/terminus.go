package frames

import (
	"fmt"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
)

// Source is the source terminus of a link.
type Source struct {
	Address               string
	Durable               uint32
	ExpiryPolicy          encoding.Symbol
	Timeout               uint32
	Dynamic               bool
	DynamicNodeProperties encoding.Map
	DistributionMode      encoding.Symbol
	// Filter maps filter names to described filter values, such as
	// com.microsoft:session-filter.
	Filter         encoding.Map
	DefaultOutcome any
	Outcomes       []encoding.Symbol
	Capabilities   []encoding.Symbol
}

// MarshalAMQP implements encoding.Marshaler.
func (s *Source) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeSource,
		s.Address, zeroAsNull(s.Durable), s.ExpiryPolicy, zeroAsNull(s.Timeout),
		falseAsNull(s.Dynamic), s.DynamicNodeProperties, s.DistributionMode,
		s.Filter, s.DefaultOutcome, s.Outcomes, s.Capabilities))
}

func decodeSource(v any) (*Source, error) {
	d, ok := v.(encoding.Described)
	if !ok {
		return nil, fmt.Errorf("source: value is %T, want described list", v)
	}
	if code, _ := DescriptorCode(d.Descriptor); code != codeSource {
		return nil, fmt.Errorf("source: unexpected descriptor %v", d.Descriptor)
	}
	f, err := newFields("source", d.Value)
	if err != nil {
		return nil, err
	}
	return &Source{
		Address:               f.string(0),
		Durable:               f.uint32(1),
		ExpiryPolicy:          f.symbol(2),
		Timeout:               f.uint32(3),
		Dynamic:               f.bool(4),
		DynamicNodeProperties: f.fieldsMap(5),
		DistributionMode:      f.symbol(6),
		Filter:                f.fieldsMap(7),
		DefaultOutcome:        f.at(8),
		Outcomes:              f.symbols(9),
		Capabilities:          f.symbols(10),
	}, f.err
}

// Target is the target terminus of a link.
type Target struct {
	Address               string
	Durable               uint32
	ExpiryPolicy          encoding.Symbol
	Timeout               uint32
	Dynamic               bool
	DynamicNodeProperties encoding.Map
	Capabilities          []encoding.Symbol
}

// MarshalAMQP implements encoding.Marshaler.
func (t *Target) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeTarget,
		t.Address, zeroAsNull(t.Durable), t.ExpiryPolicy, zeroAsNull(t.Timeout),
		falseAsNull(t.Dynamic), t.DynamicNodeProperties, t.Capabilities))
}

func decodeTarget(v any) (*Target, error) {
	d, ok := v.(encoding.Described)
	if !ok {
		return nil, fmt.Errorf("target: value is %T, want described list", v)
	}
	if code, _ := DescriptorCode(d.Descriptor); code != codeTarget {
		return nil, fmt.Errorf("target: unexpected descriptor %v", d.Descriptor)
	}
	f, err := newFields("target", d.Value)
	if err != nil {
		return nil, err
	}
	return &Target{
		Address:               f.string(0),
		Durable:               f.uint32(1),
		ExpiryPolicy:          f.symbol(2),
		Timeout:               f.uint32(3),
		Dynamic:               f.bool(4),
		DynamicNodeProperties: f.fieldsMap(5),
		Capabilities:          f.symbols(6),
	}, f.err
}

func zeroAsNull(v uint32) any {
	if v == 0 {
		return nil
	}
	return v
}

// DeliveryState is the state of a delivery: one of *Received, *Accepted,
// *Rejected, *Released or *Modified.
type DeliveryState interface {
	encoding.Marshaler
	isDeliveryState()
}

// Received reports partial receipt of a delivery.
type Received struct {
	SectionNumber uint32
	SectionOffset uint64
}

// Accepted is the terminal outcome for a successfully processed delivery.
type Accepted struct{}

// Rejected is the terminal outcome for an invalid delivery.
type Rejected struct {
	Error *Error
}

// Released is the terminal outcome for a delivery that was not processed.
type Released struct{}

// Modified is the terminal outcome for a delivery that was not processed
// and whose annotations may have changed.
type Modified struct {
	DeliveryFailed     bool
	UndeliverableHere  bool
	MessageAnnotations encoding.Map
}

func (*Received) isDeliveryState() {}
func (*Accepted) isDeliveryState() {}
func (*Rejected) isDeliveryState() {}
func (*Released) isDeliveryState() {}
func (*Modified) isDeliveryState() {}

// MarshalAMQP implements encoding.Marshaler.
func (s *Received) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(encoding.Described{
		Descriptor: uint64(codeReceived),
		Value:      []any{s.SectionNumber, s.SectionOffset},
	})
}

// MarshalAMQP implements encoding.Marshaler.
func (*Accepted) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeAccepted))
}

// MarshalAMQP implements encoding.Marshaler.
func (s *Rejected) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeRejected, s.Error))
}

// MarshalAMQP implements encoding.Marshaler.
func (*Released) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeReleased))
}

// MarshalAMQP implements encoding.Marshaler.
func (s *Modified) MarshalAMQP(w *encoding.Writer) error {
	return w.Write(composite(codeModified,
		falseAsNull(s.DeliveryFailed), falseAsNull(s.UndeliverableHere), s.MessageAnnotations))
}

// stateValue returns s as an encodable value, or nil for no state.
func stateValue(s DeliveryState) any {
	if s == nil {
		return nil
	}
	return s
}

// DecodeDeliveryState decodes a described delivery state.
func DecodeDeliveryState(v any) (DeliveryState, error) {
	d, ok := v.(encoding.Described)
	if !ok {
		return nil, fmt.Errorf("delivery state: value is %T, want described list", v)
	}
	code, _ := DescriptorCode(d.Descriptor)
	f, err := newFields("delivery state", d.Value)
	if err != nil {
		return nil, err
	}
	switch code {
	case codeReceived:
		return &Received{SectionNumber: f.uint32(0), SectionOffset: f.uint64(1)}, f.err
	case codeAccepted:
		return &Accepted{}, nil
	case codeRejected:
		s := &Rejected{}
		if f.present(0) {
			if s.Error, err = decodeError(f.at(0)); err != nil {
				return nil, err
			}
		}
		return s, nil
	case codeReleased:
		return &Released{}, nil
	case codeModified:
		return &Modified{
			DeliveryFailed:     f.bool(0),
			UndeliverableHere:  f.bool(1),
			MessageAnnotations: f.fieldsMap(2),
		}, f.err
	case codeDeclared:
		id, _ := f.at(0).([]byte)
		return &Declared{TxnID: id}, f.err
	case codeTransactionalState:
		return decodeTransactionalState(f)
	}
	return nil, fmt.Errorf("unknown delivery state descriptor %v", d.Descriptor)
}

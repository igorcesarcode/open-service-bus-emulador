package frames

import (
	"fmt"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
)

// Message section descriptor codes (AMQP 1.0, part 3).
const (
	codeHeader                = 0x70
	codeDeliveryAnnotations   = 0x71
	codeMessageAnnotations    = 0x72
	codeProperties            = 0x73
	codeApplicationProperties = 0x74
	codeData                  = 0x75
	codeAMQPSequence          = 0x76
	codeAMQPValue             = 0x77
	codeFooter                = 0x78
)

// MessageFormatBatch is the message format Service Bus uses for batches:
// each data section holds one complete encoded message.
const MessageFormatBatch uint32 = 0x80013700

// Message is an annotated AMQP message. Exactly one of Data, Sequence or
// Value (with HasValue) holds the body.
type Message struct {
	Header                *Header
	DeliveryAnnotations   encoding.Map
	MessageAnnotations    encoding.Map
	Properties            *Properties
	ApplicationProperties encoding.Map
	Data                  [][]byte
	Sequence              [][]any
	Value                 any
	HasValue              bool
	Footer                encoding.Map
}

// Header carries transport headers for a message.
type Header struct {
	Durable       bool
	Priority      *uint8
	TTL           *time.Duration
	FirstAcquirer bool
	DeliveryCount uint32
}

// Properties holds the immutable bare message properties.
type Properties struct {
	MessageID          any
	UserID             []byte
	To                 string
	Subject            string
	ReplyTo            string
	CorrelationID      any
	ContentType        encoding.Symbol
	ContentEncoding    encoding.Symbol
	AbsoluteExpiryTime time.Time
	CreationTime       time.Time
	GroupID            string
	GroupSequence      *uint32
	ReplyToGroupID     string
}

// MarshalMessage encodes m as the payload of a transfer.
func MarshalMessage(m *Message) ([]byte, error) {
	var w encoding.Writer
	if err := m.MarshalAMQP(&w); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

// MarshalAMQP implements encoding.Marshaler.
func (m *Message) MarshalAMQP(w *encoding.Writer) error {
	if h := m.Header; h != nil {
		var ttl *uint32
		if h.TTL != nil {
			ms := uint32(*h.TTL / time.Millisecond)
			ttl = &ms
		}
		// delivery-count is always written: some clients read it without
		// applying the default of 0.
		if err := w.Write(composite(codeHeader,
			falseAsNull(h.Durable), h.Priority, ttl, falseAsNull(h.FirstAcquirer),
			h.DeliveryCount)); err != nil {
			return err
		}
	}
	if err := writeMapSection(w, codeDeliveryAnnotations, m.DeliveryAnnotations); err != nil {
		return err
	}
	if err := writeMapSection(w, codeMessageAnnotations, m.MessageAnnotations); err != nil {
		return err
	}
	if p := m.Properties; p != nil {
		if err := w.Write(composite(codeProperties,
			p.MessageID, p.UserID, p.To, p.Subject, p.ReplyTo, p.CorrelationID,
			p.ContentType, p.ContentEncoding, p.AbsoluteExpiryTime, p.CreationTime,
			p.GroupID, p.GroupSequence, p.ReplyToGroupID)); err != nil {
			return err
		}
	}
	if err := writeMapSection(w, codeApplicationProperties, m.ApplicationProperties); err != nil {
		return err
	}
	for _, d := range m.Data {
		if d == nil {
			d = []byte{}
		}
		if err := w.Write(encoding.Described{Descriptor: uint64(codeData), Value: d}); err != nil {
			return err
		}
	}
	for _, s := range m.Sequence {
		if s == nil {
			s = []any{}
		}
		if err := w.Write(encoding.Described{Descriptor: uint64(codeAMQPSequence), Value: s}); err != nil {
			return err
		}
	}
	if m.HasValue {
		if err := w.Write(encoding.Described{Descriptor: uint64(codeAMQPValue), Value: m.Value}); err != nil {
			return err
		}
	}
	return writeMapSection(w, codeFooter, m.Footer)
}

func writeMapSection(w *encoding.Writer, code uint64, m encoding.Map) error {
	if m == nil {
		return nil
	}
	return w.Write(encoding.Described{Descriptor: code, Value: m})
}

// UnmarshalMessage decodes a message from a complete transfer payload.
func UnmarshalMessage(data []byte) (*Message, error) {
	m := &Message{}
	r := encoding.NewReader(data)
	for r.Len() > 0 {
		v, err := r.Read()
		if err != nil {
			return nil, fmt.Errorf("message: %w", err)
		}
		d, ok := v.(encoding.Described)
		if !ok {
			return nil, fmt.Errorf("message: section is %T, want described type", v)
		}
		code, _ := DescriptorCode(d.Descriptor)
		switch code {
		case codeHeader:
			if m.Header, err = decodeHeader(d.Value); err != nil {
				return nil, err
			}
		case codeDeliveryAnnotations:
			m.DeliveryAnnotations, err = sectionMap("delivery-annotations", d.Value)
		case codeMessageAnnotations:
			m.MessageAnnotations, err = sectionMap("message-annotations", d.Value)
		case codeProperties:
			m.Properties, err = decodeProperties(d.Value)
		case codeApplicationProperties:
			m.ApplicationProperties, err = sectionMap("application-properties", d.Value)
		case codeData:
			b, ok := d.Value.([]byte)
			if !ok {
				return nil, fmt.Errorf("message: data section is %T, want binary", d.Value)
			}
			m.Data = append(m.Data, b)
		case codeAMQPSequence:
			l, ok := d.Value.([]any)
			if !ok {
				return nil, fmt.Errorf("message: amqp-sequence section is %T, want list", d.Value)
			}
			m.Sequence = append(m.Sequence, l)
		case codeAMQPValue:
			m.Value, m.HasValue = d.Value, true
		case codeFooter:
			m.Footer, err = sectionMap("footer", d.Value)
		default:
			return nil, fmt.Errorf("message: unknown section descriptor %v", d.Descriptor)
		}
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

func sectionMap(name string, v any) (encoding.Map, error) {
	switch v := v.(type) {
	case nil:
		return encoding.Map{}, nil
	case encoding.Map:
		return v, nil
	}
	return nil, fmt.Errorf("message: %s section is %T, want map", name, v)
}

func decodeHeader(v any) (*Header, error) {
	f, err := newFields("header", v)
	if err != nil {
		return nil, err
	}
	h := &Header{
		Durable:       f.bool(0),
		Priority:      f.uint8Ptr(1),
		FirstAcquirer: f.bool(3),
		DeliveryCount: f.uint32(4),
	}
	if ms := f.uint32Ptr(2); ms != nil {
		ttl := time.Duration(*ms) * time.Millisecond
		h.TTL = &ttl
	}
	return h, f.err
}

func decodeProperties(v any) (*Properties, error) {
	f, err := newFields("properties", v)
	if err != nil {
		return nil, err
	}
	return &Properties{
		MessageID:          f.at(0),
		UserID:             f.binary(1),
		To:                 f.string(2),
		Subject:            f.string(3),
		ReplyTo:            f.string(4),
		CorrelationID:      f.at(5),
		ContentType:        f.symbol(6),
		ContentEncoding:    f.symbol(7),
		AbsoluteExpiryTime: f.timestamp(8),
		CreationTime:       f.timestamp(9),
		GroupID:            f.string(10),
		GroupSequence:      f.uint32Ptr(11),
		ReplyToGroupID:     f.string(12),
	}, f.err
}

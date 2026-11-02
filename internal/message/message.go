// Package message builds and reads the AMQP messages the broker stores, for
// the layers that send or show messages without an AMQP client: the portal
// and the MCP server.
package message

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
)

// Annotations Service Bus keeps on messages.
const (
	AnnotationScheduled    = encoding.Symbol("x-opt-scheduled-enqueue-time")
	AnnotationPartitionKey = encoding.Symbol("x-opt-partition-key")
)

// Body returns the body of m as bytes, whether it was carried as data, as a
// value or as a sequence.
func Body(m *frames.Message) []byte {
	switch {
	case len(m.Data) > 0:
		return bytes.Join(m.Data, nil)
	case m.HasValue:
		switch v := m.Value.(type) {
		case string:
			return []byte(v)
		case []byte:
			return v
		default:
			b, _ := json.Marshal(Jsonable(v))
			return b
		}
	case len(m.Sequence) > 0:
		b, _ := json.Marshal(Jsonable(m.Sequence))
		return b
	}
	return nil
}

// Prop is an application property written as text with an AMQP type name.
type Prop struct{ Key, Type, Value string }

// Spec describes a message to send.
type Spec struct {
	ContentType, Body                            string
	MessageID, SessionID, CorrelationID, Subject string
	ReplyTo, ReplyToSessionID, To, PartitionKey  string
	// TTL is an ISO 8601 duration or a number of seconds; Scheduled is an
	// RFC 3339 time or "2006-01-02T15:04".
	TTL, Scheduled string
	Custom         []Prop
}

// Build encodes the spec as a message ready for the broker.
func (f Spec) Build() (broker.IncomingMessage, error) {
	msg := &frames.Message{Properties: &frames.Properties{
		Subject: f.Subject, To: f.To, ReplyTo: f.ReplyTo, GroupID: f.SessionID, ReplyToGroupID: f.ReplyToSessionID,
		ContentType: encoding.Symbol(f.ContentType),
	}}
	id := f.MessageID
	if id == "" {
		id = NewID()
	}
	msg.Properties.MessageID = id
	if f.CorrelationID != "" {
		msg.Properties.CorrelationID = f.CorrelationID
	}
	msg.Data = [][]byte{[]byte(f.Body)}
	in := broker.IncomingMessage{MessageID: id, SessionID: f.SessionID}
	if f.TTL != "" {
		ttl, err := ParseTTL(f.TTL)
		if err != nil {
			return in, fmt.Errorf("Time to live: %w", err)
		}
		msg.Header = &frames.Header{TTL: &ttl}
		in.TimeToLive = ttl
	}
	if f.Scheduled != "" {
		t, err := time.Parse("2006-01-02T15:04", f.Scheduled)
		if err != nil {
			if t, err = time.Parse(time.RFC3339, f.Scheduled); err != nil {
				return in, fmt.Errorf("Scheduled enqueue time: want a date and time")
			}
		}
		msg.MessageAnnotations.Set(AnnotationScheduled, t.UTC())
		in.ScheduledEnqueueTime = t.UTC()
	}
	if f.PartitionKey != "" {
		msg.MessageAnnotations.Set(AnnotationPartitionKey, f.PartitionKey)
		in.PartitionKey = f.PartitionKey
	}
	for _, p := range f.Custom {
		if p.Key == "" {
			continue
		}
		v, err := CustomValue(p.Type, p.Value)
		if err != nil {
			return in, fmt.Errorf("Custom property %s: %v", p.Key, err)
		}
		msg.ApplicationProperties.Set(p.Key, v)
	}
	payload, err := frames.MarshalMessage(msg)
	if err != nil {
		return in, err
	}
	in.Payload = payload
	return in, nil
}

// Jsonable converts AMQP values to values encoding/json can write.
func Jsonable(v any) any {
	switch v := v.(type) {
	case encoding.Map:
		out := make(map[string]any, len(v))
		for _, e := range v {
			out[fmt.Sprint(e.Key)] = Jsonable(e.Value)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = Jsonable(x)
		}
		return out
	case [][]any:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = Jsonable(x)
		}
		return out
	case []byte:
		return hex.EncodeToString(v)
	case encoding.UUID:
		return v.String()
	case encoding.Symbol:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	}
	return v
}

// TypeName names the type of an application property as the SDKs do.
func TypeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case bool:
		return "boolean"
	case int8:
		return "byte"
	case int16:
		return "short"
	case int32:
		return "int"
	case int64:
		return "long"
	case uint8:
		return "ubyte"
	case uint16:
		return "ushort"
	case uint32:
		return "uint"
	case uint64:
		return "ulong"
	case float32:
		return "float"
	case float64:
		return "double"
	case time.Time:
		return "datetime"
	case encoding.UUID:
		return "uuid"
	case []byte:
		return "binary"
	case encoding.Symbol:
		return "symbol"
	}
	return fmt.Sprintf("%T", v)
}

// NewID returns a random message id of 32 hex digits, the form
// Service Bus assigns.
func NewID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// CustomValue converts a typed text value for an application property.
func CustomValue(typ, v string) (any, error) {
	switch typ {
	case "", "string":
		return v, nil
	case "int":
		n, err := strconv.ParseInt(v, 10, 32)
		return int32(n), err
	case "long":
		return strconv.ParseInt(v, 10, 64)
	case "double":
		return strconv.ParseFloat(v, 64)
	case "boolean", "bool":
		return strconv.ParseBool(v)
	case "uuid":
		u, err := uuid.Parse(v)
		return encoding.UUID(u), err
	case "datetime":
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04", "2006-01-02T15:04:05"} {
			if t, err := time.ParseInLocation(layout, v, time.UTC); err == nil {
				return t.UTC(), nil
			}
		}
		return nil, fmt.Errorf("want an RFC 3339 time")
	}
	return nil, fmt.Errorf("unknown type %q", typ)
}

// ParseTTL reads an ISO 8601 duration or a number of seconds.
func ParseTTL(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		if n <= 0 {
			return 0, fmt.Errorf("must be positive")
		}
		return time.Duration(n * float64(time.Second)), nil
	}
	d, err := duration.Parse(s)
	if err == nil && d <= 0 {
		err = fmt.Errorf("must be positive")
	}
	return d, err
}

// Resend prepares a stored message to be sent again: the system
// annotations of the old delivery are dropped and the rest is kept.
func Resend(d *broker.Delivery) (broker.IncomingMessage, error) {
	m, err := frames.UnmarshalMessage(d.Payload)
	if err != nil {
		return broker.IncomingMessage{}, err
	}
	m.DeliveryAnnotations = nil
	m.MessageAnnotations = nil
	if m.Header != nil {
		m.Header.DeliveryCount = 0
	}
	// Keep an id for deduplication and for the new reader.
	if m.Properties == nil {
		m.Properties = &frames.Properties{}
	}
	if m.Properties.MessageID == nil {
		m.Properties.MessageID = NewID()
	}
	payload, err := frames.MarshalMessage(m)
	if err != nil {
		return broker.IncomingMessage{}, err
	}
	in := broker.IncomingMessage{Payload: payload, MessageID: broker.MessageIDString(m.Properties.MessageID), SessionID: m.Properties.GroupID}
	if m.Header != nil && m.Header.TTL != nil {
		in.TimeToLive = *m.Header.TTL
	}
	return in, nil
}

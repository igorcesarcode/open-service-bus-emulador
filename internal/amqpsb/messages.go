package amqpsb

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
)

// Message annotations Service Bus uses for system properties.
const (
	annotationSequenceNumber   = encoding.Symbol("x-opt-sequence-number")
	annotationEnqueuedTime     = encoding.Symbol("x-opt-enqueued-time")
	annotationLockedUntil      = encoding.Symbol("x-opt-locked-until")
	annotationMessageState     = encoding.Symbol("x-opt-message-state")
	annotationScheduledTime    = encoding.Symbol("x-opt-scheduled-enqueue-time")
	annotationPartitionKey     = encoding.Symbol("x-opt-partition-key")
	annotationDeadLetterSource = encoding.Symbol("x-opt-deadletter-source")
	annotationLockToken        = encoding.Symbol("x-opt-lock-token")
)

// incomingMessage extracts the system properties the broker needs from an
// encoded message. Like Service Bus, it assigns a message id to messages
// sent without one.
func incomingMessage(payload []byte) (broker.IncomingMessage, error) {
	m, err := frames.UnmarshalMessage(payload)
	if err != nil {
		return broker.IncomingMessage{}, err
	}
	if m.Properties == nil || m.Properties.MessageID == nil {
		id := newMessageID()
		payload, err = frames.RewriteMessage(payload, func(env *frames.Envelope) error {
			if env.Properties == nil {
				env.Properties = &frames.Properties{}
			}
			env.Properties.MessageID = id
			env.PropertiesModified = true
			return nil
		})
		if err != nil {
			return broker.IncomingMessage{}, err
		}
		if m.Properties == nil {
			m.Properties = &frames.Properties{}
		}
		m.Properties.MessageID = id
	}
	im := broker.IncomingMessage{Payload: payload}
	if p := m.Properties; p != nil {
		im.MessageID = broker.MessageIDString(p.MessageID)
		im.SessionID = p.GroupID
	}
	if h := m.Header; h != nil && h.TTL != nil && *h.TTL > 0 {
		im.TimeToLive = *h.TTL
	}
	if v, ok := m.MessageAnnotations.Get(annotationPartitionKey); ok {
		im.PartitionKey, _ = v.(string)
	}
	if v, ok := m.MessageAnnotations.Get(annotationScheduledTime); ok {
		if t, ok := v.(time.Time); ok {
			im.ScheduledEnqueueTime = t
		}
	}
	return im, nil
}

// newMessageID returns a random message id in the 32 hex digit form
// Service Bus uses for the ids it assigns.
func newMessageID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// deliveryPayload adds the system annotations of d to its stored message.
// The bare message is kept byte for byte.
func deliveryPayload(d *broker.Delivery, locked bool) ([]byte, error) {
	return frames.RewriteMessage(d.Payload, func(env *frames.Envelope) error {
		if env.Header == nil {
			env.Header = &frames.Header{}
		}
		// The header counts earlier deliveries; clients add one.
		env.Header.DeliveryCount = uint32(max(0, d.DeliveryCount-1))
		if env.Header.TTL == nil && !d.ExpiresAt.IsZero() {
			if ttl := d.ExpiresAt.Sub(d.EnqueuedAt); ttl > 0 && ttl.Milliseconds() < 1<<32 {
				env.Header.TTL = &ttl
			}
		}
		env.MessageAnnotations.Set(annotationSequenceNumber, d.SequenceNumber)
		env.MessageAnnotations.Set(annotationEnqueuedTime, d.EnqueuedAt)
		env.MessageAnnotations.Set(annotationMessageState, int32(d.State))
		if d.DeadLetterSource != "" {
			env.MessageAnnotations.Set(annotationDeadLetterSource, d.DeadLetterSource)
		}
		env.DeliveryAnnotations = nil
		if locked {
			env.MessageAnnotations.Set(annotationLockedUntil, d.LockedUntil)
			env.DeliveryAnnotations = encoding.Map{{Key: annotationLockToken, Value: encoding.UUID(d.LockToken)}}
		} else {
			env.MessageAnnotations.Delete(annotationLockedUntil)
		}
		return nil
	})
}

// deliveryTag encodes a lock token as a delivery tag. Service Bus uses the
// .NET Guid byte layout, whose first three fields are little-endian.
func deliveryTag(t broker.LockToken) []byte {
	b := t
	b[0], b[1], b[2], b[3] = b[3], b[2], b[1], b[0]
	b[4], b[5] = b[5], b[4]
	b[6], b[7] = b[7], b[6]
	return b[:]
}

// lockTokenFromTag reverses deliveryTag.
func lockTokenFromTag(tag []byte) (broker.LockToken, bool) {
	var t broker.LockToken
	if len(tag) != len(t) {
		return t, false
	}
	copy(t[:], deliveryTag(broker.LockToken(tag)))
	return t, true
}

// stringKeyed converts a map with string or symbol keys into application
// property entries, which must have string keys.
func stringKeyed(m encoding.Map) encoding.Map {
	if len(m) == 0 {
		return nil
	}
	out := make(encoding.Map, 0, len(m))
	for _, e := range m {
		switch k := e.Key.(type) {
		case string:
			out = append(out, e)
		case encoding.Symbol:
			out = append(out, encoding.MapEntry{Key: string(k), Value: e.Value})
		}
	}
	return out
}

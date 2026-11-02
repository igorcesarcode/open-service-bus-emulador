package portal

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/duration"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/message"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/portal/views"
)

var stateNames = map[int]string{0: "Active", 1: "Deferred", 2: "Scheduled"}

// decoded is a delivery with its message decoded.
type decoded struct {
	d   *broker.Delivery
	msg *frames.Message
	// body is the message body as bytes, and bodyKind says how it was
	// carried: "data", "value" or "sequence".
	body []byte
}

func decode(d *broker.Delivery) (*decoded, error) {
	m, err := frames.UnmarshalMessage(d.Payload)
	if err != nil {
		return nil, err
	}
	out := &decoded{d: d, msg: m, body: message.Body(m)}
	return out, nil
}

// bodyText formats a body for reading: indented JSON, text, or hex.
func bodyText(b []byte) (text, kind string) {
	if len(b) == 0 {
		return "", "empty"
	}
	if utf8.Valid(b) {
		var v any
		if (b[0] == '{' || b[0] == '[') && json.Unmarshal(b, &v) == nil {
			var buf bytes.Buffer
			if json.Indent(&buf, b, "", "  ") == nil {
				return buf.String(), "json"
			}
		}
		return string(b), "text"
	}
	return hex.Dump(b), "hex"
}

// oneLine shortens a body for a table cell.
func oneLine(b []byte, max int) string {
	if !utf8.Valid(b) {
		return fmt.Sprintf("<%d bytes of binary>", len(b))
	}
	s := strings.Join(strings.Fields(string(b)), " ")
	if r := []rune(s); len(r) > max {
		return string(r[:max]) + "..."
	}
	return s
}

func (x *decoded) messageID() string {
	if x.msg.Properties == nil {
		return ""
	}
	return broker.MessageIDString(x.msg.Properties.MessageID)
}

func (x *decoded) props() frames.Properties {
	if x.msg.Properties == nil {
		return frames.Properties{}
	}
	return *x.msg.Properties
}

func timeStamp(t time.Time, loc *time.Location) string {
	return fmt.Sprintf("%q (%s)", t.UTC().Format("2006-01-02T15:04:05.000Z"), t.In(loc).Format("Mon, Jan 02, 2006, 03:04:05 PM MST"))
}

// row is the table row of a delivery.
func (x *decoded) row(loc *time.Location, utc bool) views.MessageRow {
	p := x.props()
	t := x.d.EnqueuedAt.In(loc)
	if utc {
		t = x.d.EnqueuedAt.UTC()
	}
	r := views.MessageRow{
		Seq: x.d.SequenceNumber, MessageID: x.messageID(), Enqueued: t.Format("Mon, Jan 02, 06, 03:04:05 PM MST"),
		DeliveryCount: x.d.DeliveryCount, State: stateNames[x.d.State], BodySize: formatBytes(int64(len(x.body))),
		Subject: p.Subject, Text: oneLine(x.body, 80), SessionID: x.d.SessionID,
	}
	if x.d.LockToken != (broker.LockToken{}) {
		r.LockToken = x.d.LockToken.String()
		r.LockedUntil = x.d.LockedUntil.In(loc).Format("03:04:05 PM MST")
	}
	return r
}

func (x *decoded) detail(loc *time.Location) views.MessageDetail {
	p := x.props()
	text, kind := bodyText(x.body)
	det := views.MessageDetail{Seq: x.d.SequenceNumber, Body: text, BodyKind: kind}
	add := func(k, v string) {
		if v != "" {
			det.Broker = append(det.Broker, views.KV{Key: k, Value: v})
		}
	}
	add("deliveryCount", strconv.Itoa(x.d.DeliveryCount))
	add("contentType", string(p.ContentType))
	add("messageId", x.messageID())
	add("sequenceNumber", strconv.FormatInt(x.d.SequenceNumber, 10))
	add("enqueuedTimeUtc", timeStamp(x.d.EnqueuedAt, loc))
	if !x.d.ExpiresAt.IsZero() {
		add("expiresAtUtc", timeStamp(x.d.ExpiresAt, loc))
	}
	if x.d.LockToken != (broker.LockToken{}) {
		add("lockedUntilUtc", timeStamp(x.d.LockedUntil, loc))
		add("lockToken", x.d.LockToken.String())
	}
	add("subject", p.Subject)
	add("correlationId", broker.MessageIDString(p.CorrelationID))
	add("sessionId", x.d.SessionID)
	add("replyTo", p.ReplyTo)
	add("replyToSessionId", p.ReplyToGroupID)
	add("to", p.To)
	if h := x.msg.Header; h != nil && h.TTL != nil {
		add("timeToLive", duration.Format(*h.TTL))
	}
	if v, ok := x.msg.MessageAnnotations.Get(message.AnnotationScheduled); ok {
		if t, ok := v.(time.Time); ok {
			add("scheduledEnqueueTimeUtc", timeStamp(t, loc))
		}
	}
	if v, ok := x.msg.MessageAnnotations.Get(message.AnnotationPartitionKey); ok {
		add("partitionKey", fmt.Sprint(v))
	}
	add("deadLetterSource", x.d.DeadLetterSource)
	add("state", stateNames[x.d.State])
	for _, e := range x.msg.ApplicationProperties {
		k := fmt.Sprint(e.Key)
		switch k {
		case "DeadLetterReason":
			add("deadLetterReason", fmt.Sprint(e.Value))
		case "DeadLetterErrorDescription":
			add("deadLetterErrorDescription", fmt.Sprint(e.Value))
		}
		det.Custom = append(det.Custom, views.Prop{Key: k, Value: fmt.Sprint(message.Jsonable(e.Value)), Type: message.TypeName(e.Value)})
	}
	return det
}

// parseToken reads a lock token in the form LockToken.String writes.
func parseToken(s string) (broker.LockToken, error) {
	var t broker.LockToken
	raw, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(raw) != len(t) {
		return t, fmt.Errorf("invalid lock token %q", s)
	}
	copy(t[:], raw)
	return t, nil
}

// SendForm is what the Send messages panel posts.
type sendForm struct {
	ContentType, Body                            string
	MessageID, SessionID, CorrelationID, Subject string
	ReplyTo, ReplyToSessionID, To, PartitionKey  string
	TTL, Scheduled                               string
	Custom                                       []views.Prop
	Repeat                                       int
	IntervalMs                                   int
}

// build encodes the form as a message ready for the broker.
func (f sendForm) build() (broker.IncomingMessage, error) {
	spec := message.Spec{
		ContentType: f.ContentType, Body: f.Body, MessageID: f.MessageID, SessionID: f.SessionID,
		CorrelationID: f.CorrelationID, Subject: f.Subject, ReplyTo: f.ReplyTo, ReplyToSessionID: f.ReplyToSessionID,
		To: f.To, PartitionKey: f.PartitionKey, TTL: f.TTL, Scheduled: f.Scheduled,
	}
	for _, p := range f.Custom {
		spec.Custom = append(spec.Custom, message.Prop{Key: p.Key, Type: p.Type, Value: p.Value})
	}
	return spec.Build()
}

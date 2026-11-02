package message

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
)

func TestBuildRoundTrip(t *testing.T) {
	in, err := Spec{
		Body: "hello", ContentType: "text/plain", MessageID: "m-1", SessionID: "s", CorrelationID: "c", Subject: "sub",
		To: "to", ReplyTo: "rt", ReplyToSessionID: "rs", PartitionKey: "pk", TTL: "PT1H", Scheduled: "2030-01-02T03:04:05Z",
		Custom: []Prop{{"n", "long", "7"}, {"f", "boolean", "true"}, {"s", "", "text"}, {"d", "double", "1.5"}},
	}.Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if in.MessageID != "m-1" || in.SessionID != "s" || in.TimeToLive != time.Hour || in.PartitionKey != "pk" ||
		!in.ScheduledEnqueueTime.Equal(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("Build() = %+v", in)
	}
	m, err := frames.UnmarshalMessage(in.Payload)
	if err != nil {
		t.Fatalf("UnmarshalMessage() error: %v", err)
	}
	if got := string(Body(m)); got != "hello" {
		t.Errorf("Body() = %q, want hello", got)
	}
	if p := m.Properties; p.Subject != "sub" || p.GroupID != "s" || string(p.ContentType) != "text/plain" || p.To != "to" || p.ReplyTo != "rt" {
		t.Errorf("Properties = %+v", p)
	}
	props := map[string]any{}
	for _, e := range m.ApplicationProperties {
		props[e.Key.(string)] = e.Value
	}
	if diff := cmp.Diff(map[string]any{"n": int64(7), "f": true, "s": "text", "d": 1.5}, props); diff != "" {
		t.Errorf("application properties mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildGeneratesAnID(t *testing.T) {
	a, _ := Spec{Body: "x"}.Build()
	b, _ := Spec{Body: "x"}.Build()
	if len(a.MessageID) != 32 || a.MessageID == b.MessageID {
		t.Errorf("generated ids = %q, %q, want two different 32 digit ones", a.MessageID, b.MessageID)
	}
}

func TestBuildErrors(t *testing.T) {
	for name, s := range map[string]Spec{
		"ttl":       {TTL: "soon"},
		"zero ttl":  {TTL: "0"},
		"scheduled": {Scheduled: "tomorrow"},
		"type":      {Custom: []Prop{{"k", "complex", "v"}}},
		"value":     {Custom: []Prop{{"k", "long", "many"}}},
	} {
		if _, err := s.Build(); err == nil {
			t.Errorf("%s: Build() did not fail", name)
		}
	}
}

func TestResendDropsDeliveryState(t *testing.T) {
	in, _ := Spec{Body: "x", MessageID: "m", TTL: "PT1M", PartitionKey: "k"}.Build()
	m, _ := frames.UnmarshalMessage(in.Payload)
	m.Header.DeliveryCount = 5
	payload, _ := frames.MarshalMessage(m)
	out, err := Resend(&broker.Delivery{Payload: payload})
	if err != nil {
		t.Fatalf("Resend() error: %v", err)
	}
	got, _ := frames.UnmarshalMessage(out.Payload)
	if out.MessageID != "m" || out.TimeToLive != time.Minute || got.Header.DeliveryCount != 0 || got.MessageAnnotations != nil {
		t.Errorf("Resend() = %+v, header %+v, annotations %v", out, got.Header, got.MessageAnnotations)
	}
}

func TestJsonableAndTypeName(t *testing.T) {
	if got := TypeName(int32(1)); got != "int" {
		t.Errorf("TypeName(int32) = %q", got)
	}
	if got := Jsonable([]byte{0xab}); got != "ab" {
		t.Errorf("Jsonable(bytes) = %v, want hex", got)
	}
}

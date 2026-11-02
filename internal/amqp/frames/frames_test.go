package frames

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
)

func ptr[T any](v T) *T { return &v }

func TestFrameRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		typ     uint8
		body    Body
		payload []byte
	}{
		{name: "open", body: &Open{
			ContainerID:         "container",
			Hostname:            "localhost",
			MaxFrameSize:        ptr(uint32(65536)),
			ChannelMax:          ptr(uint16(10)),
			IdleTimeout:         30 * time.Second,
			OfferedCapabilities: []encoding.Symbol{"cap"},
			Properties:          encoding.Map{{Key: encoding.Symbol("product"), Value: "osb"}},
		}},
		{name: "begin", body: &Begin{
			RemoteChannel:  ptr(uint16(3)),
			NextOutgoingID: 1,
			IncomingWindow: 5000,
			OutgoingWindow: 5000,
			HandleMax:      ptr(uint32(255)),
		}},
		{name: "attach", body: &Attach{
			Name:               "link",
			Handle:             0,
			Role:               RoleReceiver,
			SenderSettleMode:   ptr(uint8(SenderSettleModeMixed)),
			ReceiverSettleMode: ptr(uint8(ReceiverSettleModeFirst)),
			Source: &Source{
				Address: "queue",
				Filter: encoding.Map{{
					Key:   encoding.Symbol("com.microsoft:session-filter"),
					Value: encoding.Described{Descriptor: uint64(0x00000137000000C), Value: "s1"},
				}},
				Outcomes: []encoding.Symbol{"amqp:accepted:list"},
			},
			Target:               &Target{Address: "client"},
			InitialDeliveryCount: ptr(uint32(0)),
			MaxMessageSize:       1 << 20,
			Properties:           encoding.Map{{Key: encoding.Symbol("k"), Value: int64(1)}},
		}},
		{name: "flow", body: &Flow{
			NextIncomingID: ptr(uint32(0)),
			IncomingWindow: 100,
			NextOutgoingID: 2,
			OutgoingWindow: 100,
			Handle:         ptr(uint32(0)),
			DeliveryCount:  ptr(uint32(0)),
			LinkCredit:     ptr(uint32(10)),
			Drain:          true,
			Echo:           true,
		}},
		{name: "transfer", body: &Transfer{
			Handle:        1,
			DeliveryID:    ptr(uint32(7)),
			DeliveryTag:   []byte{1, 2, 3},
			MessageFormat: ptr(uint32(0)),
			Settled:       true,
			More:          true,
			State:         &Accepted{},
		}, payload: []byte{0x00, 0x53, 0x75, 0xa0, 1, 'x'}},
		{name: "disposition rejected", body: &Disposition{
			Role:    RoleReceiver,
			First:   1,
			Last:    ptr(uint32(4)),
			Settled: true,
			State: &Rejected{Error: &Error{
				Condition:   "com.microsoft:dead-letter",
				Description: "bad",
				Info:        encoding.Map{{Key: encoding.Symbol("DeadLetterReason"), Value: "r"}},
			}},
		}},
		{name: "disposition modified", body: &Disposition{
			Role:  RoleReceiver,
			First: 1,
			State: &Modified{UndeliverableHere: true, MessageAnnotations: encoding.Map{{Key: "a", Value: "b"}}},
		}},
		{name: "disposition released", body: &Disposition{First: 2, State: &Released{}}},
		{name: "disposition received", body: &Disposition{First: 2, State: &Received{SectionNumber: 1, SectionOffset: 9}}},
		{name: "detach", body: &Detach{Handle: 2, Closed: true, Error: &Error{Condition: "amqp:not-found"}}},
		{name: "end", body: &End{}},
		{name: "end with error", body: &End{Error: &Error{Condition: "amqp:internal-error"}}},
		{name: "close", body: &Close{}},
		{name: "sasl mechanisms", typ: TypeSASL, body: &SASLMechanisms{Mechanisms: []encoding.Symbol{"ANONYMOUS", "PLAIN"}}},
		{name: "sasl init", typ: TypeSASL, body: &SASLInit{Mechanism: "ANONYMOUS", InitialResponse: []byte("x"), Hostname: "h"}},
		{name: "sasl response", typ: TypeSASL, body: &SASLResponse{Response: []byte("r")}},
		{name: "sasl outcome", typ: TypeSASL, body: &SASLOutcome{Code: SASLCodeOK}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := AppendFrame(nil, tc.typ, 5, tc.body, tc.payload)
			if err != nil {
				t.Fatalf("AppendFrame() error: %v", err)
			}
			got, err := ReadFrame(bytes.NewReader(buf), 1<<20)
			if err != nil {
				t.Fatalf("ReadFrame(% x) error: %v", buf, err)
			}
			if got.Type != tc.typ || got.Channel != 5 {
				t.Errorf("ReadFrame() type/channel = %d/%d, want %d/5", got.Type, got.Channel, tc.typ)
			}
			if tr, ok := tc.body.(*Transfer); ok {
				tr.Payload = tc.payload
			}
			if diff := cmp.Diff(tc.body, got.Body, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("round trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReadFrameEmpty(t *testing.T) {
	buf, err := AppendFrame(nil, TypeAMQP, 0, nil, nil)
	if err != nil {
		t.Fatalf("AppendFrame() error: %v", err)
	}
	if want := []byte{0, 0, 0, 8, 2, 0, 0, 0}; !bytes.Equal(buf, want) {
		t.Errorf("empty frame = % x, want % x", buf, want)
	}
	f, err := ReadFrame(bytes.NewReader(buf), 512)
	if err != nil {
		t.Fatalf("ReadFrame() error: %v", err)
	}
	if f.Body != nil {
		t.Errorf("ReadFrame() body = %v, want nil", f.Body)
	}
}

func TestReadFrameErrors(t *testing.T) {
	big, err := AppendFrame(nil, TypeAMQP, 0, &Transfer{}, make([]byte, 1000))
	if err != nil {
		t.Fatalf("AppendFrame() error: %v", err)
	}
	if _, err := ReadFrame(bytes.NewReader(big), 512); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("ReadFrame(oversized) error = %v, want ErrFrameTooLarge", err)
	}
	malformed := [][]byte{
		{0, 0, 0, 4, 2, 0, 0, 0},
		{0, 0, 0, 8, 1, 0, 0, 0},
		{0, 0, 0, 8, 3, 0, 0, 0},
		{0, 0, 0, 9, 2, 0, 0, 0, 0x40},
	}
	for _, b := range malformed {
		if _, err := ReadFrame(bytes.NewReader(b), 512); err == nil {
			t.Errorf("ReadFrame(% x) succeeded, want error", b)
		}
	}
}

func TestDecodeSymbolicDescriptor(t *testing.T) {
	body, err := encoding.Marshal(encoding.Described{
		Descriptor: encoding.Symbol("amqp:close:list"),
		Value:      []any{},
	})
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	got, err := Decode(body)
	if err != nil {
		t.Fatalf("Decode() error: %v", err)
	}
	if _, ok := got.(*Close); !ok {
		t.Errorf("Decode() = %T, want *Close", got)
	}
}

func TestMessageRoundTrip(t *testing.T) {
	ttl := 90 * time.Second
	created := time.UnixMilli(1735689600000).UTC()
	tests := []struct {
		name string
		msg  *Message
	}{
		{"data", &Message{Data: [][]byte{[]byte("hello")}}},
		{"value", &Message{Value: "token", HasValue: true}},
		{"null value", &Message{HasValue: true}},
		{"sequence", &Message{Sequence: [][]any{{int32(1), "a"}}}},
		{"full", &Message{
			Header:              &Header{Durable: true, Priority: ptr(uint8(4)), TTL: &ttl, DeliveryCount: 2},
			DeliveryAnnotations: encoding.Map{{Key: encoding.Symbol("x-da"), Value: "d"}},
			MessageAnnotations: encoding.Map{
				{Key: encoding.Symbol("x-opt-sequence-number"), Value: int64(42)},
			},
			Properties: &Properties{
				MessageID:     "id-1",
				To:            "queue",
				Subject:       "label",
				ReplyTo:       "reply",
				CorrelationID: encoding.UUID{1},
				ContentType:   "application/json",
				CreationTime:  created,
				GroupID:       "session",
				GroupSequence: ptr(uint32(3)),
			},
			ApplicationProperties: encoding.Map{{Key: "event", Value: "created"}, {Key: "n", Value: int32(1)}},
			Data:                  [][]byte{[]byte("a"), []byte("b")},
			Footer:                encoding.Map{{Key: encoding.Symbol("f"), Value: true}},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := MarshalMessage(tc.msg)
			if err != nil {
				t.Fatalf("MarshalMessage() error: %v", err)
			}
			got, err := UnmarshalMessage(b)
			if err != nil {
				t.Fatalf("UnmarshalMessage(% x) error: %v", b, err)
			}
			if diff := cmp.Diff(tc.msg, got); diff != "" {
				t.Errorf("round trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRewriteMessageKeepsBareMessage(t *testing.T) {
	orig := &Message{
		Header:             &Header{Durable: true},
		MessageAnnotations: encoding.Map{{Key: encoding.Symbol("x-sender"), Value: "kept"}},
		Properties: &Properties{
			MessageID: "id",
			// Empty strings must survive: a full decode and re-encode would
			// turn them into nulls.
			Subject: "",
			To:      "dest",
		},
		ApplicationProperties: encoding.Map{{Key: "a", Value: int32(1)}},
		Data:                  [][]byte{[]byte("body")},
		Footer:                encoding.Map{{Key: encoding.Symbol("f"), Value: true}},
	}
	payload, err := MarshalMessage(orig)
	if err != nil {
		t.Fatalf("MarshalMessage() error: %v", err)
	}
	bare, err := sectionBytes(payload, codeProperties)
	if err != nil {
		t.Fatalf("sectionBytes() error: %v", err)
	}

	got, err := RewriteMessage(payload, func(env *Envelope) error {
		env.Header.DeliveryCount = 3
		env.MessageAnnotations.Set(encoding.Symbol("x-opt-sequence-number"), int64(9))
		env.DeliveryAnnotations = encoding.Map{{Key: encoding.Symbol("x-opt-lock-token"), Value: encoding.UUID{1}}}
		return nil
	})
	if err != nil {
		t.Fatalf("RewriteMessage() error: %v", err)
	}
	if !bytes.Contains(got, bare) {
		t.Errorf("rewritten message does not contain the original properties section")
	}
	m, err := UnmarshalMessage(got)
	if err != nil {
		t.Fatalf("UnmarshalMessage() error: %v", err)
	}
	if m.Header.DeliveryCount != 3 || !m.Header.Durable {
		t.Errorf("header = %+v, want durable with delivery count 3", m.Header)
	}
	if v, _ := m.MessageAnnotations.Get(encoding.Symbol("x-sender")); v != "kept" {
		t.Errorf("sender annotation lost")
	}
	if v, _ := m.MessageAnnotations.Get(encoding.Symbol("x-opt-sequence-number")); v != int64(9) {
		t.Errorf("x-opt-sequence-number = %v, want 9", v)
	}
	if len(m.DeliveryAnnotations) != 1 {
		t.Errorf("delivery annotations = %v, want the lock token", m.DeliveryAnnotations)
	}
	if diff := cmp.Diff(orig.Footer, m.Footer); diff != "" {
		t.Errorf("footer mismatch (-want +got):\n%s", diff)
	}
}

func TestRewriteMessageApplicationProperties(t *testing.T) {
	tests := []struct {
		name string
		msg  *Message
	}{
		{"existing section", &Message{
			Properties:            &Properties{MessageID: "id"},
			ApplicationProperties: encoding.Map{{Key: "a", Value: "1"}},
			Data:                  [][]byte{[]byte("x")},
		}},
		{"no section, with properties", &Message{Properties: &Properties{MessageID: "id"}, Data: [][]byte{[]byte("x")}}},
		{"no section, no properties", &Message{Data: [][]byte{[]byte("x")}}},
		{"body only value", &Message{Value: "v", HasValue: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := MarshalMessage(tc.msg)
			if err != nil {
				t.Fatalf("MarshalMessage() error: %v", err)
			}
			got, err := RewriteMessage(payload, func(env *Envelope) error {
				env.ApplicationProperties.Set("DeadLetterReason", "r")
				env.ApplicationPropertiesModified = true
				return nil
			})
			if err != nil {
				t.Fatalf("RewriteMessage() error: %v", err)
			}
			m, err := UnmarshalMessage(got)
			if err != nil {
				t.Fatalf("UnmarshalMessage() of rewritten message error: %v", err)
			}
			if v, _ := m.ApplicationProperties.Get("DeadLetterReason"); v != "r" {
				t.Errorf("DeadLetterReason = %v, want r", v)
			}
			if diff := cmp.Diff(tc.msg.Data, m.Data, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("body changed (-want +got):\n%s", diff)
			}
			// Sections must stay in spec order: properties before
			// application-properties before the body.
			if _, err := UnmarshalMessage(got); err != nil {
				t.Errorf("rewritten message invalid: %v", err)
			}
		})
	}
}

// sectionBytes returns the encoded bytes of the first section with code.
func sectionBytes(payload []byte, code uint64) ([]byte, error) {
	r := encoding.NewReader(payload)
	for r.Len() > 0 {
		start := r.Offset()
		v, err := r.Read()
		if err != nil {
			return nil, err
		}
		if d, ok := v.(encoding.Described); ok {
			if c, _ := DescriptorCode(d.Descriptor); c == code {
				return payload[start:r.Offset()], nil
			}
		}
	}
	return nil, nil
}

func TestTransactionFramesRoundTrip(t *testing.T) {
	id := []byte{1, 2, 3, 4}
	tests := []struct {
		name string
		body Body
	}{
		{"attach to a coordinator", &Attach{
			Name: "txn-ctl", Handle: 1, Role: RoleSender,
			Coordinator: &Coordinator{Capabilities: []encoding.Symbol{LocalTransactions}},
		}},
		{"transfer in a transaction", &Transfer{
			Handle: 1, DeliveryID: ptr(uint32(3)), DeliveryTag: []byte{9},
			State: &TransactionalState{TxnID: id},
		}},
		{"disposition declared", &Disposition{First: 0, Settled: true, State: &Declared{TxnID: id}}},
		{"disposition transactional outcome", &Disposition{First: 4, State: &TransactionalState{TxnID: id, Outcome: &Accepted{}}}},
		{"disposition transactional rejected", &Disposition{First: 4, State: &TransactionalState{TxnID: id, Outcome: &Rejected{Error: &Error{Condition: "amqp:x"}}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := AppendFrame(nil, TypeAMQP, 0, tc.body, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadFrame(bytes.NewReader(buf), 1<<20)
			if err != nil {
				t.Fatalf("ReadFrame() error: %v", err)
			}
			if diff := cmp.Diff(tc.body, got.Body, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("round trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTransactionBodies(t *testing.T) {
	roundTrip := func(v encoding.Marshaler) any {
		t.Helper()
		var w encoding.Writer
		if err := w.Write(v); err != nil {
			t.Fatal(err)
		}
		got, err := encoding.NewReader(w.Bytes()).Read()
		if err != nil {
			t.Fatal(err)
		}
		body, err := DecodeTransactionBody(got)
		if err != nil {
			t.Fatalf("DecodeTransactionBody() error: %v", err)
		}
		return body
	}
	if got := roundTrip(&Declare{}); !cmp.Equal(got, &Declare{}, cmpopts.EquateEmpty()) {
		t.Errorf("declare = %#v", got)
	}
	want := &Discharge{TxnID: []byte{7, 7}, Fail: true}
	if diff := cmp.Diff(want, roundTrip(want)); diff != "" {
		t.Errorf("discharge mismatch (-want +got):\n%s", diff)
	}
	if _, err := DecodeTransactionBody("hello"); err == nil {
		t.Error("a string body was accepted")
	}
}

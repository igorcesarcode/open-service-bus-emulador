package integration_test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func TestSendAndReceiveProperties(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")
	receiver := newReceiver(t, client, "orders", nil)

	before := time.Now().Add(-time.Second)
	send(t, sender, &azservicebus.Message{
		MessageID:        to.Ptr("msg-1"),
		Body:             []byte(`{"event":"created"}`),
		ContentType:      to.Ptr("application/json"),
		Subject:          to.Ptr("order-created"),
		CorrelationID:    to.Ptr("corr-1"),
		ReplyTo:          to.Ptr("replies"),
		To:               to.Ptr("someone"),
		ReplyToSessionID: to.Ptr("reply-session"),
		TimeToLive:       to.Ptr(time.Hour),
		ApplicationProperties: map[string]any{
			"event":      "created",
			"isApproved": true,
			"version":    int64(1),
			"score":      1.5,
		},
	})

	m := receive(t, receiver, 1)[0]
	if got, want := string(m.Body), `{"event":"created"}`; got != want {
		t.Errorf("Body = %q, want %q", got, want)
	}
	if m.MessageID != "msg-1" {
		t.Errorf("MessageID = %q, want msg-1", m.MessageID)
	}
	for name, got := range map[string]*string{
		"ContentType":      m.ContentType,
		"Subject":          m.Subject,
		"CorrelationID":    m.CorrelationID,
		"ReplyTo":          m.ReplyTo,
		"To":               m.To,
		"ReplyToSessionID": m.ReplyToSessionID,
	} {
		if got == nil {
			t.Errorf("%s is nil", name)
		}
	}
	if diff := cmp.Diff(map[string]any{"event": "created", "isApproved": true, "version": int64(1), "score": 1.5}, m.ApplicationProperties); diff != "" {
		t.Errorf("ApplicationProperties mismatch (-want +got):\n%s", diff)
	}
	if m.SequenceNumber == nil || *m.SequenceNumber != 1 {
		t.Errorf("SequenceNumber = %v, want 1", m.SequenceNumber)
	}
	if m.EnqueuedTime == nil || m.EnqueuedTime.Before(before) {
		t.Errorf("EnqueuedTime = %v, want a recent time", m.EnqueuedTime)
	}
	if m.DeliveryCount != 1 {
		t.Errorf("DeliveryCount = %d, want 1", m.DeliveryCount)
	}
	if m.LockedUntil == nil || !m.LockedUntil.After(time.Now()) {
		t.Errorf("LockedUntil = %v, want a future time", m.LockedUntil)
	}
	if m.TimeToLive == nil || *m.TimeToLive != time.Hour {
		t.Errorf("TimeToLive = %v, want 1h", m.TimeToLive)
	}
	if m.ExpiresAt == nil {
		t.Errorf("ExpiresAt is nil, want enqueued time plus TTL")
	}
	if m.LockToken == ([16]byte{}) {
		t.Errorf("LockToken is zero")
	}
	if err := receiver.CompleteMessage(testContext(t), m, nil); err != nil {
		t.Errorf("CompleteMessage() error: %v", err)
	}
}

func TestSendMessageEntityNamesAreCaseInsensitive(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("Orders")})
	client := newClient(t, srv)
	send(t, newSender(t, client, "ORDERS"), bodies("hi")...)
	if got := receive(t, newReceiver(t, client, "orders", nil), 1); string(got[0].Body) != "hi" {
		t.Errorf("Body = %q, want hi", got[0].Body)
	}
}

func TestSendMessageUnknownEntity(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	sender := newSender(t, newClient(t, srv), "missing")

	err := sender.SendMessage(testContext(t), &azservicebus.Message{Body: []byte("hi")}, nil)
	var sbErr *azservicebus.Error
	if !errors.As(err, &sbErr) || sbErr.Code != azservicebus.CodeNotFound {
		t.Errorf("SendMessage() error = %v, want *azservicebus.Error with code %q", err, azservicebus.CodeNotFound)
	}
}

// TestSendManyMessages sends more messages than one grant of link credit
// and session window, so credit must be replenished, and checks order.
func TestSendManyMessages(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")

	const n = 1500
	for i := range n {
		if err := sender.SendMessage(testContext(t), &azservicebus.Message{Body: fmt.Appendf(nil, "%d", i)}, nil); err != nil {
			t.Fatalf("SendMessage(%d) error: %v", i, err)
		}
	}
	receiver := newReceiver(t, client, "orders", &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	got := receive(t, receiver, n)
	for i, m := range got {
		if want := fmt.Sprint(i); string(m.Body) != want {
			t.Fatalf("message %d body = %q, want %q: order not preserved", i, m.Body, want)
		}
	}
}

// TestSendLargeMessage sends and receives a message spanning many frames.
// The default MaxMessageSizeInKilobytes, 1024, would reject it.
func TestSendLargeMessage(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("orders", func(p *broker.Properties) { p.MaxMessageSizeInKilobytes = 100 * 1024 }),
	}})
	client := newClient(t, srv)

	body := bytes.Repeat([]byte("0123456789abcdef"), 1<<16) // 1 MiB
	send(t, newSender(t, client, "orders"), &azservicebus.Message{Body: body})
	got := receive(t, newReceiver(t, client, "orders", nil), 1)
	if !bytes.Equal(got[0].Body, body) {
		t.Errorf("received body does not match the 1 MiB message sent")
	}
}

func TestSendMessageTooLarge(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders"), MaxMessageSize: 1024})
	sender := newSender(t, newClient(t, srv), "orders")

	err := sender.SendMessage(testContext(t), &azservicebus.Message{Body: make([]byte, 4096)}, nil)
	if !errors.Is(err, azservicebus.ErrMessageTooLarge) {
		t.Errorf("SendMessage() error = %v, want ErrMessageTooLarge", err)
	}
}

func TestSendMessageBatch(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")
	ctx := testContext(t)

	batch, err := sender.NewMessageBatch(ctx, nil)
	if err != nil {
		t.Fatalf("NewMessageBatch() error: %v", err)
	}
	for i := range 10 {
		msg := &azservicebus.Message{
			Body:                  fmt.Appendf(nil, "batch-%d", i),
			ApplicationProperties: map[string]any{"index": int64(i)},
		}
		if err := batch.AddMessage(msg, nil); err != nil {
			t.Fatalf("AddMessage(%d) error: %v", i, err)
		}
	}
	if err := sender.SendMessageBatch(ctx, batch, nil); err != nil {
		t.Fatalf("SendMessageBatch() error: %v", err)
	}

	got := receive(t, newReceiver(t, client, "orders", nil), 10)
	for i, m := range got {
		if want := fmt.Sprintf("batch-%d", i); string(m.Body) != want {
			t.Errorf("message %d body = %q, want %q", i, m.Body, want)
		}
		if v := m.ApplicationProperties["index"]; v != int64(i) {
			t.Errorf("message %d index = %v, want %d", i, v, i)
		}
		if *m.SequenceNumber != int64(i+1) {
			t.Errorf("message %d sequence number = %d, want %d", i, *m.SequenceNumber, i+1)
		}
	}
}

func TestSendAMQPAnnotatedMessage(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)

	err := newSender(t, client, "orders").SendAMQPAnnotatedMessage(testContext(t), &azservicebus.AMQPAnnotatedMessage{
		Body:               azservicebus.AMQPAnnotatedMessageBody{Value: map[string]any{"total": int64(42)}},
		MessageAnnotations: map[any]any{"x-custom": "annotation"},
	}, nil)
	if err != nil {
		t.Fatalf("SendAMQPAnnotatedMessage() error: %v", err)
	}
	m := receive(t, newReceiver(t, client, "orders", nil), 1)[0]
	raw := m.RawAMQPMessage
	if diff := cmp.Diff(map[string]any{"total": int64(42)}, raw.Body.Value); diff != "" {
		t.Errorf("amqp-value body mismatch (-want +got):\n%s", diff)
	}
	if v := raw.MessageAnnotations["x-custom"]; v != "annotation" {
		t.Errorf("annotation x-custom = %v, want annotation", v)
	}
}

func TestConcurrentSenders(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("a", "b", "c")})
	client := newClient(t, srv)

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for _, q := range []string{"a", "b", "c"} {
		sender := newSender(t, client, q)
		wg.Go(func() {
			for i := range 100 {
				if err := sender.SendMessage(testContext(t), &azservicebus.Message{Body: []byte(q)}, nil); err != nil {
					errs <- fmt.Errorf("queue %s message %d: %w", q, i, err)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	for _, q := range []string{"a", "b", "c"} {
		r := newReceiver(t, client, q, &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
		for _, b := range bodiesOf(receive(t, r, 100)) {
			if b != q {
				t.Fatalf("queue %s received body %q", q, b)
			}
		}
	}
}

// TestMultipleClients checks that independent connections share entities.
func TestMultipleClients(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	for i := range 3 {
		send(t, newSender(t, newClient(t, srv), "orders"), &azservicebus.Message{Body: fmt.Appendf(nil, "client-%d", i)})
	}
	got := receive(t, newReceiver(t, newClient(t, srv), "orders", nil), 3)
	if diff := cmp.Diff([]string{"client-0", "client-1", "client-2"}, bodiesOf(got)); diff != "" {
		t.Errorf("bodies mismatch (-want +got):\n%s", diff)
	}
}

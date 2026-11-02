package integration_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func withID(id, body string) *azservicebus.Message {
	return &azservicebus.Message{MessageID: to.Ptr(id), Body: []byte(body)}
}

func TestDuplicateDetectionSDK(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("orders", func(p *broker.Properties) { p.RequiresDuplicateDetection = true }),
	}})
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")
	ctx := testContext(t)

	// The duplicates are accepted, so every call below succeeds.
	send(t, sender, withID("1", "first"), withID("1", "again"))
	batch, err := sender.NewMessageBatch(ctx, nil)
	if err != nil {
		t.Fatalf("NewMessageBatch() error: %v", err)
	}
	for _, m := range []*azservicebus.Message{withID("2", "second"), withID("2", "again"), withID("1", "again")} {
		if err := batch.AddMessage(m, nil); err != nil {
			t.Fatalf("AddMessage() error: %v", err)
		}
	}
	if err := sender.SendMessageBatch(ctx, batch, nil); err != nil {
		t.Fatalf("SendMessageBatch() error: %v", err)
	}
	if _, err := sender.ScheduleMessages(ctx, []*azservicebus.Message{withID("2", "scheduled")}, time.Now(), nil); err != nil {
		t.Fatalf("ScheduleMessages() error: %v", err)
	}

	r := newReceiver(t, client, "orders", &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	if diff := cmp.Diff([]string{"first", "second"}, sortedBodies(receive(t, r, 2))); diff != "" {
		t.Errorf("received bodies mismatch (-want +got):\n%s", diff)
	}
	receiveNone(t, r, 500*time.Millisecond)
}

func isCondition(err error, cond amqp.ErrCond) bool {
	var amqpErr *amqp.Error
	return errors.As(err, &amqpErr) && amqpErr.Condition == cond
}

func TestQuotaExceededSDK(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("small", func(p *broker.Properties) { p.MaxSizeInMegabytes = 1 }),
	}})
	sender := newSender(t, newClient(t, srv), "small")

	big := bytes.Repeat([]byte("x"), 300<<10)
	send(t, sender, &azservicebus.Message{Body: big}, &azservicebus.Message{Body: big}, &azservicebus.Message{Body: big})
	err := sender.SendMessage(testContext(t), &azservicebus.Message{Body: big}, nil)
	if !isCondition(err, amqp.ErrCondResourceLimitExceeded) {
		t.Errorf("SendMessage() over the quota error = %v, want %s", err, amqp.ErrCondResourceLimitExceeded)
	}
}

func TestMaxMessageSizePerEntity(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	ac := newAdminClient(t, srv)
	ctx := testContext(t)

	// A sender attached before the limit changes still sees 1 MB, so the
	// server rejects the message.
	attached := newSender(t, client, "orders")
	send(t, attached, &azservicebus.Message{Body: []byte("attach")})
	q, err := ac.GetQueue(ctx, "orders", nil)
	if err != nil {
		t.Fatalf("GetQueue() error: %v", err)
	}
	q.MaxMessageSizeInKilobytes = to.Ptr(int64(1))
	if _, err := ac.UpdateQueue(ctx, "orders", q.QueueProperties, nil); err != nil {
		t.Fatalf("UpdateQueue() error: %v", err)
	}
	body := make([]byte, 4096)
	if err := attached.SendMessage(ctx, &azservicebus.Message{Body: body}, nil); !errors.Is(err, azservicebus.ErrMessageTooLarge) {
		t.Errorf("SendMessage() on the attached sender error = %v, want ErrMessageTooLarge", err)
	}
	// A new sender learns the limit on attach and checks it itself.
	if err := newSender(t, client, "orders").SendMessage(ctx, &azservicebus.Message{Body: body}, nil); !errors.Is(err, azservicebus.ErrMessageTooLarge) {
		t.Errorf("SendMessage() on a new sender error = %v, want ErrMessageTooLarge", err)
	}
}

func TestAutoDeleteOnIdleSDK(t *testing.T) {
	t.Parallel()
	idle := func(p *broker.Properties) { p.AutoDeleteOnIdle = time.Second }
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("idle", idle),
		queue("listened", idle),
	}})
	client := newClient(t, srv)
	ac := newAdminClient(t, srv)
	ctx := testContext(t)

	// An open link is not activity, so the listened queue goes too.
	r := newReceiver(t, client, "listened", nil)
	for _, name := range []string{"idle", "listened"} {
		for {
			q, err := ac.GetQueue(ctx, name, nil)
			if err != nil {
				t.Fatalf("GetQueue(%s) error: %v", name, err)
			}
			if q == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	_, err := r.ReceiveMessages(ctx, 1, nil)
	if sbErr := (*azservicebus.Error)(nil); !errors.As(err, &sbErr) || sbErr.Code != azservicebus.CodeNotFound {
		t.Errorf("ReceiveMessages() on a deleted queue error = %v, want CodeNotFound", err)
	}
}

func TestAccessedAtSDK(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	ac := newAdminClient(t, srv)
	ctx := testContext(t)

	before, err := ac.GetQueueRuntimeProperties(ctx, "orders", nil)
	if err != nil {
		t.Fatalf("GetQueueRuntimeProperties() error: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	send(t, newSender(t, newClient(t, srv), "orders"), &azservicebus.Message{Body: []byte("x")})
	after, err := ac.GetQueueRuntimeProperties(ctx, "orders", nil)
	if err != nil {
		t.Fatalf("GetQueueRuntimeProperties() error: %v", err)
	}
	if !after.AccessedAt.After(before.AccessedAt) {
		t.Errorf("AccessedAt = %v after a send, want later than %v", after.AccessedAt, before.AccessedAt)
	}
}

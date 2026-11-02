package integration_test

import (
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	goamqp "github.com/Azure/go-amqp"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

// managementCall sends one request to the $management node of address over
// raw AMQP, as the SDKs do, and returns the status code and the body of the
// response. No released Go or JavaScript SDK exposes batch delete, so this
// is how the operation is exercised from outside the emulator.
func managementCall(t *testing.T, srv *server.Server, address, operation string, body map[string]any) (int32, map[string]any) {
	t.Helper()
	ctx := testContext(t)
	conn, err := goamqp.Dial(ctx, "amqp://"+srv.Addr().String(), &goamqp.ConnOptions{SASLType: goamqp.SASLTypeAnonymous()})
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}
	defer conn.Close()
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := sess.NewReceiver(ctx, address+"/$management", &goamqp.ReceiverOptions{TargetAddress: "client-reply"})
	if err != nil {
		t.Fatalf("NewReceiver() error: %v", err)
	}
	sender, err := sess.NewSender(ctx, address+"/$management", nil)
	if err != nil {
		t.Fatalf("NewSender() error: %v", err)
	}
	replyTo := "client-reply"
	err = sender.Send(ctx, &goamqp.Message{
		Properties:            &goamqp.MessageProperties{MessageID: "req-1", ReplyTo: &replyTo},
		ApplicationProperties: map[string]any{"operation": operation},
		Value:                 body,
	}, nil)
	if err != nil {
		t.Fatalf("Send() error: %v", err)
	}
	resp, err := reply.Receive(ctx, nil)
	if err != nil {
		t.Fatalf("Receive() error: %v", err)
	}
	reply.AcceptMessage(ctx, resp)
	var code int32
	switch v := resp.ApplicationProperties["statusCode"].(type) {
	case int32:
		code = v
	case int64:
		code = int32(v)
	}
	out := map[string]any{}
	if m, ok := resp.Value.(map[string]any); ok {
		out = m
	}
	return code, out
}

const batchDeleteOp = "com.microsoft:batch-delete-messages"

func TestBatchDelete(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	ctx := testContext(t)
	sender := newSender(t, client, "orders")
	send(t, sender, bodies("a", "b", "c", "d", "e")...)
	later := time.Now().Add(time.Minute)
	del := func(address string, count int, before time.Time) (int32, int32) {
		code, body := managementCall(t, srv, address, batchDeleteOp, map[string]any{"message-count": int32(count), "enqueued-time-utc": before})
		n, _ := body["message-count"].(int32)
		return code, n
	}

	// The oldest go first, and only as many as asked.
	if code, n := del("orders", 3, later); code != 200 || n != 3 {
		t.Fatalf("batch delete of 3 = %d, %d; want 200, 3", code, n)
	}
	r := newReceiver(t, client, "orders", &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModePeekLock})
	if diff := cmp.Diff([]string{"d", "e"}, bodiesOf(func() []*azservicebus.ReceivedMessage {
		peeked, err := r.PeekMessages(ctx, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		out := make([]*azservicebus.ReceivedMessage, len(peeked))
		copy(out, peeked)
		return out
	}())); diff != "" {
		t.Errorf("messages left (-want +got):\n%s", diff)
	}

	// A locked message is left alone.
	locked := receive(t, r, 1)
	if code, n := del("orders", 10, later); code != 200 || n != 1 {
		t.Errorf("batch delete with one locked message = %d, %d; want 200, 1", code, n)
	}
	if err := r.CompleteMessage(ctx, locked[0], nil); err != nil {
		t.Errorf("the locked message was lost: %v", err)
	}

	// A cutoff before the messages deletes nothing, and so does an empty
	// queue: 204, which the SDKs read as zero.
	send(t, sender, bodies("f")...)
	if code, n := del("orders", 10, time.Now().Add(-time.Hour)); code != 204 || n != 0 {
		t.Errorf("batch delete before the enqueue time = %d, %d; want 204, 0", code, n)
	}
	if code, n := del("orders", 10, later); code != 200 || n != 1 {
		t.Errorf("batch delete = %d, %d; want 200, 1", code, n)
	}
	if code, _ := del("orders", 10, later); code != 204 {
		t.Errorf("batch delete of an empty queue = %d, want 204", code)
	}

	// Bad requests: no count, a count past the limit, an unknown entity.
	if code, _ := managementCall(t, srv, "orders", batchDeleteOp, map[string]any{"enqueued-time-utc": later}); code != 400 {
		t.Errorf("no message-count = %d, want 400", code)
	}
	if code, _ := del("orders", 5000, later); code != 400 {
		t.Errorf("a count above the limit = %d, want 400", code)
	}
	if code, _ := del("orders", 0, later); code != 400 {
		t.Errorf("a count of 0 = %d, want 400", code)
	}

	// The operation is in the activity log and the metrics.
	_, body := httpGet(t, srv, "/$api/activity?operation=DeleteMessages")
	mustContain(t, "activity", body, `"operation":"DeleteMessages"`, `"entity":"orders"`, "3 messages")
}

func TestBatchDeleteDeadLetterQueue(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	ctx := testContext(t)
	send(t, newSender(t, client, "orders"), bodies("a", "b", "c")...)
	r := newReceiver(t, client, "orders", nil)
	for _, m := range receive(t, r, 3) {
		if err := r.DeadLetterMessage(ctx, m, nil); err != nil {
			t.Fatal(err)
		}
	}
	code, body := managementCall(t, srv, "orders/$DeadLetterQueue", batchDeleteOp,
		map[string]any{"message-count": int32(2), "enqueued-time-utc": time.Now().Add(time.Minute)})
	if n, _ := body["message-count"].(int32); code != 200 || n != 2 {
		t.Fatalf("batch delete in the DLQ = %d, %d; want 200, 2", code, n)
	}
	ac := newAdminClient(t, srv)
	rt, err := ac.GetQueueRuntimeProperties(ctx, "orders", nil)
	if err != nil || rt.DeadLetterMessageCount != 1 || rt.ActiveMessageCount != 0 {
		t.Errorf("runtime properties = %+v, %v; want one dead-letter message", rt, err)
	}
}

func TestBatchDeleteLimitIsConfigurable(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders"), MaxBatchDelete: 2})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("a", "b", "c")...)
	later := time.Now().Add(time.Minute)
	if code, _ := managementCall(t, srv, "orders", batchDeleteOp, map[string]any{"message-count": int32(3), "enqueued-time-utc": later}); code != 400 {
		t.Errorf("a count of 3 with a limit of 2 = %d, want 400", code)
	}
	if code, body := managementCall(t, srv, "orders", batchDeleteOp, map[string]any{"message-count": int32(2), "enqueued-time-utc": later}); code != 200 || body["message-count"] != int32(2) {
		t.Errorf("a count of 2 = %d, %v", code, body)
	}
}

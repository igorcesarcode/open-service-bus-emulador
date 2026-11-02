package integration_test

import (
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func TestTransferDeadLetterQueueSDK(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{
		Queues: queues("source"),
		Topics: []server.Topic{{Name: "events", Subscriptions: []server.Subscription{{Name: "relay"}}}},
	})
	ctx := testContext(t)
	for _, path := range []string{"source", "events/Subscriptions/relay"} {
		in := broker.IncomingMessage{}
		in.Payload = mustMessage(t, "stuck in "+path)
		if _, err := srv.Broker().TransferDeadLetter(ctx, path, []broker.IncomingMessage{in}, "TransferFailed", "the destination is disabled"); err != nil {
			t.Fatal(err)
		}
	}
	client := newClient(t, srv)
	opts := &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueTransfer}

	// The Go SDK's admin client reports the count in the runtime properties.
	ac := newAdminClient(t, srv)
	rt, err := ac.GetQueueRuntimeProperties(ctx, "source", nil)
	if err != nil || rt.TransferDeadLetterMessageCount != 1 || rt.DeadLetterMessageCount != 0 || rt.ActiveMessageCount != 0 {
		t.Fatalf("GetQueueRuntimeProperties() = %+v, %v; want one transfer dead-letter message", rt, err)
	}
	srt, err := ac.GetSubscriptionRuntimeProperties(ctx, "events", "relay", nil)
	if err != nil || srt.TransferDeadLetterMessageCount != 1 {
		t.Fatalf("GetSubscriptionRuntimeProperties() = %+v, %v", srt, err)
	}

	// Peek, then receive and complete, from the queue's and the
	// subscription's transfer dead-letter queue.
	qr, err := client.NewReceiverForQueue("source", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer qr.Close(ctx)
	peeked, err := qr.PeekMessages(ctx, 10, nil)
	if err != nil || len(peeked) != 1 {
		t.Fatalf("PeekMessages() = %d, %v; want 1", len(peeked), err)
	}
	msgs := receive(t, qr, 1)
	if got := string(msgs[0].Body); got != "stuck in source" {
		t.Errorf("body = %q", got)
	}
	if str(msgs[0].DeadLetterReason) != "TransferFailed" || str(msgs[0].DeadLetterErrorDescription) != "the destination is disabled" {
		t.Errorf("reason = %q, description = %q", str(msgs[0].DeadLetterReason), str(msgs[0].DeadLetterErrorDescription))
	}
	if err := qr.CompleteMessage(ctx, msgs[0], nil); err != nil {
		t.Fatalf("CompleteMessage() error: %v", err)
	}
	sr, err := client.NewReceiverForSubscription("events", "relay", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close(ctx)
	if diff := cmp.Diff([]string{"stuck in events/Subscriptions/relay"}, bodiesOf(receive(t, sr, 1))); diff != "" {
		t.Errorf("subscription TDLQ mismatch (-want +got):\n%s", diff)
	}
	if rt, _ := ac.GetQueueRuntimeProperties(ctx, "source", nil); rt == nil || rt.TransferDeadLetterMessageCount != 0 {
		t.Errorf("transfer dead-letter count after completing = %+v", rt)
	}
}

func TestForwardingFailureReasonsSDK(t *testing.T) {
	t.Parallel()
	sessions := broker.DefaultProperties(broker.KindQueue)
	sessions.RequiresSession = true
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("loop-a", func(p *broker.Properties) { p.ForwardTo = "loop-b" }),
		queue("loop-b", func(p *broker.Properties) { p.ForwardTo = "loop-a" }),
		{Name: "sessions", Properties: sessions},
		queue("to-sessions", func(p *broker.Properties) { p.ForwardTo = "sessions" }),
	}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "loop-a"), bodies("looping")...)
	send(t, newSender(t, client, "to-sessions"), bodies("no-session")...)

	dlq := &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter, ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete}
	for queue, want := range map[string]string{
		"loop-a":      "MaxTransferHopCountExceeded",
		"to-sessions": "Session ID is null",
	} {
		msgs := receive(t, newReceiver(t, client, queue, dlq), 1)
		if got := str(msgs[0].DeadLetterReason); got != want {
			t.Errorf("%s dead-letter reason = %q, want %q", queue, got, want)
		}
	}
}

func TestCancelScheduledThroughForwardingSDK(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{
		Queues: []server.Queue{
			{Name: "dest"},
			queue("front", func(p *broker.Properties) { p.ForwardTo = "dest" }),
		},
		Topics: []server.Topic{{Name: "events", Subscriptions: []server.Subscription{
			subscription("plain", nil, nil),
			subscription("acting", []broker.Rule{sqlRule("a", "1 = 1", "SET tag = 'a'"), sqlRule("b", "1 = 1", "SET tag = 'b'")}, nil),
		}}},
	})
	client := newClient(t, srv)
	ctx := testContext(t)
	at := time.Now().Add(3 * time.Second)

	// Scheduling on an entity that forwards gives a number that cancels.
	front := newSender(t, client, "front")
	seqs, err := front.ScheduleMessages(ctx, bodies("cancel-me", "keep-me"), at, nil)
	if err != nil || len(seqs) != 2 || seqs[0] == 0 || seqs[1] == 0 {
		t.Fatalf("ScheduleMessages() = %v, %v; want sequence numbers", seqs, err)
	}
	if err := front.CancelScheduledMessages(ctx, seqs[:1], nil); err != nil {
		t.Fatalf("CancelScheduledMessages() error: %v", err)
	}

	// On a topic, the cancellation reaches the copies made by actions.
	topic := newSender(t, client, "events")
	tseqs, err := topic.ScheduleMessages(ctx, bodies("topic-cancel"), at, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := topic.CancelScheduledMessages(ctx, tseqs, nil); err != nil {
		t.Fatalf("CancelScheduledMessages() on the topic error: %v", err)
	}

	if diff := cmp.Diff([]string{"keep-me"}, bodiesOf(receive(t, newReceiver(t, client, "dest", radOptions), 1))); diff != "" {
		t.Errorf("dest mismatch (-want +got):\n%s", diff)
	}
	for _, sub := range []string{"plain", "acting"} {
		r := newSubReceiver(t, client, "events", sub, radOptions)
		receiveNone(t, r, 500*time.Millisecond)
	}
}

func mustMessage(t *testing.T, body string) []byte {
	t.Helper()
	payload, err := frames.MarshalMessage(&frames.Message{
		Properties: &frames.Properties{MessageID: body},
		Data:       [][]byte{[]byte(body)},
	})
	if err != nil {
		t.Fatalf("MarshalMessage() error: %v", err)
	}
	return payload
}

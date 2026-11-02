package integration_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func partitioned(p *broker.Properties) { p.EnablePartitioning = true }

func TestPartitionedEntitiesSDK(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("pq", func(p *broker.Properties) { p.EnablePartitioning = true; p.MaxSizeInMegabytes = 1 }),
		queue("plain", func(p *broker.Properties) { p.MaxSizeInMegabytes = 1 }),
	}})
	client := newClient(t, srv)
	ctx := testContext(t)
	sender := newSender(t, client, "pq")

	// The partition key survives the round trip, and a message with a key
	// is an ordinary message to the receiver.
	if err := sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("keyed"), PartitionKey: to.Ptr("customer-7")}, nil); err != nil {
		t.Fatalf("SendMessage() with a partition key error: %v", err)
	}
	r := newReceiver(t, client, "pq", &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	got := receive(t, r, 1)
	if str(got[0].PartitionKey) != "customer-7" {
		t.Errorf("PartitionKey = %q, want customer-7", str(got[0].PartitionKey))
	}

	// A partition key that differs from the session id is invalid on a
	// partitioned entity; on a plain one the SDK sends it and nothing checks.
	bad := &azservicebus.Message{Body: []byte("bad"), SessionID: to.Ptr("s1"), PartitionKey: to.Ptr("other")}
	if err := sender.SendMessage(ctx, bad, nil); err == nil {
		t.Error("SendMessage() with a session id and a different partition key succeeded on a partitioned queue")
	}

	// A batch with two partition keys is refused as a whole.
	batch, err := sender.NewMessageBatch(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"k1", "k2"} {
		if err := batch.AddMessage(&azservicebus.Message{Body: []byte(key), PartitionKey: to.Ptr(key)}, nil); err != nil {
			t.Fatalf("AddMessage() error: %v", err)
		}
	}
	if err := sender.SendMessageBatch(ctx, batch, nil); err == nil {
		t.Error("SendMessageBatch() with two partition keys succeeded on a partitioned queue")
	}
	if n := peekCount(t, client, "pq", nil); n != 0 {
		t.Errorf("messages after the refused sends = %d, want 0", n)
	}
	// One key for the whole batch is fine.
	batch, _ = sender.NewMessageBatch(ctx, nil)
	for _, body := range []string{"a", "b"} {
		batch.AddMessage(&azservicebus.Message{Body: []byte(body), PartitionKey: to.Ptr("same")}, nil)
	}
	if err := sender.SendMessageBatch(ctx, batch, nil); err != nil {
		t.Errorf("SendMessageBatch() with one partition key error: %v", err)
	}

	// The size quota is that of each partition times the partitions: 1 MB
	// holds 4 messages of 300 KB on a plain queue and 16 MB many more.
	payload := bytes.Repeat([]byte("x"), 300<<10)
	for i := 0; i < 8; i++ {
		if err := sender.SendMessage(ctx, &azservicebus.Message{Body: payload}, nil); err != nil {
			t.Fatalf("message %d to the partitioned queue: %v", i, err)
		}
	}
	plain := newSender(t, client, "plain")
	var refused int
	for i := 0; i < 8; i++ {
		if err := plain.SendMessage(ctx, &azservicebus.Message{Body: payload}, nil); err != nil {
			refused++
		}
	}
	if refused == 0 {
		t.Error("the plain queue of 1 MB accepted 8 messages of 300 KB")
	}
}

func TestPartitionedTransactions(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("pq", partitioned), {Name: "plain"},
	}})
	client := newClient(t, srv)
	c := dialRaw(t, srv)
	coord := c.attachCoordinator()
	pq := c.attachSender("pq", "pq", nil)
	plain := c.attachSender("plain", "plain", nil)

	keyed := func(body, key string) []byte {
		payload, err := frames.MarshalMessage(&frames.Message{
			Properties:         &frames.Properties{MessageID: body},
			MessageAnnotations: encoding.Map{{Key: encoding.Symbol("x-opt-partition-key"), Value: key}},
			Data:               [][]byte{[]byte(body)},
		})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}

	id := c.declare(coord)
	// A message without a key cannot be sent in a transaction.
	wantRejected(t, "no key", c.transfer(pq, rawMessage(t, "plain-msg", ""), txnState(id, nil)), "")
	// Messages with one key can.
	wantAccepted(t, "key 1", c.transfer(pq, keyed("a", "k1"), txnState(id, nil)), id)
	wantAccepted(t, "key 1 again", c.transfer(pq, keyed("b", "k1"), txnState(id, nil)), id)
	// A second key cannot join the transaction.
	wantRejected(t, "key 2", c.transfer(pq, keyed("c", "k2"), txnState(id, nil)), "")
	if _, ok := c.discharge(coord, id, false).(*frames.Accepted); !ok {
		t.Fatal("commit not accepted")
	}
	r := newReceiver(t, client, "pq", &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	if diff := cmp.Diff([]string{"a", "b"}, sortedBodies(receive(t, r, 2))); diff != "" {
		t.Errorf("committed messages mismatch (-want +got):\n%s", diff)
	}

	// A plain entity takes any messages in a transaction.
	id = c.declare(coord)
	wantAccepted(t, "plain entity", c.transfer(plain, rawMessage(t, "x", ""), txnState(id, nil)), id)
	c.discharge(coord, id, true)
}

// partitionOf returns the partition id in the high bits of a sequence
// number.
func partitionOf(seq *int64) int64 { return *seq >> 48 }

func TestPartitionIDInSequenceNumbers(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("pq", func(p *broker.Properties) { p.EnablePartitioning = true }),
		queue("plain", func(p *broker.Properties) {}),
	}})
	client := newClient(t, srv)
	ctx := testContext(t)

	// Messages with the same key share a partition, whatever the key; the
	// ones without a key take the partitions in turn.
	sender := newSender(t, client, "pq")
	key := func(k string) *azservicebus.Message {
		return &azservicebus.Message{Body: []byte(k), PartitionKey: to.Ptr(k)}
	}
	send(t, sender, key("a"), key("a"), key("b"), key("a"))
	for i := 0; i < 16; i++ {
		send(t, sender, &azservicebus.Message{Body: []byte("rr")})
	}
	r := newReceiver(t, client, "pq", &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	msgs := receive(t, r, 20)
	byKey := map[string]int64{}
	rr := map[int64]bool{}
	for _, m := range msgs {
		p := partitionOf(m.SequenceNumber)
		if p < 0 || p >= 16 {
			t.Fatalf("partition %d of %q out of range", p, m.Body)
		}
		switch body := string(m.Body); body {
		case "a", "b":
			if prev, ok := byKey[body]; ok && prev != p {
				t.Errorf("key %s on partitions %d and %d", body, prev, p)
			}
			byKey[body] = p
		default:
			rr[p] = true
		}
	}
	if len(rr) != 16 {
		t.Errorf("16 messages without a key reached %d partitions, want 16", len(rr))
	}

	// A session pins its messages too, and the partition is not part of
	// the order: peek pages through every message with last+1.
	for i := 0; i < 6; i++ {
		send(t, sender, &azservicebus.Message{Body: []byte{byte('0' + i)}})
	}
	peeker := newReceiver(t, client, "pq", nil)
	var seen []string
	next := int64(0)
	for {
		page, err := peeker.PeekMessages(ctx, 4, &azservicebus.PeekMessagesOptions{FromSequenceNumber: &next})
		if err != nil {
			t.Fatalf("PeekMessages() error: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, m := range page {
			seen = append(seen, string(m.Body))
		}
		next = *page[len(page)-1].SequenceNumber + 1
	}
	if diff := cmp.Diff([]string{"0", "1", "2", "3", "4", "5"}, seen); diff != "" {
		t.Errorf("paged peek mismatch (-want +got):\n%s", diff)
	}

	// Numbers go back to the broker as they came: deferred messages and
	// scheduled ones are found by them.
	var sched []*azservicebus.Message
	for i := 0; i < 4; i++ {
		sched = append(sched, &azservicebus.Message{Body: []byte("later")})
	}
	seqs, err := sender.ScheduleMessages(ctx, sched, time.Now().Add(time.Hour), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.CancelScheduledMessages(ctx, seqs, nil); err != nil {
		t.Errorf("CancelScheduledMessages() with partitioned numbers error: %v", err)
	}
	pr := newReceiver(t, client, "pq", nil)
	m := receive(t, pr, 1)[0]
	if err := pr.DeferMessage(ctx, m, nil); err != nil {
		t.Fatal(err)
	}
	deferred, err := pr.ReceiveDeferredMessages(ctx, []int64{*m.SequenceNumber}, nil)
	if err != nil || len(deferred) != 1 || *deferred[0].SequenceNumber != *m.SequenceNumber {
		t.Errorf("ReceiveDeferredMessages() = %v, %v", deferred, err)
	}

	// A queue that is not partitioned has none of it.
	psender := newSender(t, client, "plain")
	send(t, psender, &azservicebus.Message{Body: []byte("x")}, &azservicebus.Message{Body: []byte("y")})
	for _, m := range receive(t, newReceiver(t, client, "plain", radOptions), 2) {
		if partitionOf(m.SequenceNumber) != 0 {
			t.Errorf("sequence number %d of a plain queue has partition bits", *m.SequenceNumber)
		}
	}
}

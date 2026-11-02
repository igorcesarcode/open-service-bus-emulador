package integration_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func TestReceiveAndDelete(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("a", "b", "c")...)

	r := newReceiver(t, client, "orders", &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	if diff := cmp.Diff([]string{"a", "b", "c"}, bodiesOf(receive(t, r, 3))); diff != "" {
		t.Errorf("bodies mismatch (-want +got):\n%s", diff)
	}
	// The messages are gone as soon as they are received, even for a peek
	// right away.
	if peeked, err := r.PeekMessages(testContext(t), 10, nil); err != nil || len(peeked) > 0 {
		t.Errorf("PeekMessages() after receiving = %d messages, %v; want none", len(peeked), err)
	}
	other := newReceiver(t, client, "orders", nil)
	receiveNone(t, other, 300*time.Millisecond)
}

func TestPeekLockComplete(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("a", "b")...)

	r := newReceiver(t, client, "orders", nil)
	for _, m := range receive(t, r, 2) {
		if err := r.CompleteMessage(testContext(t), m, nil); err != nil {
			t.Fatalf("CompleteMessage() error: %v", err)
		}
	}
	// Completed messages are gone for every receiver. (Completing the same
	// message twice is short-circuited by the client library, and lock
	// loss is covered by TestLockExpiryRedelivers.)
	receiveNone(t, newReceiver(t, client, "orders", nil), 300*time.Millisecond)
}

func TestAbandonRedeliversWithModifiedProperties(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), &azservicebus.Message{
		Body:                  []byte("a"),
		ApplicationProperties: map[string]any{"keep": "yes"},
	})

	r := newReceiver(t, client, "orders", nil)
	m := receive(t, r, 1)[0]
	if err := r.AbandonMessage(testContext(t), m, &azservicebus.AbandonMessageOptions{
		PropertiesToModify: map[string]any{"attempt": int64(1)},
	}); err != nil {
		t.Fatalf("AbandonMessage() error: %v", err)
	}
	m = receive(t, r, 1)[0]
	if m.DeliveryCount != 2 {
		t.Errorf("DeliveryCount after abandon = %d, want 2", m.DeliveryCount)
	}
	if diff := cmp.Diff(map[string]any{"keep": "yes", "attempt": int64(1)}, m.ApplicationProperties); diff != "" {
		t.Errorf("ApplicationProperties mismatch (-want +got):\n%s", diff)
	}
}

func TestLockExpiryRedelivers(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("orders", func(p *broker.Properties) { p.LockDuration = 500 * time.Millisecond }),
	}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("a")...)

	r := newReceiver(t, client, "orders", nil)
	first := receive(t, r, 1)[0]
	if d := time.Until(*first.LockedUntil); d > time.Second {
		t.Errorf("LockedUntil is %v away, want about 500ms", d)
	}
	second := receive(t, r, 1)[0] // arrives once the first lock lapses
	if second.DeliveryCount != 2 {
		t.Errorf("DeliveryCount after lock expiry = %d, want 2", second.DeliveryCount)
	}
	err := r.CompleteMessage(testContext(t), first, nil)
	var sbErr *azservicebus.Error
	if !errors.As(err, &sbErr) || sbErr.Code != azservicebus.CodeLockLost {
		t.Errorf("CompleteMessage(expired lock) error = %v, want code %q", err, azservicebus.CodeLockLost)
	}
	if err := r.CompleteMessage(testContext(t), second, nil); err != nil {
		t.Errorf("CompleteMessage(current lock) error: %v", err)
	}
}

// TestLockedHeadDoesNotHideLaterMessages checks that a second receiver gets
// the messages behind a locked head, and that the locked ones come back in
// sequence order once their locks lapse.
func TestLockedHeadDoesNotHideLaterMessages(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("orders", func(p *broker.Properties) { p.LockDuration = time.Second }),
	}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("1", "2", "3", "4", "5")...)

	holder := newReceiver(t, client, "orders", nil)
	head := receive(t, holder, 3)
	other := newReceiver(t, client, "orders", nil)
	rest := receive(t, other, 2)
	if got := []string{string(rest[0].Body), string(rest[1].Body)}; got[0] != "4" || got[1] != "5" {
		t.Fatalf("receiver behind the locked head got %v, want [4 5]", got)
	}
	for _, m := range rest {
		if err := other.CompleteMessage(testContext(t), m, nil); err != nil {
			t.Fatal(err)
		}
	}
	again := receive(t, other, 3) // arrive once the locks of the head lapse
	for i, m := range again {
		if want := string(head[i].Body); string(m.Body) != want || m.DeliveryCount != 2 {
			t.Errorf("redelivery %d = %q (count %d), want %q (count 2)", i, m.Body, m.DeliveryCount, want)
		}
		if err := other.CompleteMessage(testContext(t), m, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRenewMessageLock(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("orders", func(p *broker.Properties) { p.LockDuration = 2 * time.Second }),
	}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("a")...)

	r := newReceiver(t, client, "orders", nil)
	m := receive(t, r, 1)[0]
	firstLock := *m.LockedUntil
	time.Sleep(1500 * time.Millisecond)
	if err := r.RenewMessageLock(testContext(t), m, nil); err != nil {
		t.Fatalf("RenewMessageLock() error: %v", err)
	}
	if !m.LockedUntil.After(firstLock) {
		t.Errorf("LockedUntil after renew = %v, want after %v", m.LockedUntil, firstLock)
	}
	time.Sleep(1 * time.Second) // past the original lock
	if err := r.CompleteMessage(testContext(t), m, nil); err != nil {
		t.Errorf("CompleteMessage() after renew error: %v", err)
	}
}

func TestMaxDeliveryCountDeadLetters(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("orders", func(p *broker.Properties) { p.MaxDeliveryCount = 2 }),
	}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("poison")...)

	r := newReceiver(t, client, "orders", nil)
	for range 2 {
		m := receive(t, r, 1)[0]
		if err := r.AbandonMessage(testContext(t), m, nil); err != nil {
			t.Fatalf("AbandonMessage() error: %v", err)
		}
	}
	receiveNone(t, r, 300*time.Millisecond)

	dlq := newReceiver(t, client, "orders", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	m := receive(t, dlq, 1)[0]
	if m.DeadLetterReason == nil || *m.DeadLetterReason != "MaxDeliveryCountExceeded" {
		t.Errorf("DeadLetterReason = %v, want MaxDeliveryCountExceeded", m.DeadLetterReason)
	}
	if err := dlq.CompleteMessage(testContext(t), m, nil); err != nil {
		t.Errorf("CompleteMessage() on dead-letter queue error: %v", err)
	}
}

func TestDeadLetterMessage(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("bad")...)

	r := newReceiver(t, client, "orders", nil)
	m := receive(t, r, 1)[0]
	if err := r.DeadLetterMessage(testContext(t), m, &azservicebus.DeadLetterOptions{
		Reason:             to.Ptr("validation"),
		ErrorDescription:   to.Ptr("missing field"),
		PropertiesToModify: map[string]any{"checked": true},
	}); err != nil {
		t.Fatalf("DeadLetterMessage() error: %v", err)
	}

	dlq := newReceiver(t, client, "orders", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	got := receive(t, dlq, 1)[0]
	if got.DeadLetterReason == nil || *got.DeadLetterReason != "validation" {
		t.Errorf("DeadLetterReason = %v, want validation", got.DeadLetterReason)
	}
	if got.DeadLetterErrorDescription == nil || *got.DeadLetterErrorDescription != "missing field" {
		t.Errorf("DeadLetterErrorDescription = %v, want missing field", got.DeadLetterErrorDescription)
	}
	if got.ApplicationProperties["checked"] != true {
		t.Errorf("ApplicationProperties[checked] = %v, want true", got.ApplicationProperties["checked"])
	}
	if *got.SequenceNumber != *m.SequenceNumber {
		t.Errorf("dead-lettered SequenceNumber = %d, want the original %d", *got.SequenceNumber, *m.SequenceNumber)
	}
}

func TestTimeToLive(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("dlq-on-expiry", func(p *broker.Properties) { p.DeadLetteringOnMessageExpiration = true }),
		queue("default-ttl", func(p *broker.Properties) { p.DefaultMessageTimeToLive = 300 * time.Millisecond }),
	}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "dlq-on-expiry"), &azservicebus.Message{Body: []byte("short"), TimeToLive: to.Ptr(200 * time.Millisecond)})
	send(t, newSender(t, client, "default-ttl"), bodies("uses entity default")...)
	time.Sleep(600 * time.Millisecond)

	receiveNone(t, newReceiver(t, client, "dlq-on-expiry", nil), 300*time.Millisecond)
	receiveNone(t, newReceiver(t, client, "default-ttl", nil), 300*time.Millisecond)

	dlq := newReceiver(t, client, "dlq-on-expiry", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	m := receive(t, dlq, 1)[0]
	if m.DeadLetterReason == nil || *m.DeadLetterReason != "TTLExpiredException" {
		t.Errorf("DeadLetterReason = %v, want TTLExpiredException", m.DeadLetterReason)
	}
	receiveNone(t, newReceiver(t, client, "default-ttl", &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter}), 300*time.Millisecond)
}

// TestTimeToLiveBeyondEmulatorLimit checks that TTLs above the official
// emulator's one hour cap are kept.
func TestTimeToLiveBeyondEmulatorLimit(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("orders", func(p *broker.Properties) { p.DefaultMessageTimeToLive = 14 * 24 * time.Hour }),
	}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("a")...)
	m := receive(t, newReceiver(t, client, "orders", nil), 1)[0]
	if m.ExpiresAt == nil || time.Until(*m.ExpiresAt) < 13*24*time.Hour {
		t.Errorf("ExpiresAt = %v, want about 14 days ahead", m.ExpiresAt)
	}
}

func TestDeferAndReceiveDeferred(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("later", "now")...)

	r := newReceiver(t, client, "orders", nil)
	ms := receive(t, r, 2)
	if err := r.DeferMessage(testContext(t), ms[0], nil); err != nil {
		t.Fatalf("DeferMessage() error: %v", err)
	}
	if err := r.CompleteMessage(testContext(t), ms[1], nil); err != nil {
		t.Fatalf("CompleteMessage() error: %v", err)
	}
	receiveNone(t, r, 300*time.Millisecond)

	deferred, err := r.ReceiveDeferredMessages(testContext(t), []int64{*ms[0].SequenceNumber}, nil)
	if err != nil {
		t.Fatalf("ReceiveDeferredMessages() error: %v", err)
	}
	if len(deferred) != 1 || string(deferred[0].Body) != "later" {
		t.Fatalf("ReceiveDeferredMessages() = %v, want the deferred message", bodiesOf(deferred))
	}
	if deferred[0].State != azservicebus.MessageStateDeferred {
		t.Errorf("State = %v, want deferred", deferred[0].State)
	}
	if err := r.CompleteMessage(testContext(t), deferred[0], nil); err != nil {
		t.Fatalf("CompleteMessage(deferred) error: %v", err)
	}
	if _, err := r.ReceiveDeferredMessages(testContext(t), []int64{*ms[0].SequenceNumber}, nil); err == nil {
		t.Errorf("ReceiveDeferredMessages() of a completed message succeeded, want error")
	}
}

func TestScheduleAndCancel(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")

	at := time.Now().Add(700 * time.Millisecond)
	seqs, err := sender.ScheduleMessages(testContext(t), bodies("scheduled", "cancelled"), at, nil)
	if err != nil {
		t.Fatalf("ScheduleMessages() error: %v", err)
	}
	if len(seqs) != 2 {
		t.Fatalf("ScheduleMessages() = %v, want 2 sequence numbers", seqs)
	}
	if err := sender.CancelScheduledMessages(testContext(t), seqs[1:], nil); err != nil {
		t.Fatalf("CancelScheduledMessages() error: %v", err)
	}

	r := newReceiver(t, client, "orders", nil)
	peeked, err := r.PeekMessages(testContext(t), 10, nil)
	if err != nil {
		t.Fatalf("PeekMessages() error: %v", err)
	}
	if len(peeked) != 1 || peeked[0].State != azservicebus.MessageStateScheduled {
		t.Fatalf("PeekMessages() before due = %v, want one scheduled message", bodiesOf(peeked))
	}
	receiveNone(t, r, 300*time.Millisecond)

	m := receive(t, r, 1)[0]
	if string(m.Body) != "scheduled" {
		t.Errorf("Body = %q, want scheduled", m.Body)
	}
	if time.Now().Before(at) {
		t.Errorf("scheduled message delivered before its time")
	}
	if m.ScheduledEnqueueTime == nil || !m.ScheduledEnqueueTime.Equal(at.Truncate(time.Millisecond)) {
		t.Errorf("ScheduledEnqueueTime = %v, want %v", m.ScheduledEnqueueTime, at)
	}
	receiveNone(t, r, 300*time.Millisecond)
}

func TestPeekMessages(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), bodies("a", "b", "c")...)

	r := newReceiver(t, client, "orders", nil)
	first, err := r.PeekMessages(testContext(t), 2, nil)
	if err != nil {
		t.Fatalf("PeekMessages() error: %v", err)
	}
	next, err := r.PeekMessages(testContext(t), 2, nil)
	if err != nil {
		t.Fatalf("PeekMessages() error: %v", err)
	}
	if diff := cmp.Diff([]string{"a", "b", "c"}, append(bodiesOf(first), bodiesOf(next)...)); diff != "" {
		t.Errorf("peeked bodies mismatch (-want +got):\n%s", diff)
	}
	from, err := r.PeekMessages(testContext(t), 5, &azservicebus.PeekMessagesOptions{FromSequenceNumber: to.Ptr(int64(2))})
	if err != nil {
		t.Fatalf("PeekMessages(from 2) error: %v", err)
	}
	if diff := cmp.Diff([]string{"b", "c"}, bodiesOf(from)); diff != "" {
		t.Errorf("PeekMessages(from 2) mismatch (-want +got):\n%s", diff)
	}
	// Peeking does not lock: every message can still be received.
	if got := receive(t, r, 3); len(got) != 3 {
		t.Errorf("received %d messages after peeking, want 3", len(got))
	}
}

// TestReleasedMessagesKeepDeliveryCount covers the SDK's releaser, which
// returns messages that arrive after ReceiveMessages returned.
func TestReleasedMessagesKeepDeliveryCount(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")
	r := newReceiver(t, client, "orders", nil)

	send(t, sender, bodies("first")...)
	ms, err := r.ReceiveMessages(testContext(t), 5, nil)
	if err != nil || len(ms) != 1 {
		t.Fatalf("ReceiveMessages() = %d messages, %v; want 1", len(ms), err)
	}
	// Credit for four more messages is still on the link.
	send(t, sender, bodies("second")...)
	time.Sleep(300 * time.Millisecond)

	m := receive(t, r, 1)[0]
	if string(m.Body) != "second" || m.DeliveryCount != 1 {
		t.Errorf("got %q with DeliveryCount %d, want second with 1", m.Body, m.DeliveryCount)
	}
}

func TestCompetingReceivers(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: queues("orders")})
	sender := newSender(t, newClient(t, srv), "orders")
	const n = 200
	for i := range n {
		send(t, sender, &azservicebus.Message{Body: fmt.Appendf(nil, "%d", i)})
	}

	var (
		mu   sync.Mutex
		seen = map[string]int{}
		wg   sync.WaitGroup
	)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for range 4 {
		r := newReceiver(t, newClient(t, srv), "orders", nil)
		wg.Go(func() {
			for {
				mu.Lock()
				total := len(seen)
				mu.Unlock()
				if total >= n || ctx.Err() != nil {
					return
				}
				rctx, rcancel := context.WithTimeout(ctx, 500*time.Millisecond)
				ms, _ := r.ReceiveMessages(rctx, 20, nil)
				rcancel()
				for _, m := range ms {
					if err := r.CompleteMessage(ctx, m, nil); err != nil {
						t.Errorf("CompleteMessage() error: %v", err)
						return
					}
					mu.Lock()
					seen[string(m.Body)]++
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("received %d distinct messages, want %d", len(seen), n)
	}
	for b, c := range seen {
		if c != 1 {
			t.Errorf("message %s completed %d times", b, c)
		}
	}
}

func TestTopicFanOut(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Topics: []server.Topic{
		{Name: "events", Subscriptions: []server.Subscription{{Name: "audit"}, {Name: "billing"}}},
		{Name: "empty"},
	}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "events"), bodies("e1", "e2")...)
	// A topic without subscriptions accepts and drops messages.
	send(t, newSender(t, client, "empty"), bodies("dropped")...)

	for _, sub := range []string{"audit", "billing"} {
		r, err := client.NewReceiverForSubscription("events", sub, nil)
		if err != nil {
			t.Fatalf("NewReceiverForSubscription(%s) error: %v", sub, err)
		}
		got := receive(t, r, 2)
		if diff := cmp.Diff([]string{"e1", "e2"}, bodiesOf(got)); diff != "" {
			t.Errorf("subscription %s bodies mismatch (-want +got):\n%s", sub, diff)
		}
		for _, m := range got {
			if err := r.CompleteMessage(testContext(t), m, nil); err != nil {
				t.Errorf("CompleteMessage() on %s error: %v", sub, err)
			}
		}
		r.Close(context.Background())
	}
}

func TestReceiveFromSessionQueueWithoutSession(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("sessions", func(p *broker.Properties) { p.RequiresSession = true }),
	}})
	client := newClient(t, srv)
	err := newSender(t, client, "sessions").SendMessage(testContext(t), &azservicebus.Message{Body: []byte("x")}, nil)
	if err == nil {
		t.Errorf("SendMessage() without session id to a session queue succeeded, want error")
	}
	r := newReceiver(t, client, "sessions", nil)
	if _, err := r.ReceiveMessages(testContext(t), 1, nil); err == nil {
		t.Errorf("ReceiveMessages() on a session queue without a session receiver succeeded, want error")
	}
}

func TestDisabledEntity(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{
		queue("off", func(p *broker.Properties) { p.Status = broker.StatusDisabled }),
	}})
	err := newSender(t, newClient(t, srv), "off").SendMessage(testContext(t), &azservicebus.Message{Body: []byte("x")}, nil)
	if err == nil {
		t.Errorf("SendMessage() to a disabled queue succeeded, want error")
	}
}

// TestPersistence restarts the server on the same database file.
func TestPersistence(t *testing.T) {
	t.Parallel()
	db := filepath.Join(t.TempDir(), "osb.db")
	opts := server.Options{Addr: "127.0.0.1:0", DBPath: db, Queues: queues("orders")}

	srv, err := server.New(context.Background(), opts)
	if err != nil {
		t.Fatalf("server.New() error: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	client, err := azservicebus.NewClientFromConnectionString(srv.ConnectionString(), nil)
	if err != nil {
		t.Fatalf("NewClientFromConnectionString() error: %v", err)
	}
	sender, err := client.NewSender("orders", nil)
	if err != nil {
		t.Fatalf("NewSender() error: %v", err)
	}
	send(t, sender, bodies("survives", "restart")...)
	client.Close(context.Background())
	if err := srv.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	srv = startServer(t, opts)
	r := newReceiver(t, newClient(t, srv), "orders", nil)
	if diff := cmp.Diff([]string{"survives", "restart"}, bodiesOf(receive(t, r, 2))); diff != "" {
		t.Errorf("bodies after restart mismatch (-want +got):\n%s", diff)
	}
}

package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

func TestDuplicateDetection(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	dedup := func(p *Properties) {
		p.RequiresDuplicateDetection = true
		p.DuplicateDetectionHistoryTimeWindow = time.Minute
	}
	newQueue(t, b, "q", dedup)
	newQueue(t, b, "plain", nil)
	newQueue(t, b, "front", func(p *Properties) { p.ForwardTo = "q" })

	// Duplicates in one batch and across sends are dropped.
	seqs := send(t, b, "q", testMsg{body: "a"}, testMsg{body: "a"}, testMsg{body: "b"})
	if seqs[1] != 0 || seqs[0] == 0 || seqs[2] == 0 {
		t.Errorf("Send() sequence numbers = %v, want 0 for the duplicate", seqs)
	}
	send(t, b, "q", testMsg{body: "a"})
	// A scheduled message counts too.
	later := testMsg{body: "s"}.incoming(t)
	later.ScheduledEnqueueTime = clock.Now().Add(time.Hour)
	if _, err := b.Send(ctx, "q", []IncomingMessage{later}); err != nil {
		t.Fatal(err)
	}
	send(t, b, "q", testMsg{body: "s"})
	// Forwarded messages are checked at the destination.
	send(t, b, "front", testMsg{body: "b"})
	// Other entities keep their duplicates.
	send(t, b, "plain", testMsg{body: "a"}, testMsg{body: "a"})

	if diff := cmp.Diff([]string{"a", "b"}, drainBodies(t, b, "q")); diff != "" {
		t.Errorf("q mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"a", "a"}, drainBodies(t, b, "plain")); diff != "" {
		t.Errorf("plain mismatch (-want +got):\n%s", diff)
	}

	// A duplicate does not extend the window, so 61s after the first "a"
	// it is accepted again although a duplicate came at 30s.
	clock.Advance(30 * time.Second)
	send(t, b, "q", testMsg{body: "a"})
	clock.Advance(31 * time.Second)
	send(t, b, "q", testMsg{body: "a"})
	if diff := cmp.Diff([]string{"a"}, drainBodies(t, b, "q")); diff != "" {
		t.Errorf("after the window mismatch (-want +got):\n%s", diff)
	}
}

func TestQuota(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	oneMB := func(p *Properties) { p.MaxSizeInMegabytes = 1 }
	newQueue(t, b, "small", oneMB)
	newQueue(t, b, "front", func(p *Properties) { p.ForwardTo = "small" })
	newTopic(t, b, "t", map[string][]Rule{"a": nil, "b": nil})
	tp := DefaultProperties(KindTopic)
	tp.MaxSizeInMegabytes = 1
	if _, err := b.UpdateEntity(ctx, "t", tp); err != nil {
		t.Fatal(err)
	}

	big := testMsg{body: strings.Repeat("x", 300<<10), id: "big"}
	send(t, b, "small", big, big, big)
	if _, err := b.Send(ctx, "small", []IncomingMessage{big.incoming(t)}); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("Send() over the quota error = %v, want ErrQuotaExceeded", err)
	}
	// A forward to a full destination is dead-lettered at the source.
	send(t, b, "front", testMsg{body: big.body, id: "fwd"})
	if got := drainBodies(t, b, "front/$DeadLetterQueue"); len(got) != 1 {
		t.Errorf("front dead letters = %d, want the forward to the full queue", len(got))
	}
	// Space freed by receiving can be used again.
	drain(t, b, "small")
	send(t, b, "small", big)

	// A topic holds what its subscriptions hold, two copies per message, so
	// the third send finds 1.2 MB stored.
	send(t, b, "t", big)
	send(t, b, "t", big)
	if _, err := b.Send(ctx, "t", []IncomingMessage{big.incoming(t)}); !errors.Is(err, ErrQuotaExceeded) {
		t.Errorf("Send(topic) over the quota error = %v, want ErrQuotaExceeded", err)
	}
}

func TestMessageSizeLimit(t *testing.T) {
	b, _ := newTestBroker(t)
	newQueue(t, b, "q", func(p *Properties) { p.MaxMessageSizeInKilobytes = 1 })
	send(t, b, "q", testMsg{body: "small"})
	if _, err := b.Send(context.Background(), "q", []IncomingMessage{testMsg{body: strings.Repeat("x", 2048)}.incoming(t)}); !errors.Is(err, ErrMessageTooLarge) {
		t.Errorf("Send(2 KB) error = %v, want ErrMessageTooLarge", err)
	}
}

// waitGone waits for the timer loop to delete the entity at path.
func waitGone(t *testing.T, b *Broker, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		b.wakeTimers()
		if _, err := b.Entity(path); errors.Is(err, ErrEntityNotFound) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s was not deleted", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// settle lets the timer loop run a few passes.
func settle(b *Broker) {
	for range 5 {
		b.wakeTimers()
		time.Sleep(10 * time.Millisecond)
	}
}

func exists(b *Broker, path string) bool {
	_, err := b.Entity(path)
	return err == nil
}

func TestAutoDeleteOnIdle(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	idle := func(p *Properties) { p.AutoDeleteOnIdle = time.Minute }
	newQueue(t, b, "idle", idle)
	newQueue(t, b, "busy", idle)
	newQueue(t, b, "target", nil)
	newQueue(t, b, "forwarder", func(p *Properties) {
		idle(p)
		p.ForwardTo = "target"
	})
	tp := DefaultProperties(KindTopic)
	tp.AutoDeleteOnIdle = time.Minute
	if _, err := b.CreateTopic(ctx, "t", tp); err != nil {
		t.Fatal(err)
	}
	sp := DefaultProperties(KindSubscription)
	if _, err := b.CreateSubscription(ctx, "t", "s", sp); err != nil {
		t.Fatal(err)
	}
	busy, _ := b.Resolve("busy")
	sub, _ := b.Resolve(SubscriptionPath("t", "s"))

	// Activity keeps entities alive: an empty receive, a peek and an
	// operation on a subscription, which counts for its topic.
	for range 3 {
		clock.Advance(40 * time.Second)
		if _, err := b.Receive(ctx, busy, 1, PeekLock); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Peek(ctx, sub, 0, 1, ""); err != nil {
			t.Fatal(err)
		}
		settle(b)
	}
	waitGone(t, b, "idle")
	for _, p := range []string{"busy", "t", "forwarder"} {
		if !exists(b, p) {
			t.Errorf("%s was deleted although it was active or forwards", p)
		}
	}

	// Without activity, the topic goes and takes its subscription.
	clock.Advance(2 * time.Minute)
	waitGone(t, b, "t")
	if exists(b, SubscriptionPath("t", "s")) {
		t.Error("the subscription outlived its deleted topic")
	}
	if !exists(b, "forwarder") {
		t.Error("forwarder was deleted, want forwarding entities never idle")
	}
}

func TestAutoDeleteIgnoresDowntime(t *testing.T) {
	b, clock := newTestBroker(t)
	newQueue(t, b, "q", func(p *Properties) { p.AutoDeleteOnIdle = time.Minute })
	clock.Advance(time.Hour)
	// A restart counts as activity: reopen a broker on the same store.
	b.Close()
	b2, err := New(context.Background(), b.store.Get(), Options{Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	settle(b2)
	if !exists(b2, "q") {
		t.Fatal("q was deleted at startup for time the broker was stopped")
	}
	clock.Advance(2 * time.Minute)
	waitGone(t, b2, "q")
}

// TestSendRacesIdleDeletion checks a send to an entity being deleted
// fails with ErrEntityNotFound rather than a store error.
func TestSendRacesIdleDeletion(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	for i := range 20 {
		name := fmt.Sprintf("q%d", i)
		newQueue(t, b, name, nil)
		var wg sync.WaitGroup
		wg.Go(func() {
			for range 20 {
				_, err := b.Send(ctx, name, []IncomingMessage{testMsg{body: "x", props: encoding.Map{}}.incoming(t)})
				if err != nil && !errors.Is(err, ErrEntityNotFound) {
					t.Errorf("Send() error = %v, want nil or ErrEntityNotFound", err)
				}
			}
		})
		wg.Go(func() {
			if err := b.DeleteEntity(ctx, name); err != nil {
				t.Errorf("DeleteEntity() error: %v", err)
			}
		})
		wg.Wait()
	}
}

func TestCountsAccessedAt(t *testing.T) {
	b, clock := newTestBroker(t)
	newQueue(t, b, "q", nil)
	clock.Advance(time.Hour)
	send(t, b, "q", testMsg{body: "x"})
	c, err := b.Counts(context.Background(), "q")
	if err != nil {
		t.Fatal(err)
	}
	if !c.AccessedAt.Equal(clock.Now().Truncate(time.Millisecond)) {
		t.Errorf("AccessedAt = %v, want the send time %v", c.AccessedAt, clock.Now())
	}
}

func TestDeadLetterForwardToFullQueue(t *testing.T) {
	b, clock := newTestBroker(t)
	newQueue(t, b, "full", func(p *Properties) { p.MaxSizeInMegabytes = 1 })
	newQueue(t, b, "short", func(p *Properties) {
		p.DefaultMessageTimeToLive = time.Second
		p.DeadLetteringOnMessageExpiration = true
		p.ForwardDeadLetteredMessagesTo = "full"
	})
	big := testMsg{body: strings.Repeat("x", 300<<10), id: "big"}
	send(t, b, "full", big, big, big)
	send(t, b, "short", testMsg{body: big.body, id: "old"})
	clock.Advance(2 * time.Second)

	// The expired message cannot move to the full queue, so it stays in
	// the source dead-letter queue.
	deadline := time.Now().Add(5 * time.Second)
	for {
		b.wakeTimers()
		if left := drain(t, b, "short/$DeadLetterQueue"); len(left) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the expired message did not stay in the dead-letter queue")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := len(drain(t, b, "full")); got != 3 {
		t.Errorf("full holds %d messages, want 3", got)
	}
}

func TestDeleteMessages(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "q", nil)
	send(t, b, "q", testMsg{body: "1"}, testMsg{body: "2"})
	clock.Advance(time.Minute)
	cutoff := clock.Now()
	clock.Advance(time.Minute)
	send(t, b, "q", testMsg{body: "3"})
	later := testMsg{body: "scheduled"}.incoming(t)
	later.ScheduledEnqueueTime = clock.Now().Add(time.Hour)
	if _, err := b.Send(ctx, "q", []IncomingMessage{later}); err != nil {
		t.Fatal(err)
	}
	addr, _ := b.Resolve("q")

	// Only what was enqueued before the cutoff, oldest first.
	n, err := b.DeleteMessages(ctx, addr, 1, cutoff, "")
	if err != nil || n != 1 {
		t.Fatalf("DeleteMessages(1) = %d, %v", n, err)
	}
	if n, err = b.DeleteMessages(ctx, addr, 10, cutoff, ""); err != nil || n != 1 {
		t.Fatalf("DeleteMessages(10, cutoff) = %d, %v; want the one left before the cutoff", n, err)
	}
	if n, err = b.DeleteMessages(ctx, addr, 10, cutoff, ""); err != nil || n != 0 {
		t.Fatalf("DeleteMessages on nothing = %d, %v", n, err)
	}
	// The scheduled message is not enqueued yet, whatever the cutoff.
	if n, err = b.DeleteMessages(ctx, addr, 10, clock.Now().Add(24*time.Hour), ""); err != nil || n != 1 {
		t.Fatalf("DeleteMessages(far cutoff) = %d, %v; want only the active message", n, err)
	}
	if c, _ := b.Counts(ctx, "q"); c.Scheduled != 1 || c.Active != 0 {
		t.Errorf("counts = %+v, want the scheduled message left", c)
	}

	// Limits and kinds.
	for _, bad := range []int{0, -1, DefaultMaxBatchDelete + 1} {
		if _, err := b.DeleteMessages(ctx, addr, bad, cutoff, ""); !errors.Is(err, ErrInvalidOperation) {
			t.Errorf("DeleteMessages(%d) error = %v, want ErrInvalidOperation", bad, err)
		}
	}
	newTopic(t, b, "t", map[string][]Rule{"s": nil})
	topic, _ := b.Resolve("t")
	if _, err := b.DeleteMessages(ctx, topic, 1, cutoff, ""); !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("DeleteMessages on a topic = %v, want ErrInvalidOperation", err)
	}
	// A subscription's dead-letter queue works like any other sub-queue.
	send(t, b, "t", testMsg{body: "x"})
	sub, _ := b.Resolve("t/subscriptions/s")
	ds, _ := b.Receive(ctx, sub, 1, PeekLock)
	if err := b.DeadLetter(ctx, sub, ds[0].LockToken, "r", "d", nil); err != nil {
		t.Fatal(err)
	}
	dlq, _ := b.Resolve("t/subscriptions/s/$DeadLetterQueue")
	if n, err := b.DeleteMessages(ctx, dlq, 5, clock.Now().Add(time.Hour), ""); err != nil || n != 1 {
		t.Errorf("DeleteMessages(subscription DLQ) = %d, %v", n, err)
	}
}

func TestDeleteMessagesBySession(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "q", func(p *Properties) { p.RequiresSession = true })
	for _, s := range []string{"s1", "s2", "s1"} {
		send(t, b, "q", testMsg{body: "m", session: s, id: s + time.Now().String()})
	}
	addr, _ := b.Resolve("q")
	n, err := b.DeleteMessages(ctx, addr, 10, clock.Now().Add(time.Hour), "s1")
	if err != nil || n != 2 {
		t.Fatalf("DeleteMessages(s1) = %d, %v; want 2", n, err)
	}
	if c, _ := b.Counts(ctx, "q"); c.Active != 1 {
		t.Errorf("active after deleting a session = %d, want 1", c.Active)
	}
}

func TestDeleteMessagesRespectsStatusAndMaxBatch(t *testing.T) {
	ctx := context.Background()
	st, _ := store.Open(ctx, store.Memory)
	defer st.Close()
	b, err := New(ctx, st, Options{MaxBatchDelete: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	newQueue(t, b, "q", func(p *Properties) { p.Status = StatusReceiveDisabled })
	addr, _ := b.Resolve("q")
	if _, err := b.DeleteMessages(ctx, addr, 1, time.Now(), ""); !errors.Is(err, ErrEntityDisabled) {
		t.Errorf("DeleteMessages on a receive-disabled queue = %v, want ErrEntityDisabled", err)
	}
	if b.MaxBatchDelete() != 5 {
		t.Errorf("MaxBatchDelete() = %d, want 5", b.MaxBatchDelete())
	}
	if _, err := b.DeleteMessages(ctx, addr, 6, time.Now(), ""); !errors.Is(err, ErrInvalidOperation) {
		t.Errorf("a count above the configured limit = %v", err)
	}
}

func TestCommit(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "in", nil)
	newQueue(t, b, "out", nil)
	send(t, b, "in", testMsg{body: "a"}, testMsg{body: "b"})
	in, _ := b.Resolve("in")
	ds, err := b.Receive(ctx, in, 2, PeekLock)
	if err != nil || len(ds) != 2 {
		t.Fatal(err)
	}
	out := []IncomingMessage{testMsg{body: "result"}.incoming(t)}

	// One transaction: complete a, dead-letter b, send the result.
	err = b.Commit(ctx, []TxOp{
		TxSettle(in, ds[0].LockToken, Settlement{Kind: SettleComplete}),
		TxSettle(in, ds[1].LockToken, Settlement{Kind: SettleDeadLetter, Reason: "r", Description: "d"}),
		TxSend("out", "in", out),
	})
	if err != nil {
		t.Fatalf("Commit() error: %v", err)
	}
	ci, _ := b.Counts(ctx, "in")
	co, _ := b.Counts(ctx, "out")
	if ci.Active != 0 || ci.DeadLetter != 1 || co.Active != 1 {
		t.Errorf("counts in = %+v, out = %+v", ci, co)
	}

	// A failing operation undoes the ones before it: the lock of a message
	// that is already gone is lost, so neither the send nor the other
	// settlement happens.
	send(t, b, "in", testMsg{body: "c"})
	ds2, _ := b.Receive(ctx, in, 1, PeekLock)
	err = b.Commit(ctx, []TxOp{
		TxSend("out", "", []IncomingMessage{testMsg{body: "must-not-appear"}.incoming(t)}),
		TxSettle(in, ds2[0].LockToken, Settlement{Kind: SettleComplete}),
		TxSettle(in, ds[0].LockToken, Settlement{Kind: SettleComplete}), // Already completed.
	})
	if !errors.Is(err, ErrLockLost) {
		t.Fatalf("Commit() with a lost lock error = %v, want ErrLockLost", err)
	}
	ci, _ = b.Counts(ctx, "in")
	co, _ = b.Counts(ctx, "out")
	if ci.Active != 1 || co.Active != 1 {
		t.Errorf("after the failed commit in = %+v, out = %+v; want nothing changed", ci, co)
	}

	// A transfer to a disabled destination goes to the transfer dead-letter
	// queue of the via entity; a plain send to it fails the commit.
	off := DefaultProperties(KindQueue)
	off.Status = StatusSendDisabled
	if _, err := b.CreateQueue(ctx, "off", off); err != nil {
		t.Fatal(err)
	}
	msg := []IncomingMessage{testMsg{body: "stuck"}.incoming(t)}
	if err := b.Commit(ctx, []TxOp{TxSend("off", "in", msg)}); err != nil {
		t.Fatalf("Commit() of a transfer to a disabled queue error: %v", err)
	}
	if ci, _ = b.Counts(ctx, "in"); ci.TransferDeadLetter != 1 {
		t.Errorf("transfer dead-letter count = %d, want 1", ci.TransferDeadLetter)
	}
	if err := b.Commit(ctx, []TxOp{TxSend("off", "", msg)}); !errors.Is(err, ErrEntityDisabled) {
		t.Errorf("Commit() of a plain send to a disabled queue = %v, want ErrEntityDisabled", err)
	}
	// An empty transaction commits.
	if err := b.Commit(ctx, nil); err != nil {
		t.Errorf("Commit(nil) error: %v", err)
	}
}

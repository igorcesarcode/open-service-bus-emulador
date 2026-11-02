package broker

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/frames"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestBroker(t *testing.T) (*Broker, *fakeClock) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, store.Memory)
	if err != nil {
		t.Fatalf("store.Open() error: %v", err)
	}
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, err := New(ctx, st, Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	t.Cleanup(func() {
		b.Close()
		st.Close()
	})
	return b, clock
}

func testPayload(t *testing.T, body string) []byte {
	t.Helper()
	p, err := frames.MarshalMessage(&frames.Message{Data: [][]byte{[]byte(body)}})
	if err != nil {
		t.Fatalf("MarshalMessage() error: %v", err)
	}
	return p
}

func TestLockExpiryCountsTowardMaxDelivery(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	props := DefaultProperties(KindQueue)
	props.MaxDeliveryCount = 2
	props.LockDuration = time.Second
	if _, err := b.CreateQueue(ctx, "q", props); err != nil {
		t.Fatalf("CreateQueue() error: %v", err)
	}
	if _, err := b.Send(ctx, "q", []IncomingMessage{{Payload: testPayload(t, "x")}}); err != nil {
		t.Fatalf("Send() error: %v", err)
	}
	addr, _ := b.Resolve("q")
	for i := range 2 {
		ds, err := b.Receive(ctx, addr, 1, PeekLock)
		if err != nil || len(ds) != 1 {
			t.Fatalf("Receive(%d) = %d deliveries, %v; want 1", i, len(ds), err)
		}
		if ds[0].DeliveryCount != i+1 {
			t.Errorf("DeliveryCount = %d, want %d", ds[0].DeliveryCount, i+1)
		}
		clock.Advance(2 * time.Second) // let the lock lapse
	}
	ds, err := b.Receive(ctx, addr, 1, PeekLock)
	if err != nil || len(ds) != 0 {
		t.Fatalf("Receive() after max deliveries = %d, %v; want none", len(ds), err)
	}
	dlq, _ := b.Resolve("q/$DeadLetterQueue")
	ds, err = b.Receive(ctx, dlq, 1, PeekLock)
	if err != nil || len(ds) != 1 {
		t.Fatalf("Receive(DLQ) = %d, %v; want 1", len(ds), err)
	}
}

func TestReleaseDoesNotCountDelivery(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	if _, err := b.CreateQueue(ctx, "q", DefaultProperties(KindQueue)); err != nil {
		t.Fatalf("CreateQueue() error: %v", err)
	}
	if _, err := b.Send(ctx, "q", []IncomingMessage{{Payload: testPayload(t, "x")}}); err != nil {
		t.Fatalf("Send() error: %v", err)
	}
	addr, _ := b.Resolve("q")
	ds, _ := b.Receive(ctx, addr, 1, PeekLock)
	if err := b.Release(ctx, addr, ds[0].LockToken); err != nil {
		t.Fatalf("Release() error: %v", err)
	}
	ds, _ = b.Receive(ctx, addr, 1, PeekLock)
	if len(ds) != 1 || ds[0].DeliveryCount != 1 {
		t.Fatalf("Receive() after release = %v, want one delivery with count 1", ds)
	}
	if err := b.Complete(ctx, addr, LockToken{}); !errorIsLockLost(err) {
		t.Errorf("Complete(unknown token) error = %v, want ErrLockLost", err)
	}
}

func TestEntityLifecycle(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	if _, err := b.CreateTopic(ctx, "Events", DefaultProperties(KindTopic)); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}
	if _, err := b.CreateSubscription(ctx, "events", "Audit", DefaultProperties(KindSubscription)); err != nil {
		t.Fatalf("CreateSubscription() error: %v", err)
	}
	if _, err := b.CreateQueue(ctx, "EVENTS", DefaultProperties(KindQueue)); err == nil {
		t.Errorf("CreateQueue() with a topic's name succeeded, want ErrEntityExists")
	}
	addr, err := b.Resolve("events/subscriptions/AUDIT/$deadletterqueue")
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}
	if addr.Subqueue != SubqueueDeadLetter || addr.String() != "Events/Subscriptions/Audit/$DeadLetterQueue" {
		t.Errorf("Resolve() = %v (subqueue %d)", addr, addr.Subqueue)
	}
	bad := DefaultProperties(KindQueue)
	bad.LockDuration = 0
	if _, err := b.CreateQueue(ctx, "q", bad); err == nil {
		t.Errorf("CreateQueue() with zero lock duration succeeded, want error")
	}
	if err := b.DeleteEntity(ctx, "events"); err != nil {
		t.Fatalf("DeleteEntity() error: %v", err)
	}
	if b.Exists("events/subscriptions/audit") {
		t.Errorf("subscription survived deleting its topic")
	}
}

func errorIsLockLost(err error) bool { return errors.Is(err, ErrLockLost) }

// TestNoDeadlockBetweenTimersAndEntityUpdates races the timer loop, which
// expires messages inside a write transaction, against entity updates.
// They once acquired the entity lock and the single writer connection in
// opposite orders.
func TestNoDeadlockBetweenTimersAndEntityUpdates(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "osb.db"))
	if err != nil {
		t.Fatalf("store.Open() error: %v", err)
	}
	defer st.Close()
	b, err := New(ctx, st, Options{})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer b.Close()
	props := DefaultProperties(KindQueue)
	props.DefaultMessageTimeToLive = time.Millisecond
	if _, err := b.CreateQueue(ctx, "q", props); err != nil {
		t.Fatalf("CreateQueue() error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range 300 {
				if _, err := b.Send(ctx, "q", []IncomingMessage{{Payload: testPayload(t, "x")}}); err != nil {
					t.Errorf("Send(%d) error: %v", i, err)
					return
				}
			}
		})
		wg.Go(func() {
			for i := range 300 {
				p := props
				p.MaxDeliveryCount = int32(i%5 + 1)
				if _, err := b.UpdateEntity(ctx, "q", p); err != nil {
					t.Errorf("UpdateEntity(%d) error: %v", i, err)
					return
				}
			}
		})
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("sends and entity updates did not finish: deadlock")
	}
}

// TestRestartWithExpiredMessages reopens a database whose messages expired
// while the broker was down, then updates the entity as server startup does.
func TestRestartWithExpiredMessages(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "osb.db")
	props := DefaultProperties(KindQueue)
	props.DefaultMessageTimeToLive = 50 * time.Millisecond

	st, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("store.Open() error: %v", err)
	}
	b, err := New(ctx, st, Options{})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if _, err := b.CreateQueue(ctx, "q", props); err != nil {
		t.Fatalf("CreateQueue() error: %v", err)
	}
	msgs := make([]IncomingMessage, 200)
	for i := range msgs {
		msgs[i] = IncomingMessage{Payload: testPayload(t, "x")}
	}
	if _, err := b.Send(ctx, "q", msgs); err != nil {
		t.Fatalf("Send() error: %v", err)
	}
	b.Close()
	st.Close()
	time.Sleep(100 * time.Millisecond)

	done := make(chan error, 1)
	go func() {
		st, err := store.Open(ctx, path)
		if err != nil {
			done <- err
			return
		}
		defer st.Close()
		b, err := New(ctx, st, Options{})
		if err != nil {
			done <- err
			return
		}
		defer b.Close()
		for range 20 {
			if _, err := b.UpdateEntity(ctx, "q", props); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("restart error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("restart with expired messages did not finish: deadlock")
	}
}

func sessionQueue(t *testing.T, b *Broker, lock time.Duration) Address {
	t.Helper()
	props := DefaultProperties(KindQueue)
	props.RequiresSession = true
	props.LockDuration = lock
	if _, err := b.CreateQueue(context.Background(), "sq", props); err != nil {
		t.Fatalf("CreateQueue() error: %v", err)
	}
	addr, _ := b.Resolve("sq")
	return addr
}

func sendSession(t *testing.T, b *Broker, session, body string) {
	t.Helper()
	if _, err := b.Send(context.Background(), "sq", []IncomingMessage{{Payload: testPayload(t, body), SessionID: session}}); err != nil {
		t.Fatalf("Send(%s) error: %v", session, err)
	}
}

func TestSessions(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	addr := sessionQueue(t, b, 10*time.Second)

	if _, err := b.AcceptNextSession(ctx, addr); !errors.Is(err, ErrNoSessionAvailable) {
		t.Fatalf("AcceptNextSession() on an empty queue error = %v, want ErrNoSessionAvailable", err)
	}
	if _, err := b.Send(ctx, "sq", []IncomingMessage{{Payload: testPayload(t, "x")}}); !errors.Is(err, ErrSessionRequired) {
		t.Errorf("Send() without session id error = %v, want ErrSessionRequired", err)
	}
	sendSession(t, b, "b", "b1")
	sendSession(t, b, "a", "a1")
	sendSession(t, b, "b", "b2")

	// The oldest message picks the session.
	lockB, err := b.AcceptNextSession(ctx, addr)
	if err != nil || lockB.SessionID != "b" {
		t.Fatalf("AcceptNextSession() = %v, %v; want session b", lockB, err)
	}
	if _, err := b.AcceptSession(ctx, addr, "b"); !errors.Is(err, ErrSessionCannotBeLocked) {
		t.Errorf("AcceptSession(b) while held error = %v, want ErrSessionCannotBeLocked", err)
	}
	lockA, err := b.AcceptNextSession(ctx, addr)
	if err != nil || lockA.SessionID != "a" {
		t.Fatalf("second AcceptNextSession() = %v, %v; want session a", lockA, err)
	}

	ds, err := b.ReceiveSession(ctx, lockB, 10, PeekLock)
	if err != nil || len(ds) != 2 {
		t.Fatalf("ReceiveSession(b) = %d, %v; want 2 messages", len(ds), err)
	}
	if !ds[0].LockedUntil.Equal(lockB.LockedUntil) {
		t.Errorf("message lock %v does not follow the session lock %v", ds[0].LockedUntil, lockB.LockedUntil)
	}
	if err := b.Complete(ctx, addr, ds[0].LockToken); err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	// State needs a held session.
	if err := b.SetSessionState(ctx, addr, "b", []byte("progress")); err != nil {
		t.Fatalf("SetSessionState() error: %v", err)
	}
	if st, err := b.SessionState(ctx, addr, "b"); err != nil || string(st) != "progress" {
		t.Errorf("SessionState() = %q, %v; want progress", st, err)
	}
	if err := b.SetSessionState(ctx, addr, "nobody", []byte("x")); !errors.Is(err, ErrSessionLockLost) {
		t.Errorf("SetSessionState(unlocked) error = %v, want ErrSessionLockLost", err)
	}

	// Renewing extends the session and its message locks.
	clock.Advance(5 * time.Second)
	until, err := b.RenewSessionLock(ctx, addr, "b")
	if err != nil {
		t.Fatalf("RenewSessionLock() error: %v", err)
	}
	clock.Advance(7 * time.Second) // past the original lock
	if err := b.Complete(ctx, addr, ds[1].LockToken); err != nil {
		t.Errorf("Complete() after session renew error: %v", err)
	}
	if !until.After(lockB.LockedUntil) {
		t.Errorf("renewed lock %v not after %v", until, lockB.LockedUntil)
	}

	// Session a lapses: another receiver can take it, and the old lock is lost.
	clock.Advance(20 * time.Second)
	if _, err := b.ReceiveSession(ctx, lockA, 1, PeekLock); !errors.Is(err, ErrSessionLockLost) {
		t.Errorf("ReceiveSession() on a lapsed lock error = %v, want ErrSessionLockLost", err)
	}
	again, err := b.AcceptSession(ctx, addr, "a")
	if err != nil {
		t.Fatalf("AcceptSession(a) after lapse error: %v", err)
	}
	if ds, err := b.ReceiveSession(ctx, again, 1, PeekLock); err != nil || len(ds) != 1 {
		t.Errorf("ReceiveSession(a) = %d, %v; want 1", len(ds), err)
	}

	ids, err := b.ListSessions(ctx, addr, time.Time{}, 0, 10)
	if err != nil {
		t.Fatalf("ListSessions() error: %v", err)
	}
	// a has a message; b has state.
	if len(ids) != 2 || ids[0] != "a" || ids[1] != "b" {
		t.Errorf("ListSessions() = %v, want [a b]", ids)
	}
}

func TestReleaseSessionUnlocksMessages(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	addr := sessionQueue(t, b, time.Minute)
	sendSession(t, b, "s", "1")

	lock, err := b.AcceptSession(ctx, addr, "s")
	if err != nil {
		t.Fatalf("AcceptSession() error: %v", err)
	}
	if ds, _ := b.ReceiveSession(ctx, lock, 1, PeekLock); len(ds) != 1 {
		t.Fatalf("ReceiveSession() = %d messages, want 1", len(ds))
	}
	if err := b.ReleaseSession(ctx, lock); err != nil {
		t.Fatalf("ReleaseSession() error: %v", err)
	}
	next, err := b.AcceptNextSession(ctx, addr)
	if err != nil {
		t.Fatalf("AcceptNextSession() after release error: %v", err)
	}
	ds, err := b.ReceiveSession(ctx, next, 1, PeekLock)
	if err != nil || len(ds) != 1 || ds[0].DeliveryCount != 2 {
		t.Fatalf("ReceiveSession() after release = %v, %v; want the message with delivery count 2", ds, err)
	}
}

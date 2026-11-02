package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/google/go-cmp/cmp"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
)

func sessionQueue(name string, lock time.Duration) server.Queue {
	return queue(name, func(p *broker.Properties) {
		p.RequiresSession = true
		p.LockDuration = lock
	})
}

func sessionMessages(session string, bodies ...string) []*azservicebus.Message {
	out := make([]*azservicebus.Message, len(bodies))
	for i, b := range bodies {
		out[i] = &azservicebus.Message{Body: []byte(b), SessionID: to.Ptr(session)}
	}
	return out
}

func acceptSession(t *testing.T, client *azservicebus.Client, queue, session string) *azservicebus.SessionReceiver {
	t.Helper()
	r, err := client.AcceptSessionForQueue(testContext(t), queue, session, nil)
	if err != nil {
		t.Fatalf("AcceptSessionForQueue(%q, %q) error: %v", queue, session, err)
	}
	t.Cleanup(func() { r.Close(context.Background()) })
	return r
}

func receiveSession(t *testing.T, r *azservicebus.SessionReceiver, n int) []*azservicebus.ReceivedMessage {
	t.Helper()
	ctx := testContext(t)
	var out []*azservicebus.ReceivedMessage
	for len(out) < n {
		msgs, err := r.ReceiveMessages(ctx, n-len(out), nil)
		if err != nil {
			t.Fatalf("ReceiveMessages() after %d/%d messages error: %v", len(out), n, err)
		}
		out = append(out, msgs...)
	}
	return out
}

func TestAcceptSession(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", time.Minute)}})
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")
	send(t, sender, sessionMessages("customer-1", "a1", "a2")...)
	send(t, sender, sessionMessages("customer-2", "b1")...)

	r := acceptSession(t, client, "orders", "customer-1")
	if r.SessionID() != "customer-1" {
		t.Errorf("SessionID() = %q, want customer-1", r.SessionID())
	}
	if d := time.Until(r.LockedUntil()); d < 50*time.Second || d > 70*time.Second {
		t.Errorf("LockedUntil() is %v away, want about one minute", d)
	}
	got := receiveSession(t, r, 2)
	if diff := cmp.Diff([]string{"a1", "a2"}, bodiesOf(got)); diff != "" {
		t.Errorf("session bodies mismatch (-want +got):\n%s", diff)
	}
	for _, m := range got {
		if m.SessionID == nil || *m.SessionID != "customer-1" {
			t.Errorf("message SessionID = %v, want customer-1", m.SessionID)
		}
		if err := r.CompleteMessage(testContext(t), m, nil); err != nil {
			t.Errorf("CompleteMessage() error: %v", err)
		}
	}
}

func TestAcceptNextSession(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", time.Minute)}})
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")
	send(t, sender, sessionMessages("first", "f1")...)
	send(t, sender, sessionMessages("second", "s1")...)

	r1, err := client.AcceptNextSessionForQueue(testContext(t), "orders", nil)
	if err != nil {
		t.Fatalf("AcceptNextSessionForQueue() error: %v", err)
	}
	defer r1.Close(context.Background())
	r2, err := client.AcceptNextSessionForQueue(testContext(t), "orders", nil)
	if err != nil {
		t.Fatalf("second AcceptNextSessionForQueue() error: %v", err)
	}
	defer r2.Close(context.Background())
	if r1.SessionID() != "first" || r2.SessionID() != "second" {
		t.Errorf("sessions = %q, %q; want first, second (oldest message first)", r1.SessionID(), r2.SessionID())
	}
	if got := receiveSession(t, r2, 1); string(got[0].Body) != "s1" {
		t.Errorf("second session body = %q, want s1", got[0].Body)
	}
}

// TestAcceptNextSessionWaitsForMessages starts the receiver before any
// message exists, as consumers usually do.
func TestAcceptNextSessionWaitsForMessages(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", time.Minute)}})
	client := newClient(t, srv)

	type result struct {
		r   *azservicebus.SessionReceiver
		err error
	}
	accepted := make(chan result, 1)
	go func() {
		r, err := client.AcceptNextSessionForQueue(testContext(t), "orders", nil)
		accepted <- result{r, err}
	}()
	time.Sleep(300 * time.Millisecond)
	select {
	case res := <-accepted:
		t.Fatalf("AcceptNextSessionForQueue() returned (%v) with no session available", res.err)
	default:
	}
	send(t, newSender(t, client, "orders"), sessionMessages("late", "x")...)
	res := <-accepted
	if res.err != nil {
		t.Fatalf("AcceptNextSessionForQueue() error: %v", res.err)
	}
	defer res.r.Close(context.Background())
	if res.r.SessionID() != "late" {
		t.Errorf("SessionID() = %q, want late", res.r.SessionID())
	}
}

func TestAcceptNextSessionTimeout(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", time.Minute)}})
	client := newClient(t, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := client.AcceptNextSessionForQueue(ctx, "orders", nil)
	var sbErr *azservicebus.Error
	if !errors.As(err, &sbErr) || sbErr.Code != azservicebus.CodeTimeout {
		t.Errorf("AcceptNextSessionForQueue() error = %v, want code %q", err, azservicebus.CodeTimeout)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("AcceptNextSessionForQueue() took %v, want about the 1.5s deadline", d)
	}
	// The connection still works afterwards.
	send(t, newSender(t, client, "orders"), sessionMessages("s", "x")...)
}

func TestSessionAlreadyLocked(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", time.Minute)}})
	client := newClient(t, srv)
	acceptSession(t, client, "orders", "busy")

	_, err := client.AcceptSessionForQueue(testContext(t), "orders", "busy", nil)
	if err == nil {
		t.Fatal("AcceptSessionForQueue() on a locked session succeeded, want error")
	}
	var sbErr *azservicebus.Error
	if errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeTimeout {
		t.Errorf("AcceptSessionForQueue() on a locked session timed out instead of failing fast: %v", err)
	}
}

func TestSessionState(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", time.Minute)}})
	client := newClient(t, srv)
	r := acceptSession(t, client, "orders", "s1")
	ctx := testContext(t)

	state, err := r.GetSessionState(ctx, nil)
	if err != nil || state != nil {
		t.Fatalf("GetSessionState() on a new session = %q, %v; want nil", state, err)
	}
	if err := r.SetSessionState(ctx, []byte("step-2"), nil); err != nil {
		t.Fatalf("SetSessionState() error: %v", err)
	}
	r.Close(ctx)

	// State outlives the lock.
	r = acceptSession(t, client, "orders", "s1")
	if state, err := r.GetSessionState(ctx, nil); err != nil || string(state) != "step-2" {
		t.Errorf("GetSessionState() = %q, %v; want step-2", state, err)
	}
	if err := r.SetSessionState(ctx, nil, nil); err != nil {
		t.Fatalf("SetSessionState(nil) error: %v", err)
	}
	if state, err := r.GetSessionState(ctx, nil); err != nil || state != nil {
		t.Errorf("GetSessionState() after clearing = %q, %v; want nil", state, err)
	}
}

func TestRenewSessionLock(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", 2*time.Second)}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), sessionMessages("s1", "x")...)
	r := acceptSession(t, client, "orders", "s1")
	m := receiveSession(t, r, 1)[0]
	first := r.LockedUntil()

	time.Sleep(1500 * time.Millisecond)
	if err := r.RenewSessionLock(testContext(t), nil); err != nil {
		t.Fatalf("RenewSessionLock() error: %v", err)
	}
	if !r.LockedUntil().After(first) {
		t.Errorf("LockedUntil() after renew = %v, want after %v", r.LockedUntil(), first)
	}
	time.Sleep(1 * time.Second) // past the original lock
	if err := r.CompleteMessage(testContext(t), m, nil); err != nil {
		t.Errorf("CompleteMessage() after session renew error: %v", err)
	}
}

// TestCloseReleasesSession lets another receiver take the session as soon
// as the first one closes, with unsettled messages redelivered.
func TestCloseReleasesSession(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", time.Minute)}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), sessionMessages("s1", "x")...)

	r := acceptSession(t, client, "orders", "s1")
	receiveSession(t, r, 1)
	if err := r.Close(testContext(t)); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	var next *azservicebus.SessionReceiver
	deadline := time.Now().Add(5 * time.Second)
	for {
		var err error
		next, err = client.AcceptSessionForQueue(testContext(t), "orders", "s1", nil)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("AcceptSessionForQueue() after close error: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer next.Close(context.Background())
	m := receiveSession(t, next, 1)[0]
	if m.DeliveryCount != 2 {
		t.Errorf("DeliveryCount after the first holder closed = %d, want 2", m.DeliveryCount)
	}
}

func TestSessionLockExpiry(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", 500*time.Millisecond)}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "orders"), sessionMessages("s1", "x")...)

	held := acceptSession(t, client, "orders", "s1")
	m := receiveSession(t, held, 1)[0]
	time.Sleep(time.Second)

	// The lapsed session can be taken by another receiver, which gets the
	// unsettled message again.
	other, err := newClient(t, srv).AcceptSessionForQueue(testContext(t), "orders", "s1", nil)
	if err != nil {
		t.Fatalf("AcceptSessionForQueue() after the lock lapsed error: %v", err)
	}
	defer other.Close(context.Background())
	got := receiveSession(t, other, 1)[0]
	if got.DeliveryCount != 2 {
		t.Errorf("DeliveryCount = %d, want 2", got.DeliveryCount)
	}
	if err := other.CompleteMessage(testContext(t), got, nil); err != nil {
		t.Fatalf("CompleteMessage() by the new holder error: %v", err)
	}
	// The old holder's lock is gone. The server rejects its settlement with
	// message-lock-lost, but the client marks the message settled on that
	// rejection and reports success on its automatic retry, so check the
	// server state instead: nothing is left to settle.
	held.CompleteMessage(testContext(t), m, nil)
	peeked, err := other.PeekMessages(testContext(t), 10, nil)
	if err != nil {
		t.Fatalf("PeekMessages() error: %v", err)
	}
	if len(peeked) != 0 {
		t.Errorf("PeekMessages() = %d messages, want none", len(peeked))
	}
}

func TestListSessions(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", time.Minute)}})
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")
	for _, s := range []string{"c", "a", "b"} {
		send(t, sender, sessionMessages(s, "x")...)
	}

	var got []string
	pager := client.NewListSessionsForQueuePager("orders", nil)
	for pager.More() {
		page, err := pager.NextPage(testContext(t))
		if err != nil {
			t.Fatalf("NextPage() error: %v", err)
		}
		got = append(got, page.Sessions...)
	}
	if diff := cmp.Diff([]string{"a", "b", "c"}, got); diff != "" {
		t.Errorf("sessions mismatch (-want +got):\n%s", diff)
	}
}

func TestSessionSubscription(t *testing.T) {
	t.Parallel()
	sub := broker.DefaultProperties(broker.KindSubscription)
	sub.RequiresSession = true
	srv := startServer(t, server.Options{Topics: []server.Topic{{
		Name:          "events",
		Subscriptions: []server.Subscription{{Name: "ordered", Properties: sub}},
	}}})
	client := newClient(t, srv)
	send(t, newSender(t, client, "events"), sessionMessages("tenant-1", "e1", "e2")...)

	r, err := client.AcceptSessionForSubscription(testContext(t), "events", "ordered", "tenant-1", nil)
	if err != nil {
		t.Fatalf("AcceptSessionForSubscription() error: %v", err)
	}
	defer r.Close(context.Background())
	if diff := cmp.Diff([]string{"e1", "e2"}, bodiesOf(receiveSession(t, r, 2))); diff != "" {
		t.Errorf("bodies mismatch (-want +got):\n%s", diff)
	}
}

func TestSessionPeekIsScoped(t *testing.T) {
	t.Parallel()
	srv := startServer(t, server.Options{Queues: []server.Queue{sessionQueue("orders", time.Minute)}})
	client := newClient(t, srv)
	sender := newSender(t, client, "orders")
	for i := range 3 {
		send(t, sender, sessionMessages(fmt.Sprintf("s%d", i%2), fmt.Sprintf("m%d", i))...)
	}
	r := acceptSession(t, client, "orders", "s0")
	peeked, err := r.PeekMessages(testContext(t), 10, nil)
	if err != nil {
		t.Fatalf("PeekMessages() error: %v", err)
	}
	if diff := cmp.Diff([]string{"m0", "m2"}, bodiesOf(peeked)); diff != "" {
		t.Errorf("peeked bodies mismatch (-want +got):\n%s", diff)
	}
}

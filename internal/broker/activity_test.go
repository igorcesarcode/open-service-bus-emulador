package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

func TestEmptyReceiveAndPeekDoNotWrite(t *testing.T) {
	b, _ := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "q", nil)
	e, _ := b.Entity("q")
	addr := Address{Entity: e}
	writes := b.store.Get().Writes
	// The first receive may write; the later empty ones must not.
	if _, err := b.Receive(ctx, addr, 1, PeekLock); err != nil {
		t.Fatal(err)
	}
	// Let the timer loop settle: it writes on its own.
	time.Sleep(300 * time.Millisecond)
	before := writes()
	for range 5 {
		if ds, err := b.Receive(ctx, addr, 1, PeekLock); err != nil || len(ds) != 0 {
			t.Fatalf("Receive() = %d messages, %v; want none", len(ds), err)
		}
		if _, err := b.Peek(ctx, addr, 0, 1, ""); err != nil {
			t.Fatal(err)
		}
	}
	if got := writes() - before; got != 0 {
		t.Errorf("empty receives and peeks made %d writes, want 0", got)
	}
	// A message sent afterwards is still found.
	if _, err := b.Send(ctx, "q", []IncomingMessage{testMsg{body: "x", id: "1"}.incoming(t)}); err != nil {
		t.Fatal(err)
	}
	if ds, err := b.Receive(ctx, addr, 1, PeekLock); err != nil || len(ds) != 1 {
		t.Fatalf("Receive() after send = %d messages, %v; want 1", len(ds), err)
	}
}

func TestReceiveAndPeekKeepEntityFromIdleDelete(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "q", func(p *Properties) { p.AutoDeleteOnIdle = time.Hour })
	e, _ := b.Entity("q")
	addr := Address{Entity: e}
	clock.Advance(50 * time.Minute)
	if _, err := b.Receive(ctx, addr, 1, PeekLock); err != nil {
		t.Fatal(err)
	}
	clock.Advance(50 * time.Minute)
	if _, err := b.Peek(ctx, addr, 0, 1, ""); err != nil {
		t.Fatal(err)
	}
	clock.Advance(50 * time.Minute)
	if err := b.deleteIfIdle(ctx, e, b.nowMillis()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Entity("q"); err != nil {
		t.Errorf("queue used by receive and peek was deleted as idle: %v", err)
	}
	counts, err := b.Counts(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if want := b.nowMillis() - 50*time.Minute.Milliseconds(); counts.AccessedAt.UnixMilli() != want {
		t.Errorf("AccessedAt = %v, want the last peek", counts.AccessedAt)
	}
	clock.Advance(2 * time.Hour)
	if err := b.deleteIfIdle(ctx, e, b.nowMillis()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Entity("q"); err == nil {
		t.Error("an idle queue was not deleted")
	}
}

func TestTimerClearsLapsedLocksAndMessageIsRedelivered(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "q", func(p *Properties) { p.LockDuration = 30 * time.Second })
	e, _ := b.Entity("q")
	addr := Address{Entity: e}
	for i := range 3 {
		if _, err := b.Send(ctx, "q", []IncomingMessage{testMsg{body: "x", id: string(rune('a' + i))}.incoming(t)}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := b.Receive(ctx, addr, 3, PeekLock)
	if err != nil || len(first) != 3 {
		t.Fatalf("Receive() = %d, %v; want 3", len(first), err)
	}
	last := b.nowMillis()
	clock.Advance(time.Minute)
	if _, err := b.processTimers(ctx, last, b.nowMillis()); err != nil {
		t.Fatal(err)
	}
	var lapsed bool
	if err := b.store.Read(ctx, func(tx *store.Tx) error {
		var err error
		lapsed, err = tx.HasLapsedLocks(b.nowMillis())
		return err
	}); err != nil || lapsed {
		t.Fatalf("lapsed locks after the timer = %v, %v; want none", lapsed, err)
	}
	if err := b.Complete(ctx, addr, first[0].LockToken); !errors.Is(err, ErrLockLost) {
		t.Errorf("Complete(lapsed token) = %v, want ErrLockLost", err)
	}
	again, err := b.Receive(ctx, addr, 3, PeekLock)
	if err != nil || len(again) != 3 {
		t.Fatalf("Receive() after the lapse = %d, %v; want 3", len(again), err)
	}
	for i, d := range again {
		if d.SequenceNumber != first[i].SequenceNumber || d.DeliveryCount != 2 {
			t.Errorf("redelivery %d: seq %d count %d, want seq %d count 2", i, d.SequenceNumber, d.DeliveryCount, first[i].SequenceNumber)
		}
	}
}

func TestWakeForLocksOnlyWhenSooner(t *testing.T) {
	// A bare Broker, so that no timer loop takes the pokes.
	b := &Broker{poke: make(chan struct{}, 1), now: time.Now}
	now := b.nowMillis()
	at := func(ms int64) []*Delivery { return []*Delivery{{LockedUntil: time.UnixMilli(ms)}} }
	drain := func() bool {
		select {
		case <-b.poke:
			return true
		default:
			return false
		}
	}
	drain()
	for _, tc := range []struct {
		name  string
		next  int64
		locks []*Delivery
		want  bool
	}{
		{"no deliveries", 0, nil, false},
		{"loop has no deadline", 0, at(now + 1000), true},
		{"lock after the deadline", now + 500, at(now + 1000), false},
		{"lock before the deadline", now + 5000, at(now + 1000), true},
		{"deadline passed", now - 1, at(now + 1000), true},
	} {
		b.nextWake.Store(tc.next)
		b.wakeForLocks(tc.locks)
		if got := drain(); got != tc.want {
			t.Errorf("%s: woke = %v, want %v", tc.name, got, tc.want)
		}
	}
}

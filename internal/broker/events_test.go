package broker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/amqp/encoding"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/eventlog"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

type syncBuffer struct {
	mu sync.Mutex
	sb strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.Write(p)
}

func (b *syncBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Split(strings.TrimSpace(b.sb.String()), "\n")
}

func TestEventsFollowAMessage(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, store.Memory)
	if err != nil {
		t.Fatal(err)
	}
	out := &syncBuffer{}
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b, err := New(ctx, st, Options{Now: clock.Now, Events: eventlog.New(out)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close(); st.Close() })

	newTopic(t, b, "events", map[string][]Rule{"big": {sqlRule("big", "quantity > 10", "")}})
	send(t, b, "events",
		testMsg{body: "TOP-SECRET-BODY", id: "m1", props: encoding.Map{{Key: "quantity", Value: int32(50)}}},
		testMsg{body: "TOP-SECRET-SMALL", id: "m2", props: encoding.Map{{Key: "quantity", Value: int32(1)}}},
	)
	addr, err := b.Resolve("events/Subscriptions/big")
	if err != nil {
		t.Fatal(err)
	}
	ds, err := b.Receive(ctx, addr, 1, PeekLock)
	if err != nil || len(ds) != 1 {
		t.Fatalf("Receive() = %d, %v", len(ds), err)
	}
	if err := b.Abandon(ctx, addr, ds[0].LockToken, nil); err != nil {
		t.Fatal(err)
	}
	ds, err = b.Receive(ctx, addr, 1, PeekLock)
	if err != nil || len(ds) != 1 {
		t.Fatalf("second Receive() = %d, %v", len(ds), err)
	}
	if err := b.DeadLetter(ctx, addr, ds[0].LockToken, "Poison", "bad", nil); err != nil {
		t.Fatal(err)
	}

	got := strings.Join(out.lines(), "\n")
	for _, want := range []string{
		"send ", "events ", "id=m1",
		"filter ", "events/Subscriptions/big", "rule=big", "matched=true", `filter="quantity > 10"`, "matched=false",
		"enqueue ", "receive ", "delivery=2", "mode=peek-lock", "abandon ", "dead-letter ", "reason=Poison",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("events lack %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "TOP-SECRET") {
		t.Errorf("events contain the body:\n%s", got)
	}
}

func TestEventsOffByDefault(t *testing.T) {
	b, _ := newTestBroker(t)
	if b.eventsOn() {
		t.Fatal("events are on without Options.Events")
	}
	newQueue(t, b, "q", nil)
	send(t, b, "q", testMsg{body: "x"})
}

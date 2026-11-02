package broker

import (
	"context"
	"testing"
	"time"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/metrics"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/store"
)

// sum adds up a series of points.
func sum(points []metrics.Point) int64 {
	var n int64
	for _, p := range points {
		n += p.Value
	}
	return n
}

func lastValue(points []metrics.Point) int64 { return points[len(points)-1].Value }

func readMetrics(t *testing.T, b *Broker, clock *fakeClock, entity string) map[metrics.Name][]metrics.Point {
	t.Helper()
	now := clock.Now()
	m, err := b.Metrics(context.Background(), entity, now.Add(-time.Hour), now, time.Minute)
	if err != nil {
		t.Fatalf("Metrics(%q) error: %v", entity, err)
	}
	return m
}

func TestMetricsCountMessagesAndRequests(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "q", nil)
	send(t, b, "q", testMsg{body: "a"}, testMsg{body: "b"}, testMsg{body: "c"})
	if _, err := b.Send(ctx, "missing", []IncomingMessage{testMsg{body: "x"}.incoming(t)}); err == nil {
		t.Fatal("Send(missing) succeeded")
	}
	addr, _ := b.Resolve("q")
	ds, err := b.Receive(ctx, addr, 2, PeekLock)
	if err != nil || len(ds) != 2 {
		t.Fatalf("Receive() = %d, %v; want 2", len(ds), err)
	}
	if err := b.Complete(ctx, addr, ds[0].LockToken); err != nil {
		t.Fatal(err)
	}
	// Completing again fails: the lock is gone.
	if err := b.Complete(ctx, addr, ds[0].LockToken); err == nil {
		t.Fatal("second Complete() succeeded")
	}

	for _, entity := range []string{"q", metrics.Namespace} {
		m := readMetrics(t, b, clock, entity)
		for name, want := range map[metrics.Name]int64{
			metrics.IncomingMessages: 3,
			metrics.OutgoingMessages: 2,
		} {
			if got := sum(m[name]); got != want {
				t.Errorf("%q %s = %d, want %d", entity, name, got, want)
			}
		}
		if got := lastValue(m[metrics.ActiveMessages]); got != 2 {
			t.Errorf("%q ActiveMessages = %d, want 2", entity, got)
		}
		if got := lastValue(m[metrics.Size]); got <= 0 {
			t.Errorf("%q Size = %d, want > 0", entity, got)
		}
	}
	// The queue saw send, receive, complete, complete(fail): 4 requests, one
	// user error. The send to the missing entity only counts for the
	// namespace.
	q, ns := readMetrics(t, b, clock, "q"), readMetrics(t, b, clock, "")
	for _, c := range []struct {
		m    map[metrics.Name][]metrics.Point
		name metrics.Name
		want int64
	}{
		{q, metrics.IncomingRequests, 4}, {q, metrics.SuccessfulRequests, 3}, {q, metrics.UserErrors, 1},
		{ns, metrics.IncomingRequests, 5}, {ns, metrics.UserErrors, 2}, {ns, metrics.ServerErrors, 0},
	} {
		if got := sum(c.m[c.name]); got != c.want {
			t.Errorf("%s = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestMetricsFlushAndRetention(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	newQueue(t, b, "q", nil)
	send(t, b, "q", testMsg{body: "a"})
	clock.Advance(2 * time.Minute)
	if err := b.flushMetrics(ctx, clock.Now(), false); err != nil {
		t.Fatal(err)
	}
	// Written minutes are read back from the store, merged with the live one.
	send(t, b, "q", testMsg{body: "b"})
	m := readMetrics(t, b, clock, "q")
	if got := sum(m[metrics.IncomingMessages]); got != 2 {
		t.Errorf("IncomingMessages after the flush = %d, want 2", got)
	}
	if pending := b.rec.Pending("q"); len(pending) == 0 {
		t.Error("the open minute was flushed")
	}

	// Closing writes the open minute too, and a second flush adds to it.
	if err := b.flushMetrics(ctx, clock.Now(), true); err != nil {
		t.Fatal(err)
	}
	send(t, b, "q", testMsg{body: "c"})
	if err := b.flushMetrics(ctx, clock.Now(), true); err != nil {
		t.Fatal(err)
	}
	if got := sum(readMetrics(t, b, clock, "q")[metrics.IncomingMessages]); got != 3 {
		t.Errorf("IncomingMessages after two final flushes = %d, want 3", got)
	}

	// Steps aggregate whole minutes.
	now := clock.Now()
	m5, err := b.Metrics(ctx, "q", now.Add(-10*time.Minute), now, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := sum(m5[metrics.IncomingMessages]); got != 3 {
		t.Errorf("5m steps total = %d, want 3", got)
	}

	// Past the retention, flushing forgets the buckets.
	clock.Advance(DefaultMetricsRetention + time.Hour)
	if err := b.flushMetrics(ctx, clock.Now(), false); err != nil {
		t.Fatal(err)
	}
	old, err := b.Metrics(ctx, "q", clock.Now().Add(-DefaultMetricsRetention-3*time.Hour), clock.Now().Add(-DefaultMetricsRetention), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if got := sum(old[metrics.IncomingMessages]); got != 0 {
		t.Errorf("IncomingMessages past retention = %d, want 0", got)
	}
}

func TestMetricsSubscriptionsCountForTheirTopic(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := context.Background()
	newTopic(t, b, "t", map[string][]Rule{"a": nil, "b": nil})
	send(t, b, "t", testMsg{body: "x"})
	addr, _ := b.Resolve("t/subscriptions/a")
	ds, err := b.Receive(ctx, addr, 1, PeekLock)
	if err != nil || len(ds) != 1 {
		t.Fatalf("Receive() = %d, %v; want 1", len(ds), err)
	}
	if err := b.Complete(ctx, addr, ds[0].LockToken); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		entity string
		name   metrics.Name
		want   int64
	}{
		{"t", metrics.IncomingMessages, 1},
		{"t/subscriptions/a", metrics.IncomingMessages, 0},
		{"t/subscriptions/a", metrics.OutgoingMessages, 1},
		{"t", metrics.OutgoingMessages, 1},
		{"", metrics.OutgoingMessages, 1},
	} {
		if got := sum(readMetrics(t, b, clock, c.entity)[c.name]); got != c.want {
			t.Errorf("%q %s = %d, want %d", c.entity, c.name, got, c.want)
		}
	}
	// One copy waits in subscription b; the topic adds up its subscriptions.
	for entity, want := range map[string]int64{"t/subscriptions/a": 0, "t/subscriptions/b": 1, "t": 1, "": 1} {
		if got := lastValue(readMetrics(t, b, clock, entity)[metrics.ActiveMessages]); got != want {
			t.Errorf("%q ActiveMessages = %d, want %d", entity, got, want)
		}
	}
}

func TestMetricsConnections(t *testing.T) {
	b, clock := newTestBroker(t)
	b.ConnectionOpened()
	b.ConnectionOpened()
	b.ConnectionClosed()
	m := readMetrics(t, b, clock, "")
	if got := sum(m[metrics.ConnectionsOpened]); got != 2 {
		t.Errorf("ConnectionsOpened = %d, want 2", got)
	}
	if got := sum(m[metrics.ConnectionsClosed]); got != 1 {
		t.Errorf("ConnectionsClosed = %d, want 1", got)
	}
	if got := lastValue(m[metrics.ActiveConnections]); got != 1 {
		t.Errorf("ActiveConnections = %d, want 1", got)
	}
}

func TestMetricTotals(t *testing.T) {
	b, _ := newTestBroker(t)
	newQueue(t, b, "q", nil)
	send(t, b, "q", testMsg{body: "a"})
	counters, gauges, err := b.MetricTotals(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	find := func(list []metrics.Total, entity string, name metrics.Name) int64 {
		for _, x := range list {
			if x.Entity == entity && x.Name == name {
				return x.Value
			}
		}
		return -1
	}
	if got := find(counters, "q", metrics.IncomingMessages); got != 1 {
		t.Errorf("IncomingMessages total = %d, want 1", got)
	}
	if got := find(gauges, "q", metrics.ActiveMessages); got != 1 {
		t.Errorf("ActiveMessages gauge = %d, want 1", got)
	}
}

func TestActivityLog(t *testing.T) {
	b, clock := newTestBroker(t)
	ctx := WithActor(context.Background(), "tester")
	if _, err := b.CreateQueue(ctx, "Orders", DefaultProperties(KindQueue)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.CreateQueue(ctx, "Orders", DefaultProperties(KindQueue)); err == nil {
		t.Fatal("duplicate CreateQueue succeeded")
	}
	newTopic(t, b, "t", map[string][]Rule{"s": nil})
	if err := b.AddRule(ctx, "t/subscriptions/s", TrueRule("r1")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.UpdateEntity(ctx, "orders", DefaultProperties(KindQueue)); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteEntity(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteEntity(ctx, "orders"); err == nil {
		t.Fatal("second DeleteEntity succeeded")
	}

	entries, err := b.Activity(context.Background(), ActivityQuery{Actor: "tester"})
	if err != nil {
		t.Fatal(err)
	}
	type row struct{ op, entity, status string }
	var got []row
	for _, e := range entries {
		got = append(got, row{e.Operation, e.Entity, e.Status})
		if e.Detail == "" && e.Status == StatusFailed {
			t.Errorf("failed entry %v has no detail", e)
		}
	}
	want := []row{ // newest first
		{"DeleteEntity", "orders", StatusFailed},
		{"DeleteQueue", "orders", StatusSucceeded},
		{"UpdateQueue", "orders", StatusSucceeded},
		{"CreateRule", "t/subscriptions/s/rules/r1", StatusSucceeded},
		{"CreateQueue", "orders", StatusFailed},
		{"CreateQueue", "orders", StatusSucceeded},
	}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d = %v, want %v", i, got[i], want[i])
		}
	}

	// Filters: by entity prefix, status and the default actor.
	under, _ := b.Activity(context.Background(), ActivityQuery{Entity: "t"})
	if len(under) < 3 { // topic, subscription and the rule
		t.Errorf("entries under t = %d, want at least 3", len(under))
	}
	failed, _ := b.Activity(context.Background(), ActivityQuery{Status: StatusFailed})
	if len(failed) != 2 {
		t.Errorf("failed entries = %d, want 2", len(failed))
	}
	if api, _ := b.Activity(context.Background(), ActivityQuery{Actor: DefaultActor}); len(api) == 0 {
		t.Error("entries without an actor are not logged as " + DefaultActor)
	}
	future, _ := b.Activity(context.Background(), ActivityQuery{From: clock.Now().Add(time.Hour)})
	if len(future) != 0 {
		t.Errorf("entries from the future = %d, want 0", len(future))
	}
}

func TestActivityLogRecordsIdleDeletion(t *testing.T) {
	b, clock := newTestBroker(t)
	newQueue(t, b, "tmp", func(p *Properties) { p.AutoDeleteOnIdle = time.Minute })
	clock.Advance(2 * time.Minute)
	if _, err := b.sweepIdle(context.Background(), clock.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	entries, err := b.Activity(context.Background(), ActivityQuery{Actor: "system"})
	if err != nil || len(entries) != 1 || entries[0].Operation != "DeleteQueue" || entries[0].Entity != "tmp" {
		t.Errorf("Activity(system) = %v, %v; want the deletion of tmp", entries, err)
	}
}

func TestMetricsSurviveRestart(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/osb.db"
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC)}
	open := func() (*Broker, *store.Store) {
		st, err := store.Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		b, err := New(ctx, st, Options{Now: clock.Now})
		if err != nil {
			t.Fatal(err)
		}
		return b, st
	}
	b, st := open()
	newQueue(t, b, "q", nil)
	send(t, b, "q", testMsg{body: "a"}, testMsg{body: "b"})
	if err := b.Close(); err != nil { // writes the open minute
		t.Fatal(err)
	}
	st.Close()

	// The emulator restarts within the same minute and counts on.
	b, st = open()
	defer func() { b.Close(); st.Close() }()
	send(t, b, "q", testMsg{body: "c"})
	if got := sum(readMetrics(t, b, clock, "q")[metrics.IncomingMessages]); got != 3 {
		t.Errorf("IncomingMessages after the restart = %d, want 3", got)
	}
	entries, err := b.Activity(ctx, ActivityQuery{Operation: "CreateQueue"})
	if err != nil || len(entries) != 1 {
		t.Errorf("Activity() = %v, %v; want the creation kept", entries, err)
	}
}

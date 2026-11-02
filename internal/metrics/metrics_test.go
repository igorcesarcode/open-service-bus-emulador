package metrics

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 10, 0, 30, 0, time.UTC)

func TestRecorderAddsToNamespaceOnce(t *testing.T) {
	r := NewRecorder()
	r.Add(t0, IncomingMessages, 3, "orders")
	r.Add(t0, IncomingMessages, 2, "events", "events/subscriptions/a")
	r.Add(t0, IncomingMessages, 1)
	got := map[string]int64{}
	for _, s := range r.Pending(Namespace) {
		got[s.Entity+"/"+string(s.Name)] += s.Value
	}
	if got["/IncomingMessages"] != 6 {
		t.Errorf("namespace = %d, want 6 (no double counting of subscription and topic)", got["/IncomingMessages"])
	}
	if p := r.Pending("events/subscriptions/a"); len(p) != 1 || p[0].Value != 2 {
		t.Errorf("subscription pending = %v", p)
	}
}

func TestDrainKeepsOpenMinute(t *testing.T) {
	r := NewRecorder()
	r.Add(t0, OutgoingMessages, 1, "q")
	r.Add(t0.Add(time.Minute), OutgoingMessages, 4, "q")
	closed := r.Drain(t0.Add(time.Minute), false)
	if len(closed) != 2 { // q and the namespace
		t.Fatalf("closed = %v, want the minute of t0 only", closed)
	}
	for _, s := range closed {
		if s.Minute != Minute(t0) || s.Value != 1 {
			t.Errorf("sample %v", s)
		}
	}
	if rest := r.Drain(t0, true); len(rest) != 2 {
		t.Errorf("rest = %v, want the open minute", rest)
	}
	if rest := r.Drain(t0, true); len(rest) != 0 {
		t.Errorf("drained twice: %v", rest)
	}
	// Totals are cumulative and survive draining.
	for _, tot := range r.Totals() {
		if tot.Entity == "q" && tot.Value != 5 {
			t.Errorf("total = %d, want 5", tot.Value)
		}
	}
}

func TestAggregateCounter(t *testing.T) {
	base := Minute(time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC))
	samples := []Sample{
		{"q", base, IncomingMessages, 1},
		{"q", base + 1, IncomingMessages, 2},
		{"q", base + 5, IncomingMessages, 4},
		{"q", base + 5, OutgoingMessages, 99},
	}
	from := MinuteTime(base)
	to := MinuteTime(base + 9)
	got := Aggregate(samples, IncomingMessages, from, to, 5*time.Minute)
	if len(got) != 2 || got[0].Value != 3 || got[1].Value != 4 {
		t.Errorf("5m = %v, want [3 4]", got)
	}
	got = Aggregate(samples, IncomingMessages, from, to, time.Minute)
	if len(got) != 10 || got[1].Value != 2 || got[3].Value != 0 {
		t.Errorf("1m = %v", got)
	}
	if got := Aggregate(samples, IncomingMessages, to, from, time.Minute); got != nil {
		t.Errorf("reversed range = %v, want nil", got)
	}
}

func TestAggregateGaugeCarriesForward(t *testing.T) {
	base := Minute(time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC))
	samples := []Sample{
		{"q", base, ActiveMessages, 5},
		{"q", base + 2, ActiveMessages, 8},
		{"q", base + 2, ActiveMessages, 7}, // later in the slice wins
		{"q", base + 1, ActiveMessages, 6},
	}
	got := Aggregate(samples, ActiveMessages, MinuteTime(base), MinuteTime(base+4), time.Minute)
	want := []int64{5, 6, 7, 7, 7}
	for i, w := range want {
		if got[i].Value != w {
			t.Errorf("point %d = %d, want %d (%v)", i, got[i].Value, w, got)
		}
	}
	got = Aggregate(samples, ActiveMessages, MinuteTime(base), MinuteTime(base+4), 5*time.Minute)
	if len(got) != 1 || got[0].Value != 7 {
		t.Errorf("one interval = %v, want the latest minute", got)
	}
}

func TestWritePrometheus(t *testing.T) {
	var buf bytes.Buffer
	err := WritePrometheus(&buf,
		[]Total{{Namespace, IncomingMessages, 7}, {"orders", IncomingMessages, 7}},
		[]Total{{"orders", ActiveMessages, 3}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# TYPE servicebus_incoming_messages_total counter\n",
		"servicebus_incoming_messages_total 7\n",
		`servicebus_incoming_messages_total{entity="orders"} 7` + "\n",
		"# TYPE servicebus_active_messages gauge\n",
		`servicebus_active_messages{entity="orders"} 3` + "\n",
	} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("missing %q in\n%s", want, buf.String())
		}
	}
}

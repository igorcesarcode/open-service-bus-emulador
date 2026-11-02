// Package metrics records the numbers the Azure portal shows for a
// namespace and its entities: requests, messages, sizes and connections.
//
// Counters accumulate in memory in one-minute buckets, which the broker
// writes to the database as the minutes close. Gauges, such as the number of
// active messages, are sampled by the broker. This package holds neither the
// clock nor the storage, so it needs neither to be tested.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Name identifies a metric. The values follow the metric names of Azure
// Monitor for Service Bus.
type Name string

// Counters add up over time.
const (
	IncomingRequests   Name = "IncomingRequests"
	SuccessfulRequests Name = "SuccessfulRequests"
	ServerErrors       Name = "ServerErrors"
	UserErrors         Name = "UserErrors"
	ThrottledRequests  Name = "ThrottledRequests"
	IncomingMessages   Name = "IncomingMessages"
	OutgoingMessages   Name = "OutgoingMessages"
	ConnectionsOpened  Name = "ConnectionsOpened"
	ConnectionsClosed  Name = "ConnectionsClosed"
)

// Gauges are values at an instant.
const (
	ActiveMessages               Name = "ActiveMessages"
	DeadletteredMessages         Name = "DeadletteredMessages"
	ScheduledMessages            Name = "ScheduledMessages"
	TransferDeadletteredMessages Name = "TransferDeadletteredMessages"
	Size                         Name = "Size"
	ActiveConnections            Name = "ActiveConnections"
)

// Counters and Gauges list every metric of each kind.
var (
	Counters = []Name{IncomingRequests, SuccessfulRequests, ServerErrors, UserErrors, ThrottledRequests,
		IncomingMessages, OutgoingMessages, ConnectionsOpened, ConnectionsClosed}
	Gauges = []Name{ActiveMessages, DeadletteredMessages, ScheduledMessages, TransferDeadletteredMessages,
		Size, ActiveConnections}
)

// Gauge reports whether n is sampled instead of added up.
func (n Name) Gauge() bool {
	for _, g := range Gauges {
		if n == g {
			return true
		}
	}
	return false
}

// Namespace is the entity of the metrics of the whole namespace.
const Namespace = ""

// Minute returns the bucket of t.
func Minute(t time.Time) int64 { return t.Unix() / 60 }

// MinuteTime is the start of a bucket.
func MinuteTime(m int64) time.Time { return time.Unix(m*60, 0).UTC() }

// Sample is the value of one metric of one entity in one minute.
type Sample struct {
	Entity string
	Minute int64
	Name   Name
	Value  int64
}

type key struct {
	entity string
	minute int64
	name   Name
}

type total struct {
	entity string
	name   Name
}

// Recorder accumulates counters. It is safe for concurrent use; recording
// takes a lock held only for a map update.
type Recorder struct {
	mu      sync.Mutex
	buckets map[key]int64
	totals  map[total]int64
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{buckets: make(map[key]int64), totals: make(map[total]int64)}
}

// Add adds n to the counter name of each entity, and once to the namespace.
func (r *Recorder) Add(at time.Time, name Name, n int64, entities ...string) {
	if n == 0 {
		return
	}
	m := Minute(at)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.add(m, name, n, Namespace)
	for _, e := range entities {
		if e != Namespace {
			r.add(m, name, n, e)
		}
	}
}

func (r *Recorder) add(minute int64, name Name, n int64, entity string) {
	r.buckets[key{entity, minute, name}] += n
	r.totals[total{entity, name}] += n
}

// Drain removes and returns the buckets of minutes before the minute of
// at, which are complete. With all, it returns the open ones too.
func (r *Recorder) Drain(at time.Time, all bool) []Sample {
	cur := Minute(at)
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Sample
	for k, v := range r.buckets {
		if all || k.minute < cur {
			out = append(out, Sample{k.entity, k.minute, k.name, v})
			delete(r.buckets, k)
		}
	}
	return out
}

// Pending returns the buckets not drained yet for entity.
func (r *Recorder) Pending(entity string) []Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Sample
	for k, v := range r.buckets {
		if k.entity == entity {
			out = append(out, Sample{k.entity, k.minute, k.name, v})
		}
	}
	return out
}

// Total is the sum of a counter since the Recorder started.
type Total struct {
	Entity string
	Name   Name
	Value  int64
}

// Totals returns every counter since the Recorder started, sorted by
// entity and name.
func (r *Recorder) Totals() []Total {
	r.mu.Lock()
	out := make([]Total, 0, len(r.totals))
	for k, v := range r.totals {
		out = append(out, Total{k.entity, k.name, v})
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Entity != out[j].Entity {
			return out[i].Entity < out[j].Entity
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Point is the value of a metric in the interval starting at Time.
type Point struct {
	Time  time.Time
	Value int64
}

// Aggregate groups samples of the metric name into intervals of step,
// covering from to to. A counter adds up the minutes in an interval and is
// zero where there are none; a gauge takes the latest minute in the
// interval, and keeps its previous value where there is none. Where two
// samples share a minute, the later one in the slice wins for gauges.
func Aggregate(samples []Sample, name Name, from, to time.Time, step time.Duration) []Point {
	step = max(step, time.Minute)
	stepSec := int64(step / time.Second)
	first := from.Unix() / stepSec * stepSec
	last := to.Unix() / stepSec * stepSec
	if last < first {
		return nil
	}
	n := (last-first)/stepSec + 1
	points := make([]Point, n)
	latest := make([]int64, n) // minute of the gauge value held in each point
	for i := range points {
		points[i].Time = time.Unix(first+int64(i)*stepSec, 0).UTC()
		latest[i] = -1
	}
	for _, s := range samples {
		if s.Name != name {
			continue
		}
		i := (s.Minute*60 - first) / stepSec
		if s.Minute*60 < first || i >= n {
			continue
		}
		if !name.Gauge() {
			points[i].Value += s.Value
		} else if s.Minute >= latest[i] {
			points[i].Value, latest[i] = s.Value, s.Minute
		}
	}
	if name.Gauge() {
		var prev int64
		for i := range points {
			if latest[i] < 0 {
				points[i].Value = prev
			}
			prev = points[i].Value
		}
	}
	return points
}

// prometheusName converts a metric name such as IncomingMessages into
// servicebus_incoming_messages.
func prometheusName(n Name) string {
	var b strings.Builder
	b.WriteString("servicebus_")
	for i, r := range string(n) {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			r += 'a' - 'A'
		}
		b.WriteRune(r)
	}
	return b.String()
}

// WritePrometheus writes counters and gauges in the Prometheus text
// format. Metrics of the namespace carry no entity label.
func WritePrometheus(w io.Writer, counters []Total, gauges []Total) error {
	write := func(kind string, list []Total) error {
		byName := map[Name][]Total{}
		for _, t := range list {
			byName[t.Name] = append(byName[t.Name], t)
		}
		names := make([]Name, 0, len(byName))
		for n := range byName {
			names = append(names, n)
		}
		sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
		for _, n := range names {
			pn := prometheusName(n)
			if kind == "counter" {
				pn += "_total"
			}
			if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", pn, kind); err != nil {
				return err
			}
			for _, t := range byName[n] {
				label := ""
				if t.Entity != Namespace {
					label = fmt.Sprintf("{entity=%q}", t.Entity)
				}
				if _, err := fmt.Fprintf(w, "%s%s %d\n", pn, label, t.Value); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := write("counter", counters); err != nil {
		return err
	}
	return write("gauge", gauges)
}

// Restore puts drained samples back, for a flush that failed.
func (r *Recorder) Restore(samples []Sample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range samples {
		r.buckets[key{s.Entity, s.Minute, s.Name}] += s.Value
	}
}

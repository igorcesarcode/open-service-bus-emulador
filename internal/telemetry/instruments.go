package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// The instruments are created from the global meter, which hands out
// delegating instruments: they record nothing until Setup installs a real
// provider, and start recording as soon as it does.
var (
	meter = otel.Meter(scope)

	MessagesSent         = counter("osb.messages.sent", "{message}", "Messages accepted by a send")
	MessagesReceived     = counter("osb.messages.received", "{message}", "Messages handed to a receiver")
	MessagesCompleted    = counter("osb.messages.completed", "{message}", "Messages settled as completed")
	MessagesAbandoned    = counter("osb.messages.abandoned", "{message}", "Messages abandoned back to the queue")
	MessagesDeferred     = counter("osb.messages.deferred", "{message}", "Messages deferred")
	MessagesDeadLettered = counter("osb.messages.dead_lettered", "{message}", "Messages moved to a dead-letter queue")
	MessagesExpired      = counter("osb.messages.expired", "{message}", "Messages whose time to live ran out")
	MessagesScheduled    = counter("osb.messages.scheduled", "{message}", "Messages sent for a later time")
	LocksExpired         = counter("osb.locks.expired", "{lock}", "Message locks that ran out before settlement")
	DuplicatesDropped    = counter("osb.duplicates.dropped", "{message}", "Messages dropped by duplicate detection")
	Requests             = counter("osb.requests", "{request}", "Broker requests by outcome: ok, user_error, throttled, server_error")
	StoreStatements      = counter("osb.store.statements", "{statement}", "SQL statements written")
	HTTPRequests         = counter("osb.http.requests", "{request}", "HTTP requests served")
	MCPToolCalls         = counter("osb.mcp.tool_calls", "{call}", "MCP tool calls")
	AMQPFrames           = counter("osb.amqp.frames", "{frame}", "AMQP frames by direction and performative")

	OperationDuration   = histogram("osb.operation.duration", "s", "Duration of broker and server operations")
	MessageSize         = histogram("osb.message.size", "By", "Size of messages sent")
	DeliveryCount       = histogram("osb.delivery.count", "{delivery}", "Delivery count when a message is settled")
	TimeInQueue         = histogram("osb.message.time_in_queue", "s", "Time from enqueue to the first delivery")
	LockHeld            = histogram("osb.lock.held", "s", "Time a lock is held until settlement")
	StoreCommitDuration = histogram("osb.store.commit.duration", "s", "Duration of a SQLite group commit")
	StoreCommitBatch    = histogram("osb.store.commit.batch_size", "{request}", "Write requests folded into one commit")
	StoreWriteWait      = histogram("osb.store.write.wait", "s", "Time a write waits for its commit")
	HTTPDuration        = histogram("osb.http.duration", "s", "Duration of HTTP requests")

	Connections = updown("osb.connections", "{connection}", "Open AMQP connections")
	Links       = updown("osb.links", "{link}", "Open AMQP links")
)

func counter(name, unit, desc string) metric.Int64Counter {
	c, _ := meter.Int64Counter(name, metric.WithUnit(unit), metric.WithDescription(desc))
	return c
}

func histogram(name, unit, desc string) metric.Float64Histogram {
	h, _ := meter.Float64Histogram(name, metric.WithUnit(unit), metric.WithDescription(desc))
	return h
}

func updown(name, unit, desc string) metric.Int64UpDownCounter {
	c, _ := meter.Int64UpDownCounter(name, metric.WithUnit(unit), metric.WithDescription(desc))
	return c
}

// Add increments c by n with attrs.
func Add(ctx context.Context, c metric.Int64Counter, n int64, attrs ...attribute.KeyValue) {
	if !Enabled() || n == 0 {
		return
	}
	c.Add(ctx, n, metric.WithAttributes(attrs...))
}

// AddUpDown changes g by n.
func AddUpDown(ctx context.Context, g metric.Int64UpDownCounter, n int64, attrs ...attribute.KeyValue) {
	if !Enabled() {
		return
	}
	g.Add(ctx, n, metric.WithAttributes(attrs...))
}

// Record adds value to h with attrs.
func Record(ctx context.Context, h metric.Float64Histogram, value float64, attrs ...attribute.KeyValue) {
	if !Enabled() {
		return
	}
	h.Record(ctx, value, metric.WithAttributes(attrs...))
}

// Seconds records d, in seconds, to h.
func Seconds(ctx context.Context, h metric.Float64Histogram, d time.Duration, attrs ...attribute.KeyValue) {
	Record(ctx, h, d.Seconds(), attrs...)
}

// Gauge registers an observable gauge whose value f computes at each export.
func Gauge(name, unit, desc string, f func(ctx context.Context, observe func(value int64, attrs ...attribute.KeyValue))) {
	g, err := meter.Int64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(desc))
	if err != nil {
		return
	}
	meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		f(ctx, func(value int64, attrs ...attribute.KeyValue) {
			o.ObserveInt64(g, value, metric.WithAttributes(attrs...))
		})
		return nil
	}, g)
}

var noFinish = func(*error) {}

// classifiers name the kind of failure an error is, in order; the first to
// return a non-empty class wins. See Classify.
var classifiers []func(error) string

// AddClassifier registers a function that classifies failures: it returns
// "user_error" for a caller's mistake the emulator reports as it should,
// "canceled" when the caller gave up or went away, "throttled", or "" when it
// does not know the error. Call it from init functions.
func AddClassifier(f func(error) string) { classifiers = append(classifiers, f) }

// Classify returns the class of err: what the registered classifiers say, or
// "server_error". Operations that fail with a server_error are the only ones
// marked as errors in traces.
func Classify(err error) string {
	for _, f := range classifiers {
		if c := f(err); c != "" {
			return c
		}
	}
	return "server_error"
}

// Operation starts a span named name and times it. The returned function
// ends the span, records the duration under the operation attribute, and
// counts and records a failure. Use it with a named result:
//
//	ctx, done := telemetry.Operation(ctx, "broker.send", attrs...)
//	defer done(&err)
//
// Off, it returns ctx and a function that does nothing.
func Operation(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func(*error)) {
	if !Enabled() {
		return ctx, noFinish
	}
	ctx, span := Start(ctx, name, attrs...)
	start := time.Now()
	return ctx, func(err *error) {
		elapsed := time.Since(start)
		outcome := "ok"
		if err != nil && *err != nil {
			outcome = Classify(*err)
		}
		OperationDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(
			attribute.String("operation", name), attribute.String("outcome", outcome)))
		span.SetAttributes(attribute.String("osb.outcome", outcome))
		if outcome == "ok" || outcome == "server_error" {
			End(span, err)
			return
		}
		// A failure the caller caused is an event, not an error status.
		span.AddEvent("failed", trace.WithAttributes(attribute.String("message", (*err).Error()), attribute.String("class", outcome)))
		span.End()
	}
}

// SpanFromContext returns the current span.
func SpanFromContext(ctx context.Context) trace.Span { return trace.SpanFromContext(ctx) }

// Entity is the attribute naming a queue, topic or subscription path.
func Entity(path string) attribute.KeyValue { return attribute.String("osb.entity", path) }

// Count is the attribute giving how many items an operation handled.
func Count(n int) attribute.KeyValue { return attribute.Int("osb.count", n) }

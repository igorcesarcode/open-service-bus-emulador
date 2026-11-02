// Package telemetry exports traces, metrics and logs of the emulator with
// OpenTelemetry (OTLP over HTTP). It is off unless Setup is called with an
// endpoint: until then the global providers are the no-op ones of
// OpenTelemetry, so the instrumentation scattered through the code costs a
// function call and opens no connection.
//
// The instrumentation never records message bodies, keys or property values:
// only names, sizes, counts and outcomes.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// scope is the instrumentation scope name of everything the emulator emits.
const scope = "github.com/igorcesarcode/open-service-bus-emulador"

// Signals selects what to export.
type Signals struct{ Traces, Metrics, Logs bool }

// ParseSignals reads a comma separated list such as "traces,metrics".
func ParseSignals(s string) (Signals, error) {
	var out Signals
	for _, p := range strings.Split(s, ",") {
		switch strings.ToLower(strings.TrimSpace(p)) {
		case "":
		case "traces":
			out.Traces = true
		case "metrics":
			out.Metrics = true
		case "logs":
			out.Logs = true
		default:
			return out, fmt.Errorf("unknown signal %q: want traces, metrics or logs", p)
		}
	}
	return out, nil
}

// Config configures Setup.
type Config struct {
	// Endpoint is the base URL of an OTLP/HTTP receiver, such as
	// http://localhost:4318. Empty leaves telemetry off.
	Endpoint string
	Signals  Signals
	// SampleRatio is the fraction of new traces recorded, 0 to 1.
	SampleRatio    float64
	MetricInterval time.Duration
	ServiceName    string
	Version        string
	// Attributes are added to the resource.
	Attributes []attribute.KeyValue
}

var enabled atomic.Bool

// Enabled reports whether telemetry was set up. Use it to skip building
// attributes that only telemetry needs.
func Enabled() bool { return enabled.Load() }

// Telemetry is a running export.
type Telemetry struct {
	shutdown []func(context.Context) error
	logs     bool
}

// Setup starts the exporters and installs them as the global providers.
// With an empty Endpoint it does nothing and returns a Telemetry whose
// Shutdown is a no-op.
func Setup(ctx context.Context, cfg Config) (*Telemetry, error) {
	t := &Telemetry{}
	if cfg.Endpoint == "" {
		return t, nil
	}
	base := strings.TrimRight(cfg.Endpoint, "/")
	if cfg.ServiceName == "" {
		cfg.ServiceName = "open-service-bus"
	}
	if cfg.MetricInterval <= 0 {
		cfg.MetricInterval = 10 * time.Second
	}
	if cfg.SampleRatio <= 0 || cfg.SampleRatio > 1 {
		cfg.SampleRatio = 1
	}
	host, _ := os.Hostname()
	attrs := append([]attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.Version),
		semconv.ServiceInstanceID(fmt.Sprintf("%s-%d", host, os.Getpid())),
		semconv.HostName(host),
		semconv.ProcessPID(os.Getpid()),
	}, cfg.Attributes...)
	res, err := resource.New(ctx, resource.WithFromEnv(), resource.WithAttributes(attrs...))
	if err != nil {
		return nil, fmt.Errorf("telemetry resource: %w", err)
	}

	fail := func(err error) (*Telemetry, error) {
		t.Shutdown(context.Background())
		return nil, err
	}
	if cfg.Signals.Traces {
		exp, err := newTraceExporter(base + "/v1/traces")
		if err != nil {
			return fail(fmt.Errorf("trace exporter: %w", err))
		}
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp,
				// The defaults (a queue of 2048) drop spans at the rates
				// a busy emulator produces; this is a diagnostic tool, so
				// buffer generously.
				sdktrace.WithMaxQueueSize(65536), sdktrace.WithMaxExportBatchSize(512), sdktrace.WithBatchTimeout(time.Second)),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
		)
		otel.SetTracerProvider(tp)
		t.shutdown = append(t.shutdown, tp.Shutdown)
	}
	if cfg.Signals.Metrics {
		exp, err := newMetricExporter(base + "/v1/metrics")
		if err != nil {
			return fail(fmt.Errorf("metric exporter: %w", err))
		}
		mp := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(cfg.MetricInterval))),
			sdkmetric.WithResource(res),
			sdkmetric.WithView(durationBuckets()),
		)
		otel.SetMeterProvider(mp)
		t.shutdown = append(t.shutdown, mp.Shutdown)
		if err := runtime.Start(runtime.WithMeterProvider(mp)); err != nil {
			return fail(fmt.Errorf("runtime metrics: %w", err))
		}
	}
	if cfg.Signals.Logs {
		exp, err := newLogExporter(base + "/v1/logs")
		if err != nil {
			return fail(fmt.Errorf("log exporter: %w", err))
		}
		lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(exp, sdklog.WithMaxQueueSize(32768), sdklog.WithExportMaxBatchSize(512))), sdklog.WithResource(res))
		global.SetLoggerProvider(lp)
		t.shutdown = append(t.shutdown, lp.Shutdown)
		t.logs = true
	}
	enabled.Store(true)
	return t, nil
}

// Shutdown flushes and stops the exporters.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	enabled.Store(false)
	var errs []error
	for i := len(t.shutdown) - 1; i >= 0; i-- {
		errs = append(errs, t.shutdown[i](ctx))
	}
	t.shutdown = nil
	return errors.Join(errs...)
}

// LogHandler returns a slog handler that also exports each record at or above
// level, when logs are being exported; otherwise it returns next unchanged.
func (t *Telemetry) LogHandler(next slog.Handler, level slog.Leveler) slog.Handler {
	if t == nil || !t.logs {
		return next
	}
	return fanOut{next, leveled{otelslog.NewHandler(scope), level}}
}

// leveled drops the records below level, which the OpenTelemetry handler
// would otherwise export whatever -log-level says.
type leveled struct {
	slog.Handler
	level slog.Leveler
}

func (l leveled) Enabled(ctx context.Context, lv slog.Level) bool {
	return lv >= l.level.Level() && l.Handler.Enabled(ctx, lv)
}

func (l leveled) WithAttrs(a []slog.Attr) slog.Handler {
	return leveled{l.Handler.WithAttrs(a), l.level}
}
func (l leveled) WithGroup(n string) slog.Handler { return leveled{l.Handler.WithGroup(n), l.level} }

// fanOut sends each record to every handler.
type fanOut []slog.Handler

func (f fanOut) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range f {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (f fanOut) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range f {
		if h.Enabled(ctx, r.Level) {
			errs = append(errs, h.Handle(ctx, r.Clone()))
		}
	}
	return errors.Join(errs...)
}

func (f fanOut) WithAttrs(a []slog.Attr) slog.Handler {
	out := make(fanOut, len(f))
	for i, h := range f {
		out[i] = h.WithAttrs(a)
	}
	return out
}

func (f fanOut) WithGroup(n string) slog.Handler {
	out := make(fanOut, len(f))
	for i, h := range f {
		out[i] = h.WithGroup(n)
	}
	return out
}

// Start begins a span named name.
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer(scope).Start(ctx, name, trace.WithAttributes(attrs...))
}

// End finishes span, recording *err when it is not nil. Use it with a named
// result: defer telemetry.End(span, &err).
func End(span trace.Span, err *error) {
	if err != nil && *err != nil {
		span.RecordError(*err)
		span.SetStatus(codes.Error, (*err).Error())
	}
	span.End()
}

// durationBuckets gives the duration histograms boundaries suited to an
// in-process broker: microseconds to seconds.
func durationBuckets() sdkmetric.View {
	return sdkmetric.NewView(
		sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram, Unit: "s"},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}},
	)
}

// StartLinked begins a span that links to other spans instead of having a
// parent: the work of one operation done on behalf of many.
func StartLinked(ctx context.Context, name string, links []trace.Link, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer(scope).Start(ctx, name, trace.WithLinks(links...), trace.WithAttributes(attrs...))
}

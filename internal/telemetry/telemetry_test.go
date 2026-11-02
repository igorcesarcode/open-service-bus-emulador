package telemetry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestParseSignals(t *testing.T) {
	got, err := ParseSignals("traces, Metrics")
	if err != nil || !got.Traces || !got.Metrics || got.Logs {
		t.Errorf("ParseSignals() = %+v, %v; want traces and metrics", got, err)
	}
	if _, err := ParseSignals("traces,spans"); err == nil {
		t.Error("ParseSignals() accepted an unknown signal")
	}
}

func TestOffDoesNothing(t *testing.T) {
	tel, err := Setup(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if Enabled() {
		t.Error("Enabled() is true without an endpoint")
	}
	ctx, done := Operation(context.Background(), "broker.send")
	var opErr error
	done(&opErr)
	if SpanFromContext(ctx).SpanContext().IsValid() {
		t.Error("Operation started a span while telemetry is off")
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() error: %v", err)
	}
}

// recording installs an in-memory tracer and turns telemetry on for the test.
func recording(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	enabled.Store(true)
	old := classifiers
	classifiers = []func(error) string{func(err error) string {
		if errors.Is(err, errUser) {
			return "user_error"
		}
		return ""
	}}
	t.Cleanup(func() {
		enabled.Store(false)
		classifiers = old
		tp.Shutdown(context.Background())
	})
	return rec
}

var errUser = errors.New("the caller was wrong")

func TestOperationSpans(t *testing.T) {
	rec := recording(t)

	_, done := Operation(context.Background(), "broker.send", Entity("orders"), Count(3))
	var err error
	done(&err)

	_, done = Operation(context.Background(), "broker.complete")
	err = errUser
	done(&err)

	_, done = Operation(context.Background(), "broker.receive")
	err = errors.New("disk is gone")
	done(&err)

	spans := rec.Ended()
	if len(spans) != 3 {
		t.Fatalf("%d spans ended, want 3", len(spans))
	}
	send := spans[0]
	if send.Name() != "broker.send" || send.Status().Code != codes.Unset {
		t.Errorf("send span = %q, status %v", send.Name(), send.Status())
	}
	if !slices.Contains(send.Attributes(), Entity("orders")) || !slices.Contains(send.Attributes(), attribute.String("osb.outcome", "ok")) {
		t.Errorf("send attributes = %v", send.Attributes())
	}
	// A mistake of the caller is an event, not an error status.
	if user := spans[1]; user.Status().Code != codes.Unset || len(user.Events()) != 1 || user.Events()[0].Name != "failed" {
		t.Errorf("user error span: status %v, events %v", user.Status(), user.Events())
	}
	if server := spans[2]; server.Status().Code != codes.Error {
		t.Errorf("server error span status = %v, want Error", server.Status())
	}
}

func TestExportsOverHTTP(t *testing.T) {
	var mu sync.Mutex
	paths := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths[r.URL.Path]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tel, err := Setup(context.Background(), Config{
		Endpoint: srv.URL, Signals: Signals{Traces: true, Metrics: true, Logs: true},
		MetricInterval: time.Hour, ServiceName: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { enabled.Store(false) }()
	if !Enabled() {
		t.Fatal("Enabled() is false after Setup with an endpoint")
	}
	_, done := Operation(context.Background(), "broker.send", Entity("orders"))
	var opErr error
	done(&opErr)
	Add(context.Background(), MessagesSent, 2, Entity("orders"))
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, p := range []string{"/v1/traces", "/v1/metrics"} {
		if paths[p] == 0 {
			t.Errorf("nothing was posted to %s; got %v", p, paths)
		}
	}
}

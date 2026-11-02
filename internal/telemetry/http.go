package telemetry

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
)

// HTTP wraps h so that each request is a server span with its route, method
// and status, and counts in the request metrics. The route is the pattern of
// the mux that served it, so paths with ids do not multiply the series.
// Off, it returns h itself.
func HTTP(h http.Handler, component string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !Enabled() {
			h.ServeHTTP(w, r)
			return
		}
		ctx := propagationExtract(r)
		ctx, span := Start(ctx, "http.server", attribute.String("http.request.method", r.Method),
			attribute.String("url.path", r.URL.Path), attribute.String("osb.component", component))
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(sw, r.WithContext(ctx))
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		attrs := []attribute.KeyValue{
			attribute.String("http.route", route), attribute.String("http.request.method", r.Method),
			attribute.Int("http.response.status_code", sw.status), attribute.String("osb.component", component),
		}
		span.SetName("http.server " + r.Method + " " + route)
		span.SetAttributes(attrs...)
		if sw.status >= 500 {
			span.SetStatus(2, "HTTP "+strconv.Itoa(sw.status))
		}
		span.End()
		Add(ctx, HTTPRequests, 1, attrs...)
		Seconds(ctx, HTTPDuration, time.Since(start), attrs...)
	})
}

func propagationExtract(r *http.Request) (ctx context.Context) {
	return propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
}

// statusWriter remembers the status code written.
type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status, w.wrote = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

// Flush lets streaming responses (server-sent events) through.
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack lets connection upgrades through.
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("telemetry: the response writer cannot be hijacked")
}

// Unwrap exposes the wrapped writer to http.ResponseController.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// ExtractTraceparent returns ctx continuing the trace named by a W3C
// traceparent value, such as the Diagnostic-Id the Azure SDKs add to messages.
// An empty or invalid value leaves ctx as it is.
func ExtractTraceparent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

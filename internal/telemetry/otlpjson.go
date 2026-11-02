package telemetry

// A small OTLP/HTTP exporter that speaks the JSON encoding of the protocol.
//
// The official exporters carry the generated protobuf types and gRPC with
// them, which is about 7 MB of the binary for what is a few POST requests.
// The JSON mapping is part of the OTLP specification and every receiver that
// takes OTLP/HTTP accepts it.

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type jsonValue map[string]any
type jsonKV struct {
	Key   string    `json:"key"`
	Value jsonValue `json:"value"`
}

// poster sends JSON documents to one OTLP endpoint.
type poster struct {
	url     string
	headers map[string]string
	client  *http.Client
	closed  atomic.Bool
}

func newPoster(endpoint string) (*poster, error) {
	if _, err := url.Parse(endpoint); err != nil {
		return nil, err
	}
	p := &poster{url: endpoint, headers: map[string]string{}, client: &http.Client{Timeout: 10 * time.Second}}
	// OTEL_EXPORTER_OTLP_HEADERS: k1=v1,k2=v2 (values may be percent-encoded).
	for _, kv := range strings.Split(os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"), ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || strings.TrimSpace(k) == "" {
			continue
		}
		if u, err := url.PathUnescape(strings.TrimSpace(v)); err == nil {
			v = u
		}
		p.headers[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return p, nil
}

var errExporterShutdown = errors.New("exporter is shut down")

// post sends body, retrying transient failures a few times.
func (p *poster) post(ctx context.Context, doc any) error {
	if p.closed.Load() {
		return errExporterShutdown
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	var zipped bytes.Buffer
	zw := gzip.NewWriter(&zipped)
	zw.Write(raw)
	zw.Close()

	var last error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*attempt) * 200 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(zipped.Bytes()))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		for k, v := range p.headers {
			req.Header.Set(k, v)
		}
		resp, err := p.client.Do(req)
		if err != nil {
			last = err
			continue
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		resp.Body.Close()
		switch {
		case resp.StatusCode < 300:
			return nil
		case resp.StatusCode == 429 || resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504:
			last = fmt.Errorf("%s: %s", p.url, resp.Status)
		default:
			return fmt.Errorf("%s: %s", p.url, resp.Status)
		}
	}
	return last
}

func nanos(t time.Time) string {
	if t.IsZero() {
		return "0"
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}

func encodeValue(v attribute.Value) jsonValue {
	arr := func(items ...jsonValue) jsonValue { return jsonValue{"arrayValue": map[string]any{"values": items}} }
	switch v.Type() {
	case attribute.BOOL:
		return jsonValue{"boolValue": v.AsBool()}
	case attribute.INT64:
		return jsonValue{"intValue": strconv.FormatInt(v.AsInt64(), 10)}
	case attribute.FLOAT64:
		return jsonValue{"doubleValue": v.AsFloat64()}
	case attribute.STRING:
		return jsonValue{"stringValue": v.AsString()}
	case attribute.BOOLSLICE:
		var out []jsonValue
		for _, b := range v.AsBoolSlice() {
			out = append(out, jsonValue{"boolValue": b})
		}
		return arr(out...)
	case attribute.INT64SLICE:
		var out []jsonValue
		for _, n := range v.AsInt64Slice() {
			out = append(out, jsonValue{"intValue": strconv.FormatInt(n, 10)})
		}
		return arr(out...)
	case attribute.FLOAT64SLICE:
		var out []jsonValue
		for _, f := range v.AsFloat64Slice() {
			out = append(out, jsonValue{"doubleValue": f})
		}
		return arr(out...)
	case attribute.STRINGSLICE:
		var out []jsonValue
		for _, s := range v.AsStringSlice() {
			out = append(out, jsonValue{"stringValue": s})
		}
		return arr(out...)
	case attribute.SLICE:
		var out []jsonValue
		for _, e := range v.AsSlice() {
			out = append(out, encodeValue(e))
		}
		return arr(out...)
	case attribute.MAP:
		return jsonValue{"kvlistValue": map[string]any{"values": encodeKVs(v.AsMap())}}
	case attribute.BYTESLICE:
		return jsonValue{"stringValue": hex.EncodeToString(v.AsByteSlice())}
	}
	return jsonValue{}
}

func encodeKVs(kvs []attribute.KeyValue) []jsonKV {
	out := make([]jsonKV, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, jsonKV{string(kv.Key), encodeValue(kv.Value)})
	}
	return out
}

func encodeSet(s attribute.Set) []jsonKV {
	out := make([]jsonKV, 0, s.Len())
	for it := s.Iter(); it.Next(); {
		kv := it.Attribute()
		out = append(out, jsonKV{string(kv.Key), encodeValue(kv.Value)})
	}
	return out
}

func encodeResource(r *resource.Resource) map[string]any {
	if r == nil {
		return map[string]any{}
	}
	return map[string]any{"attributes": encodeKVs(r.Attributes())}
}

// ---- traces ----

type traceExporter struct{ p *poster }

func newTraceExporter(endpoint string) (sdktrace.SpanExporter, error) {
	p, err := newPoster(endpoint)
	if err != nil {
		return nil, err
	}
	return &traceExporter{p}, nil
}

func (e *traceExporter) Shutdown(context.Context) error { e.p.closed.Store(true); return nil }

func (e *traceExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	type scopeKey struct{ name, version string }
	type group struct {
		res    *resource.Resource
		scopes map[scopeKey][]any
		order  []scopeKey
	}
	groups := map[*resource.Resource]*group{}
	var gorder []*resource.Resource
	for _, s := range spans {
		rk := s.Resource()
		g := groups[rk]
		if g == nil {
			g = &group{res: s.Resource(), scopes: map[scopeKey][]any{}}
			groups[rk] = g
			gorder = append(gorder, rk)
		}
		sk := scopeKey{s.InstrumentationScope().Name, s.InstrumentationScope().Version}
		if _, ok := g.scopes[sk]; !ok {
			g.order = append(g.order, sk)
		}
		g.scopes[sk] = append(g.scopes[sk], encodeSpan(s))
	}
	var rs []any
	for _, rk := range gorder {
		g := groups[rk]
		var ss []any
		for _, sk := range g.order {
			ss = append(ss, map[string]any{"scope": map[string]any{"name": sk.name, "version": sk.version}, "spans": g.scopes[sk]})
		}
		rs = append(rs, map[string]any{"resource": encodeResource(g.res), "scopeSpans": ss})
	}
	return e.p.post(ctx, map[string]any{"resourceSpans": rs})
}

func encodeSpan(s sdktrace.ReadOnlySpan) map[string]any {
	sc := s.SpanContext()
	m := map[string]any{
		"traceId":           sc.TraceID().String(),
		"spanId":            sc.SpanID().String(),
		"name":              s.Name(),
		"kind":              int(s.SpanKind()), // matches OTLP: 1 internal ... 5 consumer
		"startTimeUnixNano": nanos(s.StartTime()),
		"endTimeUnixNano":   nanos(s.EndTime()),
		"attributes":        encodeKVs(s.Attributes()),
	}
	if sc.TraceState().Len() > 0 {
		m["traceState"] = sc.TraceState().String()
	}
	if p := s.Parent(); p.SpanID().IsValid() {
		m["parentSpanId"] = p.SpanID().String()
	}
	var events []any
	for _, ev := range s.Events() {
		events = append(events, map[string]any{"timeUnixNano": nanos(ev.Time), "name": ev.Name, "attributes": encodeKVs(ev.Attributes)})
	}
	if events != nil {
		m["events"] = events
	}
	var links []any
	for _, l := range s.Links() {
		links = append(links, map[string]any{"traceId": l.SpanContext.TraceID().String(), "spanId": l.SpanContext.SpanID().String(), "attributes": encodeKVs(l.Attributes)})
	}
	if links != nil {
		m["links"] = links
	}
	status := map[string]any{}
	switch s.Status().Code {
	case 1: // codes.Error
		status["code"] = 2
		status["message"] = s.Status().Description
	case 2: // codes.Ok
		status["code"] = 1
	}
	m["status"] = status
	return m
}

// ---- metrics ----

type metricExporter struct{ p *poster }

func newMetricExporter(endpoint string) (sdkmetric.Exporter, error) {
	p, err := newPoster(endpoint)
	if err != nil {
		return nil, err
	}
	return &metricExporter{p}, nil
}

func (e *metricExporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}
func (e *metricExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}
func (e *metricExporter) ForceFlush(context.Context) error { return nil }
func (e *metricExporter) Shutdown(context.Context) error   { e.p.closed.Store(true); return nil }

func (e *metricExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	var scopes []any
	for _, sm := range rm.ScopeMetrics {
		var ms []any
		for _, m := range sm.Metrics {
			if enc := encodeMetric(m); enc != nil {
				ms = append(ms, enc)
			}
		}
		if len(ms) == 0 {
			continue
		}
		scopes = append(scopes, map[string]any{
			"scope":   map[string]any{"name": sm.Scope.Name, "version": sm.Scope.Version},
			"metrics": ms,
		})
	}
	if len(scopes) == 0 {
		return nil
	}
	return e.p.post(ctx, map[string]any{"resourceMetrics": []any{map[string]any{"resource": encodeResource(rm.Resource), "scopeMetrics": scopes}}})
}

func temporality(t metricdata.Temporality) int {
	if t == metricdata.DeltaTemporality {
		return 1
	}
	return 2
}

func number[N int64 | float64](dp metricdata.DataPoint[N]) map[string]any {
	m := map[string]any{
		"attributes":        encodeSet(dp.Attributes),
		"startTimeUnixNano": nanos(dp.StartTime),
		"timeUnixNano":      nanos(dp.Time),
	}
	switch v := any(dp.Value).(type) {
	case int64:
		m["asInt"] = strconv.FormatInt(v, 10)
	case float64:
		m["asDouble"] = v
	}
	return m
}

func numbers[N int64 | float64](dps []metricdata.DataPoint[N]) []any {
	out := make([]any, 0, len(dps))
	for _, dp := range dps {
		out = append(out, number(dp))
	}
	return out
}

func histogramPoints[N int64 | float64](dps []metricdata.HistogramDataPoint[N]) []any {
	out := make([]any, 0, len(dps))
	for _, dp := range dps {
		m := map[string]any{
			"attributes":        encodeSet(dp.Attributes),
			"startTimeUnixNano": nanos(dp.StartTime),
			"timeUnixNano":      nanos(dp.Time),
			"count":             strconv.FormatUint(dp.Count, 10),
			"sum":               float64(dp.Sum),
			"explicitBounds":    dp.Bounds,
		}
		counts := make([]string, len(dp.BucketCounts))
		for i, c := range dp.BucketCounts {
			counts[i] = strconv.FormatUint(c, 10)
		}
		m["bucketCounts"] = counts
		if v, ok := dp.Min.Value(); ok {
			m["min"] = float64(v)
		}
		if v, ok := dp.Max.Value(); ok {
			m["max"] = float64(v)
		}
		out = append(out, m)
	}
	return out
}

func encodeMetric(m metricdata.Metrics) map[string]any {
	out := map[string]any{"name": m.Name, "description": m.Description, "unit": m.Unit}
	switch d := m.Data.(type) {
	case metricdata.Gauge[int64]:
		out["gauge"] = map[string]any{"dataPoints": numbers(d.DataPoints)}
	case metricdata.Gauge[float64]:
		out["gauge"] = map[string]any{"dataPoints": numbers(d.DataPoints)}
	case metricdata.Sum[int64]:
		out["sum"] = map[string]any{"dataPoints": numbers(d.DataPoints), "aggregationTemporality": temporality(d.Temporality), "isMonotonic": d.IsMonotonic}
	case metricdata.Sum[float64]:
		out["sum"] = map[string]any{"dataPoints": numbers(d.DataPoints), "aggregationTemporality": temporality(d.Temporality), "isMonotonic": d.IsMonotonic}
	case metricdata.Histogram[int64]:
		out["histogram"] = map[string]any{"dataPoints": histogramPoints(d.DataPoints), "aggregationTemporality": temporality(d.Temporality)}
	case metricdata.Histogram[float64]:
		out["histogram"] = map[string]any{"dataPoints": histogramPoints(d.DataPoints), "aggregationTemporality": temporality(d.Temporality)}
	default:
		return nil
	}
	return out
}

// ---- logs ----

type logExporter struct{ p *poster }

func newLogExporter(endpoint string) (sdklog.Exporter, error) {
	p, err := newPoster(endpoint)
	if err != nil {
		return nil, err
	}
	return &logExporter{p}, nil
}

func (e *logExporter) ForceFlush(context.Context) error { return nil }
func (e *logExporter) Shutdown(context.Context) error   { e.p.closed.Store(true); return nil }

func (e *logExporter) Export(ctx context.Context, records []sdklog.Record) error {
	if len(records) == 0 {
		return nil
	}
	var recs []any
	var res *resource.Resource
	for i := range records {
		r := &records[i]
		if res == nil {
			res = r.Resource()
		}
		var attrs []attribute.KeyValue
		r.WalkAttributes(func(kv attribute.KeyValue) bool { attrs = append(attrs, kv); return true })
		m := map[string]any{
			"timeUnixNano":         nanos(r.Timestamp()),
			"observedTimeUnixNano": nanos(r.ObservedTimestamp()),
			"severityNumber":       int(r.Severity()),
			"severityText":         r.SeverityText(),
			"body":                 encodeValue(r.Body()),
			"attributes":           encodeKVs(attrs),
		}
		if r.TraceID().IsValid() {
			m["traceId"] = r.TraceID().String()
			m["spanId"] = r.SpanID().String()
			m["flags"] = int(r.TraceFlags())
		}
		recs = append(recs, m)
	}
	return e.p.post(ctx, map[string]any{"resourceLogs": []any{map[string]any{
		"resource":  encodeResource(res),
		"scopeLogs": []any{map[string]any{"scope": map[string]any{"name": scope}, "logRecords": recs}},
	}}})
}

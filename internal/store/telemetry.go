package store

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// commitStats follows one group commit for telemetry. The span links to the
// span of each write it carries, since many callers share one commit.
type commitStats struct {
	ctx    context.Context
	span   trace.Span
	start  time.Time
	size   int
	stmts  int
	commit time.Duration
	// fn is the time spent in the write functions: the work done while
	// holding the single writer, SQL statements included.
	fn time.Duration
}

// startCommit opens the span of a group commit and records how long each
// write waited for it.
func startCommit(group []*writeReq, live []int) commitStats {
	links := make([]trace.Link, 0, len(live))
	now := time.Now()
	for _, i := range live {
		req := group[i]
		if sc := trace.SpanContextFromContext(req.ctx); sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
		if !req.enq.IsZero() {
			telemetry.Seconds(req.ctx, telemetry.StoreWriteWait, now.Sub(req.enq))
		}
	}
	ctx, span := telemetry.StartLinked(context.Background(), "store.commit", links)
	return commitStats{ctx: ctx, span: span, start: now, size: len(live)}
}

// finish ends the span and records the metrics of the commit.
func (c *commitStats) finish(results []error, live []int) {
	failed := 0
	for _, i := range live {
		if results[i] != nil {
			failed++
		}
	}
	elapsed := time.Since(c.start)
	c.span.SetAttributes(
		attribute.Int("osb.store.requests", c.size),
		attribute.Int("osb.store.failed", failed),
		attribute.Int("osb.store.statements", c.stmts),
		attribute.Int64("osb.store.commit_us", c.commit.Microseconds()),
		attribute.Int64("osb.store.fn_us", c.fn.Microseconds()),
	)
	c.span.End()
	telemetry.Seconds(c.ctx, telemetry.StoreCommitDuration, elapsed)
	telemetry.Record(c.ctx, telemetry.StoreCommitBatch, float64(c.size))
	telemetry.Add(c.ctx, telemetry.StoreStatements, int64(c.stmts))
}

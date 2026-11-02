package server

import (
	"context"

	"go.opentelemetry.io/otel/attribute"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/broker"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/telemetry"
)

// maxGaugeEntities bounds how many entities the message gauge reads at each
// export, since each read is a query.
const maxGaugeEntities = 200

// registerGauges makes the gauges that read the state of the emulator at each
// export: how many entities there are, and how many messages each queue and
// subscription holds, by state.
func (s *Server) registerGauges() {
	telemetry.Gauge("osb.entities", "{entity}", "Queues, topics and subscriptions", func(ctx context.Context, observe func(int64, ...attribute.KeyValue)) {
		for _, n := range s.Namespaces() {
			for _, kind := range []broker.EntityKind{broker.KindQueue, broker.KindTopic, broker.KindSubscription} {
				observe(int64(len(n.Broker().Entities(kind, ""))),
					attribute.String("osb.namespace", n.Name()), attribute.String("osb.entity.kind", string(kind)))
			}
		}
	})
	telemetry.Gauge("osb.entity.messages", "{message}", "Messages held by a queue or subscription, by state", func(ctx context.Context, observe func(int64, ...attribute.KeyValue)) {
		seen := 0
		for _, n := range s.Namespaces() {
			b := n.Broker()
			for _, kind := range []broker.EntityKind{broker.KindQueue, broker.KindSubscription} {
				for _, e := range b.Entities(kind, "") {
					if seen++; seen > maxGaugeEntities {
						return
					}
					c, err := b.Counts(ctx, e.Path)
					if err != nil {
						continue
					}
					base := []attribute.KeyValue{attribute.String("osb.namespace", n.Name()), attribute.String("osb.entity", e.DisplayPath())}
					for state, v := range map[string]int64{
						"active": c.Active, "dead_letter": c.DeadLetter, "deferred": c.Deferred, "scheduled": c.Scheduled,
					} {
						observe(v, append(base[:2:2], attribute.String("state", state))...)
					}
					observe(c.SizeBytes, append(base[:2:2], attribute.String("state", "size_bytes"))...)
				}
			}
		}
	})
}

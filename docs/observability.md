# Observability with OpenTelemetry

The emulator can export **traces, metrics and logs** with
[OpenTelemetry](https://opentelemetry.io/) (OTLP over HTTP). It is meant for
looking inside the emulator while your services use it: where time goes, which
operations fail, how messages move through queues, and whether the emulator
itself leaks anything.

The data is sent as OTLP/HTTP with the JSON encoding (gzip-compressed), which every OTLP/HTTP receiver accepts. The emulator carries its own small exporter instead of the official ones, which would add about 7 MB (protobuf and gRPC) to the binary.

It is **off by default**. When off, no exporter is created, no connection is
opened, and the instrumentation costs a function call.

## Turn it on

Point it at any OTLP/HTTP receiver (an OpenTelemetry Collector, Jaeger,
Grafana Tempo/Alloy, Honeycomb, [waggle](https://github.com/danielloader/waggle), ...):

```bash
open-service-bus -otel-endpoint http://localhost:4318 -queue orders
```

or with environment variables, for a container:

```bash
docker run --rm -p 5672:5672 \
  -e OSB_OTEL_ENDPOINT=http://collector:4318 -e OSB_QUEUE=orders \
  ghcr.io/igorcesarcode/open-service-bus-emulador:latest
```

| Flag | Environment | Default | |
| --- | --- | --- | --- |
| `-otel-endpoint` | `OSB_OTEL_ENDPOINT` | empty (off) | Base URL of the receiver. The emulator posts to `/v1/traces`, `/v1/metrics` and `/v1/logs` under it |
| `-otel-signals` | `OSB_OTEL_SIGNALS` | `traces,metrics,logs` | What to export |
| `-otel-sample` | `OSB_OTEL_SAMPLE` | `1` | Fraction of new traces recorded (0 to 1). Metrics are not sampled |
| `-otel-metric-interval` | `OSB_OTEL_METRIC_INTERVAL` | `10s` | How often metrics are exported |
| `-otel-service-name` | `OSB_OTEL_SERVICE_NAME` | `open-service-bus` | The `service.name` of the data |

The standard `OTEL_EXPORTER_OTLP_HEADERS` (authentication headers) and
`OTEL_RESOURCE_ATTRIBUTES` variables are honored. Resource attributes include
the service name and version, instance id, host, process id, the
authentication mode, whether a database file is used, and the number of
partitions.

On shutdown the emulator flushes what is pending (up to 5 seconds).

An example with Jaeger is in [`examples/observability`](../examples/observability).

## Traces

Each operation is a span, so a client call can be followed from the wire down
to the SQLite commit:

```
amqp.transfer              a message (or batch) arrives from a client
 └─ broker.send            routing and enqueue
     └─ store.commit       the group commit that made it durable (linked)
amqp.deliver               a message goes to a receiver
amqp.disposition           the receiver settles it
 └─ broker.complete | broker.abandon | broker.defer | broker.dead_letter
```

| Span | Attributes (besides `osb.entity`, the queue/topic/subscription path) |
| --- | --- |
| `amqp.attach` | `osb.link.role` |
| `amqp.transfer` | `osb.bytes`, `osb.batch` |
| `amqp.deliver` | `osb.delivery_count`, `osb.bytes`, `osb.peek_lock` |
| `amqp.disposition` | `amqp.outcome` (the AMQP outcome) |
| `broker.send` | `osb.count`, `osb.bytes`; for a topic also `osb.route.subscriptions`, `osb.route.matched`, `osb.route.filter_errors`; `osb.duplicates` when duplicate detection dropped messages |
| `broker.receive`, `broker.receive_session`, `broker.peek`, `broker.receive_deferred` | `osb.count` |
| `broker.complete`, `abandon`, `defer`, `dead_letter`, `release`, `renew_locks` | |
| `broker.accept_session`, `accept_next_session`, `release_session`, `session_state`, ... | |
| `broker.create_queue`, `create_topic`, `create_subscription`, `update_entity`, `delete_entity`, `add_rule`, `update_rule`, `delete_rule`, `set_rules` | |
| `broker.commit` | an AMQP transaction |
| `broker.timers.sweep` | `osb.timers.activated`, `osb.timers.expired`, `osb.timers.locks_released`. Only sweeps that find work |
| `store.commit` | `osb.store.requests` (writes folded into the commit), `osb.store.statements`, `osb.store.commit_us` (time in SQLite's commit), `osb.store.failed`. Links to the spans of the writes it carries |
| `http.server <METHOD> <route>` | `http.route`, `http.response.status_code`, `osb.component` — ATOM management API, portal and the other HTTP endpoints |
| `mcp.tool <name>` | `mcp.tool` |
| `startup.load_config`, `startup.load_terraform`, `startup.new_server` | |

Every span has `osb.outcome`: `ok`, `user_error`, `throttled`, `canceled` or
`server_error`. **Only `server_error` marks a span as an error.** Failures
the caller causes (a lock lost, an entity not found, no session available)
are recorded as a `failed` event with the message, so a trace view stays
quiet about expected client mistakes and loud about real problems. The
trace context of an incoming HTTP request (`traceparent`) is honored. For
messages, a client that traces its sends can put the context in the
`Diagnostic-Id` application property (the Azure SDKs do) or `traceparent`;
the `amqp.transfer` span and everything under it then join the client's trace.

**Message bodies, property values, keys and tokens are never exported.**

## Metrics

| Metric | Type | Attributes |
| --- | --- | --- |
| `osb.messages.sent`, `.received`, `.completed`, `.abandoned`, `.deferred`, `.dead_lettered` (with `reason`), `.expired`, `.scheduled` | counter | `osb.entity` |
| `osb.duplicates.dropped` | counter | `osb.entity` |
| `osb.locks.expired` | counter | |
| `osb.requests` | counter | `outcome`, `osb.entity`, `osb.entity.kind` |
| `osb.operation.duration` (seconds) | histogram | `operation`, `outcome` |
| `osb.message.size` (bytes) | histogram | |
| `osb.message.time_in_queue` (seconds, first delivery) | histogram | |
| `osb.delivery.count` (at settlement) | histogram | |
| `osb.lock.held` (seconds) | histogram | |
| `osb.store.commit.duration`, `osb.store.commit.batch_size`, `osb.store.write.wait`, `osb.store.statements` | histogram / counter | |
| `osb.http.requests`, `osb.http.duration` | counter / histogram | `http.route`, `http.request.method`, `http.response.status_code` |
| `osb.mcp.tool_calls` | counter | `mcp.tool`, `error` |
| `osb.connections` | up-down counter | `osb.namespace` |
| `osb.entities` | gauge | `osb.namespace`, `osb.entity.kind` |
| `osb.entity.messages` | gauge (first 200 queues and subscriptions) | `osb.entity`, `state` = `active`, `dead_letter`, `deferred`, `scheduled`, `size_bytes` |
| `process.runtime.go.*` | Go runtime: goroutines, heap, GC | |

## Logs

When `logs` is exported, the emulator's own log records (at or above
`-log-level`) are sent too, with the trace and span ids when they were
emitted inside a span.

## What to look for

- **Slow writes:** `store.commit` duration and `osb.store.commit.batch_size`.
  Batches near 1 under concurrent load mean writes are not being grouped.
- **Busy loops:** many `broker.receive` spans with `osb.count` 0 from one
  client.
- **Redelivery storms:** `osb.delivery.count` high, `osb.locks.expired` growing,
  `osb.lock.held` close to the lock duration. Your handler is slower than the
  lock.
- **Routing surprises:** `osb.route.matched` equal to `osb.route.subscriptions`
  for every send means every subscription still has its `$Default` rule.
- **Dead letters:** `osb.messages.dead_lettered` by `reason`.
- **Growing queues:** `osb.entity.messages` by `state`.
- **Leaks in a long run:** `osb.connections`, goroutines and heap should return
  to a baseline.

## Cost and sampling

Tracing every operation is a lot of data for a busy emulator: thousands of
spans per second. Receivers differ in how much they accept (a small receiver
may answer "buffer full"). Use `-otel-sample 0.05` (or less) for load tests;
metrics are not sampled, so rates and latencies stay exact. Spans are buffered
in memory (up to 65,536) and dropped when the queue is full, so telemetry never
slows the emulator down.

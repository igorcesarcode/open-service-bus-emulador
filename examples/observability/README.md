# Observability (OpenTelemetry)

The emulator can export traces, metrics and logs with OpenTelemetry. It is off
until you give it an endpoint. See [Observability](../../docs/observability.md)
for the spans, metrics and what to look for.

## Jaeger

[`compose.yaml`](compose.yaml) runs the emulator next to Jaeger:

```bash
OSB_IMAGE=open-service-bus:local docker compose up       # see ../docker for the image
go run ./examples/go-sdk                                 # some traffic
```

Open <http://localhost:16686>, pick the service `open-service-bus`, and open a
trace: an `amqp.transfer` span has the `broker.send` under it, and the
`store.commit` that made the message durable is linked to it.

## Without Docker

Any OTLP/HTTP receiver works, for example an OpenTelemetry Collector listening
on port 4318:

```bash
open-service-bus -otel-endpoint http://localhost:4318 -config examples/config-json/Config.json
```

For a busy emulator, sample the traces and keep the metrics exact:

```bash
open-service-bus -otel-endpoint http://localhost:4318 -otel-sample 0.05
```

## Environment variables

```bash
OSB_OTEL_ENDPOINT=http://collector:4318 \
OSB_OTEL_SIGNALS=traces,metrics \
OSB_OTEL_SERVICE_NAME=servicebus-emulator \
OTEL_RESOURCE_ATTRIBUTES=deployment.environment=ci \
OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer\ token \
open-service-bus -queue orders
```

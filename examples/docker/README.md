# Docker and Docker Compose

Most people run the emulator as a container next to the services under test.
These Compose files cover the usual setups. All of them configure the emulator
through `OSB_*` environment variables (every command-line flag has one; the
full list is in [Configuration](../../docs/configuration.md#environment-variables)).

Every file uses `${OSB_IMAGE:-ghcr.io/igorcesarcode/open-service-bus-emulador:latest}`.
To use an image you built from source instead, set the variable:

```bash
make image IMAGE=open-service-bus:local        # at the repository root
export OSB_IMAGE=open-service-bus:local        # PowerShell: $env:OSB_IMAGE = "open-service-bus:local"
```

**Ports.** These files publish only 5672 and their connection strings carry
`UseDevelopmentEmulator=true`, which is what makes the SDKs use plain AMQP on
the port you give. A connection string without the flag and without a port
makes the SDK dial 5671; see
[Connecting without `UseDevelopmentEmulator`](../../docs/usage.md#connecting-without-usedevelopmentemulator).

| File | Setup |
| --- | --- |
| [`compose.yaml`](compose.yaml) | The emulator alone: entities from `Config.json`, extra queues from `OSB_QUEUE`, state in a volume, the portal on <https://localhost:5672/$portal/> |
| [`compose.with-app.yaml`](compose.with-app.yaml) | The emulator plus a Node.js program that reaches it as `servicebus:5672` |
| [`compose.terraform.yaml`](compose.terraform.yaml) | Entities read from Terraform files (variables, module adapter, `$Default` rule) |
| [`compose.auth.yaml`](compose.auth.yaml) | Shared access signatures validated (`OSB_AUTH=sas`) with named policies |
| [`compose.mcp.yaml`](compose.mcp.yaml) | The MCP endpoint for AI agents on a loopback-only port |

```bash
docker compose up                                  # compose.yaml
docker compose -f compose.with-app.yaml up --abort-on-container-exit
docker compose -f compose.terraform.yaml up
```

## Plain `docker run`

```bash
# Entities from variables, nothing persisted
docker run --rm -p 5672:5672 \
  -e OSB_QUEUE=orders,payments -e OSB_TOPIC=events -e OSB_SUBSCRIPTION=events/audit \
  open-service-bus:local

# Config.json and a persistent database
docker run -d --name servicebus -p 5672:5672 \
  -v osb-data:/data -v "$PWD/Config.json:/config/Config.json:ro" \
  -e OSB_CONFIG=/config/Config.json -e OSB_DB=/data/osb.db \
  open-service-bus:local

# Same, with arguments instead of variables
docker run --rm -p 5672:5672 open-service-bus:local -queue orders -log-level debug
```

## Connection strings

| Client runs... | Connection string endpoint |
| --- | --- |
| on the host | `sb://localhost:5672` |
| in another container of the same Compose project | `sb://servicebus:5672` (the service name) |
| in a container, emulator published on the host | `sb://host.docker.internal:5672` (on Linux add `extra_hosts: ["host.docker.internal:host-gateway"]`) |

Always append `;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true`
(any key works with `OSB_AUTH=none`).

## Things to know

- **No shell in the image.** It is `FROM scratch`: no `curl`, no `HEALTHCHECK`,
  no `docker exec ... sh`. Have clients retry the first connection, or wait for
  the port from the outside (`nc -z localhost 5672`).
- **Ports.** One port (5672) carries AMQP, AMQP over WebSocket, the management
  API and the portal. Publish others only if you set `OSB_AMQP_ADDR`,
  `OSB_HTTP_ADDR`, `OSB_MCP_ADDR` or `OSB_PPROF`.
- **Persistence.** Without `OSB_DB` state lives in memory and disappears when the
  container stops. Put the database on a volume (`/data`). Each extra namespace
  gets its own file next to it.
- **Self-signed TLS.** Messaging clients using `UseDevelopmentEmulator=true` do not
  need the certificate. The Go and .NET administration clients use HTTPS: skip
  verification in tests, or trust the certificate (`OSB_TLS_CERT_OUT=/data/cert.pem`
  writes it to the volume). Add the names clients use with `OSB_TLS_HOSTS`.
- **Listening address.** The container listens on all interfaces (`:5672`), as
  Docker needs; restrict exposure when you publish the port
  (`-p 127.0.0.1:5672:5672`).
- **The MCP endpoint has no login.** Publish its port on loopback only.
- **Reset between test runs.** `docker compose down -v` removes the volume.

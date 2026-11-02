# open-service-bus-emulador

A native Go emulator of **Azure Service Bus**, shipped as a **single binary**.
It speaks AMQP 1.0 and the management (ATOM) API, so the official Azure SDKs
connect to it exactly as they connect to Azure. State lives in SQLite. No
Docker, no SQL Server, and none of the artificial limits of the official
emulator: every property of every queue, topic, subscription and rule is
configurable.

> [!WARNING]
> **For testing only.** This project is strongly recommended for local
> development, integration tests and CI. It is **not recommended for
> production**: it has no high availability, no real security boundary (see
> [Security notes](docs/security.md)), and its behavior is an emulation, not
> the Azure service. Never point production traffic at it.

## Highlights

- **One binary, no dependencies.** Pure Go (`CGO_ENABLED=0`), SQLite
  persistence (or in-memory), nothing else to install.
- **Windows, Linux and macOS** (amd64 and arm64). Container image: coming soon.
- **Compatible with the official SDKs** for Go, JavaScript/TypeScript and .NET
  (queues, topics, subscriptions, rules, sessions, dead-lettering, scheduled
  messages, deferral, transactions, batch delete, duplicate detection,
  auto-forwarding, ...).
- **No limits on properties.** TTL, lock duration, max delivery count, entity
  count and message size are whatever you set.
- **Configure from your production Terraform.** Point `-terraform` at your
  `azurerm_servicebus_*` files and get the same entities locally.
- **Config.json compatible** with the official emulator, plus extensions
  (policies, role assignments, locks, tags, several namespaces, Geo-DR).
- **Web portal** with a Service Bus Explorer (send, peek, receive, settle,
  purge, resend), metrics, activity log and access policies.
- **MCP server** (off by default, own port) so an AI agent can manage the
  emulator.
- **OpenTelemetry** traces, metrics and logs (off by default) to see where
  time goes and what fails: [Observability](docs/observability.md).
- **Authentication when you need it**: none, SAS, or SAS plus Entra ID tokens.

## Installation

Download the binary for your platform from the
[Releases page](https://github.com/igorcesarcode/open-service-bus-emulador/releases)
or use Docker, or build
from source. The [installation guide](docs/installation.md) has the details for
Windows, Linux, macOS, Docker and `go install`.

```bash
# Linux / macOS (after downloading and making the file executable)
./open-service-bus -queue orders -topic events -subscription events/audit
```

```powershell
# Windows
.\open-service-bus.exe -queue orders -topic events -subscription events/audit
```

### Docker

> The image is `ghcr.io/igorcesarcode/open-service-bus-emulador`: `latest`
> follows `main`, and each release has its version tag (`1.0.0`). To build it
> yourself: `make image IMAGE=open-service-bus:local`.

Everything the command line does can be done with `OSB_*` environment
variables, which is how you configure a container:

```bash
docker run --rm -p 5672:5672 \
  -e OSB_QUEUE=orders,payments \
  -e OSB_TOPIC=events -e OSB_SUBSCRIPTION=events/audit \
  ghcr.io/igorcesarcode/open-service-bus-emulador:latest
```

Entities from a file, state kept in a volume:

```bash
docker run --rm -p 5672:5672 -v osb-data:/data -v "$PWD/Config.json:/config/Config.json:ro" \
  -e OSB_CONFIG=/config/Config.json -e OSB_DB=/data/osb.db \
  ghcr.io/igorcesarcode/open-service-bus-emulador:latest
```

Docker Compose files for the emulator alone, with an application, from
Terraform, with authentication and with the MCP endpoint are in
[`examples/docker`](examples/docker).

## Quick start

1. Start the emulator, declaring what you need:

   ```bash
   open-service-bus -queue orders -topic events -subscription events/audit
   ```

2. It prints the connection string:

   ```
   Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true
   ```

3. Use it with any official SDK. The same connection string works in all of them:

   **Go**

   ```go
   client, _ := azservicebus.NewClientFromConnectionString(connectionString, nil)
   sender, _ := client.NewSender("orders", nil)
   _ = sender.SendMessage(ctx, &azservicebus.Message{Body: []byte("hello")}, nil)

   receiver, _ := client.NewReceiverForQueue("orders", nil)
   msgs, _ := receiver.ReceiveMessages(ctx, 1, nil)
   _ = receiver.CompleteMessage(ctx, msgs[0], nil)
   ```

   **JavaScript**

   ```js
   const client = new ServiceBusClient(connectionString);
   await client.createSender("orders").sendMessages({ body: "hello" });
   const [message] = await client.createReceiver("orders").receiveMessages(1);
   ```

   **.NET**

   ```csharp
   await using var client = new ServiceBusClient(connectionString);
   await client.CreateSender("orders").SendMessageAsync(new ServiceBusMessage("hello"));
   var message = await client.CreateReceiver("orders").ReceiveMessageAsync();
   ```

   Complete programs are in [`examples/`](examples/README.md).

4. Open the portal at <https://localhost:5672/$portal/> (self-signed
   certificate; `-tls-cert-out cert.pem` writes it so clients can trust it).

`UseDevelopmentEmulator=true` in the connection string makes the SDKs connect
over plain AMQP to the local endpoint, the same way they do with the official
emulator, so sending and receiving need no certificate setup. TLS is also
accepted on the same port, and the administration clients have a few
SDK-specific notes. See [usage](docs/usage.md#connecting-the-sdks).

## Declaring entities

Flags, a `Config.json` (the official emulator's format, without range limits),
or your production Terraform. They combine.

```bash
open-service-bus -config Config.json
open-service-bus -terraform ./infra                  # reads the azurerm_servicebus_* resources
open-service-bus -terraform ./infra -tf-var environment=dev
```

A `Config.json` with a queue that dead-letters into another, a session queue
and a subscription filtered by an SQL rule:

```json
{
  "UserConfig": {
    "Namespaces": [{
      "Name": "demo",
      "Queues": [
        { "Name": "jobs", "Properties": { "LockDuration": "PT30S", "MaxDeliveryCount": 3,
                                          "ForwardDeadLetteredMessagesTo": "jobs-poison" } },
        { "Name": "jobs-poison" },
        { "Name": "replies", "Properties": { "RequiresSession": true } }
      ],
      "Topics": [{
        "Name": "events",
        "Subscriptions": [{
          "Name": "big-orders",
          "Rules": [{ "Name": "big", "Properties": { "FilterType": "Sql",
                      "SqlFilter": { "SqlExpression": "quantity > 10" } } }]
        }]
      }]
    }]
  }
}
```

More in [Configuration](docs/configuration.md) and
[`examples/config-json`](examples/config-json). Entities can also be created
at run time with the SDKs' administration clients, the portal or the MCP server.

## Environment variables

Every flag has an `OSB_*` variable: the flag name in upper case with dashes
turned into underscores (`-max-message-size` is `OSB_MAX_MESSAGE_SIZE`). A flag
on the command line wins over its variable.

| Variable | Flag | Default | Description |
| --- | --- | --- | --- |
| `OSB_LISTEN` | `-listen` | `:5672` | Address shared by AMQP, WebSocket, the management API and the portal |
| `OSB_AMQP_ADDR` / `OSB_HTTP_ADDR` | `-amqp-addr` / `-http-addr` | off | Dedicated AMQP / HTTP addresses |
| `OSB_DB` | `-db` | in memory | SQLite file; set it to keep state |
| `OSB_NAMESPACE` | `-namespace` | `emulator` | Default namespace name |
| `OSB_QUEUE`, `OSB_TOPIC` | `-queue`, `-topic` | | Comma separated names |
| `OSB_SUBSCRIPTION` | `-subscription` | | Comma separated `topic/name` |
| `OSB_CONFIG` | `-config` | | Config.json path |
| `OSB_TERRAFORM` | `-terraform` | | Terraform directory or file |
| `OSB_TF_VAR`, `OSB_TF_VAR_FILE` | `-tf-var`, `-tf-var-file` | | `name=value` / file path, one per line |
| `OSB_TF_MODULE_MAP`, `OSB_TF_DROP_DEFAULT_RULE` | `-tf-module-map`, `-tf-drop-default-rule` | | Terraform adapter options |
| `OSB_AUTH` | `-auth` | `none` | `none`, `sas` or `full` |
| `OSB_DISABLE_LOCAL_AUTH`, `OSB_DEV_ISSUER`, `OSB_OIDC_ISSUER`, `OSB_OIDC_JWKS` | same names as flags | | Authentication options |
| `OSB_TLS_CERT`, `OSB_TLS_KEY`, `OSB_TLS_CERT_OUT`, `OSB_TLS_HOSTS` | `-tls-*` | self-signed | TLS certificate options |
| `OSB_PORTAL`, `OSB_PROMETHEUS`, `OSB_METRICS_RETENTION` | same names | `true`, `false`, `720h` | Portal and metrics |
| `OSB_PARTITIONS`, `OSB_MAX_BATCH_DELETE`, `OSB_MAX_MESSAGE_SIZE`, `OSB_TXN_TIMEOUT` | same names | | Limits |
| `OSB_MCP_ADDR`, `OSB_MCP_READONLY`, `OSB_MCP_ALLOW_DATA` | `-mcp-*` | off | [MCP server](docs/MCP.md) |
| `OSB_OTEL_ENDPOINT`, `OSB_OTEL_SIGNALS`, `OSB_OTEL_SAMPLE`, `OSB_OTEL_METRIC_INTERVAL`, `OSB_OTEL_SERVICE_NAME` | `-otel-*` | off | [OpenTelemetry](docs/observability.md) export |
| `OSB_LOG_LEVEL`, `OSB_PPROF` | `-log-level`, `-pprof` | `info`, off | Logging and profiling |

The full table, with value formats, is in
[Configuration](docs/configuration.md#environment-variables).

## Tips for tests

- Without `OSB_DB` every start is clean: ideal for CI.
- Values Azure refuses are accepted: `LockDuration` of `PT2S`, a tiny
  `MaxDeliveryCount`, a TTL of `PT1S` make failure paths fast to test.
- Point `-terraform` at your real infrastructure code to get the same topology.
- `docker compose` examples and a GitHub Actions workflow are in
  [`examples/`](examples/README.md); more in [Tips](docs/tips.md).

## Documentation

| Document | What is in it |
| --- | --- |
| [Installation](docs/installation.md) | Windows, Linux, macOS, Docker, from source |
| [Usage guide](docs/usage.md) | Flags, declaring entities, connecting SDKs, authentication, portal, several namespaces |
| [Configuration](docs/configuration.md) | `Config.json`, Terraform, environment variables |
| [How it works](docs/how-it-works.md) | Architecture overview, storage, protocols, delivery semantics |
| [Tips and troubleshooting](docs/tips.md) | Patterns for tests and CI, common problems |
| [Differences from Azure](docs/differences-from-azure.md) | What is emulated differently or left out |
| [Testing and validation](docs/testing.md) | How the project is validated against the official SDKs |
| [Security notes](docs/security.md) | What the emulator does and does not protect |
| [Observability](docs/observability.md) | OpenTelemetry traces, metrics and logs |
| [MCP server](docs/MCP.md) | Managing the emulator from an AI agent |
| [Examples](examples/README.md) | Runnable examples, each showing a different way to use the emulator |
| [ATOM wire format](docs/atom-wire-spec.md) | The management API as the official SDKs use it |

## Validation

The emulator is validated against the **official Azure SDKs** with regression
suites that run the same scenarios through each of them:

- Go: `github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus`, including
  the `admin.Client`;
- JavaScript: `@azure/service-bus`;
- .NET: `Azure.Messaging.ServiceBus`;
- end-to-end tests where Go and JavaScript mini-services talk to each other
  through the emulator, in every combination;
- raw AMQP and ATOM wire-level tests, unit tests for the SQL filter language,
  the Terraform reader and the broker.

CI runs the suite with the race detector on Linux, macOS and Windows. Details
in [Testing and validation](docs/testing.md).

## Platform support

| OS | Architectures | Status |
| --- | --- | --- |
| Linux | amd64, arm64 | tested in CI |
| macOS | amd64 (Intel), arm64 (Apple Silicon) | tested in CI |
| Windows | amd64, arm64 | tested in CI |

## Acknowledgements

Thanks to the **Azure Service Bus emulator** team at Microsoft
([Azure/azure-service-bus-emulator-installer](https://github.com/Azure/azure-service-bus-emulator-installer)).
Its `Config.json` format, documented limitations and behavior were the main
reference for this project, and keeping compatibility with it made migration
easy. Thanks also to
[AlmostServiceBus](https://github.com/gkinsman/AlmostServiceBus) for another
point of comparison. No code was copied from either project.

This project is independent and is not affiliated with or endorsed by
Microsoft. "Azure" and "Service Bus" are trademarks of Microsoft Corporation.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE). See also
[NOTICE](NOTICE).

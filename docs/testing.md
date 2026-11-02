# Testing and validation

The emulator is validated against the **official Azure SDKs**, and every new
feature needs scenarios in both the Go and the JavaScript suites. This is what
guards against regressions.

## What runs

| Suite | Location | What it does |
| --- | --- | --- |
| Unit tests | `internal/...` | AMQP codec, broker, SQL filter language, Terraform reader, config, auth, portal, MCP |
| Go SDK integration | `test/integration` | `github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus`, including the `admin` client, against the emulator started in process or as the real binary |
| JavaScript SDK | `test/node`, run by `TestNodeSDK` | `@azure/service-bus` (messaging and `ServiceBusAdministrationClient`) |
| .NET SDK | `test/dotnet`, run by `TestDotnetSDK` | `Azure.Messaging.ServiceBus`, the same scenarios plus transactions and batch delete; runs when `dotnet` (or Docker) is available |
| MCP | `test/integration/mcp_test.go`, `test/node/mcp.test.mjs` | the MCP server with a minimal client and the official `@modelcontextprotocol/sdk` |
| End-to-end | `test/e2e` | the binary runs as a separate process (and optionally in Docker) and mini-services written in Go and in JavaScript talk to each other **through it**, in every combination (Go↔Go, Go↔JS, JS↔Go, JS↔JS) |
| Wire level | `test/integration/rawamqp_test.go` and friends | raw AMQP frames, ATOM payloads and error conditions |
| Variants | `OSB_NO_EMULATOR_FLAG=1` | the clients connect as they do to Azure (a `servicebus.windows.net` name, TLS, no `UseDevelopmentEmulator`) |

Scenarios cover queues, topics, subscriptions and rules, sessions, peek-lock
and receive-and-delete, abandon/defer/dead-letter, lock renewal, scheduled
messages, duplicate detection, auto-forwarding, TTL, transactions, batch delete,
partitioned entities, authentication (SAS and Entra ID), multiple namespaces,
Geo-DR, locks, tags, export, WebSocket transport and the unsupported-feature
reporting.

## Running them

```bash
cd test/node && npm ci && cd ../..     # once, for the JavaScript suites
go vet ./...
go test -race ./...
```

Without `node_modules` in `test/node`, the JavaScript suites are skipped. The
.NET suite runs when `dotnet` is installed (or with `OSB_DOTNET_DOCKER`).
`go test -short` skips the tests that build the binary.

Useful subsets:

```bash
go test -race ./internal/...
go test -race -run TestNodeSDK ./test/integration
go test -race ./test/e2e
OSB_NO_EMULATOR_FLAG=1 go test -race ./test/integration
```

## Continuous integration

`.github/workflows/ci.yml` runs `go vet` and `go test -race ./...` on Ubuntu,
macOS and Windows for every push and pull request, and builds the release
binaries for a `v*` tag.

## The contract between the e2e services

The Go and JavaScript mini-services follow one contract
(`test/e2e/spec` and `test/node/e2e/spec.mjs`) that must stay in sync.

## Limits of this validation

The emulator is verified against the **behavior the SDKs exercise**. The real
service has behavior (throttling, regional failover, subtle ordering under
load) that cannot be reproduced; see [Differences from Azure](differences-from-azure.md).
Treat a green suite against the emulator as strong evidence, not as proof, and
keep a test stage against real Azure.

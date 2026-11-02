# Examples

Each folder shows a different way to use open-service-bus. Unless noted, they
need the emulator running; see the [installation guide](../docs/installation.md)
(the container image and release downloads are placeholders until they are
published; building from source works today).

| Example | Shows | Needs |
| --- | --- | --- |
| [cli-flags](cli-flags) | Starting the emulator with flags only, persistence, export | the binary |
| [config-json](config-json) | A `Config.json` with queues, topics, rules, sessions, DLQ forwarding, duplicate detection | the binary |
| [terraform](terraform) | Creating the entities from `azurerm_servicebus_*` Terraform | the binary |
| [go-sdk](go-sdk) | Official Go SDK: send/receive, topic routing, dead-letter, admin client | Go |
| [node-sdk](node-sdk) | Official JavaScript SDK: the same scenarios plus scheduling | Node.js 20+ |
| [dotnet-sdk](dotnet-sdk) | Official .NET SDK | .NET 8 SDK |
| [go-test-helper](go-test-helper) | A Go integration test that starts the emulator as a child process | Go |
| [docker](docker) | Docker Compose, alone and together with an app | Docker |
| [github-actions](github-actions) | Using the emulator in CI | GitHub Actions |
| [auth-sas](auth-sas) | Shared access policies enforced with `-auth sas` | Node.js |
| [multi-namespace](multi-namespace) | Several namespaces in one emulator | Go |
| [observability](observability) | OpenTelemetry traces to Jaeger | Docker |
| [mcp](mcp) | Managing the emulator from an AI agent | an MCP client |

The examples that connect use this connection string, which is also printed
by the emulator at startup. Override it with `OSB_CONNECTION_STRING`:

```
Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true
```

The Go examples are part of the repository's module: run them from the
repository root with `go run ./examples/<name>`.

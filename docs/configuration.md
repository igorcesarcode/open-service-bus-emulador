# Configuration

Entities can be declared with [flags](usage.md#flags), a `Config.json` file, or
Terraform. This page documents the last two and the environment variables.

## Config.json

`-config Config.json`. The format is that of the official Azure Service Bus
emulator (comments with `//` and `/* */` are accepted), with every property
free of range limits. Unknown fields are rejected, so typos fail at startup.

```json
{
  "UserConfig": {
    "Namespaces": [
      {
        "Name": "demo",
        "Queues": [
          { "Name": "orders", "Properties": { "LockDuration": "PT30S", "MaxDeliveryCount": 5 } }
        ],
        "Topics": [
          {
            "Name": "events",
            "Subscriptions": [
              {
                "Name": "big-orders",
                "Rules": [
                  {
                    "Name": "big",
                    "Properties": {
                      "FilterType": "Sql",
                      "SqlFilter": { "SqlExpression": "quantity > 10" }
                    }
                  }
                ]
              }
            ]
          }
        ]
      }
    ]
  }
}
```

A complete example is in [`examples/config-json`](../examples/config-json/Config.json).

### Properties

Durations are ISO 8601 (`PT30S`, `P14D`). Properties that are not set get the
Service Bus defaults.

| Property | Applies to | Notes |
| --- | --- | --- |
| `LockDuration` | queue, subscription | |
| `MaxSizeInMegabytes` | queue, topic | multiplied by `-partitions` for partitioned entities |
| `RequiresDuplicateDetection`, `DuplicateDetectionHistoryTimeWindow` | queue, topic | |
| `RequiresSession` | queue, subscription | |
| `DefaultMessageTimeToLive` | all | |
| `DeadLetteringOnMessageExpiration` | queue, subscription | |
| `DeadLetteringOnFilterEvaluationExceptions` | subscription | |
| `MaxDeliveryCount` | queue, subscription | |
| `EnableBatchedOperations` | all | |
| `Status` | all | `Active`, `Disabled`, `SendDisabled`, `ReceiveDisabled` |
| `AutoDeleteOnIdle` | all | |
| `EnablePartitioning`, `EnableExpress`, `SupportOrdering` | queue, topic | |
| `ForwardTo`, `ForwardDeadLetteredMessagesTo` | queue, subscription | auto-forwarding chains |
| `MaxMessageSizeInKilobytes` | queue, topic | |
| `UserMetadata` | all | |

### Rules

`FilterType` is `Sql`, `Correlation`, `True` or `False`.

- `SqlFilter.SqlExpression`: the SQL-92 subset of Service Bus: comparison
  operators, `LIKE`, `IN`, `IS NULL`, `AND`/`OR`/`NOT`, system properties
  (`sys.Label`, `sys.To`, ...), user properties, and the Service Bus functions.
- `CorrelationFilter`: `CorrelationId`, `MessageId`, `To`, `ReplyTo`, `Label`
  (or `Subject`), `SessionId`, `ReplyToSessionId`, `ContentType` and a
  `Properties` map.
- `Action.SqlExpression`: `SET`/`REMOVE` statements applied to the message
  delivered to that subscription.

Expressions are parsed at startup, so an invalid one fails fast.

### Extensions

Fields that the official format lacks:

| Where | Field | Purpose |
| --- | --- | --- |
| namespace | `AuthorizationRules` | shared access policies: `Name`, `Rights` (`Manage`, `Send`, `Listen`), optional `PrimaryKey`/`SecondaryKey` |
| queue, topic | `AuthorizationRules` | entity-level policies |
| namespace | `RoleAssignments` | Entra ID principals: `Principal`, `Role` (Data Owner, Sender, Receiver), optional `Scope` |
| namespace | `LocalAuthEnabled` | `false` rejects SAS |
| namespace | `Tags` | resource tags |
| namespace | `Locks` | `Name`, `Level` (`CanNotDelete`, `ReadOnly`), `Scope`, `Notes` |
| namespace | `Encryption` | recorded only (see [differences](differences-from-azure.md)) |
| namespace | `GeoReplication` | `Mode` (`Synchronous`/`Asynchronous`), `SecondaryRegion`, lag and delay |
| top level | `GeoDR` | aliases pairing namespaces: `Alias`, `Primary`, `Secondary` |
| namespace | `NetworkRuleSet`, `PublicNetworkAccess`, `PrivateEndpoints` | accepted so a namespace copied from Azure loads; ignored with a warning |

A document with several namespaces declares one each; see
[`examples/multi-namespace`](../examples/multi-namespace/Config.json).

## Terraform

`-terraform <dir-or-file>`. The `.tf` and `.tf.json` files are evaluated
**without running Terraform** (no state, no providers, no Azure access).

Supported language features: variables (`-tf-var`, `-tf-var-file`,
`terraform.tfvars`, `*.auto.tfvars` and `.json` forms), locals, `count`,
`for_each`, `dynamic` blocks, references between resources, conditionals,
`for` expressions and the common functions. Local modules, modules downloaded
by `terraform init`, and `count`/`for_each` on modules are read. Environment
variables named `TF_VAR_*` are honored.

Recognized resources:

- `azurerm_servicebus_namespace` (`tags`, `local_auth_enabled`,
  `premium_messaging_partitions`, `customer_managed_key`);
- `azurerm_servicebus_queue`, `_topic`, `_subscription`, `_subscription_rule`;
- the `_authorization_rule` resources;
- `azurerm_role_assignment`;
- `azurerm_servicebus_namespace_disaster_recovery_config`;
- `azurerm_management_lock`.

Attributes that only matter in Azure (location, SKU, network settings) are
parsed and ignored; networking features are reported as unsupported in the
log.

### Modules whose source is not available

If a module comes from a private registry or private git repository that the
machine cannot reach, the emulator reads it with an adapter that recognizes
the arguments `asb_queues`, `asb_topics_name` and `asb_subs`. To name other
arguments, pass `-tf-module-map map.json`:

```json
{"bus": {"queues": "my_queues", "topics": "my_topics", "subscriptions": "my_subs"}}
```

where `bus` is the module block name. A test fixture showing the adapter is
in `test/integration/testdata/private-module`.

### The `$Default` rule

Azure creates a `$Default` rule (`1=1`) with every subscription and keeps it
when other rules are added, so such a subscription receives everything. The
emulator does the same and warns; pass `-tf-drop-default-rule` to remove it.

### Combining sources

Flags, `-config` and `-terraform` can be used together. For an entity declared
by both `-config` and `-terraform`, the properties of `-config` win.

## Export

`open-service-bus -export dir` writes `Config.json` and `main.tf` for the
loaded state and exits. The portal's *Export template* page downloads the same
files from a running emulator.

## Environment variables

Every command-line flag has an environment variable, which is how a container
is configured. The name is `OSB_` plus the flag name in upper case with dashes
turned into underscores: `-max-message-size` is `OSB_MAX_MESSAGE_SIZE`.

- A flag given on the command line **wins** over its variable.
- An empty variable counts as unset.
- A value that does not parse (`OSB_PARTITIONS=many`, `OSB_MCP_READONLY=maybe`)
  aborts startup with a message naming the variable.
- Booleans are `true`/`false` (also `1`/`0`).
- Variables that take several values (`OSB_QUEUE`, `OSB_TOPIC`,
  `OSB_SUBSCRIPTION`, `OSB_TLS_HOSTS`) take them **comma separated**.
  `OSB_TF_VAR` and `OSB_TF_VAR_FILE` take them **one per line**, because a
  Terraform value may contain commas.
- Durations use Go syntax (`30s`, `2m`, `720h`).

| Variable | Flag | Default | Description and example |
| --- | --- | --- | --- |
| `OSB_LISTEN` | `-listen` | `:5672` | Address shared by every protocol. `OSB_LISTEN=0.0.0.0:5672`; empty-by-flag `-listen ""` disables it |
| `OSB_AMQP_ADDR` | `-amqp-addr` | off | Extra address for AMQP/AMQPS alone. `:5671` |
| `OSB_HTTP_ADDR` | `-http-addr` | off | Extra address for HTTP/HTTPS alone (management API, WebSocket). `:8443` |
| `OSB_DB` | `-db` | in memory | SQLite file. `/data/osb.db` |
| `OSB_NAMESPACE` | `-namespace` | first declared, else `emulator` | Default namespace name. `orders` |
| `OSB_QUEUE` | `-queue` | | Queues to create. `orders,payments` |
| `OSB_TOPIC` | `-topic` | | Topics to create. `events` |
| `OSB_SUBSCRIPTION` | `-subscription` | | Subscriptions as `topic/name`. `events/audit,events/billing` |
| `OSB_CONFIG` | `-config` | | Config.json path. `/config/Config.json` |
| `OSB_TERRAFORM` | `-terraform` | | Terraform directory or `.tf` file. `/infra` |
| `OSB_TF_VAR` | `-tf-var` | | `name=value`, one per line |
| `OSB_TF_VAR_FILE` | `-tf-var-file` | | `.tfvars` paths, one per line |
| `OSB_TF_MODULE_MAP` | `-tf-module-map` | | JSON file naming module argument lists |
| `OSB_TF_DROP_DEFAULT_RULE` | `-tf-drop-default-rule` | `false` | Remove `$Default` from subscriptions with rules |
| `OSB_EXPORT` | `-export` | | Write Config.json and main.tf to this directory and exit |
| `OSB_AUTH` | `-auth` | `none` | `none`, `sas` or `full` |
| `OSB_DISABLE_LOCAL_AUTH` | `-disable-local-auth` | `false` | Reject shared access signatures |
| `OSB_DEV_ISSUER` | `-dev-issuer` | `false` | With `full`, issue test tokens at `/$auth/token` |
| `OSB_OIDC_ISSUER` | `-oidc-issuer` | any | With `full`, the accepted token issuer |
| `OSB_OIDC_JWKS` | `-oidc-jwks` | | With `full`, URL of the signing keys |
| `OSB_TLS_CERT` / `OSB_TLS_KEY` | `-tls-cert` / `-tls-key` | self-signed | PEM files for your own certificate |
| `OSB_TLS_CERT_OUT` | `-tls-cert-out` | | Write the generated certificate here |
| `OSB_TLS_HOSTS` | `-tls-hosts` | | Extra names/IPs for the generated certificate. `servicebus,10.0.0.5` |
| `OSB_PORTAL` | `-portal` | `true` | Serve the portal |
| `OSB_PROMETHEUS` | `-prometheus` | `false` | Serve `/$metrics` |
| `OSB_METRICS_RETENTION` | `-metrics-retention` | `720h` | How long metrics and the activity log are kept |
| `OSB_PARTITIONS` | `-partitions` | `16` | Partitions of partitioned entities |
| `OSB_MAX_BATCH_DELETE` | `-max-batch-delete` | `4000` | Most messages one purge removes |
| `OSB_MAX_MESSAGE_SIZE` | `-max-message-size` | `104857600` | Largest message, in bytes |
| `OSB_TXN_TIMEOUT` | `-txn-timeout` | `2m` | AMQP transaction timeout |
| `OSB_MCP_ADDR` | `-mcp-addr` | off | MCP endpoint address. `127.0.0.1:5680`; inside a container `0.0.0.0:5680` |
| `OSB_MCP_READONLY` | `-mcp-readonly` | `false` | Only tools that read |
| `OSB_MCP_ALLOW_DATA` | `-mcp-allow-data` | `false` | Tools that read, send and delete messages |
| `OSB_OTEL_ENDPOINT` | `-otel-endpoint` | off | OpenTelemetry (OTLP/HTTP) receiver base URL; see [Observability](observability.md) |
| `OSB_OTEL_SIGNALS` | `-otel-signals` | `traces,metrics,logs` | What to export |
| `OSB_OTEL_SAMPLE` | `-otel-sample` | `1` | Fraction of traces recorded |
| `OSB_OTEL_METRIC_INTERVAL` | `-otel-metric-interval` | `10s` | Metric export interval |
| `OSB_OTEL_SERVICE_NAME` | `-otel-service-name` | `open-service-bus` | `service.name` |
| `OSB_LOG_LEVEL` | `-log-level` | `info` | `debug`, `info`, `warn`, `error` |
| `OSB_PPROF` | `-pprof` | off | Profiling address. `localhost:6060` |

`-version` has no variable. Terraform also reads its own `TF_VAR_<name>`
variables when evaluating files.

### Example

```bash
docker run --rm -p 5672:5672 \
  -e OSB_CONFIG=/config/Config.json -v "$PWD/Config.json:/config/Config.json:ro" \
  -e OSB_DB=/data/osb.db -v osb-data:/data \
  -e OSB_AUTH=sas \
  -e OSB_LOG_LEVEL=warn \
  -e OSB_QUEUE=extra \
  ghcr.io/igorcesarcode/open-service-bus-emulador:latest
```

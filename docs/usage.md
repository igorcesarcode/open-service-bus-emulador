# Usage guide

This guide covers running the emulator, declaring entities, connecting the
official SDKs, authentication and the portal. For the file formats see
[Configuration](configuration.md).

## Starting the emulator

```bash
open-service-bus [flags]
```

With no flags it listens on `:5672`, keeps state in memory, and has no
entities (create them at run time with an SDK or the portal, or declare them
at startup).

```bash
open-service-bus -queue orders -topic events -subscription events/audit
```

Run `open-service-bus -help` for every flag.

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-listen` | `:5672` | TCP address shared by every protocol (AMQP, AMQP over TLS, AMQP over WebSocket, HTTP and HTTPS management API, portal) |
| `-amqp-addr` | off | Extra address serving AMQP/AMQPS alone |
| `-http-addr` | off | Extra address serving HTTP/HTTPS alone (management API, WebSocket) |
| `-db` | in memory | SQLite database file. Empty keeps state in memory and loses it at exit |
| `-namespace` | first declared, else `emulator` | Name of the default namespace |
| `-queue`, `-topic` | | Create queues/topics with the Service Bus defaults. Repeatable or comma separated |
| `-subscription` | | Create a subscription as `topic/name`. Repeatable or comma separated |
| `-config` | | Config.json file ([format](configuration.md#configjson)) |
| `-terraform` | | Terraform directory or `.tf` file ([details](configuration.md#terraform)) |
| `-tf-var`, `-tf-var-file` | | Terraform variables (`name=value`) and `.tfvars` files |
| `-tf-module-map` | | JSON file naming the arguments of modules whose source is unavailable |
| `-tf-drop-default-rule` | off | Drop the `$Default` rule of Terraform subscriptions that declare rules |
| `-export` | | Write the loaded state as `Config.json` and `main.tf` to this directory and exit |
| `-auth` | `none` | `none`, `sas` or `full` ([authentication](#authentication)) |
| `-disable-local-auth` | off | Reject shared access signatures, as `local_auth_enabled = false` does |
| `-oidc-issuer`, `-oidc-jwks`, `-dev-issuer` | | Entra ID token validation with `-auth full` |
| `-tls-cert`, `-tls-key` | self-signed | Your own certificate and key |
| `-tls-cert-out` | | Write the generated certificate to this file |
| `-tls-hosts` | | Extra host names or IPs for the generated certificate |
| `-partitions` | `16` | Partitions of a partitioned queue or topic |
| `-max-batch-delete` | `4000` | Most messages one purge request removes |
| `-max-message-size` | 100 MB | Largest accepted message, in bytes |
| `-txn-timeout` | `2m` | How long an AMQP transaction may stay open |
| `-portal` | `true` | Serve the web portal at `/$portal/` |
| `-prometheus` | off | Serve Prometheus metrics at `/$metrics` |
| `-metrics-retention` | `720h` | How long metrics and the activity log are kept |
| `-mcp-addr`, `-mcp-readonly`, `-mcp-allow-data` | off | [MCP server](MCP.md) (also `OSB_MCP_*` environment variables) |
| `-log-level` | `info` | `debug`, `info`, `warn`, `error` |
| `-log-events` | off | One short line on stdout for each message event; see [Following the messages](#following-the-messages) |
| `-pprof` | off | Serve Go profiling on this address, such as `localhost:6060` |
| `-version` | | Print the version and exit |

Every flag also has an `OSB_*` environment variable (`-db` is `OSB_DB`), which is
how containers are configured; see [Configuration](configuration.md#environment-variables).

### Ports

Everything shares one port (`-listen`, default 5672). The emulator looks at
the first bytes of each connection to tell AMQP, TLS and HTTP apart. Use
`-amqp-addr` and `-http-addr` only if you want separate ports.

### Persistence

- `-db orders.db` keeps state across restarts. Messages, entities created at
  run time, rules, policies, metrics and the activity log are stored.
- Without `-db`, nothing is written to disk.
- Declared entities (flags, Config.json, Terraform) are **reconciled at every
  start**: created if missing, updated to the declared properties if they
  exist. Entities that exist only in the database are kept.
- Each additional namespace gets its own SQLite file next to `-db`.

Delete the `.db`, `.db-wal` and `.db-shm` files to start clean.

## Connecting the SDKs

The startup log prints a connection string like this:

```
Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true
```

`UseDevelopmentEmulator=true` is understood by the official SDKs: the
messaging clients connect to the given host and port over plain AMQP, with no
TLS. With `-auth none` the key is not checked, so the value can be anything.
The same connection string works with the official emulator, which makes
switching between them a configuration change.

The **administration clients** (create/delete/list entities) use the HTTP
management API, and how they do it depends on the SDK:

| SDK | Administration client in emulator mode |
| --- | --- |
| JavaScript | plain HTTP on the connection string's port; nothing to configure |
| Go | always HTTPS; the certificate is self-signed, so pass an HTTP transport that skips verification (or trusts the certificate written by `-tls-cert-out`) |
| .NET | always HTTPS on port 443; pass a transport that trusts the certificate and sends the requests to the emulator's port |

The examples show each of these.

### Go

```go
client, err := azservicebus.NewClientFromConnectionString(cs, nil)
adminClient, err := admin.NewClientFromConnectionString(cs, &admin.ClientOptions{
	ClientOptions: azcore.ClientOptions{Transport: insecureHTTPClient}, // see examples/go-sdk
})
```

### JavaScript

```js
const client = new ServiceBusClient(cs);
const admin = new ServiceBusAdministrationClient(cs);
```

### .NET

```csharp
var client = new ServiceBusClient(cs);
// The administration client needs a custom transport; see examples/dotnet-sdk.
```

Complete programs are in [`examples/`](../examples/README.md).

### Connecting without `UseDevelopmentEmulator`

Clients can also connect as they do to Azure, over TLS, using a
`<namespace>.servicebus.windows.net` style host. The TLS certificate is
self-signed: either pass it to the client (`-tls-cert-out cert.pem`, then
for Node `NODE_EXTRA_CA_CERTS=cert.pem`) or give the emulator your own with
`-tls-cert` and `-tls-key`. Use `-tls-hosts` to add names to the generated
certificate. The project's own test suite verifies this mode too.

### From another machine or container

Use the host name of the machine in the connection string and make sure the
port is reachable. Inside Docker Compose, use the service name
(`Endpoint=sb://emulator:5672;...;UseDevelopmentEmulator=true`). The
emulator listens on all interfaces by default (`:5672`); use
`-listen 127.0.0.1:5672` to restrict it to the local machine.

## Declaring entities

Three sources, which can be combined:

1. **Flags**: `-queue`, `-topic`, `-subscription topic/name`. Defaults are the
   Service Bus ones.
2. **Config.json** (`-config`): the format of the official emulator, with
   every property free of range limits.
3. **Terraform** (`-terraform`): the `azurerm_servicebus_*` resources of your
   real infrastructure.

Entities can also be created at run time through any administration client,
the portal, or the MCP server.

When `-config` and `-terraform` define the same entity, the properties of
`-config` win.

## Several namespaces

The first label of the host selects the namespace: `orders.servicebus.windows.net`
and `orders.localhost` reach the namespace `orders`. A host with no dot, or an
IP address (`localhost`, `127.0.0.1`) reaches the default one. Declare several
namespaces in Config.json (or several `azurerm_servicebus_namespace`
resources in Terraform).

```
Endpoint=sb://orders.localhost:5672;...;UseDevelopmentEmulator=true
```

Geo-DR aliases and Geo-Replication can be declared in Config.json or set up in
the portal.

## Authentication

| Mode | Behavior |
| --- | --- |
| `-auth none` (default) | Nothing is validated. Any connection string works. |
| `-auth sas` | Shared access signatures (policy name and key, or SAS tokens) are validated against the namespace and entity policies and their rights (Listen, Send, Manage). |
| `-auth full` | SAS plus Microsoft Entra ID (JWT) tokens, with role assignments (Data Owner, Sender, Receiver). Configure with `-oidc-issuer` and `-oidc-jwks`, or use `-dev-issuer` to have the emulator issue test tokens at `/$auth/token` and publish their keys at `/$auth/jwks.json`. |

`-disable-local-auth` rejects SAS entirely, as `local_auth_enabled = false`
does in Azure.

Policies and role assignments are declared in Config.json or Terraform, or
managed in the portal. A policy declared without keys gets deterministic keys
derived from its name, so tests can compute them.

## Portal

`https://localhost:5672/$portal/` (disable with `-portal=false`). It offers:

- **Overview** of the namespace, queues, topics and subscriptions, with
  message counts (active, dead-letter, scheduled, deferred).
- **Service Bus Explorer**: send messages (body, properties, session,
  scheduling, TTL), peek, receive with lock, complete/abandon/dead-letter/
  defer, purge, resend from the dead-letter queue.
- **Metrics** and **activity log**.
- **Shared access policies**, **access control (IAM)**, **locks**, **tags**.
- **Geo-Recovery** and **Geo-Replication**.
- **Export template**: download the current state as `Config.json` or
  `main.tf`.

With authentication on, the portal asks for the name and key of a policy that
has Manage.

## Monitoring

- `-prometheus` serves metrics in the Prometheus format at `/$metrics`.
- The portal shows time series (requests, messages in and out, sizes,
  connections).
- `-log-level debug` logs protocol-level detail.
- `-log-events` logs what happens to each message; see below.
- `-pprof localhost:6060` exposes Go profiling data.

### Following the messages

`-log-events` writes one line to stdout for each thing that happens to a
message, so you can follow a flow without a debugger or a tracing backend:

```
11:35:33.334 send         events seq=1 bytes=76 id=order-1
11:35:33.334 filter       events/Subscriptions/big rule=big matched=true id=order-1 filter="quantity > 10"
11:35:33.334 enqueue      events/Subscriptions/big seq=1 bytes=76 id=order-1
11:35:33.341 receive      events/Subscriptions/big seq=1 id=order-1 lock=c536782f delivery=1 mode=peek-lock subqueue=active
11:35:33.347 abandon      events/Subscriptions/big seq=1 id=order-1 delivery=1
11:35:33.350 dead-letter  events/Subscriptions/big seq=1 id=order-1 reason=Poison delivery=2
```

| Event | When |
| --- | --- |
| `send` | A message was accepted on a queue or topic (`scheduled=` for a scheduled one) |
| `filter` | A subscription rule was evaluated: its name, the SQL filter, `matched=` and `error=` if it failed |
| `enqueue` | A copy was stored on a subscription, or on the destination of a forward |
| `forward` | A message was passed on to the entity in `ForwardTo` |
| `duplicate` | A message was dropped by duplicate detection |
| `receive` | A message was delivered: the start of its lock token, the delivery count, `mode=peek-lock` or `receive-and-delete` |
| `complete`, `abandon`, `release`, `defer` | The receiver settled a message |
| `dead-letter` | A message moved to the dead-letter queue, with the `reason=` |
| `expire` | A message's time to live passed |
| `activate` | A scheduled message became available |
| `lock-expired` | Locks lapsed and their messages became available again |
| `renew-lock`, `cancel-scheduled` | Other operations on locked or scheduled messages |

`entity` is the queue, or `topic/Subscriptions/name`; `seq` is the sequence
number and `id` the MessageId, the same values the SDKs show. With more than
one namespace each line also has `ns=`. A line is written once the change is
committed, so a rolled-back operation leaves none.

The lines never include the body or the properties of a message, and they go
to stdout, apart from the diagnostics on stderr (`-log-level`). Redirect them
to a file, or `grep id=order-1` for the history of one message. The same
information, with timings, is available as traces with `-otel-endpoint`.

## Export the current state

```bash
open-service-bus -db orders.db -export ./out
```

writes `./out/Config.json` and `./out/main.tf` describing everything loaded
(from `-db`, `-config` and `-terraform`) and exits. Useful to turn entities
created by hand into a declaration you can commit.

## Managing from an AI agent

The optional [MCP server](MCP.md) lets agents such as Claude Code manage the
emulator. It is off by default.

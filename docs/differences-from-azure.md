# Differences from Azure and from the official emulator

open-service-bus emulates the data plane (AMQP 1.0) and the management plane
(ATOM) of Azure Service Bus closely enough for the official SDKs, but it is an
emulator. This page lists what is different or missing.

## Compared with the official Azure Service Bus emulator

| | Official emulator | open-service-bus |
| --- | --- | --- |
| Distribution | Docker containers (emulator + SQL Server) | single binary; SQLite |
| Entity property ranges | limited (e.g. lock duration, TTL, delivery count) | none: any value |
| Number of entities | capped | not capped |
| Configuration | `Config.json` | `Config.json` (same format, plus extensions), Terraform, flags, portal, SDK admin clients, MCP |
| Management API (create/update/delete entities at run time) | not supported | supported (ATOM) |
| Several namespaces | one | many, plus Geo-DR and Geo-Replication |
| Authentication | none | none, SAS, SAS + Entra ID tokens |
| Portal | none | built in |

## Compared with Azure

**Left out, and said so.** A configuration that declares one of these loads
without it and logs `level=WARN unsupported=<id>`; the portal lists them under
*Not supported*:

- Private Link / private endpoints, IP and virtual network rules;
- JMS client-scoped subscriptions;
- the portal flows for migrating to Premium, tasks, diagnostics and support
  requests.

**Emulated differently:**

- **Resource locks, tags and encryption** are settings of the emulator. Locks
  stop changes of entities and rules with the Azure `ScopeLocked` error, never
  messages. Encryption is only recorded: the SQLite file is **not** encrypted.
- **Geo-Replication** copies by replaying statements to a second SQLite store;
  the regions are labels. **Geo-DR** aliases and failover are emulated at the
  naming level.
- **Sequence numbers** carry no partition id.
- **Partitioning** is emulated with the number of partitions in `-partitions`
  (the size quota is `MaxSizeInMegabytes` times that).
- **Throughput, throttling and quotas** of the real service (messaging units,
  connection limits, 429 responses) are not modeled.
- **Metrics** are the emulator's own time series, not Azure Monitor.
- **Time**: the emulator uses the host clock.
- **TLS**: the certificate is self-signed.
- **Identity**: with `-auth full`, token validation uses the issuer and keys
  you configure (or the built-in test issuer); real Entra ID is not contacted
  unless you point `-oidc-jwks` at it.

**Not in scope:** the Event Hubs and Relay services, Azure Resource Manager
APIs as such (the Terraform reader reads declarations, it does not call ARM),
and anything that needs Azure itself.

## Reporting a difference

If a supported SDK behaves differently against the emulator than against
Azure, please open an issue with a minimal program, the SDK and version, and
what Azure does.

# Flags only

No files needed: declare entities on the command line.

```bash
# Queues, a topic and a subscription (topic/name), state in memory
open-service-bus -queue orders,payments -topic events -subscription events/audit

# Keep state across restarts in a SQLite file
open-service-bus -db ./osb.db -queue orders

# Another port, and write the TLS certificate for clients that validate it
open-service-bus -listen :6000 -tls-cert-out ./osb-cert.pem -queue orders

# Turn what is loaded (or created later through the portal/SDK) into files
open-service-bus -db ./osb.db -export ./exported     # writes Config.json and main.tf
```

Entities created with the flags have the Service Bus defaults (lock duration
1 minute, max delivery count 10, TTL practically unlimited, ...). For other
values use [config-json](../config-json) or the administration client.

Open the portal at <https://localhost:5672/$portal/>.

[`run.sh`](run.sh) and [`run.ps1`](run.ps1) are the same first command for
Linux/macOS and Windows.

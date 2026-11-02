# How it works

This is an overview of the design.

## The big picture

```
 SDK / client                         open-service-bus (one process)
 ───────────                          ─────────────────────────────────────────
 AMQP 1.0  ──┐                        ┌─ protocol sniffing on one TCP port
 AMQPS     ──┤                        │    AMQP / TLS / HTTP
 AMQP/WS   ──┼──  :5672  ─────────────┤
 ATOM (HTTP)─┤                        ├─ AMQP 1.0 engine (own implementation)
 Portal    ──┘                        ├─ Service Bus semantics (links ↔ broker)
 MCP (own port, optional)             ├─ broker: entities, rules, locks, sessions
                                      └─ SQLite (one file per namespace, or memory)
```

- **One port, several protocols.** The listener looks at the first bytes of each
  connection: `AMQP` starts AMQP (with or without SASL), a TLS handshake starts
  TLS (then AMQP or HTTP), anything else is HTTP. AMQP over WebSocket lives at
  `/$servicebus/websocket`. You can add dedicated ports with `-amqp-addr` and
  `-http-addr`.
- **AMQP 1.0 is implemented from scratch** (codec, framing, sessions, links,
  SASL, transactions) together with the Service Bus extensions the SDKs rely
  on: claims-based security on `$cbs`, the `$management` node, session
  filters, batch message format, dead-letter conditions.
- **The broker** maps AMQP operations to Service Bus behavior: send, peek-lock
  and receive-and-delete, complete/abandon/defer/dead-letter, lock renewal,
  sessions and session state, scheduled messages, duplicate detection,
  auto-forwarding, topic fan-out through subscription rules, TTL expiry.
- **The management (ATOM) API** is what the administration clients speak to
  create, read, update, delete and list queues, topics, subscriptions and
  rules. It follows the wire format captured from the official SDKs
  ([atom-wire-spec.md](atom-wire-spec.md)).
- **The portal** (Go, Templ and HTMX) and the optional **MCP server** call the
  same broker as the protocols do.

## Storage

State is stored in SQLite (pure-Go driver, no CGO): entities and their
properties as JSON, rules, messages, sessions, duplicate-detection history,
sequence numbers, metrics and the activity log.

- Messages are kept as the **original AMQP bytes**, so a receiver gets exactly
  what the sender sent, with property types intact.
- A send to a topic evaluates every subscription's rules and writes one row per
  accepting subscription, in one transaction.
- Locks are also kept in memory to wake waiting receivers; the database is the
  source of truth.
- Without `-db` everything is in memory and disappears at exit.
- Every namespace has its own database file.

## Delivery semantics (what to expect)

| Behavior | In the emulator |
| --- | --- |
| Peek-lock | The message is locked for `LockDuration`; complete, abandon, defer or dead-letter settles it; an expired lock makes it available again and increments the delivery count |
| Max delivery count | After `MaxDeliveryCount` deliveries the message is dead-lettered with reason `MaxDeliveryCountExceeded` |
| TTL | Expired messages are dropped, or dead-lettered if `DeadLetteringOnMessageExpiration` is on |
| Sessions | Messages with one `SessionId` go to one receiver at a time, in order; session state is stored |
| Scheduling | `ScheduledEnqueueTime` and `scheduleMessages` hold a message until its time; scheduled messages can be cancelled |
| Duplicate detection | A repeated `MessageId` inside the history window is dropped silently |
| Dead-letter queues | `<entity>/$deadletterqueue` and `$Transfer/$deadletterqueue`; reason and description are kept |
| Auto-forwarding | `ForwardTo` and `ForwardDeadLetteredMessagesTo` chains are followed |
| Transactions | AMQP transactions (declare, discharge) work across sends and settlements |
| Batch delete / purge | Up to `-max-batch-delete` messages per request |

## Entities at startup

1. The SQLite database (if any) is opened and its entities are loaded.
2. Declared entities (flags, `-config`, `-terraform`) are **reconciled**:
   created if absent, updated to the declared properties otherwise.
3. Unsupported features found in the declarations (network rules, private
   endpoints, ...) are logged as `level=WARN unsupported=<id>` and ignored.
4. The listeners start and the connection string is printed.

## Several namespaces

The namespace is selected by the first label of the host name the client uses
(`orders.localhost`, `orders.servicebus.windows.net`). Each has isolated
entities, policies, metrics and a database file. Geo-DR aliases make a name
point to a primary namespace and can be failed over; Geo-Replication copies
data to a second store by replaying statements.

## Testing against it is the point

The emulator is meant to make integration tests easy: start fresh per test run
(in memory), declare entities from your Terraform, and use the same SDK code
you ship. See [Tips](tips.md).

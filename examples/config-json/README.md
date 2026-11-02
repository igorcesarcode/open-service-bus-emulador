# Config.json

[`Config.json`](Config.json) declares:

| Entity | What it demonstrates |
| --- | --- |
| queue `orders` | defaults |
| queue `jobs` | lock duration, max delivery count 3, TTL 14 days, dead-lettering on expiration, dead letters forwarded to `jobs-poison` |
| queue `replies` | sessions (`RequiresSession`) |
| queue `payments` | duplicate detection with a 1 hour window |
| topic `events` | four subscriptions: no rule, a SQL filter (`quantity > 10`), a correlation filter (`Subject` + a custom property) and a SQL rule with an action |

```bash
open-service-bus -config examples/config-json/Config.json
```

The format is that of the official emulator's `Config.json` (including `//`
and `/* */` comments), with two differences:

- no property is restricted to a range (a lock duration of 2 seconds, a TTL of
  10 years, 100000 entities, ... are all accepted);
- extra fields: `AuthorizationRules`, `RoleAssignments`, `Locks`, `Tags`,
  `Encryption`, `GeoReplication` per namespace, and `GeoDR` at the top. See
  [Configuration](../../docs/configuration.md).

Run the [Go](../go-sdk), [Node](../node-sdk) or [.NET](../dotnet-sdk) example
against it.

# Several namespaces

[`Config.json`](Config.json) declares the namespaces `orders` and `billing`,
both with a queue called `incoming`. The first label of the host selects the
namespace (`orders.localhost`, `billing.localhost`), so the same queue name
holds different messages in each.

```bash
open-service-bus -config examples/multi-namespace/Config.json   # terminal 1
go run ./examples/multi-namespace                               # terminal 2, repository root
```

Each namespace has its own SQLite file next to `-db`. A plain host
(`localhost`, an IP) reaches the default namespace, the first declared unless
`-namespace` says otherwise.

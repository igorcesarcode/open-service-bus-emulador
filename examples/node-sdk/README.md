# JavaScript SDK

[`index.mjs`](index.mjs) uses `@azure/service-bus` for: a queue round trip,
topic routing with a subscription rule, a scheduled message and the
administration client.

```bash
open-service-bus -config examples/config-json/Config.json    # terminal 1
cd examples/node-sdk && npm install && npm start             # terminal 2
```

Output:

```
queue orders: received {"id":1}
events/big-orders: received "order of 50"
queue orders: scheduled message arrived: later
admin: created queue node-sdk-demo, lock duration PT5S
```

In emulator mode the JavaScript administration client uses plain HTTP on the
connection string's port, so nothing else needs configuring.

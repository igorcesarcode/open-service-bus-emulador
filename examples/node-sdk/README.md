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

With `@azure/service-bus` 7.10.0 or newer (what this example installs), the
administration client uses plain HTTP on the connection string's port in
emulator mode, so nothing else needs configuring. Older versions (7.9.5 and
before) always use HTTPS: start the emulator with `-tls-cert-out cert.pem` and
run Node with `NODE_EXTRA_CA_CERTS=cert.pem`, or the client fails with
`self-signed certificate`.

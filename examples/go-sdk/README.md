# Go SDK

[`main.go`](main.go) uses `github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus`
to run four scenarios against the entities of [config-json](../config-json):

1. send to the queue `orders` and receive;
2. publish to the topic `events` and receive from the subscription with the
   rule `quantity > 10`;
3. dead-letter a message and read it from the queue it is forwarded to;
4. create, read and delete a queue with the `admin` client, using a lock
   duration of 5 seconds that Azure would refuse.

```bash
open-service-bus -config examples/config-json/Config.json    # terminal 1
go run ./examples/go-sdk                                     # terminal 2, repository root
```

Output:

```
queue orders: received {"id": 1}
events/big-orders: received "order of 50"
queue jobs-poison: received "bad job"
admin: created queue go-sdk-demo, lock duration PT5S
```

Notes:

- The messaging client works with the connection string as printed.
- The Go **administration client always uses HTTPS** and the emulator's
  certificate is self-signed, so the example passes an HTTP transport that
  skips verification. In your own tests you can instead write the certificate
  with `-tls-cert-out` and trust it.

# Tips and troubleshooting

## Tips for tests and CI

- **Start fresh, in memory.** Without `-db` every start is clean, so tests do
  not depend on leftovers. Use `-db` only when you want persistence across
  restarts.
- **Reuse your production definition.** `-terraform ./infra` creates the same
  queues, topics, subscriptions and filters your services use in Azure, so
  routing bugs show up locally.
- **One emulator per test run, one queue per test.** Entities are cheap and
  there is no limit on how many you create. Unique queue names (or a
  fresh emulator on a free port per test, see
  [`examples/go-test-helper`](../examples/go-test-helper)) make tests
  independent and parallel-safe.
- **Shorten timings to test failure paths.** The emulator accepts values Azure
  refuses: a lock duration of `PT2S` and a TTL of `PT1S` make redelivery and
  expiry tests fast. Set a small `MaxDeliveryCount` to reach the dead-letter
  queue quickly.
- **Wait for the port, not a sleep.** The emulator is ready when 5672 accepts
  connections.
- **Inspect with the portal.** `https://localhost:5672/$portal/` shows message
  counts and lets you peek into the dead-letter queue while a test is paused.
- **Use the activity log and `-log-level debug`** to see what a client really
  did.
- **Seed with `-export`.** Create entities by hand in the portal, run
  `-export`, commit the `Config.json`.
- **Let an agent help.** With the [MCP server](MCP.md) an assistant can create
  entities, test a rule against a sample message (`test_rule`), and explain
  why messages ended up in a dead-letter queue.

## Connection string cheat sheet

| Situation | Endpoint |
| --- | --- |
| Local process | `sb://localhost:5672` |
| Another container in Compose | `sb://<service-name>:5672` |
| From a container to the host | `sb://host.docker.internal:5672` |
| Second namespace `billing` | `sb://billing.localhost:5672` |

Always append `;UseDevelopmentEmulator=true`.

## Troubleshooting

**`x509: certificate signed by unknown authority` (Go admin client, HTTPS).**
The management API uses the emulator's self-signed certificate. Pass an HTTP
transport that skips verification in tests, or write the certificate with
`-tls-cert-out cert.pem` and trust it: `SSL_CERT_FILE=cert.pem` on Linux,
`NODE_EXTRA_CA_CERTS=cert.pem` for Node, or install it in the system store on
macOS and Windows. For another host name, add it with `-tls-hosts`.

**The application hangs or retries forever, and the emulator logs nothing.**
Without `UseDevelopmentEmulator=true` the SDK uses TLS on port **5671** unless
the `Endpoint` names a port, and the emulator listens on 5672. Add the port
(`Endpoint=sb://servicebus:5672/;...`), or run the emulator with
`-amqp-addr :5671` and publish that port. See
[Connecting without `UseDevelopmentEmulator`](usage.md#connecting-without-usedevelopmentemulator).

**`RestError: self-signed certificate` (JavaScript admin client).**
`@azure/service-bus` 7.9.5 and older always call the management API over
HTTPS, ignoring `UseDevelopmentEmulator`. Update to 7.10.0, where emulator mode
uses plain HTTP, or trust the certificate as above
(`NODE_EXTRA_CA_CERTS=cert.pem`).

**Connection refused / timeouts.** Check the port in the startup log; check
the firewall on Windows; inside containers do not use `localhost` for another
container.

**`UnauthorizedAccess`.** You started with `-auth sas` or `-auth full`. Use the
name and key of a policy that has the needed right, or run with `-auth none`.

**Entity not found.** Entities are not auto-created. Declare them (flags,
Config.json, Terraform) or create them with an administration client. With
several namespaces, check the host name selects the namespace you expect.

**Messages stay locked / reappear.** The lock lasts `LockDuration` (default
one minute). Complete messages, or lower the lock duration for tests.

**A subscription receives everything although it has a filter.** Azure keeps
the `$Default` rule (`1=1`) when other rules are added; so does the emulator.
Delete `$Default` explicitly or start with `-tf-drop-default-rule` for
Terraform.

**Database is locked / state from an old run.** Stop other emulators using the
same `.db` file, or delete the `.db`, `.db-wal` and `.db-shm` files.

**macOS refuses to run the binary.** Remove the quarantine attribute:
`xattr -d com.apple.quarantine open-service-bus`.

**An SDK feature behaves differently from Azure.** See
[Differences from Azure](differences-from-azure.md), and open an issue with a
minimal reproduction.

## Performance

The default settings fit tests. For heavy loads: keep state in memory, avoid
`-log-level debug`, and use `-pprof localhost:6060` to profile.

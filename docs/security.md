# Security notes

**open-service-bus is a testing tool. Do not use it in production, and do not
expose it to untrusted networks.**

## Defaults

- **Authentication is off** (`-auth none`): any client, with any key, can do
  anything. Use `-auth sas` or `-auth full` to exercise your authorization
  paths in tests; they are not a hardened implementation.
- **It listens on all interfaces** (`:5672`). To keep it on the local machine
  use `-listen 127.0.0.1:5672`.
- **TLS uses a self-signed certificate** generated at startup (or the one you
  give with `-tls-cert`/`-tls-key`). Plain AMQP and HTTP are also accepted on
  the same port.
- **Data is not encrypted at rest.** The SQLite files hold message bodies in
  clear text, and the `Encryption` setting is only recorded.
- The portal and management API follow the authentication mode; with `-auth
  none` they are open.

## MCP server

The [MCP server](MCP.md) is off by default and has **no login**: whoever
reaches its port manages the emulator. Bind it to loopback
(`-mcp-addr 127.0.0.1:5680`). Requests from browsers with a non-loopback
`Origin` are refused, which blocks other sites and DNS rebinding. Tools that
touch messages need `-mcp-allow-data`, and tools that delete data need
`confirm: true`. Message bodies reach the agent as untrusted data.

## Profiling

`-pprof` exposes Go profiling data without authentication; use a loopback
address.

## Secrets in test data

Do not load real secrets or production messages into the emulator. The
Terraform reader evaluates your files locally and never contacts Azure, but
variables you pass (`-tf-var`) are visible in the process list.

## Reporting a vulnerability

Please do not open a public issue for a security problem. Use GitHub's
private vulnerability reporting for this repository (Security tab → *Report a
vulnerability*), or contact the maintainer directly.

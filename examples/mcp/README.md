# MCP (AI agents)

The MCP server is **off by default**. Turn it on with a flag or environment
variables; it listens on its own port and has no login, so keep it on loopback.

```bash
# flag
open-service-bus -queue orders -mcp-addr 127.0.0.1:5680

# or environment (handy in containers); read-only, no message access:
OSB_MCP_ADDR=127.0.0.1:5680 OSB_MCP_READONLY=true open-service-bus -queue orders
```

Register it in Claude Code:

```bash
claude mcp add --transport http osb http://127.0.0.1:5680/mcp
```

or commit [`.mcp.json`](.mcp.json) in your project. Then ask the agent things
like *"create a topic `invoices` with a subscription that only takes
`amount > 1000`"*, *"why are there dead-lettered messages in `orders`?"* or
*"export the current configuration as Terraform"*. Tools that read, send or
delete **messages** exist only with `-mcp-allow-data`; tools that delete data
need `confirm: true`. Full reference: [MCP server](../../docs/MCP.md).

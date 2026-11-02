// Validates the MCP management endpoint of open-service-bus with the official
// @modelcontextprotocol/sdk client, and checks its effects with the official
// @azure/service-bus SDK.
//
// Run through the Go test TestNodeMCP, which starts the emulator and sets
// OSB_CONNECTION_STRING, OSB_MCP_URL (every tool) and OSB_MCP_READONLY_URL.
import { test, after, before } from "node:test";
import assert from "node:assert/strict";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { ServiceBusAdministrationClient, ServiceBusClient } from "@azure/service-bus";

const connectionString = process.env.OSB_CONNECTION_STRING;
const mcpUrl = process.env.OSB_MCP_URL;
const readonlyUrl = process.env.OSB_MCP_READONLY_URL;
if (!connectionString || !mcpUrl || !readonlyUrl) {
  throw new Error("OSB_CONNECTION_STRING, OSB_MCP_URL or OSB_MCP_READONLY_URL is not set; run through `go test ./test/integration -run TestNodeMCP`");
}

async function connect(url) {
  const client = new Client({ name: "osb-js-test", version: "1.0.0" });
  await client.connect(new StreamableHTTPClientTransport(new URL(url)));
  return client;
}

let mcp;
let readonly;
const sb = new ServiceBusClient(connectionString, { retryOptions: { maxRetries: 1, retryDelayInMs: 100, timeoutInMs: 10000 } });
const admin = new ServiceBusAdministrationClient(connectionString);

before(async () => {
  mcp = await connect(mcpUrl);
  readonly = await connect(readonlyUrl);
});
after(async () => {
  await mcp?.close();
  await readonly?.close();
  await sb.close();
});

// call runs a tool that must succeed and returns what it reports.
async function call(name, args = {}, client = mcp) {
  const res = await client.callTool({ name, arguments: args });
  assert.notEqual(res.isError, true, `${name}(${JSON.stringify(args)}) failed: ${res.content?.[0]?.text}`);
  return res.structuredContent;
}

// fails runs a tool that must report an error holding want.
async function fails(name, args, want) {
  const res = await mcp.callTool({ name, arguments: args });
  assert.equal(res.isError, true, `${name}(${JSON.stringify(args)}) succeeded`);
  assert.match(res.content[0].text, want);
}

async function send(entity, ...messages) {
  const sender = sb.createSender(entity);
  try {
    await sender.sendMessages(messages);
  } finally {
    await sender.close();
  }
}

async function receive(receiver, n) {
  const out = [];
  const deadline = Date.now() + 15000;
  while (out.length < n) {
    assert.ok(Date.now() < deadline, `received ${out.length}/${n} messages before the deadline`);
    out.push(...(await receiver.receiveMessages(n - out.length, { maxWaitTimeInMs: 3000 })));
  }
  return out;
}

test("the server describes itself and its tools", async () => {
  const info = mcp.getServerVersion();
  assert.equal(info.name, "open-service-bus");
  const caps = mcp.getServerCapabilities();
  assert.ok(caps.tools && caps.resources && caps.prompts);
  const { tools } = await mcp.listTools();
  const byName = Object.fromEntries(tools.map((t) => [t.name, t]));
  for (const name of ["list_entities", "create_queue", "peek_messages", "apply_config", "reset_namespace", "test_rule"]) {
    assert.ok(byName[name], `${name} is not offered`);
  }
  assert.equal(byName.delete_entity.annotations.destructiveHint, true);
  assert.equal(byName.list_entities.annotations.readOnlyHint, true);
  assert.deepEqual(byName.create_queue.inputSchema.required, ["name"]);
  assert.equal(byName.create_queue.inputSchema.properties.name.type, "string");
});

test("read-only mode offers only the tools that read", async () => {
  const { tools } = await readonly.listTools();
  const names = new Set(tools.map((t) => t.name));
  assert.ok(names.has("list_entities") && names.has("peek_messages") && names.has("diff_config"));
  for (const name of ["create_queue", "send_message", "delete_entity", "apply_config", "set_lock"]) {
    assert.ok(!names.has(name), `${name} is offered in read-only mode`);
  }
  await assert.rejects(readonly.callTool({ name: "create_queue", arguments: { name: "x" } }), /unknown tool/);
});

test("entities made through MCP are seen by the administration client", async () => {
  const q = await call("create_queue", { name: "js-mcp-orders", properties: { lockDuration: "PT45M", maxDeliveryCount: 3, maxSizeInMegabytes: 4096 } });
  assert.equal(q.properties.lockDuration, "PT45M");
  const got = await admin.getQueue("js-mcp-orders");
  assert.equal(got.lockDuration, "PT45M");
  assert.equal(got.maxDeliveryCount, 3);
  assert.equal(got.maxSizeInMegabytes, 4096);

  await fails("create_queue", { name: "js-mcp-orders" }, /already exists/);
  await fails("create_queue", { name: "bad", properties: { nope: 1 } }, /unknown property/);
  await call("update_entity", { path: "js-mcp-orders", properties: { maxDeliveryCount: 9 } });
  assert.equal((await admin.getQueue("js-mcp-orders")).maxDeliveryCount, 9);

  await call("create_topic", { name: "js-mcp-events" });
  await call("create_subscription", { topic: "js-mcp-events", name: "audit" });
  const subs = await call("list_entities", { kind: "subscription", topic: "js-mcp-events" });
  assert.deepEqual(subs.items.map((i) => i.path), ["js-mcp-events/Subscriptions/audit"]);
  assert.ok(await admin.getSubscription("js-mcp-events", "audit"));

  await fails("delete_entity", { path: "js-mcp-events" }, /confirm/);
  const del = await call("delete_entity", { path: "js-mcp-events", confirm: true });
  assert.deepEqual(del.deleted, ["js-mcp-events", "js-mcp-events/Subscriptions/audit"]);
  await assert.rejects(admin.getTopic("js-mcp-events"));
});

test("messages sent by the SDK are peeked, and the ones sent by MCP are received", async () => {
  await call("create_queue", { name: "js-mcp-msgs" });
  await send("js-mcp-msgs", {
    body: { id: 7 }, messageId: "m-7", contentType: "application/json", subject: "hello",
    applicationProperties: { n: 3, ok: true, s: "text" },
  });
  const peek = await call("peek_messages", { path: "js-mcp-msgs" });
  assert.equal(peek.count, 1);
  const m = peek.items[0];
  assert.equal(m.messageId, "m-7");
  assert.equal(m.subject, "hello");
  assert.deepEqual(JSON.parse(m.body.text), { id: 7 });
  assert.deepEqual(m.applicationProperties, { n: 3, ok: true, s: "text" });

  await call("purge_entity", { path: "js-mcp-msgs", confirm: true });
  await call("send_message", {
    path: "js-mcp-msgs", body: "from mcp", content_type: "text/plain", message_id: "mcp-1", subject: "s",
    properties: { count: 5, ratio: 1.5, flag: true, name: "x" },
  });
  const receiver = sb.createReceiver("js-mcp-msgs");
  try {
    const [got] = await receive(receiver, 1);
    assert.equal(Buffer.from(got.body).toString(), "from mcp");
    assert.equal(got.messageId, "mcp-1");
    assert.equal(got.subject, "s");
    assert.deepEqual(got.applicationProperties, { count: 5, ratio: 1.5, flag: true, name: "x" });
    await receiver.completeMessage(got);
  } finally {
    await receiver.close();
  }
  await fails("send_message", { path: "missing", body: "x" }, /not found/);
});

test("dead letters are peeked with their reason and requeued", async () => {
  await call("create_queue", { name: "js-mcp-dead" });
  await send("js-mcp-dead", { body: "bad1" }, { body: "bad2" });
  const receiver = sb.createReceiver("js-mcp-dead");
  try {
    for (const msg of await receive(receiver, 2)) {
      await receiver.deadLetterMessage(msg, { deadLetterReason: "Poison", deadLetterErrorDescription: `cannot parse ${msg.body}` });
    }
  } finally {
    await receiver.close();
  }
  const dead = await call("peek_messages", { path: "js-mcp-dead", queue: "deadletter" });
  assert.equal(dead.count, 2);
  assert.equal(dead.items[0].deadLetterReason, "Poison");
  assert.equal(dead.items[0].deadLetterErrorDescription, "cannot parse bad1");

  await fails("requeue_dead_letters", { path: "js-mcp-dead" }, /confirm/);
  const moved = await call("requeue_dead_letters", { path: "js-mcp-dead", confirm: true });
  assert.equal(moved.moved, 2);
  const again = sb.createReceiver("js-mcp-dead");
  try {
    assert.deepEqual((await receive(again, 2)).map((x) => x.body), ["bad1", "bad2"]);
  } finally {
    await again.close();
  }
  assert.equal((await call("get_entity", { path: "js-mcp-dead" })).counts.deadLetter, 0);
});

test("rules made through MCP route the SDK's messages as test_rule predicts", async () => {
  await call("create_topic", { name: "js-mcp-routing" });
  await call("create_subscription", { topic: "js-mcp-routing", name: "red" });
  const path = "js-mcp-routing/Subscriptions/red";
  await call("delete_rule", { path, name: "$Default" });
  await call("add_rule", { path, rule: { name: "red", sql: "color = 'red' AND qty > @min", parameters: { min: 2 }, action: "SET picked = TRUE" } });
  await fails("add_rule", { path, rule: { name: "bad", sql: "color =" } }, /bad/);

  const hit = await call("test_rule", { path, properties: { color: "red", qty: 5 } });
  assert.equal(hit.results[0].matched, true);
  assert.equal(hit.results[0].after.properties.picked, true);
  const miss = await call("test_rule", { path, properties: { color: "red", qty: 1 } });
  assert.equal(miss.results[0].matched, false);

  await send("js-mcp-routing",
    { body: "m1", applicationProperties: { color: "red", qty: 5 } },
    { body: "m2", applicationProperties: { color: "blue", qty: 5 } },
    { body: "m3", applicationProperties: { color: "red", qty: 1 } });
  const receiver = sb.createReceiver("js-mcp-routing", "red");
  try {
    const [m] = await receive(receiver, 1);
    assert.equal(m.body, "m1");
    assert.equal(m.applicationProperties.picked, true);
    await receiver.completeMessage(m);
    assert.equal((await receiver.receiveMessages(1, { maxWaitTimeInMs: 500 })).length, 0);
  } finally {
    await receiver.close();
  }
});

test("a Terraform is compared and applied, and entities are pruned", async () => {
  const content = `
resource "azurerm_servicebus_namespace" "main" {
  name                = "sb-js"
  location            = "East US"
  resource_group_name = "rg"
  sku                 = "Standard"
}
resource "azurerm_servicebus_queue" "inbox" {
  name               = "js-tf-inbox"
  namespace_id       = azurerm_servicebus_namespace.main.id
  lock_duration      = "PT3M"
  max_delivery_count = 12
}
`;
  const diff = await call("diff_config", { format: "terraform", content });
  assert.equal(diff.dryRun, true);
  assert.deepEqual(diff.changes.map((c) => `${c.action} ${c.path}`), ["create js-tf-inbox"]);
  await assert.rejects(admin.getQueue("js-tf-inbox"));

  await call("apply_config", { format: "terraform", content });
  const q = await admin.getQueue("js-tf-inbox");
  assert.equal(q.lockDuration, "PT3M");
  assert.equal(q.maxDeliveryCount, 12);
  const again = await call("diff_config", { format: "terraform", content });
  assert.deepEqual(again.changes.map((c) => c.action), ["unchanged"]);

  await fails("apply_config", { format: "terraform", content, prune: true }, /confirm/);
  const pruned = await call("apply_config", { format: "terraform", content, prune: true, confirm: true });
  assert.ok(pruned.changes.some((c) => c.action === "delete"), "prune deleted nothing");
  const left = await call("list_entities");
  assert.deepEqual(left.items.map((i) => i.path), ["js-tf-inbox"]);

  const exported = await call("export_config");
  assert.match(exported.terraform, /js-tf-inbox/);
  assert.match(exported.config_json, /js-tf-inbox/);
});

test("resources and prompts are served", async () => {
  const { resources } = await mcp.listResources();
  assert.ok(resources.some((r) => r.uri === "osb://emulator/entities"));
  const { resourceTemplates } = await mcp.listResourceTemplates();
  assert.equal(resourceTemplates.length, 3);
  const entities = await mcp.readResource({ uri: "osb://emulator/entities" });
  assert.match(entities.contents[0].text, /js-tf-inbox/);
  const cert = await mcp.readResource({ uri: "osb://certificate.pem" });
  assert.match(cert.contents[0].text, /BEGIN CERTIFICATE/);
  await assert.rejects(mcp.readResource({ uri: "osb://emulator/queues/missing" }));

  const { prompts } = await mcp.listPrompts();
  assert.deepEqual(prompts.map((p) => p.name).sort(), ["diagnose_dead_letters", "explain_routing", "prepare_integration_test"]);
  const p = await mcp.getPrompt({ name: "explain_routing", arguments: { topic: "ev", message: "red order" } });
  assert.match(p.messages[0].content.text, /test_rule/);
  await assert.rejects(mcp.getPrompt({ name: "explain_routing" }));
});

test("observability reflects the SDK's traffic and the activity of MCP", async () => {
  await call("create_queue", { name: "js-mcp-watched" });
  await send("js-mcp-watched", { body: "1" }, { body: "2" });
  const totals = await call("get_metric_totals");
  const incoming = totals.counters.find((c) => c.metric === "IncomingMessages" && c.entity === "js-mcp-watched");
  assert.equal(incoming?.value, 2);
  const log = await call("query_activity_log", { actor: "mcp", entity: "js-mcp-watched" });
  assert.equal(log.items[0].operation, "CreateQueue");
  const info = await call("get_namespace_info");
  assert.match(info.connectionString, /UseDevelopmentEmulator=true/);
  const overview = await call("get_overview");
  assert.ok(overview.biggestBacklogs.some((b) => b.path === "js-mcp-watched" && b.active === 2));
});

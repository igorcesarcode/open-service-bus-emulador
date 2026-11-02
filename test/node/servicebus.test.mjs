// Validates open-service-bus with the official @azure/service-bus SDK.
//
// Run through the Go test TestNodeSDK, which starts the emulator with the
// entities below and sets OSB_CONNECTION_STRING.
import { test, after } from "node:test";
import assert from "node:assert/strict";
import { ServiceBusAdministrationClient, ServiceBusClient } from "@azure/service-bus";

const connectionString = process.env.OSB_CONNECTION_STRING;
if (!connectionString) {
  throw new Error("OSB_CONNECTION_STRING is not set; run through `go test ./test/integration -run TestNodeSDK`");
}

// OSB_NO_EMULATOR_FLAG=1 connects as a client of the real service does: the
// namespace's servicebus.windows.net name, no UseDevelopmentEmulator, AMQP
// and HTTPS over TLS. Only the socket goes to the emulator: AMQP through
// customEndpointAddress, HTTPS through the name mapped by dns-map.cjs.
const noFlag = Boolean(process.env.OSB_NO_EMULATOR_FLAG);
const realHost = "osb-test.servicebus.windows.net";
const emulatorHostPort = /Endpoint=sb:\/\/([^;\/]+)/.exec(connectionString)[1];
const withoutFlag = (hostPart) =>
  connectionString.replace(/;?UseDevelopmentEmulator=true/, "").replace(/Endpoint=sb:\/\/[^;]+/, `Endpoint=sb://${hostPart}`);
const clientCs = noFlag ? withoutFlag(`${realHost}/`) : connectionString;
const adminCs = noFlag ? withoutFlag(`${realHost}:${emulatorHostPort.split(":").pop()}/`) : connectionString;
const clientExtra = noFlag ? { customEndpointAddress: `sb://${emulatorHostPort}` } : {};

const client = new ServiceBusClient(clientCs, {
  ...clientExtra,
  retryOptions: { maxRetries: 1, retryDelayInMs: 100, timeoutInMs: 10000 },
});

// The SDK closes its connection when the last link on it closes, and a link
// opened at that moment writes to a connection that is ending ("write after
// end", mostly on slow machines). A receiver that stays open for the whole
// run keeps the connection up, as it is in an application.
const keepAlive = client.createReceiver("node-keepalive").subscribe({
  async processMessage() {},
  async processError() {},
});
after(async () => {
  await keepAlive.close();
  await client.close();
});

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function send(entity, ...messages) {
  const sender = client.createSender(entity);
  try {
    await sender.sendMessages(messages);
  } finally {
    await sender.close();
  }
}

// receive collects exactly n messages.
async function receive(receiver, n, maxWaitTimeInMs = 5000) {
  const out = [];
  const deadline = Date.now() + 15000;
  while (out.length < n) {
    if (Date.now() > deadline) {
      assert.fail(`received ${out.length}/${n} messages before the deadline`);
    }
    out.push(...(await receiver.receiveMessages(n - out.length, { maxWaitTimeInMs })));
  }
  return out;
}

async function receiveNone(receiver, waitMs = 500) {
  const msgs = await receiver.receiveMessages(1, { maxWaitTimeInMs: waitMs });
  assert.equal(msgs.length, 0, `want no message, got ${msgs.map((m) => m.body)}`);
}

test("send and receive keep every property", async () => {
  await send("node-basic", {
    body: { event: "created", total: 42 },
    messageId: "msg-1",
    contentType: "application/json",
    subject: "order-created",
    correlationId: "corr-1",
    replyTo: "replies",
    to: "someone",
    timeToLive: 3600 * 1000,
    applicationProperties: { event: "created", isApproved: true, version: 1, score: 1.5 },
  });
  const receiver = client.createReceiver("node-basic");
  try {
    const [m] = await receive(receiver, 1);
    assert.deepEqual(m.body, { event: "created", total: 42 });
    assert.equal(m.messageId, "msg-1");
    assert.equal(m.contentType, "application/json");
    assert.equal(m.subject, "order-created");
    assert.equal(m.correlationId, "corr-1");
    assert.equal(m.replyTo, "replies");
    assert.equal(m.to, "someone");
    assert.equal(m.timeToLive, 3600 * 1000);
    assert.deepEqual(m.applicationProperties, { event: "created", isApproved: true, version: 1, score: 1.5 });
    assert.equal(Number(m.sequenceNumber), 1);
    assert.equal(m.deliveryCount, 0, "the JS SDK counts earlier deliveries");
    assert.ok(m.enqueuedTimeUtc instanceof Date);
    assert.ok(m.lockedUntilUtc > new Date(), "lock must be in the future");
    assert.ok(m.lockToken, "lock token must be set");
    await receiver.completeMessage(m);
    await receiveNone(receiver);
  } finally {
    await receiver.close();
  }
});

test("missing entity reports MessagingEntityNotFound", async () => {
  const sender = client.createSender("node-missing");
  try {
    await assert.rejects(sender.sendMessages({ body: "x" }), (err) => {
      assert.equal(err.code, "MessagingEntityNotFound", `got ${err.code}: ${err.message}`);
      return true;
    });
  } finally {
    await sender.close();
  }
});

test("receive and delete", async () => {
  await send("node-rad", { body: "a" }, { body: "b" });
  const receiver = client.createReceiver("node-rad", { receiveMode: "receiveAndDelete" });
  try {
    const msgs = await receive(receiver, 2);
    assert.deepEqual(msgs.map((m) => m.body), ["a", "b"]);
    // The messages are gone as soon as they are received, even for a peek
    // right away.
    assert.deepEqual(await receiver.peekMessages(10), []);
    await receiveNone(receiver);
  } finally {
    await receiver.close();
  }
});

test("batch send", async () => {
  const sender = client.createSender("node-batch");
  try {
    const batch = await sender.createMessageBatch();
    for (let i = 0; i < 10; i++) {
      assert.ok(batch.tryAddMessage({ body: `batch-${i}`, applicationProperties: { index: i } }));
    }
    await sender.sendMessages(batch);
  } finally {
    await sender.close();
  }
  const receiver = client.createReceiver("node-batch", { receiveMode: "receiveAndDelete" });
  try {
    const msgs = await receive(receiver, 10);
    assert.deepEqual(msgs.map((m) => m.body), Array.from({ length: 10 }, (_, i) => `batch-${i}`));
    assert.deepEqual(msgs.map((m) => m.applicationProperties.index), [...Array(10).keys()]);
  } finally {
    await receiver.close();
  }
});

test("abandon redelivers with modified properties", async () => {
  await send("node-abandon", { body: "a", applicationProperties: { keep: "yes" } });
  const receiver = client.createReceiver("node-abandon");
  try {
    let [m] = await receive(receiver, 1);
    await receiver.abandonMessage(m, { attempt: 1 });
    [m] = await receive(receiver, 1);
    assert.equal(m.deliveryCount, 1);
    assert.deepEqual(m.applicationProperties, { keep: "yes", attempt: 1 });
    await receiver.completeMessage(m);
  } finally {
    await receiver.close();
  }
});

test("dead-letter with reason and max delivery count", async () => {
  await send("node-dlq", { body: "bad" }, { body: "poison" });
  const receiver = client.createReceiver("node-dlq");
  const dlq = client.createReceiver("node-dlq", { subQueueType: "deadLetter" });
  try {
    const [bad] = await receive(receiver, 1);
    await receiver.deadLetterMessage(bad, { deadLetterReason: "validation", deadLetterErrorDescription: "missing field" });
    // node-dlq has MaxDeliveryCount 2.
    for (let i = 0; i < 2; i++) {
      const [poison] = await receive(receiver, 1);
      await receiver.abandonMessage(poison);
    }
    await receiveNone(receiver);

    const dead = await receive(dlq, 2);
    assert.equal(dead[0].body, "bad");
    assert.equal(dead[0].deadLetterReason, "validation");
    assert.equal(dead[0].deadLetterErrorDescription, "missing field");
    assert.equal(dead[1].body, "poison");
    assert.equal(dead[1].deadLetterReason, "MaxDeliveryCountExceeded");
    for (const m of dead) {
      await dlq.completeMessage(m);
    }
  } finally {
    await receiver.close();
    await dlq.close();
  }
});

test("lock expiry, renew and lock lost", async () => {
  // node-lock has a 2 second lock.
  await send("node-lock", { body: "slow" });
  const receiver = client.createReceiver("node-lock", { maxAutoLockRenewalDurationInMs: 0 });
  try {
    const [m] = await receive(receiver, 1);
    const first = m.lockedUntilUtc;
    await sleep(800);
    const renewed = await receiver.renewMessageLock(m);
    assert.ok(renewed > first, `renewed lock ${renewed} must be after ${first}`);
    await sleep(1000);
    await receiver.completeMessage(m);

    await send("node-lock", { body: "expires" });
    const [expiring] = await receive(receiver, 1);
    await sleep(2500);
    const [again] = await receive(receiver, 1);
    assert.equal(again.deliveryCount, 1);
    await assert.rejects(receiver.completeMessage(expiring), (err) => {
      assert.equal(err.code, "MessageLockLost", `got ${err.code}: ${err.message}`);
      return true;
    });
    await receiver.completeMessage(again);
  } finally {
    await receiver.close();
  }
});

test("messages behind a locked head are delivered, and the head returns in order", async () => {
  // node-lock has a 2 second lock.
  await send("node-lock", { body: "h1" }, { body: "h2" }, { body: "t1" }, { body: "t2" });
  const holder = client.createReceiver("node-lock", { maxAutoLockRenewalDurationInMs: 0 });
  const other = client.createReceiver("node-lock", { maxAutoLockRenewalDurationInMs: 0 });
  try {
    const head = await receive(holder, 2);
    assert.deepEqual(head.map((m) => m.body), ["h1", "h2"]);
    const tail = await receive(other, 2);
    assert.deepEqual(tail.map((m) => m.body), ["t1", "t2"]);
    for (const m of tail) await other.completeMessage(m);
    await sleep(2500);
    const again = await receive(other, 2);
    assert.deepEqual(again.map((m) => m.body), ["h1", "h2"]);
    // The JS SDK counts earlier deliveries, so a second delivery reads 1.
    assert.ok(again.every((m) => m.deliveryCount === 1), "redelivered messages count one earlier delivery");
    for (const m of again) await other.completeMessage(m);
  } finally {
    await holder.close();
    await other.close();
  }
});

test("defer and receive deferred", async () => {
  await send("node-defer", { body: "later" }, { body: "now" });
  const receiver = client.createReceiver("node-defer");
  try {
    const [later, now] = await receive(receiver, 2);
    await receiver.deferMessage(later);
    await receiver.completeMessage(now);
    await receiveNone(receiver);
    const [deferred] = await receiver.receiveDeferredMessages(later.sequenceNumber);
    assert.equal(deferred.body, "later");
    assert.equal(deferred.state, "deferred");
    await receiver.completeMessage(deferred);
  } finally {
    await receiver.close();
  }
});

test("schedule and cancel", async () => {
  const sender = client.createSender("node-schedule");
  const receiver = client.createReceiver("node-schedule");
  try {
    const at = new Date(Date.now() + 1000);
    const [keep, cancel] = await sender.scheduleMessages([{ body: "scheduled" }, { body: "cancelled" }], at);
    await sender.cancelScheduledMessages(cancel);
    const peeked = await receiver.peekMessages(10);
    assert.equal(peeked.length, 1);
    assert.equal(peeked[0].state, "scheduled");
    assert.equal(peeked[0].sequenceNumber.toString(), keep.toString());
    await receiveNone(receiver, 300);
    const [m] = await receive(receiver, 1);
    assert.equal(m.body, "scheduled");
    assert.ok(Date.now() >= at.getTime(), "delivered before its scheduled time");
    await receiver.completeMessage(m);
  } finally {
    await sender.close();
    await receiver.close();
  }
});

test("peek does not lock", async () => {
  await send("node-peek", { body: "a" }, { body: "b" }, { body: "c" });
  const receiver = client.createReceiver("node-peek");
  try {
    const first = await receiver.peekMessages(2);
    const next = await receiver.peekMessages(2);
    assert.deepEqual([...first, ...next].map((m) => m.body), ["a", "b", "c"]);
    const msgs = await receive(receiver, 3);
    assert.equal(msgs.length, 3);
    for (const m of msgs) {
      await receiver.completeMessage(m);
    }
  } finally {
    await receiver.close();
  }
});

test("subscribe streams messages", async () => {
  await send("node-subscribe", { body: 1 }, { body: 2 }, { body: 3 });
  const receiver = client.createReceiver("node-subscribe");
  const got = [];
  let subscription;
  try {
    // The handler settles each message itself and returns, so closing the
    // subscription below never races a settlement that is still in flight.
    await new Promise((resolve, reject) => {
      subscription = receiver.subscribe(
        {
          async processMessage(m) {
            await receiver.completeMessage(m);
            got.push(m.body);
            if (got.length === 3) {
              resolve();
            }
          },
          async processError(args) {
            reject(args.error);
          },
        },
        { autoCompleteMessages: false },
      );
      setTimeout(() => reject(new Error(`subscribe received ${got.length}/3`)), 10000).unref();
    });
    assert.deepEqual(got, [1, 2, 3]);
  } finally {
    await subscription?.close();
    await receiver.close();
  }
});

test("topic fan-out to subscriptions", async () => {
  await send("node-events", { body: "e1" }, { body: "e2" });
  for (const sub of ["audit", "billing"]) {
    const receiver = client.createReceiver("node-events", sub);
    try {
      const msgs = await receive(receiver, 2);
      assert.deepEqual(msgs.map((m) => m.body), ["e1", "e2"], `subscription ${sub}`);
      for (const m of msgs) {
        await receiver.completeMessage(m);
      }
    } finally {
      await receiver.close();
    }
  }
});

test("sessions", async () => {
  await send(
    "node-sessions",
    { body: "a1", sessionId: "customer-1" },
    { body: "b1", sessionId: "customer-2" },
    { body: "a2", sessionId: "customer-1" },
  );

  const one = await client.acceptSession("node-sessions", "customer-1");
  try {
    assert.equal(one.sessionId, "customer-1");
    assert.ok(one.sessionLockedUntilUtc > new Date());
    const msgs = await receive(one, 2);
    assert.deepEqual(msgs.map((m) => m.body), ["a1", "a2"]);
    for (const m of msgs) {
      assert.equal(m.sessionId, "customer-1");
      await one.completeMessage(m);
    }

    assert.equal(await one.getSessionState() ?? undefined, undefined, "a new session has no state");
    await one.setSessionState("step-2");
    assert.equal((await one.getSessionState()).toString(), "step-2");
    const before = one.sessionLockedUntilUtc;
    await sleep(50);
    await one.renewSessionLock();
    assert.ok(one.sessionLockedUntilUtc >= before);

    // A held session cannot be accepted again.
    await assert.rejects(client.acceptSession("node-sessions", "customer-1"), (err) => {
      assert.equal(err.code, "SessionCannotBeLocked", `got ${err.code}: ${err.message}`);
      return true;
    });
  } finally {
    await one.close();
  }

  const next = await client.acceptNextSession("node-sessions");
  try {
    assert.equal(next.sessionId, "customer-2");
    const [m] = await receive(next, 1);
    assert.equal(m.body, "b1");
    await next.completeMessage(m);
  } finally {
    await next.close();
  }
});

test("accept next session times out when none is available", async () => {
  await assert.rejects(client.acceptNextSession("node-sessions-empty", { maxAutoLockRenewalDurationInMs: 0 }), (err) => {
    assert.ok(["SessionCannotBeLocked", "ServiceTimeout", "OperationTimeoutError"].includes(err.code),
      `got ${err.code}: ${err.message}`);
    return true;
  });
});

async function drainBodies(entity, subscription, n) {
  const receiver = subscription
    ? client.createReceiver(entity, subscription, { receiveMode: "receiveAndDelete" })
    : client.createReceiver(entity, { receiveMode: "receiveAndDelete" });
  try {
    const msgs = await receive(receiver, n);
    await receiveNone(receiver);
    return msgs;
  } finally {
    await receiver.close();
  }
}

const bodiesOf = (msgs) => msgs.map((m) => m.body).sort();

test("subscription filters", async () => {
  // JS integers travel as AMQP uint, which numeric filters must accept.
  await send("node-filters",
    { body: "m1", subject: "order.created", applicationProperties: { event: "completed", isApproved: true, quantity: 20, region: "eu" } },
    { body: "m2", subject: "order.updated", applicationProperties: { event: "approved", isApproved: false, quantity: 50, region: "eu" } },
    { body: "m3", subject: "other", applicationProperties: { event: "completed", isApproved: true, quantity: 5.5 } },
  );
  assert.deepEqual(bodiesOf(await drainBodies("node-filters", "approved", 2)), ["m1", "m3"]);
  assert.deepEqual(bodiesOf(await drainBodies("node-filters", "big", 2)), ["m1", "m2"]);
  assert.deepEqual(bodiesOf(await drainBodies("node-filters", "eu", 1)), ["m1"]);
});

test("rule manager", async () => {
  const rules = client.createRuleManager("node-rules", "managed");
  const list = async () => {
    const out = [];
    for await (const r of rules.listRules()) out.push(r);
    return out.sort((a, b) => a.name.localeCompare(b.name));
  };
  const initial = await list();
  assert.deepEqual(initial.map((r) => [r.name, r.filter.sqlExpression]), [["$Default", "1=1"]]);

  await rules.createRule("red", { sqlExpression: "color = 'red'" }, { sqlExpression: "SET quantity = quantity / 2; REMOVE color" });
  await rules.createRule("eu", { subject: "order", applicationProperties: { region: "eu" } });
  await assert.rejects(rules.createRule("red", { sqlExpression: "1=1" }), (err) => {
    assert.equal(err.code, "MessagingEntityAlreadyExists", `got ${err.code}: ${err.message}`);
    return true;
  });
  await assert.rejects(rules.deleteRule("missing"), (err) => {
    assert.equal(err.code, "MessagingEntityNotFound", `got ${err.code}: ${err.message}`);
    return true;
  });
  await rules.deleteRule("$Default");

  const got = await list();
  assert.deepEqual(got.map((r) => r.name), ["eu", "red"]);
  const red = got.find((r) => r.name === "red");
  assert.equal(red.filter.sqlExpression, "color = 'red'");
  assert.equal(red.action.sqlExpression, "SET quantity = quantity / 2; REMOVE color");
  const eu = got.find((r) => r.name === "eu");
  assert.equal(eu.filter.subject, "order");
  assert.deepEqual(eu.filter.applicationProperties, { region: "eu" });

  await send("node-rules",
    { body: "red", applicationProperties: { color: "red", quantity: 10 } },
    { body: "eu", subject: "order", applicationProperties: { region: "eu" } },
    { body: "other", subject: "order", applicationProperties: { region: "us" } },
  );
  const msgs = await drainBodies("node-rules", "managed", 2);
  assert.deepEqual(bodiesOf(msgs), ["eu", "red"]);
  const halved = msgs.find((m) => m.body === "red");
  assert.equal(halved.applicationProperties.RuleName, "red");
  assert.equal(halved.applicationProperties.quantity, 5);
  assert.equal(halved.applicationProperties.color, undefined);
});

test("auto-forwarding", async () => {
  await send("node-fwd-front", { body: "forwarded" });
  assert.deepEqual(bodiesOf(await drainBodies("node-fwd-inbox", undefined, 1)), ["forwarded"]);

  await send("node-fwd-work", { body: "bad" });
  const receiver = client.createReceiver("node-fwd-work");
  try {
    const [m] = await receive(receiver, 1);
    await receiver.deadLetterMessage(m, { deadLetterReason: "Invalid", deadLetterErrorDescription: "test" });
  } finally {
    await receiver.close();
  }
  const [p] = await drainBodies("node-fwd-poison", undefined, 1);
  assert.equal(p.body, "bad");
  assert.equal(p.deadLetterReason, "Invalid");
});

// The administration client uses plain HTTP on the connection string's
// port in emulator mode (@azure/service-bus 7.10.0 and newer; test/node-legacy
// covers 7.9.5, which always uses HTTPS).
const adminClient = new ServiceBusAdministrationClient(adminCs);

async function collect(iterator) {
  const out = [];
  for await (const x of iterator) out.push(x);
  return out;
}

test("admin queues", async () => {
  const ns = await adminClient.getNamespaceProperties();
  assert.ok(ns.name);

  const created = await adminClient.createQueue("adm-orders", {
    lockDuration: "PT10M",
    maxDeliveryCount: 500,
    defaultMessageTimeToLive: "P30D",
    userMetadata: "meta <&>",
  });
  assert.equal(created.name, "adm-orders");
  assert.equal(created.lockDuration, "PT10M");
  await assert.rejects(adminClient.createQueue("adm-orders"), (err) => {
    assert.equal(err.code, "MessageEntityAlreadyExistsError", `got ${err.code}: ${err.message}`);
    return true;
  });

  const q = await adminClient.getQueue("adm-orders");
  assert.equal(q.maxDeliveryCount, 500);
  assert.equal(q.defaultMessageTimeToLive, "P30D");
  assert.equal(q.userMetadata, "meta <&>");
  assert.equal(await adminClient.queueExists("adm-orders"), true);
  assert.equal(await adminClient.queueExists("adm-missing"), false);
  await assert.rejects(adminClient.getQueue("adm-missing"), (err) => {
    assert.equal(err.code, "MessageEntityNotFoundError", `got ${err.code}: ${err.message}`);
    return true;
  });

  q.maxDeliveryCount = 3;
  const updated = await adminClient.updateQueue(q);
  assert.equal(updated.maxDeliveryCount, 3);
  assert.equal(updated.lockDuration, "PT10M");

  await send("adm-orders", { body: "a" }, { body: "b" });
  const sender = client.createSender("adm-orders");
  try {
    await sender.scheduleMessages({ body: "later" }, new Date(Date.now() + 3600_000));
  } finally {
    await sender.close();
  }
  const rt = await adminClient.getQueueRuntimeProperties("adm-orders");
  assert.equal(rt.activeMessageCount, 2);
  assert.equal(rt.scheduledMessageCount, 1);
  assert.equal(rt.totalMessageCount, 3);
  assert.ok(rt.sizeInBytes > 0);

  for (const name of ["adm-a1", "adm-a2", "adm-a3"]) await adminClient.createQueue(name);
  const pages = await collect(adminClient.listQueues().byPage({ maxPageSize: 2 }));
  assert.ok(pages.length > 1, `want several pages, got ${pages.length}`);
  const names = pages.flat().map((x) => x.name).filter((n) => n.startsWith("adm-"));
  assert.deepEqual(names.sort(), ["adm-a1", "adm-a2", "adm-a3", "adm-orders"]);
  const runtimes = await collect(adminClient.listQueuesRuntimeProperties());
  assert.ok(runtimes.some((x) => x.name === "adm-orders" && x.activeMessageCount === 2));

  await adminClient.deleteQueue("adm-orders");
  await assert.rejects(adminClient.deleteQueue("adm-orders"), (err) => {
    assert.equal(err.code, "MessageEntityNotFoundError", `got ${err.code}: ${err.message}`);
    return true;
  });
});

test("admin topics, subscriptions and rules", async () => {
  await adminClient.createTopic("adm-events", { defaultMessageTimeToLive: "P7D" });
  assert.equal(await adminClient.topicExists("adm-events"), true);
  await adminClient.createSubscription("adm-events", "all");
  await adminClient.createSubscription("adm-events", "red", {
    maxDeliveryCount: 2,
    defaultRuleOptions: {
      name: "red",
      filter: { sqlExpression: "color = @c", sqlParameters: { "@c": "red" } },
      action: { sqlExpression: "SET routed = TRUE" },
    },
  });
  const sub = await adminClient.getSubscription("adm-events", "red");
  assert.equal(sub.topicName, "adm-events");
  assert.equal(sub.subscriptionName, "red");
  assert.equal(sub.maxDeliveryCount, 2);
  sub.lockDuration = "PT2M";
  assert.equal((await adminClient.updateSubscription(sub)).lockDuration, "PT2M");

  const rules = await collect(adminClient.listRules("adm-events", "red"));
  assert.deepEqual(rules.map((r) => r.name), ["red"]);
  assert.equal(rules[0].filter.sqlExpression, "color = @c");
  assert.deepEqual(rules[0].filter.sqlParameters, { "@c": "red" });
  assert.equal(rules[0].action.sqlExpression, "SET routed = TRUE");

  await adminClient.createRule("adm-events", "all", "eu", { subject: "order", applicationProperties: { region: "eu", n: 7 } });
  await adminClient.createRule("adm-events", "all", "big", { sqlExpression: "quantity > 10" }, { sqlExpression: "SET size = 'big'" });
  await adminClient.deleteRule("adm-events", "all", "$Default");
  const eu = await adminClient.getRule("adm-events", "all", "eu");
  assert.equal(eu.filter.subject, "order");
  assert.deepEqual(eu.filter.applicationProperties, { region: "eu", n: 7 });
  assert.equal(await adminClient.ruleExists("adm-events", "all", "big"), true);
  assert.equal(await adminClient.ruleExists("adm-events", "all", "$Default"), false);
  const big = await adminClient.getRule("adm-events", "all", "big");
  big.filter = { sqlExpression: "quantity > 100" };
  await adminClient.updateRule("adm-events", "all", big);
  assert.equal((await adminClient.getRule("adm-events", "all", "big")).filter.sqlExpression, "quantity > 100");

  await send("adm-events",
    { body: "r", applicationProperties: { color: "red" } },
    { body: "eu", subject: "order", applicationProperties: { region: "eu", n: 7 } },
    { body: "medium", applicationProperties: { quantity: 50 } },
    { body: "huge", applicationProperties: { quantity: 500 } },
  );
  const red = await drainBodies("adm-events", "red", 1);
  assert.equal(red[0].body, "r");
  assert.equal(red[0].applicationProperties.routed, true);
  assert.deepEqual(bodiesOf(await drainBodies("adm-events", "all", 2)), ["eu", "huge"]);

  const trt = await adminClient.getTopicRuntimeProperties("adm-events");
  assert.equal(trt.subscriptionCount, 2);
  const subs = await collect(adminClient.listSubscriptions("adm-events"));
  assert.deepEqual(subs.map((s) => s.subscriptionName).sort(), ["all", "red"]);
  const srt = await adminClient.getSubscriptionRuntimeProperties("adm-events", "all");
  assert.equal(srt.activeMessageCount, 0);

  await adminClient.deleteSubscription("adm-events", "all");
  assert.equal(await adminClient.subscriptionExists("adm-events", "all"), false);
  await adminClient.deleteTopic("adm-events");
  assert.equal(await adminClient.topicExists("adm-events"), false);
});

test("duplicate detection", async () => {
  const sender = client.createSender("node-dedup");
  try {
    // The duplicates are accepted, so every call below succeeds.
    await sender.sendMessages([{ messageId: "1", body: "first" }, { messageId: "1", body: "again" }]);
    const batch = await sender.createMessageBatch();
    for (const m of [{ messageId: "2", body: "second" }, { messageId: "2", body: "again" }, { messageId: "1", body: "again" }]) {
      assert.ok(batch.tryAddMessage(m));
    }
    await sender.sendMessages(batch);
    await sender.scheduleMessages({ messageId: "2", body: "scheduled" }, new Date());
  } finally {
    await sender.close();
  }
  const receiver = client.createReceiver("node-dedup", { receiveMode: "receiveAndDelete" });
  try {
    const got = (await receive(receiver, 2)).map((m) => m.body).sort();
    assert.deepEqual(got, ["first", "second"]);
    await receiveNone(receiver);
  } finally {
    await receiver.close();
  }
});

test("quota exceeded", async () => {
  const sender = client.createSender("node-quota");
  const big = "x".repeat(300 * 1024);
  try {
    for (let i = 0; i < 3; i++) {
      await sender.sendMessages({ body: big });
    }
    await assert.rejects(sender.sendMessages({ body: big }), (err) => {
      assert.equal(err.code, "QuotaExceeded", `got ${err.code}: ${err.message}`);
      return true;
    });
  } finally {
    await sender.close();
  }
});

test("per-entity message size", async () => {
  // A sender attached before the limit changes still sees 1 MB, so the
  // server rejects the message.
  const attached = client.createSender("node-size");
  try {
    await attached.sendMessages({ body: "attach" });
    const q = await adminClient.getQueue("node-size");
    await adminClient.updateQueue({ ...q, maxMessageSizeInKilobytes: 1 });
    await assert.rejects(attached.sendMessages({ body: "x".repeat(4096) }), (err) => {
      assert.equal(err.code, "MessageSizeExceeded", `got ${err.code}: ${err.message}`);
      return true;
    });
  } finally {
    await attached.close();
  }
  // A new sender learns the limit on attach and checks it itself.
  const fresh = client.createSender("node-size");
  try {
    await assert.rejects(fresh.sendMessages({ body: "x".repeat(4096) }), (err) => {
      assert.equal(err.code, "MessageSizeExceeded", `got ${err.code}: ${err.message}`);
      return true;
    });
  } finally {
    await fresh.close();
  }
});

test("auto delete on idle and accessedAt", async () => {
  const before = await adminClient.getQueueRuntimeProperties("node-basic");
  await sleep(20);
  await send("node-basic", { body: "touch" });
  const after = await adminClient.getQueueRuntimeProperties("node-basic");
  assert.ok(after.accessedAt > before.accessedAt, `accessedAt ${after.accessedAt} not after ${before.accessedAt}`);
  const receiver = client.createReceiver("node-basic", { receiveMode: "receiveAndDelete" });
  try {
    await receive(receiver, 1);
  } finally {
    await receiver.close();
  }

  const deadline = Date.now() + 15000;
  while (await adminClient.queueExists("node-idle")) {
    assert.ok(Date.now() < deadline, "node-idle was not deleted");
    await sleep(100);
  }
});

// Entities declared by test/integration/testdata/terraform.
const regionEvents = [
  { body: "eu", applicationProperties: { region: "eu" } },
  { body: "us", applicationProperties: { region: "us" } },
  { body: "big", subject: "big", applicationProperties: { size: "L" } },
];

test("terraform entities", async () => {
  const q = await adminClient.getQueue("node-tf-inbox");
  assert.equal(q.lockDuration, "PT45S");
  assert.equal(q.maxDeliveryCount, 7);

  await send("node-tf-events", ...regionEvents);
  const [eu] = await drainBodies("node-tf-events", "eu", 1);
  assert.equal(eu.body, "eu");
  assert.equal(eu.applicationProperties.routed, true);
  const [fwd] = await drainBodies("node-tf-inbox", undefined, 1);
  assert.equal(fwd.body, "big");
});

test("terraform keeps $Default as in Azure", async () => {
  await send("node-tfk-events", ...regionEvents);
  // eu comes twice: for $Default and for the rule with an action.
  assert.deepEqual(bodiesOf(await drainBodies("node-tfk-events", "eu", 4)), ["big", "eu", "eu", "us"]);
});

// webSocketClient connects over WebSocket, with Node's built-in WebSocket.
// TestNodeSDK makes Node trust the emulator's certificate.
function webSocketClient(cs, webSocket = WebSocket) {
  return new ServiceBusClient(cs, {
    retryOptions: { maxRetries: 1, retryDelayInMs: 100, timeoutInMs: 10000 },
    webSocketOptions: { webSocket },
  });
}

// AmqpSubprotocolWebSocket offers "amqp", the subprotocol of the final AMQP
// WebSocket binding (what Azure.Amqp for .NET sends), instead of the
// AMQPWSB10 of the drafts that rhea sends.
class AmqpSubprotocolWebSocket extends WebSocket {
  constructor(url, _protocols, ...rest) {
    super(url, "amqp", ...rest);
  }
}

test("websocket", async () => {
  const ws = webSocketClient(connectionString);
  try {
    const sender = ws.createSender("node-ws");
    await sender.sendMessages([{ body: "over" }, { body: "websocket" }]);
    await sender.close();
    const receiver = ws.createReceiver("node-ws");
    const msgs = await receive(receiver, 2);
    for (const m of msgs) await receiver.completeMessage(m);
    assert.deepEqual(bodiesOf(msgs), ["over", "websocket"]);
    await receiver.close();
  } finally {
    await ws.close();
  }
});

test("websocket with the amqp subprotocol", async () => {
  const ws = webSocketClient(connectionString, AmqpSubprotocolWebSocket);
  try {
    const sender = ws.createSender("node-ws");
    await sender.sendMessages([{ body: "final" }]);
    await sender.close();
    const receiver = ws.createReceiver("node-ws");
    const msgs = await receive(receiver, 1);
    await receiver.completeMessage(msgs[0]);
    assert.deepEqual(bodiesOf(msgs), ["final"]);
    await receiver.close();
  } finally {
    await ws.close();
  }
});

test("separate AMQP and HTTP ports", async () => {
  const amqpCs = process.env.OSB_SPLIT_AMQP_CONNECTION_STRING;
  const httpCs = process.env.OSB_SPLIT_HTTP_CONNECTION_STRING;
  const admin = new ServiceBusAdministrationClient(httpCs);
  await admin.createQueue("split");
  const amqp = new ServiceBusClient(amqpCs);
  const ws = webSocketClient(httpCs);
  try {
    for (const [c, body] of [[amqp, "amqp"], [ws, "ws"]]) {
      const sender = c.createSender("split");
      await sender.sendMessages({ body });
      await sender.close();
    }
    const receiver = amqp.createReceiver("split", { receiveMode: "receiveAndDelete" });
    assert.deepEqual(bodiesOf(await receive(receiver, 2)), ["amqp", "ws"]);
    await receiver.close();
  } finally {
    await ws.close();
    await amqp.close();
  }
});

// emulatorURL returns the HTTP base URL of the emulator, which serves plain
// HTTP on the port of the connection string.
function emulatorURL() {
  const m = /Endpoint=sb:\/\/([^;\/]+)/.exec(connectionString);
  return `http://${m[1]}`;
}

async function getJSON(path) {
  const res = await fetch(emulatorURL() + path);
  assert.equal(res.status, 200, `GET ${path}`);
  return res.json();
}

const metricSum = (m, name) => m.metrics[name].reduce((n, p) => n + p.value, 0);

test("metrics and activity log", async () => {
  await send("node-metrics", { body: "a" }, { body: "b" }, { body: "c" });
  const receiver = client.createReceiver("node-metrics");
  const msgs = await receive(receiver, 2);
  for (const m of msgs) await receiver.completeMessage(m);
  await receiver.close();

  const m = await getJSON("/$api/metrics?entity=node-metrics");
  assert.equal(metricSum(m, "IncomingMessages"), 3);
  assert.equal(metricSum(m, "OutgoingMessages"), 2);
  assert.ok(metricSum(m, "SuccessfulRequests") > 0);
  assert.equal(m.metrics.ActiveMessages.at(-1).value, 1);

  const text = await (await fetch(emulatorURL() + "/$metrics")).text();
  assert.match(text, /^servicebus_incoming_messages_total\{entity="node-metrics"\} 3$/m);
  assert.match(text, /^servicebus_active_messages\{entity="node-metrics"\} 1$/m);

  const admin = new ServiceBusAdministrationClient(adminCs);
  await admin.createQueue("node-metrics-admin");
  await admin.deleteQueue("node-metrics-admin");
  const log = await getJSON("/$api/activity?entity=node-metrics-admin");
  assert.deepEqual(
    log.map((e) => [e.actor, e.operation, e.status]),
    [["atom", "DeleteQueue", "Succeeded"], ["atom", "CreateQueue", "Succeeded"]],
  );
});

async function portalPost(path, fields) {
  const body = new URLSearchParams();
  for (const [k, v] of fields) body.append(k, v);
  const res = await fetch(emulatorURL() + path, {
    method: "POST",
    headers: { "HX-Request": "true", "Content-Type": "application/x-www-form-urlencoded" },
    body,
  });
  return { status: res.status, text: await res.text() };
}

test("portal explorer", async () => {
  // JS SDK to portal.
  await send("node-portal", {
    body: { id: 7 },
    messageId: "js-1",
    subject: "order.created",
    contentType: "application/json",
    applicationProperties: { tenant: "acme", retries: 3, vip: true },
  });
  const view = await (await fetch(emulatorURL() + "/$portal/explorer/view?path=node-portal&op=start")).text();
  assert.match(view, /Showing 1 of 1 messages/);
  assert.match(view, /js-1/);
  assert.match(view, /order\.created/);
  const detail = await (await fetch(emulatorURL() + "/$portal/explorer/detail?path=node-portal&seq=1")).text();
  for (const want of ["tenant", "acme", "retries", "vip", "boolean", "application/json"]) {
    assert.ok(detail.includes(want), `detail lacks ${want}`);
  }

  // Portal to JS SDK.
  const sent = await portalPost("/$portal/explorer/send", [
    ["path", "node-portal-in"],
    ["contentType", "application/json"],
    ["body", '{"from": "portal"}'],
    ["messageId", "portal-1"],
    ["subject", "from.portal"],
    ["correlationId", "corr-9"],
    ["ttl", "PT30M"],
    ["pkey", "tenant"], ["ptype", "string"], ["pvalue", "acme"],
    ["pkey", "count"], ["ptype", "int"], ["pvalue", "5"],
    ["pkey", "ok"], ["ptype", "boolean"], ["pvalue", "true"],
  ]);
  assert.equal(sent.status, 200, sent.text);
  const receiver = client.createReceiver("node-portal-in");
  const [m] = await receive(receiver, 1);
  assert.equal(m.messageId, "portal-1");
  assert.equal(m.subject, "from.portal");
  assert.equal(m.correlationId, "corr-9");
  assert.equal(m.contentType, "application/json");
  assert.equal(m.timeToLive, 30 * 60 * 1000);
  assert.deepEqual(m.body, { from: "portal" });
  assert.equal(m.applicationProperties.tenant, "acme");
  assert.equal(m.applicationProperties.count, 5);
  assert.equal(m.applicationProperties.ok, true);
  await receiver.completeMessage(m);
  await receiver.close();

  // Receive Mode in the portal holds the lock.
  await send("node-portal", { body: "x" });
  const recv = await portalPost("/$portal/explorer/view", [
    ["path", "node-portal"], ["mode", "receive"], ["op", "receive"], ["count", "10"],
  ]);
  const tokens = [...recv.text.matchAll(/name="token" value="([0-9a-f-]+)"/g)].map((x) => x[1]);
  assert.equal(tokens.length, 2);
  const other = client.createReceiver("node-portal");
  await receiveNone(other);
  const settled = await portalPost("/$portal/explorer/settle", [
    ["path", "node-portal"], ["mode", "receive"], ["action", "complete"], ...tokens.map((t) => ["token", t]),
  ]);
  assert.match(settled.text, /Complete: 2 messages/);
  assert.equal((await other.peekMessages(10)).length, 0);
  await other.close();
});

// withKey rewrites the shared access policy of a connection string.
const withKey = (cs, name, key) =>
  cs.replace("SharedAccessKeyName=RootManageSharedAccessKey", `SharedAccessKeyName=${name}`).replace("SharedAccessKey=SAS_KEY_VALUE", `SharedAccessKey=${key}`);

async function expectUnauthorized(promise, what) {
  await assert.rejects(promise, (err) => {
    const text = `${err.code} ${err.name} ${err.message}`.toLowerCase();
    return text.includes("unauthorized");
  }, `${what} should be unauthorized`);
}

test("shared access policies", async () => {
  const root = process.env.OSB_AUTH_CONNECTION_STRING;
  const opts = { retryOptions: { maxRetries: 0, timeoutInMs: 5000 } };
  const trySend = async (cs, queue) => {
    const c = new ServiceBusClient(cs, opts);
    try {
      const s = c.createSender(queue);
      await s.sendMessages({ body: "x" });
      await s.close();
    } finally {
      await c.close();
    }
  };
  const tryReceive = async (cs, queue) => {
    const c = new ServiceBusClient(cs, opts);
    try {
      const r = c.createReceiver(queue);
      await r.peekMessages(1);
      await r.close();
    } finally {
      await c.close();
    }
  };
  const sender = withKey(root, "sender", "send-key");
  const listener = withKey(root, "listener", "listen-key");

  await trySend(root, "orders"); // The development key.
  await trySend(sender, "orders");
  await tryReceive(listener, "orders");
  await expectUnauthorized(tryReceive(sender, "orders"), "receive with a Send-only policy");
  await expectUnauthorized(trySend(listener, "orders"), "send with a Listen-only policy");
  await expectUnauthorized(trySend(sender, "payments"), "send to another entity with an entity policy");
  await expectUnauthorized(trySend(withKey(root, "sender", "wrong"), "orders"), "send with a wrong key");
  await expectUnauthorized(trySend(withKey(root, "ghost", "send-key"), "orders"), "send with an unknown policy");

  // The administration client needs Manage.
  await new ServiceBusAdministrationClient(root).getQueue("orders");
  await assert.rejects(
    new ServiceBusAdministrationClient(withKey(root, "app", "app-key")).getQueue("orders"),
    (err) => err.statusCode === 401,
  );

  // Policies declared through the administration API work.
  const admin = new ServiceBusAdministrationClient(root);
  await admin.createQueue("jobs-js", {
    authorizationRules: [{ claimType: "SharedAccessKey", claimValue: "None", accessRights: ["Send"], keyName: "js-producer", primaryKey: "js-key", secondaryKey: "js-key-2" }],
  });
  const created = await admin.getQueue("jobs-js");
  assert.equal(created.authorizationRules.length, 1);
  assert.equal(created.authorizationRules[0].keyName, "js-producer");
  await trySend(withKey(root, "js-producer", "js-key"), "jobs-js");
  await trySend(withKey(root, "js-producer", "js-key-2"), "jobs-js");
  await expectUnauthorized(tryReceive(withKey(root, "js-producer", "js-key"), "jobs-js"), "receive with a Send policy");
});

test("entra id roles", async () => {
  const root = process.env.OSB_AUTH_CONNECTION_STRING;
  const m = /Endpoint=sb:\/\/([^;\/]+)/.exec(root);
  const host = m[1];
  // A TokenCredential that asks the emulator's development issuer.
  const credential = (principal) => ({
    async getToken() {
      const res = await fetch(`http://${host}/$auth/token`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ principal, ttl: "3600" }),
      });
      const body = await res.json();
      return { token: body.access_token, expiresOnTimestamp: body.expires_on * 1000 };
    },
  });
  const clientFor = (principal) =>
    new ServiceBusClient("localhost", credential(principal), {
      customEndpointAddress: `sb://${host}`,
      retryOptions: { maxRetries: 0, timeoutInMs: 5000 },
    });
  const trySend = async (principal, queue) => {
    const c = clientFor(principal);
    try {
      const s = c.createSender(queue);
      await s.sendMessages({ body: "x" });
      await s.close();
    } finally {
      await c.close();
    }
  };
  const tryPeek = async (principal, queue) => {
    const c = clientFor(principal);
    try {
      const r = c.createReceiver(queue);
      await r.peekMessages(1);
      await r.close();
    } finally {
      await c.close();
    }
  };

  await trySend("owner", "payments");
  await tryPeek("owner", "payments");
  await trySend("sender", "orders");
  await tryPeek("receiver", "payments");
  await expectUnauthorized(trySend("sender", "payments"), "Data Sender on another entity");
  await expectUnauthorized(tryPeek("sender", "orders"), "Data Sender receiving");
  await expectUnauthorized(trySend("receiver", "payments"), "Data Receiver sending");
  await expectUnauthorized(trySend("nobody", "orders"), "a principal without a role");
});

test("transfer dead-letter queue", async () => {
  const admin = new ServiceBusAdministrationClient(adminCs);
  const rt = await admin.getQueueRuntimeProperties("node-tdlq");
  assert.equal(rt.transferDeadLetterMessageCount, 1);
  assert.equal(rt.deadLetterMessageCount, 0);
  const receiver = client.createReceiver("node-tdlq", { subQueueType: "transferDeadLetter" });
  const peeked = await receiver.peekMessages(5);
  assert.equal(peeked.length, 1);
  const [m] = await receive(receiver, 1);
  assert.equal(Buffer.from(m.body).toString(), "stuck");
  assert.equal(m.deadLetterReason, "TransferFailed");
  assert.equal(m.deadLetterErrorDescription, "the destination is disabled");
  await receiver.completeMessage(m);
  await receiver.close();
  assert.equal((await admin.getQueueRuntimeProperties("node-tdlq")).transferDeadLetterMessageCount, 0);
});

test("forwarding failure reasons", async () => {
  await send("node-loop-a", { body: "looping" });
  const receiver = client.createReceiver("node-loop-a", { subQueueType: "deadLetter", receiveMode: "receiveAndDelete" });
  const [m] = await receive(receiver, 1);
  assert.equal(Buffer.from(m.body).toString(), "looping");
  assert.equal(m.deadLetterReason, "MaxTransferHopCountExceeded");
  await receiver.close();
});

test("cancel scheduled messages through forwarding and actions", async () => {
  const at = new Date(Date.now() + 3000);
  const front = client.createSender("node-cancel-front");
  const seqs = await front.scheduleMessages([{ body: "cancel-me" }, { body: "keep-me" }], at);
  assert.equal(seqs.length, 2);
  assert.ok(seqs.every((s) => s.greaterThan(0)), "a sequence number of the destination is returned");
  await front.cancelScheduledMessages([seqs[0]]);
  await front.close();

  const topic = client.createSender("node-cancel-topic");
  const tseqs = await topic.scheduleMessages([{ body: "topic-cancel" }], at);
  await topic.cancelScheduledMessages(tseqs);
  await topic.close();

  const dest = client.createReceiver("node-cancel-dest", { receiveMode: "receiveAndDelete" });
  assert.deepEqual(bodiesOf(await receive(dest, 1)), ["keep-me"]);
  await dest.close();
  for (const sub of ["plain", "acting"]) {
    const r = client.createReceiver("node-cancel-topic", sub, { receiveMode: "receiveAndDelete" });
    await receiveNone(r, 500);
    await r.close();
  }
});

test("portal purge", async () => {
  await send("node-purge", { body: "a" }, { body: "b" }, { body: "c" });
  const receiver = client.createReceiver("node-purge");
  const [held] = await receive(receiver, 1);
  const res = await portalPost("/$portal/explorer/purge", [["path", "node-purge"]]);
  assert.equal(res.status, 200);
  assert.match(res.text, /Purged 2 messages from node-purge/);
  await receiver.completeMessage(held);
  assert.equal((await receiver.peekMessages(10)).length, 0);
  await receiver.close();
});

test("partitioned queue", async () => {
  // The partition key survives the round trip.
  await send("node-partitioned", { body: "keyed", partitionKey: "customer-7" });
  const receiver = client.createReceiver("node-partitioned", { receiveMode: "receiveAndDelete" });
  const [m] = await receive(receiver, 1);
  assert.equal(m.partitionKey, "customer-7");
  // The partition id is in the 16 high bits of the sequence number.
  const partition = (msg) => Number(msg.sequenceNumber.shiftRightUnsigned(48).toString());
  const first = partition(m);
  assert.ok(first >= 0 && first < 16, `partition ${first}`);
  await send("node-partitioned", { body: "same key", partitionKey: "customer-7" });
  const [again] = await receive(receiver, 1);
  assert.equal(partition(again), first, "the same key must keep its partition");
  await receiver.close();

  // The size quota is that of each partition times 16: 8 messages of
  // 300 KB do not fit in the 1 MB of a plain queue.
  const payload = Buffer.alloc(300 * 1024, "x");
  const sender = client.createSender("node-partitioned");
  for (let i = 0; i < 8; i++) await sender.sendMessages({ body: payload });
  await sender.close();
  const admin = new ServiceBusAdministrationClient(adminCs);
  assert.ok((await admin.getQueueRuntimeProperties("node-partitioned")).sizeInBytes > 2 * 1024 * 1024);
  assert.equal((await admin.getQueue("node-partitioned")).enablePartitioning, true);
});

// atNamespace connects to the emulator presenting a namespace's host name:
// the connection goes to the emulator's address, and the name picks the
// namespace.
function atNamespace(host) {
  const emulator = /Endpoint=sb:\/\/([^;\/]+)/.exec(connectionString)[1];
  return new ServiceBusClient(`Endpoint=sb://${host};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE`, {
    customEndpointAddress: `sb://${emulator}`,
    retryOptions: { maxRetries: 0, timeoutInMs: 5000 },
  });
}

async function sendAt(host, queue, body) {
  const c = atNamespace(host);
  try {
    const s = c.createSender(queue);
    await s.sendMessages({ body });
    await s.close();
  } finally {
    await c.close();
  }
}

async function peekAt(host, queue) {
  const c = atNamespace(host);
  try {
    const r = c.createReceiver(queue);
    const msgs = await r.peekMessages(10);
    await r.close();
    return msgs.map((m) => Buffer.from(m.body).toString());
  } finally {
    await c.close();
  }
}

test("namespaces are told apart by hostname", async () => {
  await sendAt("js-a.servicebus.windows.net", "shared", "to-a");
  await sendAt("js-b.localhost", "shared", "to-b");
  assert.deepEqual(await peekAt("js-a.localhost", "shared"), ["to-a"]);
  assert.deepEqual(await peekAt("js-b.servicebus.windows.net", "shared"), ["to-b"]);
  // A queue exists only where it was declared (js-b has it too: it is the
  // Geo-DR secondary of js-a, which copies the entities).
  await assert.rejects(sendAt("localhost", "only-a", "x"));
  await sendAt("js-a.localhost", "only-a", "x");
});

test("geo-dr alias and failover", async () => {
  await sendAt("js-bus.servicebus.windows.net", "shared", "via-alias-1");
  assert.ok((await peekAt("js-a.localhost", "shared")).includes("via-alias-1"));
  assert.ok(!(await peekAt("js-b.localhost", "shared")).includes("via-alias-1"));

  const base = emulatorURL();
  const call = async (body) => {
    const res = await fetch(`${base}/$portal/api/geo-dr`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    return { status: res.status, body: await res.json() };
  };
  const failed = await call({ action: "failover", alias: "js-bus" });
  assert.equal(failed.status, 200, JSON.stringify(failed.body));
  // The alias now serves the former secondary, with the same entities.
  await sendAt("js-bus.servicebus.windows.net", "shared", "via-alias-2");
  assert.ok((await peekAt("js-b.localhost", "shared")).includes("via-alias-2"));
  assert.ok(!(await peekAt("js-a.localhost", "shared")).includes("via-alias-2"));
  const again = await call({ action: "failover", alias: "js-bus" });
  assert.equal(again.status, 400);
});

test("geo-replication promotion keeps the data", async () => {
  const api = async (action, extra = {}) => {
    const res = await fetch(`${emulatorURL()}/$portal/api/geo-replication?ns=js-c`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, ...extra }),
    });
    return { status: res.status, body: await res.json() };
  };
  const state = async () => {
    const res = await fetch(`${emulatorURL()}/$portal/api/geo?ns=js-c`);
    return (await res.json()).geoReplication;
  };
  assert.equal((await state()).enabled, false);
  const enabled = await api("enable", { synchronous: true, primaryRegion: "west", secondaryRegion: "north" });
  assert.equal(enabled.status, 200, JSON.stringify(enabled.body));
  assert.equal((await state()).state, "Ready");

  await sendAt("js-c.localhost", "rep", "m1");
  await sendAt("js-c.localhost", "rep", "m2");
  const c = atNamespace("js-c.localhost");
  const receiver = c.createReceiver("rep");
  const [held] = await receive(receiver, 1);
  const promoted = await api("promote");
  assert.equal(promoted.status, 200, JSON.stringify(promoted.body));
  const after = await state();
  assert.equal(after.primaryRegion, "north");
  assert.equal(after.lostChanges, 0);
  // The lock taken before the promotion still holds, and nothing is lost.
  await receiver.completeMessage(held);
  await receiver.close();
  await c.close();
  assert.equal((await peekAt("js-c.localhost", "rep")).length, 1);
});

test("resource locks stop changes, not messages", async () => {
  const admin = new ServiceBusAdministrationClient(adminCs);
  const frozen = await admin.getQueue("node-frozen");
  frozen.maxDeliveryCount = 3;
  await assert.rejects(admin.updateQueue(frozen), (e) => e.statusCode === 409 && /ScopeLocked/.test(e.message) && /locked/.test(e.message));
  await assert.rejects(admin.deleteQueue("node-frozen"), (e) => e.statusCode === 409);

  // The data plane is not locked.
  await send("node-frozen", { body: "still flows" });
  const receiver = client.createReceiver("node-frozen");
  const [m] = await receive(receiver, 1);
  assert.equal(m.body, "still flows");
  await receiver.completeMessage(m);
  await receiver.close();

  // CanNotDelete allows changes.
  const kept = await admin.getQueue("node-kept");
  kept.maxDeliveryCount = 7;
  await admin.updateQueue(kept);
  assert.equal((await admin.getQueue("node-kept")).maxDeliveryCount, 7);
  await assert.rejects(admin.deleteQueue("node-kept"), (e) => e.statusCode === 409);
});

test("properties and export template", async () => {
  const base = emulatorURL();
  // Tags and a lock added in the portal.
  let res = await portalPost("/$portal/properties/action", [["action", "set-tags"], ["tags", "env=js\nteam=sdk"]]);
  assert.equal(res.status, 200, res.text);
  res = await portalPost("/$portal/properties/action", [["action", "add-lock"], ["name", "js-lock"], ["level", "CanNotDelete"], ["scope", "node-portal"]]);
  assert.equal(res.status, 200, res.text);
  const admin = new ServiceBusAdministrationClient(adminCs);
  await assert.rejects(admin.deleteQueue("node-portal"), (e) => e.statusCode === 409);

  // The export carries the entities, the locks and the tags.
  const config = JSON.parse(await (await fetch(`${base}/$portal/export/Config.json`)).text());
  const ns = config.UserConfig.Namespaces.find((n) => n.Queues.some((q) => q.Name === "node-frozen"));
  assert.ok(ns, "the default namespace is exported");
  assert.deepEqual(ns.Tags, { env: "js", team: "sdk" });
  assert.ok(ns.Locks.some((l) => l.Name === "freeze" && l.Level === "ReadOnly" && l.Scope === "node-frozen"));
  assert.ok(ns.Locks.some((l) => l.Name === "js-lock" && l.Scope === "node-portal"));
  const tf = await (await fetch(`${base}/$portal/export/main.tf`)).text();
  assert.match(tf, /resource "azurerm_management_lock"/);
  assert.match(tf, /resource "azurerm_servicebus_queue"/);

  res = await portalPost("/$portal/properties/action", [["action", "delete-lock"], ["name", "js-lock"]]);
  assert.equal(res.status, 200, res.text);
});

test("sql filter examples of the documentation", async () => {
  await send("node-doc-filters",
    { body: "m1", applicationProperties: { Code: "order-1", "Order Type": "priority", CustomerName: "O'Brien", Quantity: 15, Price: 3.0 } },
    { body: "m2", applicationProperties: { Code: "100%", "Order Type": "standard", Quantity: 9, Price: 1.5 } },
    { body: "m3", applicationProperties: { Code: "preorder-1", Quantity: 20, Price: 2.5 } },
  );
  const want = { prefix: ["m1"], escape: ["m2"], delimited: ["m1"], quote: ["m1"], numeric: ["m1", "m3"] };
  for (const [sub, bodies] of Object.entries(want)) {
    assert.deepEqual(bodiesOf(await drainBodies("node-doc-filters", sub, bodies.length)).sort(), bodies, sub);
  }
});

test("admin update replaces the description and keeps the immutable properties", async () => {
  const admin = new ServiceBusAdministrationClient(adminCs);
  await admin.createQueue("node-replace", { maxDeliveryCount: 7, lockDuration: "PT3M", requiresDuplicateDetection: true, userMetadata: "meta" });
  try {
    // A partial update: what it leaves out goes back to the default.
    const partial = await admin.updateQueue({ name: "node-replace", maxDeliveryCount: 2 });
    assert.equal(partial.maxDeliveryCount, 2);
    assert.equal(partial.lockDuration, "PT1M");
    assert.ok(!partial.userMetadata);
    assert.equal(partial.requiresDuplicateDetection, true, "an immutable property that is left out is kept");

    // Get, change, update keeps the rest.
    const cur = await admin.getQueue("node-replace");
    cur.lockDuration = "PT2M";
    const full = await admin.updateQueue(cur);
    assert.equal(full.lockDuration, "PT2M");
    assert.equal(full.maxDeliveryCount, 2);

    // Immutable properties refuse a new value.
    cur.requiresDuplicateDetection = false;
    await assert.rejects(admin.updateQueue(cur), (e) => e.statusCode === 400 && /Sub code=40000\. The value for the 'requiresDuplicateDetection' property of an existing Queue can't be changed\./.test(e.message));
    cur.requiresDuplicateDetection = true;
    cur.requiresSession = true;
    await assert.rejects(admin.updateQueue(cur), (e) => e.statusCode === 400 && /The value for RequiresSession property of an existing Queue can't be changed\./.test(e.message));
  } finally {
    await admin.deleteQueue("node-replace");
  }
});

test("features the emulator leaves out say so", async () => {
  // A JMS client-scoped subscription is refused.
  const body = `<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml"><SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"><IsClientAffine>true</IsClientAffine><ClientAffineProperties><ClientId>jms-1</ClientId></ClientAffineProperties></SubscriptionDescription></content></entry>`;
  const res = await fetch(`${emulatorURL()}/node-events/Subscriptions/jms`, {
    method: "PUT",
    headers: { "Content-Type": "application/atom+xml" },
    body,
  });
  assert.equal(res.status, 400);
  assert.match(await res.text(), /JMS client-scoped subscriptions are not supported/);
  const admin = new ServiceBusAdministrationClient(adminCs);
  assert.equal(await admin.subscriptionExists("node-events", "jms"), false);

  // The portal lists what is left out.
  const page = await (await fetch(`${emulatorURL()}/$portal/unsupported`)).text();
  for (const name of ["Private Link", "IP firewall rules", "Virtual network rules", "Migrate to premium", "Tasks", "Diagnose and solve problems", "New support request"]) {
    assert.ok(page.includes(name), `the page lacks ${name}`);
  }
});

// A small JavaScript service for the end-to-end tests, using the official
// @azure/service-bus SDK the way a real service would. It implements the
// same commands as test/e2e/goservice:
//
//   node service.mjs produce --count n
//   node service.mjs work --name w1 [--idle 3000]
//   node service.mjs respond [--idle 3000]
//   node service.mjs request --count n
//   node service.mjs publish-events --count n
//   node service.mjs consume-events [--idle 3000]
//   node service.mjs send-payments --count n
//   node service.mjs collect-payments [--idle 3000]
//   node service.mjs publish-alerts --count n
//   node service.mjs consume-alerts [--idle 3000]
//
// It connects to OSB_CONNECTION_STRING and writes JSON event lines to
// stdout. With OSB_TRANSPORT=websocket it connects over WebSocket with
// Node's built-in WebSocket, trusting NODE_EXTRA_CA_CERTS.
import { parseArgs } from "node:util";
import { ServiceBusClient } from "@azure/service-bus";
import * as spec from "./spec.mjs";

const lang = "js";

const { positionals, values: flags } = parseArgs({
  allowPositionals: true,
  options: {
    count: { type: "string", default: "24" },
    name: { type: "string", default: "worker" },
    idle: { type: "string", default: "3000" },
  },
});
const count = Number(flags.count);
const idle = Number(flags.idle);

const transport = process.env.OSB_TRANSPORT || "amqp";
if (!["amqp", "websocket"].includes(transport)) {
  console.error(`unknown OSB_TRANSPORT ${transport}: want amqp or websocket`);
  process.exit(2);
}
const client = new ServiceBusClient(process.env.OSB_CONNECTION_STRING, {
  retryOptions: { maxRetries: 1, retryDelayInMs: 100, timeoutInMs: 10000 },
  ...(transport === "websocket" && { webSocketOptions: { webSocket: WebSocket } }),
});

const emit = (event) => console.log(JSON.stringify(event));

async function produce() {
  const sender = client.createSender(spec.ordersTopic);
  try {
    for (const o of spec.orders(count)) {
      await sender.sendMessages({
        messageId: o.id,
        sessionId: o.customer,
        subject: "order.created",
        contentType: "application/json",
        body: { id: o.id, customer: o.customer, seq: o.seq, quantity: o.quantity, amount: o.amount },
        applicationProperties: {
          customer: o.customer,
          quantity: o.quantity,
          amount: o.amount,
          express: o.express,
          kind: o.kind,
          createdAt: o.createdAt,
        },
      });
      emit({ event: "sent", id: o.id });
    }
  } finally {
    await sender.close();
  }
}

// work processes the orders of one customer session at a time, in order.
async function work() {
  const results = client.createSender(spec.resultsQueue);
  try {
    for (;;) {
      let receiver;
      try {
        receiver = await client.acceptNextSession(spec.ordersTopic, spec.fulfillmentSub, {
          maxAutoLockRenewalDurationInMs: 0,
          // By default the SDK turns Date properties into epoch numbers.
          skipConvertingDate: true,
          abortSignal: AbortSignal.timeout(idle),
        });
      } catch (err) {
        if (["SessionCannotBeLocked", "ServiceTimeout", "OperationTimeoutError"].includes(err.code) ||
            err.name === "AbortError") {
          return; // No session left to work on.
        }
        throw err;
      }
      try {
        await session(receiver, results);
      } finally {
        await receiver.close();
      }
    }
  } finally {
    await results.close();
  }
}

// session handles the messages of a session until it is empty. It takes one
// message at a time: a retried message must be processed before the ones
// after it.
async function session(receiver, results) {
  for (;;) {
    const [m] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 1000 });
    if (!m) return;
    // The JS SDK counts earlier deliveries; the attempt number starts at 1.
    const attempt = m.deliveryCount + 1;
    const kind = m.applicationProperties.kind;
    if (kind === "poison" || (kind === "flaky" && attempt === 1)) {
      emit({ event: "abandoned", id: m.messageId, attempt });
      await receiver.abandonMessage(m);
      continue;
    }
    const p = m.applicationProperties;
    await results.sendMessages({
      correlationId: m.messageId,
      subject: "order.processed",
      contentType: "application/json",
      body: {
        id: m.messageId,
        customer: m.sessionId,
        seq: m.body.seq,
        attempt,
        worker: flags.name,
        lang,
        body: m.body,
        props: {
          customer: p.customer,
          quantity: p.quantity,
          amount: p.amount,
          express: p.express,
          kind: p.kind,
          createdAt: toDate(p.createdAt).toISOString(),
        },
        subject: m.subject,
      },
      applicationProperties: { worker: flags.name, lang },
    });
    await receiver.completeMessage(m);
    emit({ event: "processed", id: m.messageId, attempt });
  }
}

// toDate reads a timestamp application property, which arrives as a Date
// with skipConvertingDate.
function toDate(v) {
  if (v instanceof Date) return v;
  throw new Error(`createdAt is ${typeof v} ${v}, want a Date`);
}

async function respond() {
  const receiver = client.createReceiver(spec.pricingQueue);
  const senders = new Map();
  try {
    for (;;) {
      const msgs = await receiver.receiveMessages(10, { maxWaitTimeInMs: idle });
      if (msgs.length === 0) return;
      for (const m of msgs) {
        if (!senders.has(m.replyTo)) senders.set(m.replyTo, client.createSender(m.replyTo));
        const reply = { correlationId: m.messageId, sessionId: m.replyToSessionId, subject: "price.quoted" };
        if (m.contentType === "application/json") {
          const { sku, qty } = m.body;
          reply.body = { sku, qty, total: qty * spec.unitPrice, responder: lang };
          reply.contentType = "application/json";
        } else {
          reply.body = Buffer.from(m.body).reverse();
          reply.contentType = "application/octet-stream";
        }
        await senders.get(m.replyTo).sendMessages(reply);
        await receiver.completeMessage(m);
        emit({ event: "replied", id: m.messageId });
      }
    }
  } finally {
    for (const s of senders.values()) await s.close();
    await receiver.close();
  }
}

async function request() {
  const sessionId = `${lang}-${Date.now().toString(36)}`;
  const sender = client.createSender(spec.pricingQueue);
  try {
    for (let i = 0; i < count; i++) {
      const msg = { messageId: `req-${i}`, replyTo: spec.pricingRepliesQueue, replyToSessionId: sessionId };
      if (spec.isBinaryRequest(i)) {
        msg.body = spec.binaryRequest(i);
        msg.contentType = "application/octet-stream";
      } else {
        msg.body = { sku: `sku-${i}`, qty: i + 1 };
        msg.contentType = "application/json";
      }
      await sender.sendMessages(msg);
    }
  } finally {
    await sender.close();
  }

  const receiver = await client.acceptSession(spec.pricingRepliesQueue, sessionId, { maxAutoLockRenewalDurationInMs: 0 });
  try {
    for (let got = 0; got < count;) {
      for (const m of await receiver.receiveMessages(count - got, { maxWaitTimeInMs: 5000 })) {
        const e = {
          event: "reply",
          correlationId: m.correlationId,
          sessionId: m.sessionId,
          contentType: m.contentType,
          subject: m.subject,
        };
        if (m.contentType === "application/json") e.body = m.body;
        else e.bodyHex = Buffer.from(m.body).toString("hex");
        await receiver.completeMessage(m);
        emit(e);
        got++;
      }
    }
  } finally {
    await receiver.close();
  }
}

async function publishEvents() {
  const sender = client.createSender(spec.eventsTopic);
  try {
    for (const e of spec.routedEvents(count)) {
      await sender.sendMessages({
        messageId: e.id,
        subject: e.subject,
        body: Buffer.from(e.id),
        applicationProperties: { event: e.event, isApproved: e.isApproved, quantity: e.quantity, region: e.region },
      });
    }
  } finally {
    await sender.close();
  }
  const jobs = client.createSender(spec.jobsQueue);
  try {
    for (let i = 0; i < spec.jobCount; i++) {
      await jobs.sendMessages({ messageId: `job-${i}`, body: "job" });
    }
  } finally {
    await jobs.close();
  }
}

// consumeEvents drains the routed subscriptions and the escalations queue,
// reporting what arrived where, then rejects the jobs.
async function consumeEvents() {
  const drain = async (entity, receiver) => {
    try {
      for (;;) {
        const msgs = await receiver.receiveMessages(10, { maxWaitTimeInMs: idle });
        if (msgs.length === 0) return;
        for (const m of msgs) {
          const p = m.applicationProperties ?? {};
          const e = { event: "routed", id: m.messageId, entity };
          if (p.RuleName !== undefined) e.ruleName = p.RuleName;
          if (p.priced !== undefined) e.priced = p.priced;
          if (p.total !== undefined) e.total = p.total;
          emit(e);
        }
      }
    } finally {
      await receiver.close();
    }
  };
  for (const sub of spec.routedSubscriptions) {
    await drain(sub, client.createReceiver(spec.eventsTopic, sub, { receiveMode: "receiveAndDelete" }));
  }
  await drain(spec.escalationsQueue, client.createReceiver(spec.escalationsQueue, { receiveMode: "receiveAndDelete" }));

  const jobs = client.createReceiver(spec.jobsQueue);
  try {
    for (let got = 0; got < spec.jobCount;) {
      for (const m of await jobs.receiveMessages(spec.jobCount - got, { maxWaitTimeInMs: 5000 })) {
        const reason = spec.jobRejectedPrefix + lang;
        await jobs.deadLetterMessage(m, { deadLetterReason: reason, deadLetterErrorDescription: "e2e" });
        emit({ event: "rejected", id: m.messageId, reason });
        got++;
      }
    }
  } finally {
    await jobs.close();
  }
}

// sendPayments sends the payments one by one, then all of them again in a
// batch, as a service retrying after timeouts would; see spec.PaymentID.
async function sendPayments() {
  const sender = client.createSender(spec.paymentsQueue);
  const payment = (i) => ({ messageId: spec.paymentId(i), body: lang });
  try {
    for (let i = 0; i < count; i++) {
      await sender.sendMessages(payment(i));
      emit({ event: "sent", id: spec.paymentId(i) });
    }
    const batch = await sender.createMessageBatch();
    for (let i = 0; i < count; i++) {
      if (!batch.tryAddMessage(payment(i))) throw new Error("the payments do not fit in a batch");
    }
    await sender.sendMessages(batch);
    for (let i = 0; i < count; i++) emit({ event: "sent", id: spec.paymentId(i) });
  } finally {
    await sender.close();
  }
}

// drainReceiver hands every message of receiver to handle until none
// arrives for idle.
async function drainReceiver(receiver, handle) {
  try {
    for (;;) {
      const msgs = await receiver.receiveMessages(10, { maxWaitTimeInMs: idle });
      if (msgs.length === 0) return;
      for (const m of msgs) await handle(m);
    }
  } finally {
    await receiver.close();
  }
}

async function collectPayments() {
  const receiver = client.createReceiver(spec.paymentsQueue);
  await drainReceiver(receiver, async (m) => {
    await receiver.completeMessage(m);
    emit({ event: "received", id: m.messageId, entity: spec.paymentsQueue });
  });
}

async function publishAlerts() {
  const sender = client.createSender(spec.alertsTopic);
  try {
    for (let i = 0; i < count; i++) {
      const { id, channel } = spec.alert(i);
      await sender.sendMessages({ messageId: id, body: id, applicationProperties: { channel } });
    }
  } finally {
    await sender.close();
  }
}

async function consumeAlerts() {
  for (const sub of spec.alertSubscriptions) {
    const receiver = client.createReceiver(spec.alertsTopic, sub, { receiveMode: "receiveAndDelete" });
    await drainReceiver(receiver, async (m) => emit({ event: "routed", id: m.messageId, entity: sub }));
  }
}

const commands = {
  produce,
  work,
  respond,
  request,
  "publish-events": publishEvents,
  "consume-events": consumeEvents,
  "send-payments": sendPayments,
  "collect-payments": collectPayments,
  "publish-alerts": publishAlerts,
  "consume-alerts": consumeAlerts,
};
const command = commands[positionals[0]];
if (!command) {
  console.error(`unknown command ${positionals[0]}: want ${Object.keys(commands).join(", ")}`);
  process.exit(2);
}
try {
  await command();
} finally {
  await client.close();
}

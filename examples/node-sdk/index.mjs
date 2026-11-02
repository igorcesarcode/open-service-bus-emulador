// Uses the official @azure/service-bus SDK against open-service-bus.
//
// Start the emulator with the entities of examples/config-json:
//
//   open-service-bus -config examples/config-json/Config.json
//
// then:
//
//   npm install
//   npm start
import { ServiceBusAdministrationClient, ServiceBusClient } from "@azure/service-bus";

// What the emulator prints at startup.
const connectionString =
  process.env.OSB_CONNECTION_STRING ??
  "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true";

const client = new ServiceBusClient(connectionString);

async function queueRoundTrip() {
  const sender = client.createSender("orders");
  await sender.sendMessages({ body: { id: 1 }, contentType: "application/json" });
  await sender.close();

  const receiver = client.createReceiver("orders");
  const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 5000 });
  console.log("queue orders: received", JSON.stringify(message.body));
  await receiver.completeMessage(message);
  await receiver.close();
}

async function topicRouting() {
  const sender = client.createSender("events");
  await sender.sendMessages([
    { body: "order of 3", applicationProperties: { quantity: 3 } },
    { body: "order of 50", applicationProperties: { quantity: 50 } },
  ]);
  await sender.close();

  // The "big-orders" subscription has the rule "quantity > 10".
  const receiver = client.createReceiver("events", "big-orders");
  const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 5000 });
  console.log("events/big-orders: received", JSON.stringify(message.body));
  await receiver.completeMessage(message);
  await receiver.close();
}

async function scheduled() {
  const sender = client.createSender("orders");
  const when = new Date(Date.now() + 2000);
  await sender.scheduleMessages({ body: "later" }, when);
  await sender.close();

  const receiver = client.createReceiver("orders");
  const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10000 });
  console.log("queue orders: scheduled message arrived:", message.body);
  await receiver.completeMessage(message);
  await receiver.close();
}

// The JavaScript administration client uses plain HTTP on the connection
// string's port in emulator mode. Values that Azure would refuse, such as a
// lock duration of 5 seconds, are fine here.
async function administration() {
  const admin = new ServiceBusAdministrationClient(connectionString);
  await admin.createQueue("node-sdk-demo", { lockDuration: "PT5S", maxDeliveryCount: 1 });
  const queue = await admin.getQueue("node-sdk-demo");
  console.log("admin: created queue node-sdk-demo, lock duration", queue.lockDuration);
  await admin.deleteQueue("node-sdk-demo");
}

try {
  await queueRoundTrip();
  await topicRouting();
  await scheduled();
  await administration();
} finally {
  await client.close();
}

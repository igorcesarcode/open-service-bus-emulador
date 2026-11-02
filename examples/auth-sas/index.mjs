// Shows shared access policies being enforced with `-auth sas`.
//
//   open-service-bus -auth sas -config examples/auth-sas/Config.json
//   npm install && npm start
import { ServiceBusClient } from "@azure/service-bus";

const endpoint = process.env.OSB_ENDPOINT ?? "sb://localhost:5672";
// The SDK retries refused connections; fail fast so the demo is quick.
const connect = (policy, key) =>
  new ServiceBusClient(
    `Endpoint=${endpoint};SharedAccessKeyName=${policy};SharedAccessKey=${key};EntityPath=orders;UseDevelopmentEmulator=true`,
    { retryOptions: { maxRetries: 0, timeoutInMs: 5000 } },
  );

// A policy with Send can send...
const producer = connect("producer", "producer-key");
const sender = producer.createSender("orders");
await sender.sendMessages({ body: "hello" });
console.log("producer (Send): sent");
await sender.close();
await producer.close();

// ...but not receive.
const asProducer = connect("producer", "producer-key");
try {
  const receiver = asProducer.createReceiver("orders");
  await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
  console.log("producer (Send): received?! authentication is not on");
} catch (err) {
  console.log("producer (Send): receive refused:", err.code ?? err.name);
}
await asProducer.close();

// A policy with Listen can receive.
const consumer = connect("consumer", "consumer-key");
const receiver = consumer.createReceiver("orders");
const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 5000 });
console.log("consumer (Listen): received", message.body);
await receiver.completeMessage(message);
await receiver.close();
await consumer.close();

// A wrong key is refused.
const wrong = connect("consumer", "not-the-key");
try {
  await wrong.createReceiver("orders").receiveMessages(1, { maxWaitTimeInMs: 2000 });
  console.log("wrong key accepted?!");
} catch (err) {
  console.log("wrong key: refused:", err.code ?? err.name);
}
await wrong.close();

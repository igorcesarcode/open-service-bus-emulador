// Sends, receives and completes a message with @azure/service-bus through an
// emulator that exports OpenTelemetry. Run by TestNodeTelemetry, which owns
// the OTLP receiver and checks what arrives.
import { test } from "node:test";
import assert from "node:assert/strict";
import { ServiceBusClient } from "@azure/service-bus";

const connectionString = process.env.OSB_CONNECTION_STRING;
if (!connectionString) {
  throw new Error("OSB_CONNECTION_STRING is not set; run through `go test ./test/integration -run TestNodeTelemetry`");
}

test("a message round trip with telemetry on", async () => {
  const client = new ServiceBusClient(connectionString);
  try {
    const sender = client.createSender("node-telemetry");
    await sender.sendMessages({ body: process.env.OSB_SECRET_BODY, applicationProperties: { kind: "traced" } });
    await sender.close();
    const receiver = client.createReceiver("node-telemetry");
    const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 5000 });
    assert.equal(message.body, process.env.OSB_SECRET_BODY);
    await receiver.completeMessage(message);
    await receiver.close();
  } finally {
    await client.close();
  }
});

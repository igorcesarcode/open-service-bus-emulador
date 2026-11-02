// @azure/service-bus 7.9.5 ignores UseDevelopmentEmulator for the management
// API and always calls https://. The emulator answers TLS on the same port with
// its self-signed certificate, which Node must be told to trust:
//
//   open-service-bus -listen :5672 -tls-cert-out cert.pem &
//   NODE_EXTRA_CA_CERTS=cert.pem npm test
import assert from "node:assert/strict";
import { ServiceBusAdministrationClient } from "@azure/service-bus";

const port = process.env.OSB_PORT ?? "5672";
const cs = `Endpoint=sb://localhost:${port}/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;`;
const admin = new ServiceBusAdministrationClient(cs);

await admin.createQueue("legacy-admin", { lockDuration: "PT7S" });
assert.equal((await admin.getQueue("legacy-admin")).lockDuration, "PT7S");

const names = [];
for await (const q of admin.listQueues()) names.push(q.name);
assert.ok(names.includes("legacy-admin"), `listQueues: ${names}`);

await admin.deleteQueue("legacy-admin");
assert.equal(await admin.queueExists("legacy-admin"), false);
console.log("7.9.5 administration client over HTTPS: ok");

// Contract shared by the end-to-end mini-services. Mirrors
// test/e2e/spec/spec.go; both must stay in sync.

export const ordersTopic = "orders";
export const fulfillmentSub = "fulfillment";
export const resultsQueue = "results";
export const pricingQueue = "pricing";
export const pricingRepliesQueue = "pricing-replies";

export const customers = 4;
export const unitPrice = 2.5;

const createdBase = Date.UTC(2026, 0, 2, 3, 4, 5, 678);

// orders returns the first n orders; see spec.Orders.
export function orders(n) {
  const out = [];
  for (let i = 0; i < n; i++) {
    let kind = "normal";
    if (i % 11 === 5) kind = "poison";
    else if (i % 6 === 1) kind = "flaky";
    out.push({
      id: `order-${i}`,
      customer: `customer-${i % customers}`,
      seq: Math.floor(i / customers),
      quantity: i + 1,
      amount: i * 10 + 0.25,
      express: i % 2 === 0,
      kind,
      createdAt: new Date(createdBase + i * 1000),
    });
  }
  return out;
}

export const isBinaryRequest = (i) => i % 3 === 2;

export const binaryRequest = (i) => Buffer.from([0xff, i & 0xff, 0x00, 0x10, 0x80]);

export const eventsTopic = "events";
export const escalationsQueue = "escalations";
export const jobsQueue = "jobs";
export const jobCount = 2;
export const jobRejectedPrefix = "rejected-by-";
export const routedSubscriptions = ["approved", "bulk", "eu-orders", "priced"];

// routedEvents returns the first n events; see spec.RoutedEvents.
export function routedEvents(n) {
  const kinds = ["completed", "approved", "created", "escalated"];
  const out = [];
  for (let i = 0; i < n; i++) {
    out.push({
      id: `event-${i}`,
      subject: i % 3 === 0 ? "order.created" : "order.updated",
      event: kinds[i % 4],
      isApproved: i % 3 !== 0,
      quantity: i * 3,
      region: i % 2 === 0 ? "eu" : "us",
    });
  }
  return out;
}

export const paymentsQueue = "payments";
export const paymentId = (i) => `payment-${i}`;

export const alertsTopic = "alerts";
export const alertChannels = ["sms", "email"];
export const alertSubscriptions = [...alertChannels, "everything"];

// alert returns alert i; see spec.Alert.
export function alert(i) {
  const channels = [...alertChannels, "push"];
  return { id: `alert-${i}`, channel: channels[i % channels.length] };
}

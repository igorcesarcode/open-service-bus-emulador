// Uses the official Azure.Messaging.ServiceBus SDK against open-service-bus.
//
// Start the emulator with the entities of examples/config-json:
//
//   open-service-bus -config examples/config-json/Config.json
//
// then:  dotnet run
using Azure.Messaging.ServiceBus;

// What the emulator prints at startup.
var connectionString = Environment.GetEnvironmentVariable("OSB_CONNECTION_STRING")
    ?? "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true";

await using var client = new ServiceBusClient(connectionString);

// Queue round trip.
await using (var sender = client.CreateSender("orders"))
{
    await sender.SendMessageAsync(new ServiceBusMessage("{\"id\": 1}") { ContentType = "application/json" });
}
await using (var receiver = client.CreateReceiver("orders"))
{
    var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
    Console.WriteLine($"queue orders: received {message.Body}");
    await receiver.CompleteMessageAsync(message);
}

// Topic routing: the "big-orders" subscription has the rule "quantity > 10".
await using (var sender = client.CreateSender("events"))
{
    foreach (var quantity in new[] { 3, 50 })
    {
        var message = new ServiceBusMessage($"order of {quantity}");
        message.ApplicationProperties["quantity"] = quantity;
        await sender.SendMessageAsync(message);
    }
}
await using (var receiver = client.CreateReceiver("events", "big-orders"))
{
    var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
    Console.WriteLine($"events/big-orders: received {message.Body}");
    await receiver.CompleteMessageAsync(message);
}

// Dead-lettering.
await using (var sender = client.CreateSender("jobs"))
{
    await sender.SendMessageAsync(new ServiceBusMessage("bad job"));
}
await using (var receiver = client.CreateReceiver("jobs"))
{
    var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
    await receiver.DeadLetterMessageAsync(message, "unparseable", "the job could not be parsed");
}
// "jobs" forwards its dead-lettered messages to the queue "jobs-poison".
await using (var receiver = client.CreateReceiver("jobs-poison"))
{
    var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
    Console.WriteLine($"queue jobs-poison: received {message.Body}");
    await receiver.CompleteMessageAsync(message);
}

// The administration client (ServiceBusAdministrationClient) always talks
// HTTPS on port 443 and does not know the emulator's port. To use it, give it
// an HttpClientTransport whose handler trusts the emulator's self-signed
// certificate and rewrites port 443 to the emulator's port; see
// test/dotnet/Harness.cs in the repository for a working handler.

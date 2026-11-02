// Validates open-service-bus with the official Azure.Messaging.ServiceBus SDK
// for .NET: transactions, send via and batch delete (which only this SDK
// has), and the same messaging, sessions, rules, admin, partitioning,
// namespace, lock and export scenarios as the Go and JavaScript suites.
//
// Run through the Go test TestDotnetSDK, which starts the emulator with the
// entities the scenarios use and sets OSB_CONNECTION_STRING. Each scenario
// prints "OK <name>"; a failure prints "FAIL <name>: <reason>" and the
// program exits non-zero.
var connectionString = Environment.GetEnvironmentVariable("OSB_CONNECTION_STRING")
    ?? throw new InvalidOperationException("OSB_CONNECTION_STRING is not set; run through `go test ./test/integration -run TestDotnetSDK`");

var c = Ctx.Create(connectionString);
await Transactions.Run(c);
await Messaging.Run(c);
await Sessions.Run(c);
await RulesAndAdmin.Run(c);
await Namespaces.Run(c);
await c.Client.DisposeAsync();
return H.Failures == 0 ? 0 : 1;

# .NET SDK

[`Program.cs`](Program.cs) uses `Azure.Messaging.ServiceBus` for a queue
round trip, topic routing with a subscription rule and dead-lettering with
forwarding, against the entities of [config-json](../config-json).

```bash
open-service-bus -config examples/config-json/Config.json    # terminal 1
cd examples/dotnet-sdk && dotnet run                          # terminal 2
```

The administration client (`ServiceBusAdministrationClient`) needs a custom
transport in emulator mode; the comment at the end of `Program.cs` explains
how, and `test/dotnet/Harness.cs` in this repository has a working one.

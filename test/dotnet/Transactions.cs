using System.Transactions;
using Azure.Messaging.ServiceBus;

/// <summary>Transactions, send via and batch delete: only this SDK has them.</summary>
static class Transactions
{
    public static async Task Run(Ctx c)
    {
        var client = c.Client;
        await using var txClient = c.NewClient(new ServiceBusClientOptions { EnableCrossEntityTransactions = true });
        // Cross-entity transactions need every link on one session, which the
        // option asks for.

        await H.Scenario("transaction: receive, send via and complete commit together", async () =>
        {
            await using var seed = client.CreateSender("dn-input");
            await seed.SendMessageAsync(new ServiceBusMessage("order"));
            await using var receiver = txClient.CreateReceiver("dn-input");
            await using var sender = txClient.CreateSender("dn-output");
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            H.Expect(message != null, "no message to receive");
            using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                await receiver.CompleteMessageAsync(message!);
                await sender.SendMessageAsync(new ServiceBusMessage("shipped"));
                scope.Complete();
            }
            H.Expect(await H.Count(client, "dn-input") == 0, "the input message was not consumed");
            H.Expect(await H.Count(client, "dn-output") == 1, "the output message did not appear");
        });

        await H.Scenario("transaction: rollback leaves the input and sends nothing", async () =>
        {
            await using var seed = client.CreateSender("dn-input");
            await seed.SendMessageAsync(new ServiceBusMessage("order-2"));
            await using var receiver = txClient.CreateReceiver("dn-input");
            await using var sender = txClient.CreateSender("dn-output");
            var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            H.Expect(message != null, "no message to receive");
            using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                await receiver.CompleteMessageAsync(message!);
                await sender.SendMessageAsync(new ServiceBusMessage("never"));
                // No scope.Complete(): the transaction rolls back.
            }
            H.Expect(await H.Count(client, "dn-output") == 1, "a rolled back send appeared");
            H.Expect(await H.Count(client, "dn-input") == 1, "the input message is gone");
        });

        await H.Scenario("transaction: sends to one entity commit together", async () =>
        {
            // A cross-entity client remembers the first entity it received from
            // and refuses to reconnect a sender elsewhere, so this one is fresh.
            await using var freshClient = c.NewClient(new ServiceBusClientOptions { EnableCrossEntityTransactions = true });
            await using var sender = freshClient.CreateSender("dn-tx-send");
            using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                await sender.SendMessageAsync(new ServiceBusMessage("a"));
                await sender.SendMessageAsync(new ServiceBusMessage("b"));
                H.Expect(await H.Count(client, "dn-tx-send") == 0, "messages are visible before the commit");
                scope.Complete();
            }
            H.Expect(await H.Count(client, "dn-tx-send") == 2, "the committed messages did not appear");
        });

        await H.Scenario("batch delete: DeleteMessagesAsync removes the oldest first", async () =>
        {
            await using var sender = client.CreateSender("dn-purge");
            for (var i = 0; i < 5; i++) await sender.SendMessageAsync(new ServiceBusMessage($"m{i}"));
            await using var receiver = client.CreateReceiver("dn-purge");
            var deleted = await receiver.DeleteMessagesAsync(3);
            H.Expect(deleted == 3, $"deleted {deleted}, want 3");
            var left = await receiver.PeekMessagesAsync(10);
            H.Expect(left.Count == 2 && left[0].Body.ToString() == "m3", "the oldest were not the ones deleted");
        });

        await H.Scenario("batch delete: PurgeMessagesAsync empties the queue", async () =>
        {
            await using var receiver = client.CreateReceiver("dn-purge");
            var purged = await receiver.PurgeMessagesAsync();
            H.Expect(purged == 2, $"purged {purged}, want 2");
            H.Expect(await H.Count(client, "dn-purge") == 0, "messages are left");
        });


    }
}

using Azure.Messaging.ServiceBus;

/// <summary>Sending and receiving: the scenarios of the Go and JavaScript suites.</summary>
static class Messaging
{
    public static async Task Run(Ctx c)
    {
        var client = c.Client;

        await H.Scenario("send and receive keep every property", async () =>
        {
            await using var sender = client.CreateSender("dn-basic");
            var msg = new ServiceBusMessage(BinaryData.FromString("{\"n\":1}"))
            {
                MessageId = "m-1", Subject = "order.created", ContentType = "application/json", CorrelationId = "corr-1",
                To = "dest", ReplyTo = "reply", TimeToLive = TimeSpan.FromMinutes(5),
            };
            msg.ApplicationProperties["str"] = "v";
            msg.ApplicationProperties["num"] = 42;
            msg.ApplicationProperties["flag"] = true;
            msg.ApplicationProperties["dbl"] = 1.5;
            await sender.SendMessageAsync(msg);
            await using var receiver = client.CreateReceiver("dn-basic");
            var got = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            H.Expect(got != null, "no message");
            H.Expect(got!.MessageId == "m-1" && got.Subject == "order.created" && got.ContentType == "application/json", "system properties differ");
            H.Expect(got.CorrelationId == "corr-1" && got.To == "dest" && got.ReplyTo == "reply", "addressing properties differ");
            H.Expect(got.TimeToLive == TimeSpan.FromMinutes(5), $"TTL {got.TimeToLive}");
            H.Expect((string)got.ApplicationProperties["str"] == "v" && (int)got.ApplicationProperties["num"] == 42
                && (bool)got.ApplicationProperties["flag"] && (double)got.ApplicationProperties["dbl"] == 1.5, "application properties differ");
            H.Expect(got.Body.ToString() == "{\"n\":1}", "body differs");
            H.Expect(got.DeliveryCount == 1, $"DeliveryCount {got.DeliveryCount}");
            H.Expect(got.SequenceNumber > 0 && got.EnqueuedTime > DateTimeOffset.UtcNow.AddMinutes(-5), "broker properties missing");
            await receiver.CompleteMessageAsync(got);
        });

        await H.Scenario("a missing entity reports MessagingEntityNotFound", async () =>
        {
            await using var receiver = client.CreateReceiver("dn-does-not-exist");
            var e = await H.Throws<ServiceBusException>(() => receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3)), "receive");
            H.Expect(e.Reason == ServiceBusFailureReason.MessagingEntityNotFound, $"reason {e.Reason}");
        });

        await H.Scenario("receive and delete removes the message", async () =>
        {
            await using var sender = client.CreateSender("dn-rad");
            await sender.SendMessageAsync(new ServiceBusMessage("x"));
            await using var receiver = client.CreateReceiver("dn-rad", H.Rad);
            var got = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            H.Expect(got?.Body.ToString() == "x", "no message");
            H.Expect(await H.Count(client, "dn-rad") == 0, "the message is still there");
        });

        await H.Scenario("batch send", async () =>
        {
            await using var sender = client.CreateSender("dn-batch");
            using var batch = await sender.CreateMessageBatchAsync();
            for (var i = 0; i < 20; i++) H.Expect(batch.TryAddMessage(new ServiceBusMessage($"b{i}")), "batch full");
            await sender.SendMessagesAsync(batch);
            await using var receiver = client.CreateReceiver("dn-batch", H.Rad);
            var got = await H.Receive(receiver, 20);
            H.Expect(got.Select(m => m.Body.ToString()).SequenceEqual(Enumerable.Range(0, 20).Select(i => $"b{i}")), "order differs");
        });

        await H.Scenario("abandon redelivers with modified properties", async () =>
        {
            await using var sender = client.CreateSender("dn-abandon");
            await sender.SendMessageAsync(new ServiceBusMessage("a"));
            await using var receiver = client.CreateReceiver("dn-abandon");
            var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await receiver.AbandonMessageAsync(first!, new Dictionary<string, object> { ["attempt"] = "one" });
            var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            H.Expect(second != null && second.DeliveryCount == 2, $"DeliveryCount {second?.DeliveryCount}");
            H.Expect((string)second!.ApplicationProperties["attempt"] == "one", "modified property missing");
            await receiver.CompleteMessageAsync(second);
        });

        await H.Scenario("dead-letter with reason and the max delivery count", async () =>
        {
            await using var sender = client.CreateSender("dn-dlq");
            await sender.SendMessagesAsync(new[] { new ServiceBusMessage("explicit"), new ServiceBusMessage("poison") });
            await using var receiver = client.CreateReceiver("dn-dlq");
            var explicitMsg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await receiver.DeadLetterMessageAsync(explicitMsg!, "Bad", "it is bad");
            // MaxDeliveryCount is 2: the second abandon moves it.
            for (var i = 0; i < 2; i++)
            {
                var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
                H.Expect(m != null, "poison message missing");
                await receiver.AbandonMessageAsync(m!);
            }
            await using var dlq = client.CreateReceiver("dn-dlq", new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter, ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
            var got = await H.Receive(dlq, 2);
            var byBody = got.ToDictionary(m => m.Body.ToString());
            H.Expect(byBody["explicit"].DeadLetterReason == "Bad" && byBody["explicit"].DeadLetterErrorDescription == "it is bad", "explicit reason differs");
            H.Expect(byBody["poison"].DeadLetterReason == "MaxDeliveryCountExceeded", $"reason {byBody["poison"].DeadLetterReason}");
        });

        await H.Scenario("lock expiry, renewal and lock lost", async () =>
        {
            await using var sender = client.CreateSender("dn-lock");
            await sender.SendMessageAsync(new ServiceBusMessage("l"));
            await using var receiver = client.CreateReceiver("dn-lock");
            var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            var until = m!.LockedUntil;
            await Task.Delay(500);
            await receiver.RenewMessageLockAsync(m);
            H.Expect(m.LockedUntil > until, "the lock was not renewed");
            await Task.Delay(TimeSpan.FromSeconds(3.5));
            var e = await H.Throws<ServiceBusException>(() => receiver.CompleteMessageAsync(m), "complete after expiry");
            H.Expect(e.Reason == ServiceBusFailureReason.MessageLockLost, $"reason {e.Reason}");
            var again = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            H.Expect(again != null && again.DeliveryCount == 2, "the message was not redelivered");
            await receiver.CompleteMessageAsync(again!);
        });

        await H.Scenario("defer and receive deferred", async () =>
        {
            await using var sender = client.CreateSender("dn-defer");
            await sender.SendMessageAsync(new ServiceBusMessage("d"));
            await using var receiver = client.CreateReceiver("dn-defer");
            var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            await receiver.DeferMessageAsync(m!);
            H.Expect(await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) == null, "a deferred message was delivered");
            var deferred = await receiver.ReceiveDeferredMessageAsync(m!.SequenceNumber);
            H.Expect(deferred.Body.ToString() == "d", "deferred body differs");
            await receiver.CompleteMessageAsync(deferred);
        });

        await H.Scenario("schedule and cancel", async () =>
        {
            await using var sender = client.CreateSender("dn-schedule");
            var at = DateTimeOffset.UtcNow.AddSeconds(2);
            var keep = await sender.ScheduleMessageAsync(new ServiceBusMessage("keep"), at);
            var cancel = await sender.ScheduleMessageAsync(new ServiceBusMessage("cancel"), at);
            await sender.CancelScheduledMessageAsync(cancel);
            await using var receiver = client.CreateReceiver("dn-schedule", H.Rad);
            H.Expect(await receiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500)) == null, "delivered early");
            var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(6));
            H.Expect(m?.Body.ToString() == "keep" && m.SequenceNumber == keep, $"got {m?.Body}");
            await H.ExpectNone(receiver, "cancelled message");
        });

        await H.Scenario("peek does not lock", async () =>
        {
            await using var sender = client.CreateSender("dn-peek");
            await sender.SendMessagesAsync(new[] { new ServiceBusMessage("p1"), new ServiceBusMessage("p2"), new ServiceBusMessage("p3") });
            await using var receiver = client.CreateReceiver("dn-peek");
            var peeked = await receiver.PeekMessagesAsync(10);
            H.Expect(peeked.Count == 3, $"peeked {peeked.Count}");
            var from = await receiver.PeekMessagesAsync(10, peeked[1].SequenceNumber);
            H.Expect(from.Count == 2 && from[0].Body.ToString() == "p2", "peek from a sequence number differs");
            var received = await H.Receive(receiver, 3);
            foreach (var m in received) await receiver.CompleteMessageAsync(m);
        });

        await H.Scenario("the processor streams messages", async () =>
        {
            await using var sender = client.CreateSender("dn-processor");
            await sender.SendMessagesAsync(Enumerable.Range(0, 5).Select(i => new ServiceBusMessage($"s{i}")));
            await using var processor = client.CreateProcessor("dn-processor", new ServiceBusProcessorOptions { MaxConcurrentCalls = 1 });
            var seen = new List<string>();
            var done = new TaskCompletionSource();
            processor.ProcessMessageAsync += async args =>
            {
                lock (seen) seen.Add(args.Message.Body.ToString());
                await args.CompleteMessageAsync(args.Message);
                if (seen.Count == 5) done.TrySetResult();
            };
            processor.ProcessErrorAsync += args => { done.TrySetException(args.Exception); return Task.CompletedTask; };
            await processor.StartProcessingAsync();
            await Task.WhenAny(done.Task, Task.Delay(TimeSpan.FromSeconds(10)));
            await processor.StopProcessingAsync();
            H.Expect(seen.Count == 5, $"processed {seen.Count}/5");
            H.Expect(await H.Count(client, "dn-processor") == 0, "messages were left");
        });

        await H.Scenario("topic fan-out reaches every subscription", async () =>
        {
            await using var sender = client.CreateSender("dn-events");
            await sender.SendMessageAsync(new ServiceBusMessage("event"));
            foreach (var sub in new[] { "audit", "billing" })
            {
                await using var receiver = client.CreateReceiver("dn-events", sub, H.Rad);
                var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
                H.Expect(m?.Body.ToString() == "event", $"{sub} got nothing");
            }
        });

        await H.Scenario("auto-forwarding delivers to the destination", async () =>
        {
            await using var sender = client.CreateSender("dn-fwd-front");
            await sender.SendMessageAsync(new ServiceBusMessage("forwarded"));
            await using var receiver = client.CreateReceiver("dn-fwd-inbox", H.Rad);
            H.Expect((await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString() == "forwarded", "nothing arrived");
        });

        await H.Scenario("duplicate detection drops a repeated message id", async () =>
        {
            await using var sender = client.CreateSender("dn-dedup");
            await sender.SendMessageAsync(new ServiceBusMessage("one") { MessageId = "same" });
            await sender.SendMessageAsync(new ServiceBusMessage("two") { MessageId = "same" });
            H.Expect(await H.Count(client, "dn-dedup") == 1, "the duplicate was stored");
        });

        await H.Scenario("a full entity reports QuotaExceeded", async () =>
        {
            await using var sender = client.CreateSender("dn-quota");
            var body = new byte[200 * 1024];
            ServiceBusException? failure = null;
            for (var i = 0; i < 12 && failure == null; i++)
            {
                try { await sender.SendMessageAsync(new ServiceBusMessage(body)); }
                catch (ServiceBusException e) { failure = e; }
            }
            H.Expect(failure != null && failure.Reason == ServiceBusFailureReason.QuotaExceeded, $"reason {failure?.Reason}");
        });

        await H.Scenario("a partitioned queue keeps the partition key and the session id", async () =>
        {
            await using var sender = client.CreateSender("dn-partitioned");
            await sender.SendMessageAsync(new ServiceBusMessage("k") { PartitionKey = "p1" });
            await using var receiver = client.CreateReceiver("dn-partitioned", H.Rad);
            var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            H.Expect(m?.PartitionKey == "p1", $"partition key {m?.PartitionKey}");
            var partition = m!.SequenceNumber >> 48;
            H.Expect(partition >= 0 && partition < 16, $"partition {partition} in the sequence number");
            await sender.SendMessageAsync(new ServiceBusMessage("k2") { PartitionKey = "p1" });
            var sameKey = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            H.Expect(sameKey!.SequenceNumber >> 48 == partition, "the same key moved to another partition");
            // A message with a session id is accepted too: the partition key is the session id.
            await sender.SendMessageAsync(new ServiceBusMessage("sess") { SessionId = "s-1" });
            var again = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            H.Expect(again?.SessionId == "s-1", $"session id {again?.SessionId}");
        });
    }
}

using Azure.Messaging.ServiceBus;

static class Sessions
{
    public static async Task Run(Ctx c)
    {
        var client = c.Client;

        await H.Scenario("sessions: ordered delivery, state and a specific session", async () =>
        {
            await using var sender = client.CreateSender("dn-sessions");
            foreach (var (sid, body) in new[] { ("A", "a1"), ("B", "b1"), ("A", "a2"), ("B", "b2") })
                await sender.SendMessageAsync(new ServiceBusMessage(body) { SessionId = sid });

            await using var a = await client.AcceptSessionAsync("dn-sessions", "A");
            var got = await H.Receive(a, 2);
            H.Expect(got.Select(m => m.Body.ToString()).SequenceEqual(new[] { "a1", "a2" }), "session A order differs");
            foreach (var m in got) await a.CompleteMessageAsync(m);
            await a.SetSessionStateAsync(BinaryData.FromString("state-A"));
            H.Expect((await a.GetSessionStateAsync()).ToString() == "state-A", "session state differs");

            // The next available session is the other one.
            await using var next = await client.AcceptNextSessionAsync("dn-sessions");
            H.Expect(next.SessionId == "B", $"next session {next.SessionId}");
            var b = await H.Receive(next, 2);
            H.Expect(b.Select(m => m.Body.ToString()).SequenceEqual(new[] { "b1", "b2" }), "session B order differs");
            foreach (var m in b) await next.CompleteMessageAsync(m);
        });

        await H.Scenario("a message without a session id is refused by a session queue", async () =>
        {
            await using var sender = client.CreateSender("dn-sessions");
            var e = await H.Throws<Exception>(() => sender.SendMessageAsync(new ServiceBusMessage("no session")), "send without a session id");
            H.Expect(e.Message.Contains("session"), $"message: {e.Message}");
        });

        await H.Scenario("accepting a session of an empty queue times out", async () =>
        {
            await using var quick = c.NewClient(new ServiceBusClientOptions
            {
                RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(3) },
            });
            var e = await H.Throws<ServiceBusException>(() => quick.AcceptNextSessionAsync("dn-sessions-empty"), "accept next session");
            H.Expect(e.Reason == ServiceBusFailureReason.ServiceTimeout, $"reason {e.Reason}");
        });
    }
}

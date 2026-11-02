using Azure.Messaging.ServiceBus;

/// <summary>Several namespaces, Geo-DR, the portal API and the export.</summary>
static class Namespaces
{
    public static async Task Run(Ctx c)
    {
        async Task Send(string host, string queue, string body)
        {
            await using var client = c.ClientFor(host);
            await using var sender = client.CreateSender(queue);
            await sender.SendMessageAsync(new ServiceBusMessage(body));
        }
        async Task<List<string>> Peek(string host, string queue)
        {
            await using var client = c.ClientFor(host);
            await using var receiver = client.CreateReceiver(queue);
            return (await receiver.PeekMessagesAsync(100)).Select(m => m.Body.ToString()).ToList();
        }

        // These two connect over AMQP with TLS to names the emulator's
        // certificate covers. The SDK has no certificate callback, and only
        // OpenSSL honors the SSL_CERT_FILE the test sets; macOS and Windows
        // use the system store.
        if (OperatingSystem.IsLinux())
        {
            await H.Scenario("namespaces are told apart by hostname", async () =>
            {
                await Send("dn-a.localhost", "dn-shared", "for-a");
                await Send("dn-b.localhost", "dn-shared", "for-b");
                H.Expect((await Peek("dn-a.localhost", "dn-shared")).SequenceEqual(new[] { "for-a" }), "namespace a differs");
                H.Expect((await Peek("dn-b.localhost", "dn-shared")).SequenceEqual(new[] { "for-b" }), "namespace b differs");
            });

            await H.Scenario("a Geo-DR alias serves the primary and follows a failover", async () =>
            {
                await Send("dn-bus.servicebus.windows.net", "dn-shared", "via-alias");
                H.Expect((await Peek("dn-a.localhost", "dn-shared")).Contains("via-alias"), "the alias did not reach the primary");
                var res = await c.Http.PostAsync($"https://{c.HostPort}/$portal/api/geo-dr",
                    new StringContent("{\"action\":\"failover\",\"alias\":\"dn-bus\"}", System.Text.Encoding.UTF8, "application/json"));
                H.Expect(res.IsSuccessStatusCode, $"failover: {(int)res.StatusCode} {await res.Content.ReadAsStringAsync()}");
                await Send("dn-bus.servicebus.windows.net", "dn-shared", "after-failover");
                H.Expect((await Peek("dn-b.localhost", "dn-shared")).Contains("after-failover"), "the alias did not follow the failover");
            });
        }

        await H.Scenario("the export carries the entities and the locks", async () =>
        {
            var config = await c.Http.GetStringAsync($"https://{c.HostPort}/$portal/export/Config.json");
            H.Expect(config.Contains("dn-frozen") && config.Contains("ReadOnly") && config.Contains("CanNotDelete"), "Config.json lacks the entities or the locks");
            var tf = await c.Http.GetStringAsync($"https://{c.HostPort}/$portal/export/main.tf");
            H.Expect(tf.Contains("azurerm_management_lock") && tf.Contains("azurerm_servicebus_queue"), "main.tf lacks the resources");
        });
    }
}

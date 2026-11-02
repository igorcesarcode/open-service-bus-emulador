using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

/// <summary>Subscription rules and the management API.</summary>
static class RulesAndAdmin
{
    static async Task<Exception> Fails(Func<Task> action, string what)
    {
        try { await action(); }
        catch (Exception e) { return e; }
        throw new Exception($"{what}: it did not fail");
    }

    static int Status(Exception e) => e is RequestFailedException r ? r.Status : 0;

    public static async Task Run(Ctx c)
    {
        var client = c.Client;
        var admin = c.Admin;

        await H.Scenario("sql filters: strings, numbers, LIKE and parameters of every type", async () =>
        {
            await admin.CreateTopicAsync("dn-typed");
            var started = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
            async Task Sub(string name, SqlRuleFilter filter)
            {
                await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("dn-typed", name), new CreateRuleOptions("r", filter));
            }
            await Sub("like", new SqlRuleFilter("Code LIKE 'order-%' AND Quantity >= 10"));
            await Sub("escape", new SqlRuleFilter("Code LIKE '%!%%' ESCAPE '!'"));
            await Sub("delimited", new SqlRuleFilter("[Order Type] = 'priority'"));
            var span = new SqlRuleFilter("(Done - Started) <= @span");
            span.Parameters["@span"] = TimeSpan.FromHours(1);
            await Sub("timespan", span);
            var when = new SqlRuleFilter("Started < @when");
            when.Parameters["@when"] = started.AddMinutes(1);
            await Sub("datetime", when);
            var param = new SqlRuleFilter("Quantity = @n AND Code = @c AND Price > @f");
            param.Parameters["@n"] = 15;
            param.Parameters["@c"] = "order-1";
            param.Parameters["@f"] = 2.5;
            await Sub("params", param);

            await using var sender = client.CreateSender("dn-typed");
            ServiceBusMessage M(string body, Action<IDictionary<string, object>> set)
            {
                var m = new ServiceBusMessage(body);
                set(m.ApplicationProperties);
                return m;
            }
            await sender.SendMessagesAsync(new[]
            {
                M("m1", p => { p["Code"] = "order-1"; p["Quantity"] = 15; p["Order Type"] = "priority"; p["Price"] = 3.0; p["Started"] = started; p["Done"] = started.AddMinutes(30); }),
                M("m2", p => { p["Code"] = "100%"; p["Quantity"] = 9; p["Order Type"] = "standard"; p["Price"] = 1.0; p["Started"] = started.AddHours(2); p["Done"] = started.AddHours(5); }),
            });
            var want = new Dictionary<string, string[]>
            {
                ["like"] = new[] { "m1" }, ["escape"] = new[] { "m2" }, ["delimited"] = new[] { "m1" },
                ["timespan"] = new[] { "m1" }, ["datetime"] = new[] { "m1" }, ["params"] = new[] { "m1" },
            };
            foreach (var (sub, bodies) in want)
            {
                await using var receiver = client.CreateReceiver("dn-typed", sub, H.Rad);
                var got = await H.Receive(receiver, bodies.Length);
                H.Expect(got.Select(m => m.Body.ToString()).SequenceEqual(bodies), $"{sub}: got {string.Join(",", got.Select(m => m.Body.ToString()))}");
                await H.ExpectNone(receiver, sub);
            }
        });

        await H.Scenario("the rule keeps its parameters when read back", async () =>
        {
            var rule = await admin.GetRuleAsync("dn-typed", "timespan", "r");
            var filter = (SqlRuleFilter)rule.Value.Filter;
            H.Expect(filter.SqlExpression == "(Done - Started) <= @span", $"expression {filter.SqlExpression}");
            H.Expect(filter.Parameters["@span"] is TimeSpan t && t == TimeSpan.FromHours(1), $"@span is {filter.Parameters["@span"]}");
        });

        await H.Scenario("correlation filters and rule actions", async () =>
        {
            await admin.CreateTopicAsync("dn-corr");
            var corr = new CorrelationRuleFilter { Subject = "order.created" };
            corr.ApplicationProperties["region"] = "eu";
            await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("dn-corr", "eu"), new CreateRuleOptions("r", corr));
            await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("dn-corr", "acting"),
                new CreateRuleOptions("a", new TrueRuleFilter()) { Action = new SqlRuleAction("SET quantity = quantity / 2; SET sys.CorrelationId = 'low'") });
            await using var sender = client.CreateSender("dn-corr");
            var hit = new ServiceBusMessage("hit") { Subject = "order.created" };
            hit.ApplicationProperties["region"] = "eu";
            hit.ApplicationProperties["quantity"] = 10;
            var miss = new ServiceBusMessage("miss") { Subject = "order.created" };
            miss.ApplicationProperties["region"] = "us";
            await sender.SendMessagesAsync(new[] { hit, miss });
            await using var eu = client.CreateReceiver("dn-corr", "eu", H.Rad);
            H.Expect((await H.Receive(eu, 1))[0].Body.ToString() == "hit", "correlation filter picked the wrong message");
            await H.ExpectNone(eu, "eu");
            await using var acting = client.CreateReceiver("dn-corr", "acting", H.Rad);
            var both = await H.Receive(acting, 2);
            var first = both.First(m => m.Body.ToString() == "hit");
            H.Expect(Convert.ToInt64(first.ApplicationProperties["quantity"]) == 5 && first.CorrelationId == "low", "the rule action did not apply");
        });

        await H.Scenario("rule manager creates, lists and deletes rules", async () =>
        {
            await admin.CreateTopicAsync("dn-rm");
            await admin.CreateSubscriptionAsync("dn-rm", "managed");
            await using var rules = client.CreateRuleManager("dn-rm", "managed");
            await rules.CreateRuleAsync("big", new SqlRuleFilter("quantity > 10"));
            var names = new List<string>();
            await foreach (var r in rules.GetRulesAsync()) names.Add(r.Name);
            H.Expect(names.Contains("big") && names.Contains("$Default"), $"rules {string.Join(",", names)}");
            await rules.DeleteRuleAsync("$Default");
            var created = await Fails(() => rules.CreateRuleAsync("big", new TrueRuleFilter()), "create a rule twice");
            H.Expect(created is ServiceBusException, $"duplicate rule: {created.GetType().Name}");
        });

        await H.Scenario("admin: queues, runtime properties and listing", async () =>
        {
            var created = await admin.CreateQueueAsync(new CreateQueueOptions("dn-admin")
            {
                LockDuration = TimeSpan.FromMinutes(10), MaxDeliveryCount = 500, DefaultMessageTimeToLive = TimeSpan.FromDays(30), UserMetadata = "meta <&>",
            });
            H.Expect(created.Value.LockDuration == TimeSpan.FromMinutes(10) && created.Value.MaxDeliveryCount == 500, "created properties differ");
            var dup = await Fails(() => admin.CreateQueueAsync("dn-admin"), "create again");
            H.Expect(Status(dup) == 409 || dup is ServiceBusException, $"duplicate: {dup.GetType().Name}");
            H.Expect(await admin.QueueExistsAsync("dn-admin") && !await admin.QueueExistsAsync("dn-admin-missing"), "QueueExists differs");

            await using var sender = client.CreateSender("dn-admin");
            await sender.SendMessagesAsync(new[] { new ServiceBusMessage("a"), new ServiceBusMessage("b") });
            var rt = await admin.GetQueueRuntimePropertiesAsync("dn-admin");
            H.Expect(rt.Value.TotalMessageCount == 2 && rt.Value.ActiveMessageCount == 2 && rt.Value.SizeInBytes > 0, $"runtime {rt.Value.TotalMessageCount}/{rt.Value.ActiveMessageCount}");

            var names = new List<string>();
            await foreach (var q in admin.GetQueuesAsync()) names.Add(q.Name);
            H.Expect(names.Contains("dn-admin"), "the queue is not listed");
            await admin.DeleteQueueAsync("dn-admin");
            H.Expect(!await admin.QueueExistsAsync("dn-admin"), "the queue was not deleted");
        });

        await H.Scenario("admin: an update keeps the description and the immutable properties", async () =>
        {
            await admin.CreateQueueAsync(new CreateQueueOptions("dn-replace")
            {
                MaxDeliveryCount = 7, LockDuration = TimeSpan.FromMinutes(3), RequiresDuplicateDetection = true, UserMetadata = "meta",
            });
            var cur = (await admin.GetQueueAsync("dn-replace")).Value;
            cur.LockDuration = TimeSpan.FromMinutes(2);
            var full = (await admin.UpdateQueueAsync(cur)).Value;
            H.Expect(full.LockDuration == TimeSpan.FromMinutes(2) && full.MaxDeliveryCount == 7 && full.UserMetadata == "meta", "get, change, update lost properties");

            // The SDK cannot change RequiresDuplicateDetection or RequiresSession on
            // an update (they are read-only), and the update keeps them.
            H.Expect(full.RequiresDuplicateDetection && !full.RequiresSession, "an update changed an immutable property");
            await admin.DeleteQueueAsync("dn-replace");
        });

        await H.Scenario("admin: topics, subscriptions and their runtime properties", async () =>
        {
            await admin.CreateTopicAsync(new CreateTopicOptions("dn-adm-topic") { DefaultMessageTimeToLive = TimeSpan.FromDays(2) });
            await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions("dn-adm-topic", "s1") { MaxDeliveryCount = 7 });
            var sub = (await admin.GetSubscriptionAsync("dn-adm-topic", "s1")).Value;
            H.Expect(sub.MaxDeliveryCount == 7, "subscription properties differ");
            await using var sender = client.CreateSender("dn-adm-topic");
            await sender.SendMessageAsync(new ServiceBusMessage("t"));
            var rt = (await admin.GetSubscriptionRuntimePropertiesAsync("dn-adm-topic", "s1")).Value;
            H.Expect(rt.TotalMessageCount == 1, $"subscription count {rt.TotalMessageCount}");
            await admin.DeleteTopicAsync("dn-adm-topic");
            H.Expect(!await admin.TopicExistsAsync("dn-adm-topic"), "the topic was not deleted");
        });

        await H.Scenario("resource locks stop changes, not messages", async () =>
        {
            var frozen = (await admin.GetQueueAsync("dn-frozen")).Value;
            frozen.MaxDeliveryCount = 3;
            var e = await Fails(() => admin.UpdateQueueAsync(frozen), "update a ReadOnly queue");
            H.Expect(e.Message.Contains("ScopeLocked") || e.Message.Contains("locked"), $"message: {e.Message}");
            e = await Fails(() => admin.DeleteQueueAsync("dn-frozen"), "delete a ReadOnly queue");
            H.Expect(e.Message.Contains("locked"), $"message: {e.Message}");
            await using var sender = client.CreateSender("dn-frozen");
            await sender.SendMessageAsync(new ServiceBusMessage("flows"));
            await using var receiver = client.CreateReceiver("dn-frozen", H.Rad);
            H.Expect((await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5)))?.Body.ToString() == "flows", "messages do not flow");

            var kept = (await admin.GetQueueAsync("dn-kept")).Value;
            kept.MaxDeliveryCount = 6;
            await admin.UpdateQueueAsync(kept);
            await Fails(() => admin.DeleteQueueAsync("dn-kept"), "delete a CanNotDelete queue");
        });
    }
}

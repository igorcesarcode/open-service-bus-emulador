using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

/// <summary>What every scenario shares.</summary>
sealed class Ctx
{
    public required string ConnectionString { get; init; }
    public ServiceBusClient Client { get; private set; } = null!;
    public required ServiceBusAdministrationClient Admin { get; init; }
    public required HttpClient Http { get; init; }
    /// <summary>host:port the emulator listens on.</summary>
    public required string HostPort { get; init; }

    /// <summary>
    /// A client for another namespace. The development emulator flag only
    /// accepts a local endpoint, so this one presents the namespace's host
    /// name over AMQP with TLS, connecting to the emulator through a custom
    /// endpoint address. The test sets SSL_CERT_FILE to the emulator's
    /// certificate, which covers the names used.
    /// </summary>
    public ServiceBusClient ClientFor(string host) => new(
        $"Endpoint=sb://{host};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE",
        new ServiceBusClientOptions { CustomEndpointAddress = new Uri($"sb://{HostPort}") });

    /// <summary>
    /// A client of the main namespace. With OSB_NO_EMULATOR_FLAG the
    /// connection string is the real service's: its servicebus.windows.net
    /// name, no UseDevelopmentEmulator, AMQP over TLS. Only the socket goes
    /// to the emulator, through a custom endpoint address.
    /// </summary>
    public ServiceBusClient NewClient(ServiceBusClientOptions? options = null)
    {
        options ??= new ServiceBusClientOptions();
        if (NoEmulatorFlag) options.CustomEndpointAddress = new Uri($"sb://{HostPort}");
        return new ServiceBusClient(ConnectionString, options);
    }

    public static bool NoEmulatorFlag => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("OSB_NO_EMULATOR_FLAG"));

    /// <summary>The namespace name of the real service the variant presents.</summary>
    public const string RealHost = "osb-test.servicebus.windows.net";

    public static Ctx Create(string connectionString)
    {
        var endpoint = connectionString.Split(';').First(p => p.StartsWith("Endpoint=")).Substring("Endpoint=sb://".Length);
        var hostPort = endpoint.TrimEnd('/');
        var port = int.Parse(hostPort.Split(':')[1]);
        HttpMessageHandler handler;
        if (NoEmulatorFlag)
        {
            // The management API is HTTPS on port 443 of the namespace's name,
            // as for the real service; the socket goes to the emulator.
            connectionString = $"Endpoint=sb://{RealHost}/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE";
            handler = new SocketsHttpHandler
            {
                SslOptions = { RemoteCertificateValidationCallback = (_, _, _, _) => true },
                ConnectCallback = async (_, ct) =>
                {
                    var socket = new System.Net.Sockets.Socket(System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
                    await socket.ConnectAsync("127.0.0.1", port, ct);
                    return new System.Net.Sockets.NetworkStream(socket, ownsSocket: true);
                },
            };
        }
        else
        {
            // The management API is HTTPS with the emulator's self-signed
            // certificate, on the emulator's port instead of 443.
            handler = new PortRewrite(new HttpClientHandler { ServerCertificateCustomValidationCallback = (_, _, _, _) => true }, port);
        }
        var http = new HttpClient(handler);
        var admin = new ServiceBusAdministrationClient(connectionString,
            new ServiceBusAdministrationClientOptions { Transport = new HttpClientTransport(http) });
        var ctx = new Ctx
        {
            ConnectionString = connectionString,
            Admin = admin,
            Http = http,
            HostPort = hostPort,
        };
        ctx.Client = ctx.NewClient();
        return ctx;
    }
}

static class H
{
    public static int Failures;

    public static async Task Scenario(string name, Func<Task> body)
    {
        try
        {
            await body();
            Console.WriteLine($"OK {name}");
        }
        catch (Exception e)
        {
            Failures++;
            Console.WriteLine($"FAIL {name}: {e.Message}");
        }
    }

    public static void Expect(bool condition, string what)
    {
        if (!condition) throw new Exception(what);
    }

    public static async Task<int> Count(ServiceBusClient client, string queue)
    {
        await using var receiver = client.CreateReceiver(queue);
        return (await receiver.PeekMessagesAsync(1000)).Count;
    }

    public static ServiceBusReceiverOptions Rad => new() { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete };

    public static async Task<List<ServiceBusReceivedMessage>> Receive(ServiceBusReceiver r, int n, int seconds = 10)
    {
        var got = new List<ServiceBusReceivedMessage>();
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (got.Count < n && DateTime.UtcNow < deadline)
            got.AddRange(await r.ReceiveMessagesAsync(n - got.Count, TimeSpan.FromSeconds(2)));
        Expect(got.Count == n, $"received {got.Count}/{n} messages");
        return got;
    }

    public static async Task ExpectNone(ServiceBusReceiver r, string where)
    {
        var m = await r.ReceiveMessageAsync(TimeSpan.FromMilliseconds(500));
        Expect(m == null, $"{where}: unexpected message {m?.Body}");
    }

    public static async Task<T> Throws<T>(Func<Task> action, string what) where T : Exception
    {
        try { await action(); }
        catch (T e) { return e; }
        throw new Exception($"{what}: no {typeof(T).Name}");
    }
}

/// <summary>
/// The administration client takes only the host of the endpoint and talks to
/// port 443, so the requests are sent to the emulator's port instead.
/// </summary>
sealed class PortRewrite : DelegatingHandler
{
    readonly int _port;
    public PortRewrite(HttpMessageHandler inner, int port) : base(inner) { _port = port; }
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (request.RequestUri is { } uri && uri.Port == 443)
            request.RequestUri = new UriBuilder(uri) { Port = _port }.Uri;
        return base.SendAsync(request, cancellationToken);
    }
}

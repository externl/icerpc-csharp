// Copyright (c) ZeroC, Inc.

using IceRpc;
using IceRpc.Transports;
using IceRpc.Transports.Quic;
using IceRpc.Transports.Slic;
using IceRpc.Transports.Tcp;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Quic;
using System.Net.Sockets;
using System.Net.Security;

// Connects to the build-telemetry server the way the build-telemetry generator does, then closes the connection,
// printing one line per attempt with the time each phase took. Each round opens --parallel connections at once.
bool tcp = false;
int timeoutSeconds = 60;
int count = 1;
int parallel = 1;
int fill = 0;
int holdSeconds = 600;
int roundIntervalMs = 0;
string uri = "icerpc://build-telemetry.icerpc.dev";
for (int i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--transport":
            tcp = args[++i] == "tcp";
            break;
        case "--timeout":
            timeoutSeconds = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--count":
            count = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--parallel":
            parallel = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--uri":
            uri = args[++i];
            break;
        case "--fill":
            fill = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--hold":
            holdSeconds = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--round-interval":
            roundIntervalMs = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        default:
            Console.Error.WriteLine($"unknown argument: {args[i]}");
            return 2;
    }
}

if (fill > 0)
{
    await FillAsync(uri, fill, holdSeconds);
    return 0;
}

int failures = 0;
DateTime first = DateTime.UtcNow;
for (int round = 0; round < count; round++)
{
    string[] lines = await Task.WhenAll(Enumerable.Range(0, parallel).Select(_ => AttemptAsync(uri, tcp, timeoutSeconds)));
    foreach (string line in lines)
    {
        Console.WriteLine(line);
        if (line.Contains("result=FAIL"))
        {
            failures++;
        }
    }

    // Start rounds on a fixed cadence, so the connection rate stays put as the burst size changes.
    if (roundIntervalMs > 0)
    {
        TimeSpan delay = first.AddMilliseconds((double)roundIntervalMs * (round + 1)) - DateTime.UtcNow;
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay);
        }
    }
}

return failures == 0 ? 0 : 1;

static async Task<string> AttemptAsync(string uri, bool tcp, int timeoutSeconds)
{
    string start = DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);
    var timings = new List<string>();
    string result = "ok";
    string error = "";
    var total = Stopwatch.StartNew();
    try
    {
        await ConnectAndCloseAsync(uri, tcp, timeoutSeconds, timings);
    }
    catch (Exception exception)
    {
        result = "FAIL";
        error = Describe(exception);
    }

    return $"PROBE start={start} total_ms={total.ElapsedMilliseconds} {string.Join(' ', timings)} result={result} error=\"{error}\"";
}

static async Task ConnectAndCloseAsync(string uri, bool tcp, int timeoutSeconds, List<string> timings)
{
    var options = new ClientConnectionOptions
    {
        ServerAddress = new ServerAddress(new Uri(uri)),
        ClientAuthenticationOptions = new SslClientAuthenticationOptions(),
        ConnectTimeout = TimeSpan.FromSeconds(timeoutSeconds),
        ShutdownTimeout = TimeSpan.FromSeconds(timeoutSeconds),
    };
    IMultiplexedClientTransport transport = tcp || !QuicConnection.IsSupported ?
        new SlicClientTransport(new TcpClientTransport()) :
        new QuicClientTransport();
    timings.Add($"transport={(transport is QuicClientTransport ? "quic" : "tcp")}");

    await using var connection = new ClientConnection(options, multiplexedClientTransport: transport);

    var phase = Stopwatch.StartNew();
    await connection.ConnectAsync();
    timings.Add($"connect_ms={phase.ElapsedMilliseconds}");

    phase.Restart();
    await connection.ShutdownAsync();
    timings.Add($"shutdown_ms={phase.ElapsedMilliseconds}");
}

// Occupies NAT mappings without QUIC: one datagram from each of `count` sockets, re-sent every 200 s so the mappings
// outlive Azure's 4-minute idle timeout, for `holdSeconds`.
static async Task FillAsync(string uri, int count, int holdSeconds)
{
    var serverUri = new Uri(uri);
    IPAddress address = (await Dns.GetHostAddressesAsync(serverUri.Host, AddressFamily.InterNetwork))[0];
    var endpoint = new IPEndPoint(address, serverUri.IsDefaultPort ? 4062 : serverUri.Port);
    var sockets = new List<Socket>();
    for (int i = 0; i < count; i++)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        socket.Connect(endpoint);
        sockets.Add(socket);
    }

    var deadline = DateTime.UtcNow.AddSeconds(holdSeconds);
    byte[] payload = [0];
    do
    {
        foreach (Socket socket in sockets)
        {
            socket.Send(payload);
        }
        Console.WriteLine($"FILL sockets={sockets.Count} sent={DateTime.UtcNow:HH:mm:ss}Z");
        await Task.Delay(TimeSpan.FromSeconds(Math.Min(200, Math.Max(1, (deadline - DateTime.UtcNow).TotalSeconds))));
    }
    while (DateTime.UtcNow < deadline);
}

static string Describe(Exception exception)
{
    string text = $"{exception.GetType().Name}: {exception.Message}";
    for (Exception? inner = exception.InnerException; inner is not null; inner = inner.InnerException)
    {
        text += $" <- {inner.GetType().Name}: {inner.Message}";
    }
    return text.Replace('\n', ' ').Replace('"', '\'');
}

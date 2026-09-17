// Copyright (c) ZeroC, Inc.

using IceRpc;
using IceRpc.Transports;
using IceRpc.Transports.Quic;
using IceRpc.Transports.Slic;
using IceRpc.Transports.Tcp;
using System.Diagnostics;
using System.Globalization;
using System.Net.Quic;
using System.Net.Security;

// Connects to the build-telemetry server the way the build-telemetry generator does, then closes the connection,
// printing one line per attempt with the time each phase took.
bool tcp = false;
int timeoutSeconds = 60;
int count = 1;
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
        case "--uri":
            uri = args[++i];
            break;
        default:
            Console.Error.WriteLine($"unknown argument: {args[i]}");
            return 2;
    }
}

int failures = 0;
for (int i = 0; i < count; i++)
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
        failures++;
        error = Describe(exception);
    }

    Console.WriteLine(
        $"PROBE start={start} total_ms={total.ElapsedMilliseconds} {string.Join(' ', timings)} result={result} error=\"{error}\"");
}

return failures == 0 ? 0 : 1;

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

static string Describe(Exception exception)
{
    string text = $"{exception.GetType().Name}: {exception.Message}";
    for (Exception? inner = exception.InnerException; inner is not null; inner = inner.InnerException)
    {
        text += $" <- {inner.GetType().Name}: {inner.Message}";
    }
    return text.Replace('\n', ' ').Replace('"', '\'');
}

// Copyright (c) ZeroC, Inc. All rights reserved.

using System.Diagnostics.Tracing;
using System.Net;
using System.Runtime.CompilerServices;

namespace IceRpc.Internal;

/// <summary>An <see cref="EventSource"/> implementation used to log server events.</summary>
internal sealed class ServerEventSource : EventSource
{
    internal static readonly ServerEventSource Log = new("IceRpc-Server");

    // The number of connections that were accepted and are being connected.
    private long _currentBacklog;
    private PollingCounter? _currentBacklogCounter;

    // The number of active (accepted and connected) connections.
    private long _currentConnections;
    private PollingCounter? _currentConnectionsCounter;

    // Tracks the connection rate
    private IncrementingPollingCounter? _connectionsPerSecondCounter;

    // The number of connection that have been accepted and connected.
    private long _totalConnections;
    private PollingCounter? _totalConnectionsCounter;

    // The number of connections that were accepted and failed later on.
    private long _totalFailedConnections;
    private PollingCounter? _totalFailedConnectionsCounter;

    /// <summary>Creates a new instance of the <see cref="ServerEventSource"/> class with the specified name.
    /// </summary>
    /// <param name="eventSourceName">The name to apply to the event source.</param>
    internal ServerEventSource(string eventSourceName)
        : base(eventSourceName)
    {
    }

    [NonEvent]
    internal void ConnectFailure(ServerAddress serverAddress, EndPoint clientNetworkAddress, Exception exception)
    {
        if (IsEnabled(EventLevel.Error, EventKeywords.None))
        {
            ConnectFailure(
                serverAddress.ToString(),
                clientNetworkAddress?.ToString(),
                exception.GetType().FullName,
                exception.ToString());
        }
    }

    [NonEvent]
    internal void ConnectStart(ServerAddress serverAddress, EndPoint clientNetworkAddress)
    {
        Interlocked.Increment(ref _currentBacklog);
        if (IsEnabled(EventLevel.Informational, EventKeywords.None))
        {
            ConnectStart(serverAddress.ToString(), clientNetworkAddress?.ToString());
        }
    }

    [NonEvent]
    internal void ConnectStop(ServerAddress serverAddress, EndPoint clientNetworkAddress)
    {
        Interlocked.Decrement(ref _currentBacklog);
        if (IsEnabled(EventLevel.Informational, EventKeywords.None))
        {
            ConnectStop(serverAddress.ToString(), clientNetworkAddress.ToString());
        }
    }

    [NonEvent]
    internal void ConnectSuccess(ServerAddress serverAddress, EndPoint clientNetworkAddress)
    {
        Interlocked.Increment(ref _currentConnections);
        if (IsEnabled(EventLevel.Informational, EventKeywords.None))
        {
            ConnectSuccess(serverAddress.ToString(), clientNetworkAddress.ToString());
        }
    }

    [NonEvent]
    internal void ConnectionFailure(ServerAddress serverAddress, EndPoint clientNetworkAddress, Exception exception)
    {
        Interlocked.Increment(ref _totalFailedConnections);
        if (IsEnabled(EventLevel.Error, EventKeywords.None))
        {
            ConnectionFailure(
                serverAddress.ToString(),
                clientNetworkAddress.ToString(),
                exception.GetType().FullName,
                exception.ToString());
        }
    }

    [NonEvent]
    internal void ConnectionShutdown(ServerAddress serverAddress, EndPoint clientNetworkAddress, string message)
    {
        if (IsEnabled(EventLevel.Informational, EventKeywords.None))
        {
            ConnectionShutdown(serverAddress.ToString(), clientNetworkAddress.ToString(), message);
        }
    }

    [NonEvent]
    internal void ConnectionStart(ServerAddress serverAddress, EndPoint clientNetworkAddress)
    {
        Interlocked.Increment(ref _totalConnections);
        if (IsEnabled(EventLevel.Informational, EventKeywords.None))
        {
            ConnectionStart(serverAddress.ToString(), clientNetworkAddress?.ToString());
        }
    }

    [NonEvent]
    internal void ConnectionStop(ServerAddress serverAddress, EndPoint clientNetworkAddress)
    {
        Interlocked.Decrement(ref _currentConnections);
        if (IsEnabled(EventLevel.Informational, EventKeywords.None))
        {
            ConnectionStop(serverAddress.ToString(), clientNetworkAddress.ToString());
        }
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        _currentConnectionsCounter?.Dispose();
        _currentBacklogCounter?.Dispose();
        _connectionsPerSecondCounter?.Dispose();
        _totalConnectionsCounter?.Dispose();
        _totalFailedConnectionsCounter?.Dispose();
        base.Dispose(disposing);
    }

    /// <inheritdoc/>
    protected override void OnEventCommand(EventCommandEventArgs command)
    {
        if (command.Command == EventCommand.Enable)
        {
            // Initializing counters lazily on the first enable command, they aren't disabled afterwards...

            _currentBacklogCounter ??= new PollingCounter(
                "current-backlog",
                this,
                () => Volatile.Read(ref _currentBacklog))
                {
                    DisplayName = "Current Backlog",
                };

            _currentConnectionsCounter ??= new PollingCounter(
                "current-connections",
                this,
                () => Volatile.Read(ref _currentConnections))
                {
                    DisplayName = "Current Connections",
                };

            _connectionsPerSecondCounter ??= new IncrementingPollingCounter(
                "connections-per-second",
                this,
                () => Volatile.Read(ref _totalConnections))
                {
                    DisplayName = "Connections Rate",
                    DisplayRateTimeScale = TimeSpan.FromSeconds(1)
                };

            _totalConnectionsCounter ??= new PollingCounter(
                "total-connections",
                this,
                () => Volatile.Read(ref _totalConnections))
                {
                    DisplayName = "Total Connections",
                };

            _totalFailedConnectionsCounter ??= new PollingCounter(
                "total-failed-connections",
                this,
                () => Volatile.Read(ref _totalFailedConnections))
                {
                    DisplayName = "Total Failed Connections",
                };
        }
    }

    // Event methods sorted by eventId

    [MethodImpl(MethodImplOptions.NoInlining)]
    [Event(1, Level = EventLevel.Informational, Opcode = EventOpcode.Start)]
    private void ConnectionStart(string serverAddress, string? clientNetworkAddress) =>
        WriteEvent(1, serverAddress, clientNetworkAddress);

    [MethodImpl(MethodImplOptions.NoInlining)]
    [Event(2, Level = EventLevel.Informational, Opcode = EventOpcode.Stop)]
    private void ConnectionStop(string serverAddress, string? clientNetworkAddress) =>
        WriteEvent(2, serverAddress, clientNetworkAddress);

    [MethodImpl(MethodImplOptions.NoInlining)]
    [Event(3, Level = EventLevel.Error)]
    private void ConnectionFailure(
        string serverAddress,
        string? clientNetworkAddress,
        string? exceptionType,
        string exceptionDetails) =>
        WriteEvent(3, serverAddress, clientNetworkAddress, exceptionType, exceptionDetails);

    [MethodImpl(MethodImplOptions.NoInlining)]
    [Event(4, Level = EventLevel.Informational, Opcode = EventOpcode.Start)]
    private void ConnectStart(string serverAddress, string? clientNetworkAddress) =>
        WriteEvent(4, serverAddress, clientNetworkAddress);

    [MethodImpl(MethodImplOptions.NoInlining)]
    [Event(5, Level = EventLevel.Informational, Opcode = EventOpcode.Stop)]
    private void ConnectStop(string serverAddress, string? clientNetworkAddress) =>
        WriteEvent(5, serverAddress, clientNetworkAddress);

    [MethodImpl(MethodImplOptions.NoInlining)]
    [Event(6, Level = EventLevel.Error)]
    private void ConnectFailure(
        string serverAddress,
        string? clientNetworkAddress,
        string? exceptionType,
        string exceptionDetails) =>
        WriteEvent(6, serverAddress, clientNetworkAddress, exceptionType, exceptionDetails);

    [MethodImpl(MethodImplOptions.NoInlining)]
    [Event(7, Level = EventLevel.Informational)]
    private void ConnectSuccess(string serverAddress, string? clientNetworkAddress) =>
        WriteEvent(7, serverAddress, clientNetworkAddress);

    [MethodImpl(MethodImplOptions.NoInlining)]
    [Event(8, Level = EventLevel.Informational)]
    private void ConnectionShutdown(string serverAddress, string? clientNetworkAddress, string message) =>
        WriteEvent(8, serverAddress, clientNetworkAddress, message);
}

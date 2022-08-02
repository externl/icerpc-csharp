// Copyright (c) ZeroC, Inc. All rights reserved.

using System.Diagnostics;
using System.IO.Pipelines;

namespace IceRpc.Transports.Internal;

/// <summary>The colocated connection class to exchange data within the same process. The implementation copies the send
/// buffer into the receive buffer.</summary>
internal class ColocConnection : IDuplexConnection
{
    public Endpoint Endpoint { get; }

    private readonly Func<Endpoint, (PipeReader, PipeWriter)> _connect;

    // Remember the failure that caused the connection failure to raise the same exception from WriteAsync or
    // ReadAsync
    private Exception? _exception;
    private PipeReader? _reader;
    private int _state;
    private PipeWriter? _writer;

    public Task<TransportConnectionInformation> ConnectAsync(CancellationToken cancel)
    {
        (_reader, _writer) = _connect(Endpoint);
        var colocEndPoint = new ColocEndPoint(Endpoint);
        return Task.FromResult(new TransportConnectionInformation(colocEndPoint, colocEndPoint, null));
    }

    public void Dispose()
    {
        _exception ??= new ObjectDisposedException($"{typeof(ColocConnection)}");

        if (_state.TrySetFlag(State.Disposed))
        {
            // _reader and _writer can be null if connection establishment failed.

            if (_reader is not null)
            {
                if (_state.HasFlag(State.Reading))
                {
                    _reader.CancelPendingRead();
                }
                else
                {
                    _reader.Complete(new ConnectionLostException());
                }
            }

            if (_writer is not null)
            {
                if (_state.HasFlag(State.Writing))
                {
                    _writer.CancelPendingFlush();
                }
                else
                {
                    _writer.Complete(new ConnectionLostException());
                }
            }
        }
    }

    public async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancel)
    {
        Debug.Assert(_reader is not null && _writer is not null);

        if (!_state.TrySetFlag(State.Reading))
        {
            throw new InvalidOperationException($"{nameof(ReadAsync)} is not thread safe");
        }

        try
        {
            if (_state.HasFlag(State.Disposed))
            {
                throw _exception!;
            }

            ReadResult readResult = await _reader.ReadAsync(cancel).ConfigureAwait(false);
            if (readResult.IsCompleted && readResult.Buffer.IsEmpty)
            {
                return 0;
            }

            if (_state.HasFlag(State.Disposed))
            {
                throw _exception!;
            }

            Debug.Assert(!readResult.IsCanceled);

            // We could eventually add a CopyTo(this ReadOnlySequence<byte> src, Memory<byte> dest) extension method
            // if we need this in other places.
            int read;
            if (readResult.Buffer.IsSingleSegment)
            {
                read = CopySegmentToMemory(readResult.Buffer.First, buffer);
            }
            else
            {
                read = 0;
                foreach (ReadOnlyMemory<byte> segment in readResult.Buffer)
                {
                    read += CopySegmentToMemory(segment, buffer[read..]);
                    if (read == buffer.Length)
                    {
                        break;
                    }
                }
            }
            _reader.AdvanceTo(readResult.Buffer.GetPosition(read));
            return read;
        }
        catch (Exception exception)
        {
            _exception ??= exception;
            throw;
        }
        finally
        {
            if (_state.HasFlag(State.Disposed))
            {
                await _reader.CompleteAsync(new ConnectionLostException()).ConfigureAwait(false);
            }
            _state.ClearFlag(State.Reading);
        }

        static int CopySegmentToMemory(ReadOnlyMemory<byte> source, Memory<byte> destination)
        {
            if (source.Length > destination.Length)
            {
                source[0..destination.Length].CopyTo(destination);
                return destination.Length;
            }
            else
            {
                source.CopyTo(destination);
                return source.Length;
            }
        }
    }

    public async Task ShutdownAsync(CancellationToken cancel)
    {
        Debug.Assert(_reader is not null && _writer is not null);

        if (_state.TrySetFlag(State.ShuttingDown))
        {
            if (_state.TrySetFlag(State.Writing))
            {
                await _writer.CompleteAsync().ConfigureAwait(false);
                _state.ClearFlag(State.Writing);
            }
            else
            {
                // WriteAsync will take care of completing the writer once it's done writing.
            }
        }
    }

    public async ValueTask WriteAsync(IReadOnlyList<ReadOnlyMemory<byte>> buffers, CancellationToken cancel)
    {
        Debug.Assert(_reader is not null && _writer is not null);

        if (!_state.TrySetFlag(State.Writing))
        {
            throw new InvalidOperationException($"{nameof(WriteAsync)} is not thread safe");
        }

        try
        {
            foreach (ReadOnlyMemory<byte> buffer in buffers)
            {
                if (_state.HasFlag(State.Disposed))
                {
                    throw _exception!;
                }
                else if (_state.HasFlag(State.ShuttingDown))
                {
                    throw new TransportException("connection is shutdown");
                }

                _ = await _writer.WriteAsync(buffer, cancel).ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            _exception ??= exception;
            throw;
        }
        finally
        {
            if (_state.HasFlag(State.Disposed))
            {
                await _writer.CompleteAsync(new ConnectionLostException()).ConfigureAwait(false);
            }
            else if (_state.HasFlag(State.ShuttingDown))
            {
                await _writer.CompleteAsync().ConfigureAwait(false);
            }
            _state.ClearFlag(State.Writing);
        }
    }

    public ColocConnection(Endpoint endpoint, Func<Endpoint, (PipeReader, PipeWriter)> connect)
    {
        Endpoint = endpoint;
        _connect = connect;
    }

    private enum State : int
    {
        Disposed = 1,
        Reading = 2,
        ShuttingDown = 4,
        Writing = 8,
    }
}
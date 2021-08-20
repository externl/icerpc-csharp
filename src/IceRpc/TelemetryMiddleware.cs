﻿// Copyright (c) ZeroC, Inc. All rights reserved.

using IceRpc.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;

namespace IceRpc
{
    /// <summary>A middleware that starts an <see cref="Activity"/> per request, following OpenTelemetry
    /// conventions. The Activity is started if <see cref="Activity.Current"/> is not null or if "IceRpc" logging
    /// is enabled. The middleware restores the parent invocation activity before starting the dispatch activity.</summary>
    /// <seealso cref="TelemetryInterceptor"/>
    public class TelemetryMiddleware : IDispatcher
    {
        private readonly ILogger _logger;
        private readonly IDispatcher _next;
        private readonly Configure.TelemetryOptions _options;

        /// <summary>Constructs a telemetry middleware.</summary>
        /// <param name="next">The next dispatcher in the dispatch pipeline.</param>
        /// <param name="options">The options to configure the telemetry middleware.</param>
        public TelemetryMiddleware(IDispatcher next, Configure.TelemetryOptions options)
        {
            _next = next;
            _options = options;
            _logger = options.LoggerFactory?.CreateLogger("IceRpc") ?? NullLogger.Instance;
        }

        async ValueTask<OutgoingResponse> IDispatcher.DispatchAsync(IncomingRequest request, CancellationToken cancel)
        {
            if (request.Protocol != Protocol.Ice1)
            {
                Activity? activity = _options.ActivitySource?.CreateActivity(
                    $"{request.Path}/{request.Operation}",
                    ActivityKind.Server);
                if (activity == null && (_logger.IsEnabled(LogLevel.Critical) || Activity.Current != null))
                {
                    activity = new Activity($"{request.Path}/{request.Operation}");
                }

                if (activity != null)
                {
                    activity.AddTag("rpc.system", "icerpc");
                    activity.AddTag("rpc.service", request.Path);
                    activity.AddTag("rpc.method", request.Operation);
                    // TODO add additional attributes
                    // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/rpc.md#common-remote-procedure-call-conventions
                    RestoreActivityContext(request, activity);
                    activity.Start();
                }

                try
                {
                    return await _next.DispatchAsync(request, cancel).ConfigureAwait(false);
                }
                finally
                {
                    activity?.Stop();
                    activity?.Dispose();
                }
            }
            else
            {
                return await _next.DispatchAsync(request, cancel).ConfigureAwait(false);
            }
        }

        private static void RestoreActivityContext(IncomingRequest request, Activity activity)
        {
            Debug.Assert(request.Protocol != Protocol.Ice1);
            if (request.Fields.TryGetValue((int)Ice2FieldKey.TraceContext, out ReadOnlyMemory<byte> buffer))
            {
                // Read W3C traceparent binary encoding (1 byte version, 16 bytes trace Id, 8 bytes span Id,
                // 1 byte flags) https://www.w3.org/TR/trace-context/#traceparent-header-field-values
                int i = 0;
                byte traceIdVersion = buffer.Span[i++];
                var traceId = ActivityTraceId.CreateFromBytes(buffer.Span.Slice(i, 16));
                i += 16;
                var spanId = ActivitySpanId.CreateFromBytes(buffer.Span.Slice(i, 8));
                i += 8;
                var traceFlags = (ActivityTraceFlags)buffer.Span[i++];

                activity.SetParentId(traceId, spanId, traceFlags);

                // Read tracestate encoded as a string
                var decoder = new Ice20Decoder(buffer[i..]);
                activity.TraceStateString = decoder.DecodeString();

                // The min element size is 2 bytes for a struct with two empty strings.
                IEnumerable<(string key, string value)> baggage = decoder.DecodeSequence(
                    minElementSize: 2,
                    decoder =>
                    {
                        string key = decoder.DecodeString();
                        string value = decoder.DecodeString();
                        return (key, value);
                    });

                // Restore in reverse order to keep the order in witch the peer add baggage entries,
                // this is important when there are duplicate keys.
                foreach ((string key, string value) in baggage.Reverse())
                {
                    activity.AddBaggage(key, value);
                }
            }
        }

    }
}
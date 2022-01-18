// Copyright (c) ZeroC, Inc. All rights reserved.

using IceRpc.Slice;
using IceRpc.Transports;

namespace IceRpc.Internal
{
    /// <summary>The IceRPC protocol class.</summary>
    internal sealed class IceRpcProtocol : Protocol
    {
        public override bool HasFieldSupport => true;
        public override bool IsSupported => true;

        /// <summary>The IceRpc protocol singleton.</summary>
        internal static IceRpcProtocol Instance { get; } = new();

        internal IProtocolConnectionFactory<IMultiplexedNetworkConnection> ProtocolConnectionFactory { get; } =
            new IceRpcProtocolConnectionFactory();

        internal override IceEncoding? SliceEncoding => Encoding.Slice20;

        private IceRpcProtocol()
            : base(IceRpcName)
        {
        }
    }
}
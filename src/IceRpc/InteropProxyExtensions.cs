// Copyright (c) ZeroC, Inc. All rights reserved.

using IceRpc.Internal;
using IceRpc.Transports.Internal;
using System.Text;

namespace IceRpc
{
    /// <summary>Provides extension methods for Proxy.</summary>
    public static class InteropProxyExtensions
    {
        /// <summary>Converts a proxy into a string, using the format specified by ToStringMode.</summary>
        /// <param name="proxy">The service proxy.</param>
        /// <param name="mode">Specifies how non-printable ASCII characters are escaped in the resulting string. See
        /// <see cref="ToStringMode"/>.</param>
        /// <returns>The string representation of this proxy.</returns>
        public static string ToString(this Proxy proxy, ToStringMode mode)
        {
            if (proxy.Protocol != Protocol.Ice1)
            {
                return proxy.ToString()!;
            }

            var identityAndFacet = IdentityAndFacet.FromPath(proxy.Path);

            var sb = new StringBuilder();

            // If the encoded identity string contains characters which the reference parser uses as separators,
            // then we enclose the identity string in quotes.
            string id = identityAndFacet.Identity.ToString(mode);
            if (StringUtil.FindFirstOf(id, " :@") != -1)
            {
                sb.Append('"');
                sb.Append(id);
                sb.Append('"');
            }
            else
            {
                sb.Append(id);
            }

            if (identityAndFacet.Facet.Length > 0)
            {
                // If the encoded facet string contains characters which the reference parser uses as separators,
                // then we enclose the facet string in quotes.
                sb.Append(" -f ");
                string fs = StringUtil.EscapeString(identityAndFacet.Facet, mode);
                if (StringUtil.FindFirstOf(fs, " :@") != -1)
                {
                    sb.Append('"');
                    sb.Append(fs);
                    sb.Append('"');
                }
                else
                {
                    sb.Append(fs);
                }
            }

            if (proxy.Endpoint?.Transport == TransportNames.Udp)
            {
                sb.Append(" -d");
            }
            else
            {
                sb.Append(" -t");
            }

            // Always print the encoding version to ensure a stringified proxy will convert back to a proxy with the
            // same encoding with StringToProxy. (Only needed for backwards compatibility).
            sb.Append(" -e ");
            sb.Append(proxy.Encoding);

            if (proxy.Endpoint != null)
            {
                if (proxy.Endpoint.Transport == TransportNames.Loc)
                {
                    string adapterId = proxy.Endpoint.Host;

                    sb.Append(" @ ");

                    // If the encoded adapter ID contains characters which the proxy parser uses as separators, then
                    // we enclose the adapter ID string in double quotes.
                    adapterId = StringUtil.EscapeString(adapterId, mode);
                    if (StringUtil.FindFirstOf(adapterId, " :@") != -1)
                    {
                        sb.Append('"');
                        sb.Append(adapterId);
                        sb.Append('"');
                    }
                    else
                    {
                        sb.Append(adapterId);
                    }
                }
                else
                {
                    sb.Append(':');
                    sb.Append(proxy.Endpoint);

                    foreach (Endpoint e in proxy.AltEndpoints)
                    {
                        sb.Append(':');
                        sb.Append(e);
                    }
                }
            }
            return sb.ToString();
        }
    }
}
/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Collections.Generic;
using System.Globalization;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.TestServer;
using AdbcDrivers.Tests.HiveServer2.MockServer;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Factories for a Hive2-flavored <see cref="MockServerScenario"/>:
    /// <see cref="Create"/> uses the in-process HTTP mock,
    /// <see cref="CreateStandard"/> uses the TCP + SASL PLAIN mock. Tests
    /// override the default parameter set via <paramref name="parameters"/>.
    /// </summary>
    internal static class HiveMockServer
    {
        /// <summary>Default Hive HTTP+basic-auth parameters (URI is injected by Create).</summary>
        public static IReadOnlyDictionary<string, string> DefaultParameters => new Dictionary<string, string>
        {
            { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Http },
            { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.Basic },
            { AdbcOptions.Username, "mock-user" },
            { AdbcOptions.Password, "mock-password" },
        };

        /// <summary>
        /// Default Hive Standard (TCP+SASL+framed)+basic-auth parameters
        /// (host/port injected by CreateStandard). TLS is disabled because
        /// the driver defaults <c>StandardTlsOptions.IsTlsEnabled</c> to
        /// true and the in-process mock speaks plain TCP.
        /// </summary>
        public static IReadOnlyDictionary<string, string> DefaultStandardParameters => new Dictionary<string, string>
        {
            { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Standard },
            { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.Basic },
            { StandardTlsOptions.IsTlsEnabled, "false" },
            { AdbcOptions.Username, "mock-user" },
            { AdbcOptions.Password, "mock-password" },
        };

        public static MockServerScenario Create(
            HiveServer2StubHandler? stub = null,
            IReadOnlyDictionary<string, string>? parameters = null)
        {
            stub ??= new HiveServer2StubHandler();
            var server = new HiveServer2TestServer(stub);
            var full = MockServerScenario.CopyParameters(parameters ?? DefaultParameters);
            full[AdbcOptions.Uri] = server.Uri.AbsoluteUri;
            return new MockServerScenario(new HiveServer2Driver(), server, full, stub);
        }

        public static MockServerScenario CreateStandard(
            HiveServer2StubHandler? stub = null,
            IReadOnlyDictionary<string, string>? parameters = null)
        {
            stub ??= new HiveServer2StubHandler();
            var server = new HiveServer2StandardTestServer(stub);
            var full = MockServerScenario.CopyParameters(parameters ?? DefaultStandardParameters);
            full[HiveServer2Parameters.HostName] = server.HostName;
            full[HiveServer2Parameters.Port] = server.Port.ToString(CultureInfo.InvariantCulture);
            return new MockServerScenario(new HiveServer2Driver(), server, full, stub);
        }
    }
}

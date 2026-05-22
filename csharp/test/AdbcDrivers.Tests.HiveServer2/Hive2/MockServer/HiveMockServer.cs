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
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.TestServer;
using AdbcDrivers.Tests.HiveServer2.MockServer;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Factory for a Hive2-flavored <see cref="MockServerScenario"/>: HTTP
    /// transport, basic auth, in-process mock server. Tests can override
    /// the default parameter set via <see cref="Create"/>'s
    /// <paramref name="parameters"/>.
    /// </summary>
    internal static class HiveMockServer
    {
        /// <summary>Default Hive HTTP+basic-auth parameters; URI is filled in by the scenario.</summary>
        public static IReadOnlyDictionary<string, string> DefaultParameters => new Dictionary<string, string>
        {
            { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Http },
            { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.Basic },
            { AdbcOptions.Username, "mock-user" },
            { AdbcOptions.Password, "mock-password" },
        };

        public static MockServerScenario Create(
            HiveServer2StubHandler? stub = null,
            IReadOnlyDictionary<string, string>? parameters = null) =>
            new(new HiveServer2Driver(),
                parameters ?? DefaultParameters,
                stub ?? new HiveServer2StubHandler());
    }
}

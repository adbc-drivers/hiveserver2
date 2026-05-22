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
using AdbcDrivers.HiveServer2.Spark;
using AdbcDrivers.HiveServer2.TestServer;
using AdbcDrivers.Tests.HiveServer2.MockServer;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Tests.HiveServer2.Spark.MockServer
{
    /// <summary>
    /// Factory for a Spark-flavored <see cref="MockServerScenario"/>: HTTP
    /// transport, basic auth, in-process mock server. Use a non-null
    /// <paramref name="parameters"/> to exercise other auth modes (token,
    /// OAuth, username_only).
    /// </summary>
    internal static class SparkMockServer
    {
        /// <summary>Default Spark HTTP+basic-auth parameters; URI is filled in by the scenario.</summary>
        public static IReadOnlyDictionary<string, string> DefaultParameters => new Dictionary<string, string>
        {
            { SparkParameters.Type, SparkServerTypeConstants.Http },
            { SparkParameters.AuthType, SparkAuthTypeConstants.Basic },
            { AdbcOptions.Username, "mock-user" },
            { AdbcOptions.Password, "mock-password" },
        };

        public static MockServerScenario Create(
            HiveServer2StubHandler? stub = null,
            IReadOnlyDictionary<string, string>? parameters = null) =>
            new(new SparkDriver(),
                parameters ?? DefaultParameters,
                stub ?? new HiveServer2StubHandler());
    }
}

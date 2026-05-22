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
using AdbcDrivers.HiveServer2.Spark;
using AdbcDrivers.HiveServer2.TestServer;
using AdbcDrivers.Tests.HiveServer2.MockServer;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Tests.HiveServer2.Spark.MockServer
{
    /// <summary>
    /// Factories for a Spark-flavored <see cref="MockServerScenario"/>.
    /// <see cref="Create"/> uses the HTTP mock, <see cref="CreateStandard"/>
    /// uses the TCP + SASL PLAIN mock.
    /// </summary>
    internal static class SparkMockServer
    {
        public static IReadOnlyDictionary<string, string> DefaultParameters => new Dictionary<string, string>
        {
            { SparkParameters.Type, SparkServerTypeConstants.Http },
            { SparkParameters.AuthType, SparkAuthTypeConstants.Basic },
            { AdbcOptions.Username, "mock-user" },
            { AdbcOptions.Password, "mock-password" },
        };

        public static IReadOnlyDictionary<string, string> DefaultStandardParameters => new Dictionary<string, string>
        {
            { SparkParameters.Type, SparkServerTypeConstants.Standard },
            { SparkParameters.AuthType, SparkAuthTypeConstants.Basic },
            // TLS is on by default for the Standard transport; the mock speaks plain TCP.
            { AdbcDrivers.HiveServer2.Hive2.StandardTlsOptions.IsTlsEnabled, "false" },
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
            return new MockServerScenario(new SparkDriver(), server, full, stub);
        }

        public static MockServerScenario CreateStandard(
            HiveServer2StubHandler? stub = null,
            IReadOnlyDictionary<string, string>? parameters = null)
        {
            stub ??= new HiveServer2StubHandler();
            var server = new HiveServer2StandardTestServer(stub);
            var full = MockServerScenario.CopyParameters(parameters ?? DefaultStandardParameters);
            full[SparkParameters.HostName] = server.HostName;
            full[SparkParameters.Port] = server.Port.ToString(CultureInfo.InvariantCulture);
            return new MockServerScenario(new SparkDriver(), server, full, stub);
        }
    }
}

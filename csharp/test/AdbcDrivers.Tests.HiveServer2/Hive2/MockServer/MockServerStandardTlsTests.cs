/*
 * Copyright (c) 2026 ADBC Drivers Contributors
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
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Impala;
using AdbcDrivers.HiveServer2.Spark;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Drives the TLS-on branch of <c>CreateTransport</c> for each Standard
    /// connection flavor. The mock server speaks plain TCP — the handshake
    /// will fail — but the test just needs <c>TTlsSocketTransport</c>
    /// construction and the cert-validator setup to fire before that
    /// happens. We assert a <see cref="HiveServer2Exception"/> specifically,
    /// since that's what the driver's OpenAsync catch wraps the handshake
    /// failure with — anything else (e.g. validation throwing before TLS
    /// setup runs) would mean this test had stopped exercising the
    /// transport-setup path.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerStandardTlsTests
    {
        private static void ConnectAndAssertTlsFailure(AdbcDriver driver, IReadOnlyDictionary<string, string> props)
        {
            using var database = driver.Open(props);
            Assert.Throws<HiveServer2Exception>(() => database.Connect(props));
        }

        [Fact]
        public void Hive_Standard_TlsOn_FiresTlsTransportSetup()
        {
            var stub = new HiveServer2StubHandler();
            using var server = new HiveServer2StandardTestServer(stub);
            using var driver = new HiveServer2Driver();
            var props = new Dictionary<string, string>
            {
                { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Standard },
                { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.Basic },
                { HiveServer2Parameters.HostName, server.HostName },
                { HiveServer2Parameters.Port, server.Port.ToString(System.Globalization.CultureInfo.InvariantCulture) },
                // TLS on — drives the TTlsSocketTransport(hostName,...) ctor
                // and the cert-validator wiring.
                { StandardTlsOptions.IsTlsEnabled, "true" },
                { StandardTlsOptions.AllowSelfSigned, "true" },
                { StandardTlsOptions.AllowHostnameMismatch, "true" },
                { StandardTlsOptions.DisableServerCertificateValidation, "true" },
                { AdbcOptions.Username, "u" },
                { AdbcOptions.Password, "p" },
            };
            ConnectAndAssertTlsFailure(driver, props);
        }

        [Fact]
        public void Hive_Standard_TlsOn_WithIpAddressHost_TakesIpBranch()
        {
            // When hostname parses as an IPAddress the driver takes a
            // separate TTlsSocketTransport(IPAddress, ...) overload.
            var stub = new HiveServer2StubHandler();
            using var server = new HiveServer2StandardTestServer(stub);
            using var driver = new HiveServer2Driver();
            var props = new Dictionary<string, string>
            {
                { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Standard },
                { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.Basic },
                { HiveServer2Parameters.HostName, server.HostName }, // 127.0.0.1 parses as IP
                { HiveServer2Parameters.Port, server.Port.ToString(System.Globalization.CultureInfo.InvariantCulture) },
                { StandardTlsOptions.IsTlsEnabled, "true" },
                { StandardTlsOptions.DisableServerCertificateValidation, "true" },
                { AdbcOptions.Username, "u" },
                { AdbcOptions.Password, "p" },
            };
            ConnectAndAssertTlsFailure(driver, props);
        }

        [Fact]
        public void Spark_Standard_TlsOn_FiresTlsTransportSetup()
        {
            var stub = new HiveServer2StubHandler();
            using var server = new HiveServer2StandardTestServer(stub);
            using var driver = new SparkDriver();
            var props = new Dictionary<string, string>
            {
                { SparkParameters.Type, SparkServerTypeConstants.Standard },
                { SparkParameters.AuthType, SparkAuthTypeConstants.Basic },
                { SparkParameters.HostName, server.HostName },
                { SparkParameters.Port, server.Port.ToString(System.Globalization.CultureInfo.InvariantCulture) },
                { StandardTlsOptions.IsTlsEnabled, "true" },
                { StandardTlsOptions.DisableServerCertificateValidation, "true" },
                { AdbcOptions.Username, "u" },
                { AdbcOptions.Password, "p" },
            };
            ConnectAndAssertTlsFailure(driver, props);
        }

        [Fact]
        public void Impala_Standard_TlsOn_FiresTlsTransportSetup()
        {
            var stub = new HiveServer2StubHandler();
            using var server = new HiveServer2StandardTestServer(stub);
            using var driver = new ImpalaDriver();
            var props = new Dictionary<string, string>
            {
                { ImpalaParameters.Type, ImpalaServerTypeConstants.Standard },
                { ImpalaParameters.AuthType, ImpalaAuthTypeConstants.Basic },
                { ImpalaParameters.HostName, server.HostName },
                { ImpalaParameters.Port, server.Port.ToString(System.Globalization.CultureInfo.InvariantCulture) },
                { StandardTlsOptions.IsTlsEnabled, "true" },
                { StandardTlsOptions.DisableServerCertificateValidation, "true" },
                { AdbcOptions.Username, "u" },
                { AdbcOptions.Password, "p" },
            };
            ConnectAndAssertTlsFailure(driver, props);
        }
    }
}

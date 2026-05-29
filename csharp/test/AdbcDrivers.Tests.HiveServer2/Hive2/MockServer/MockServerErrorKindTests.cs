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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow.Adbc;
using Apache.Hive.Service.Rpc.Thrift.Reference;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Asserts the <c>db.error.kind</c> taxonomy tag is emitted on
    /// <c>Status=Error</c> spans. Implements
    /// adbc-drivers/databricks#481 — automated dashboards need to be able to
    /// group failure spans by category (network vs. server-side vs. auth)
    /// without parsing <c>exception.type</c>/<c>exception.message</c>.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerErrorKindTests
    {
        private const string ErrorKindTag = "db.error.kind";

        /// <summary>
        /// Operation-status reports ERROR_STATE → the helper at
        /// <c>HiveServer2Connection.PollForResponseAsync</c> throws a
        /// <see cref="HiveServer2Exception"/>. The originating Activity
        /// should carry <c>db.error.kind = "server_error"</c>.
        /// </summary>
        [Fact]
        public async Task OperationError_TagsServerError()
        {
            (var captured, var listener) = StartListener();
            using (listener)
            {
                using var scenario = HiveMockServer.Create();
                scenario.Stub.OnExecuteStatement = _ => MockResult.OperationError(
                    MockSchema.Of(("v", TTypeId.STRING_TYPE)),
                    message: "syntax error near 'BANANA'",
                    sqlState: "42000",
                    errorCode: 1064);

                using var statement = scenario.NewStatement();
                statement.SqlQuery = "BANANA";
                await Assert.ThrowsAnyAsync<HiveServer2Exception>(async () =>
                    await statement.ExecuteQueryAsync());
            }

            AssertAnyErrorActivityCarriesKind(captured, "server_error");
        }

        /// <summary>
        /// OpenSession returns an ERROR status → the catch in
        /// <c>HiveServer2Connection.OpenAsync</c> throws. Same
        /// classification as the polled path: <c>server_error</c>.
        /// </summary>
        [Fact]
        public void OpenSessionError_TagsServerError()
        {
            (var captured, var listener) = StartListener();

            using (listener)
            {
                var stub = new HiveServer2StubHandler
                {
                    OpenSessionStatus = new TStatus(TStatusCode.ERROR_STATUS)
                    {
                        ErrorMessage = "session denied",
                        SqlState = "28000",
                        ErrorCode = 42,
                    },
                };

                using var server = new HiveServer2TestServer(stub);
                using var driver = new HiveServer2Driver();
                var parameters = new Dictionary<string, string>
                {
                    { AdbcOptions.Uri, server.Uri.AbsoluteUri },
                    { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Http },
                    { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.Basic },
                    { AdbcOptions.Username, "u" },
                    { AdbcOptions.Password, "p" },
                };
                using AdbcDatabase database = driver.Open(parameters);
                Assert.ThrowsAny<HiveServer2Exception>(() => database.Connect(parameters).Dispose());
            }

            AssertAnyErrorActivityCarriesKind(captured, "server_error");
        }

        /// <summary>
        /// Connect against a TCP port that nothing is listening on → the
        /// driver surfaces a transport-level failure (HttpRequestException /
        /// SocketException / IOException, all wrapped in TTransportException).
        /// The originating Activity should carry
        /// <c>db.error.kind = "network"</c>.
        /// </summary>
        [Fact]
        public void Connect_RefusedTcp_TagsNetwork()
        {
            (var captured, var listener) = StartListener();

            using (listener)
            {
                // Reserve a port then immediately release it. There is a tiny
                // race window where the kernel could re-assign it before our
                // connect, but for an in-process test run on loopback it is
                // safe enough; if the test ever flakes we can switch to a
                // reserved port number that is known-closed on CI.
                int port = ReserveAndReleaseLoopbackPort();
                using var driver = new HiveServer2Driver();
                var parameters = new Dictionary<string, string>
                {
                    { AdbcOptions.Uri, $"http://127.0.0.1:{port}/cliservice" },
                    { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Http },
                    { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.Basic },
                    { AdbcOptions.Username, "u" },
                    { AdbcOptions.Password, "p" },
                };
                using AdbcDatabase database = driver.Open(parameters);
                Assert.ThrowsAny<System.Exception>(() => database.Connect(parameters).Dispose());
            }

            AssertAnyErrorActivityCarriesKind(captured, "network");
        }

        private static (ConcurrentBag<Activity> captured, ActivityListener listener) StartListener()
        {
            var captured = new ConcurrentBag<Activity>();
            var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "AdbcDrivers.HiveServer2",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = activity => captured.Add(activity),
            };
            ActivitySource.AddActivityListener(listener);
            return (captured, listener);
        }

        /// <summary>
        /// Pre-check: an Activity with Status=Error exists. Then assert at
        /// least one such activity carries the expected db.error.kind value.
        /// This permits inner-most-only tagging (the outer wrapper spans
        /// inherit the error status but it is fine for them to omit the kind).
        /// </summary>
        private static void AssertAnyErrorActivityCarriesKind(IEnumerable<Activity> captured, string expectedKind)
        {
            var errorActivities = captured.Where(a => a.Status == ActivityStatusCode.Error).ToList();
            Assert.NotEmpty(errorActivities);
            Assert.Contains(errorActivities, a =>
                a.TagObjects.Any(t => t.Key == ErrorKindTag && (t.Value as string) == expectedKind));
        }

        private static int ReserveAndReleaseLoopbackPort()
        {
            var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            int port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            return port;
        }
    }
}

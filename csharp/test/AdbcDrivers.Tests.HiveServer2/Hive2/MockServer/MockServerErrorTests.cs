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
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow.Adbc;
using Apache.Hive.Service.Rpc.Thrift.Reference;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Server-reported error paths. Covers <see cref="HiveServer2Exception"/>
    /// construction, the error-status branch of
    /// <c>HiveServer2Connection.HandleOpenSessionResponse</c>, and the
    /// operation-poll error path in <c>HiveServer2Connection.PollForResponseAsync</c>.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerErrorTests
    {
        [Fact]
        public void OpenSession_ErrorStatus_PropagatesAsHiveServer2Exception()
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

            // Construct the scenario lazily so OpenSession actually runs and throws.
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

            var ex = Assert.ThrowsAny<HiveServer2Exception>(() => database.Connect(parameters).Dispose());
            Assert.Contains("session denied", ex.Message);
        }

        [Fact]
        public async Task OperationError_ReportsErrorStateOnGetOperationStatus()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ => MockResult.OperationError(
                MockSchema.Of(("v", TTypeId.STRING_TYPE)),
                message: "syntax error near 'BANANA'",
                sqlState: "42000",
                errorCode: 1064);

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "BANANA";
            var ex = await Assert.ThrowsAnyAsync<HiveServer2Exception>(async () =>
                await statement.ExecuteQueryAsync());
            Assert.Contains("syntax error near 'BANANA'", ex.Message);
        }

        [Fact]
        public async Task DefaultStub_HappyPathExecuteStillWorks()
        {
            // Smoke test: with the default stub configuration — which leaves
            // several RPCs (GetTypeInfo, delegation tokens, etc.) wired to
            // throw NotImplementedException — the basic ExecuteStatement /
            // FetchResults flow still completes. This guards against a
            // regression where one unimplemented-but-uninvoked RPC accidentally
            // breaks the rest of the session.
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT 1";
            var result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);
        }
    }
}

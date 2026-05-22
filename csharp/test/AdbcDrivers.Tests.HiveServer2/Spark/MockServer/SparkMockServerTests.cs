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
using AdbcDrivers.HiveServer2.Spark;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Spark.MockServer
{
    /// <summary>
    /// Drive the real <see cref="SparkDriver"/> against the in-process mock.
    /// Exercises Spark's HTTP / connection / statement / driver paths
    /// (SparkHttpConnection, SparkConnection, SparkDriver, SparkDatabase,
    /// SparkStatement) — which are currently at 0% coverage in CI.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class SparkMockServerTests
    {
        [Fact]
        public async Task CanExecuteSimpleSelect()
        {
            using var scenario = SparkMockServer.Create();
            using AdbcStatement statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT 42";

            QueryResult result = await statement.ExecuteQueryAsync();
            using IArrowArrayStream stream = result.Stream!;
            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            var column = Assert.IsType<Int64Array>(batch.Column(0));
            Assert.Equal(42L, column.GetValue(0));
        }

        [Fact]
        public async Task CanExecuteDataTypeRoundTrip()
        {
            using var scenario = SparkMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ => MockResult.Builder()
                .String("name", "alice", "bob")
                .Bigint("age", 30, 25)
                .Build();

            using AdbcStatement statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT name, age FROM users";
            QueryResult result = await statement.ExecuteQueryAsync();
            using IArrowArrayStream stream = result.Stream!;
            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            var names = Assert.IsType<StringArray>(batch.Column(0));
            var ages = Assert.IsType<Int64Array>(batch.Column(1));
            Assert.Equal("alice", names.GetString(0));
            Assert.Equal(30L, ages.GetValue(0));
        }

        [Theory]
        [InlineData(SparkAuthTypeConstants.Basic)]
        [InlineData(SparkAuthTypeConstants.UsernameOnly)]
        [InlineData(SparkAuthTypeConstants.None)]
        public async Task AuthType_HappyPath(string authType)
        {
            var parameters = new Dictionary<string, string>
            {
                { SparkParameters.Type, SparkServerTypeConstants.Http },
                { SparkParameters.AuthType, authType },
            };
            if (authType is SparkAuthTypeConstants.Basic or SparkAuthTypeConstants.UsernameOnly)
            {
                parameters[AdbcOptions.Username] = "u";
            }
            if (authType is SparkAuthTypeConstants.Basic)
            {
                parameters[AdbcOptions.Password] = "p";
            }

            using var scenario = SparkMockServer.Create(parameters: parameters);
            using AdbcStatement statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT 1";
            QueryResult result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);
        }

        // The two tests below confirm the driver wires up the auth path
        // without throwing when configured with Token / OAuth credentials.
        // They do *not* assert the exact outgoing Authorization header
        // value — the mock server doesn't capture it today. A follow-up
        // could extend HiveServer2TestServer to expose the inbound headers
        // so these can tighten into "Bearer X was sent" assertions.

        [Fact]
        public async Task AuthType_Token_OpensConnection()
        {
            var parameters = new Dictionary<string, string>
            {
                { SparkParameters.Type, SparkServerTypeConstants.Http },
                { SparkParameters.AuthType, SparkAuthTypeConstants.Token },
                { SparkParameters.Token, "my-bearer-token" },
            };
            using var scenario = SparkMockServer.Create(parameters: parameters);
            using AdbcStatement statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT 1";
            QueryResult result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);
        }

        [Fact]
        public async Task AuthType_OAuth_OpensConnection()
        {
            var parameters = new Dictionary<string, string>
            {
                { SparkParameters.Type, SparkServerTypeConstants.Http },
                { SparkParameters.AuthType, SparkAuthTypeConstants.OAuth },
                { SparkParameters.AccessToken, "oauth-access-token" },
            };
            using var scenario = SparkMockServer.Create(parameters: parameters);
            using AdbcStatement statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT 1";
            QueryResult result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);
        }

        [Fact]
        public async Task MultipleStatements_OnSameConnection()
        {
            using var scenario = SparkMockServer.Create();
            for (int i = 0; i < 3; i++)
            {
                using AdbcStatement statement = scenario.NewStatement();
                statement.SqlQuery = $"SELECT {i}";
                QueryResult result = await statement.ExecuteQueryAsync();
                using var stream = result.Stream!;
                using var batch = await stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
            }
        }
    }
}

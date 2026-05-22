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
using AdbcDrivers.HiveServer2.Impala;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Impala.MockServer
{
    /// <summary>
    /// Drive the real <see cref="ImpalaDriver"/> against the in-process mock.
    /// Exercises Impala's HTTP / connection / statement / driver paths
    /// (ImpalaHttpConnection, ImpalaConnection, ImpalaDriver, ImpalaDatabase,
    /// ImpalaStatement) — currently at 0% coverage in CI.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class ImpalaMockServerTests
    {
        [Fact]
        public async Task CanExecuteSimpleSelect()
        {
            using var scenario = ImpalaMockServer.Create();
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
            using var scenario = ImpalaMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ => MockResult.Builder()
                .String("city", "berlin", "tokyo", null)
                .Double("temp", 21.5, 28.0, null)
                .Build();

            using AdbcStatement statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT city, temp FROM weather";
            QueryResult result = await statement.ExecuteQueryAsync();
            using IArrowArrayStream stream = result.Stream!;
            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            var cities = Assert.IsType<StringArray>(batch.Column(0));
            var temps = Assert.IsType<DoubleArray>(batch.Column(1));
            Assert.Equal("berlin", cities.GetString(0));
            Assert.Equal(21.5, temps.GetValue(0));
            Assert.True(cities.IsNull(2));
            Assert.Null(temps.GetValue(2));
        }

        [Theory]
        [InlineData(ImpalaAuthTypeConstants.Basic, true)]
        [InlineData(ImpalaAuthTypeConstants.UsernameOnly, true)]
        [InlineData(ImpalaAuthTypeConstants.None, false)]
        public async Task AuthType_HappyPath(string authType, bool sendCredentials)
        {
            var parameters = new Dictionary<string, string>
            {
                { ImpalaParameters.Type, ImpalaServerTypeConstants.Http },
                { ImpalaParameters.AuthType, authType },
            };
            if (sendCredentials)
            {
                parameters[AdbcOptions.Username] = "u";
                if (authType == ImpalaAuthTypeConstants.Basic)
                    parameters[AdbcOptions.Password] = "p";
            }

            using var scenario = ImpalaMockServer.Create(parameters: parameters);
            using AdbcStatement statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT 1";
            QueryResult result = await statement.ExecuteQueryAsync();
            Assert.NotNull(result.Stream);
        }

        [Fact]
        public async Task MultipleStatements_OnSameConnection()
        {
            using var scenario = ImpalaMockServer.Create();
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

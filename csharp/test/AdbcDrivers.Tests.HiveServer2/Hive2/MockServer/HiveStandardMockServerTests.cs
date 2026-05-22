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

using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Drive the real <see cref="AdbcDrivers.HiveServer2.Hive2.HiveServer2Driver"/>
    /// against the TCP + SASL PLAIN + framed-binary mock. Exercises
    /// <c>HiveServer2StandardConnection</c>, the SASL handshake
    /// (<c>TSaslTransport</c>, <c>PlainSaslMechanism</c>,
    /// <c>NegotiationStatus</c>), and the framed-binary transport
    /// post-handshake — none of which the HTTP path reaches.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class HiveStandardMockServerTests
    {
        [Fact]
        public async Task CanExecuteSimpleSelect()
        {
            using var scenario = HiveMockServer.CreateStandard();
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
            using var scenario = HiveMockServer.CreateStandard();
            scenario.Stub.OnExecuteStatement = _ => MockResult.Builder()
                .String("city", "berlin", "tokyo", null)
                .Bigint("pop", 3_700_000L, 13_960_000L, null)
                .Build();

            using AdbcStatement statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT city, pop FROM cities";
            QueryResult result = await statement.ExecuteQueryAsync();
            using IArrowArrayStream stream = result.Stream!;
            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            var cities = Assert.IsType<StringArray>(batch.Column(0));
            var pops = Assert.IsType<Int64Array>(batch.Column(1));
            Assert.Equal("berlin", cities.GetString(0));
            Assert.Equal(3_700_000L, pops.GetValue(0));
            Assert.True(cities.IsNull(2));
            Assert.Null(pops.GetValue(2));
        }

        [Fact]
        public async Task MultipleStatementsOnSameConnection_ShareSaslSession()
        {
            // The SASL handshake happens once at OpenSession time; subsequent
            // statements reuse the authenticated framed-binary channel. If
            // anything in the framing or post-handshake stream gets out of
            // sync, the second statement would fail.
            using var scenario = HiveMockServer.CreateStandard();
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

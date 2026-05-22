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
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift.Reference;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Statement-side paths: multi-batch fetch pagination, captured
    /// statement text, and basic exec-and-discard. Covers chunks of
    /// <c>HiveServer2Statement</c> + <c>HiveServer2Reader</c>'s loop.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerStatementTests
    {
        [Fact]
        public async Task MultipleBatches_AreAllReturnedInOrder()
        {
            using var scenario = HiveMockServer.Create();
            var schema = MockSchema.Of(("v", TTypeId.BIGINT_TYPE));
            var batch1 = MockResult.Builder().Bigint("v", 1, 2, 3).Build().Batches[0];
            var batch2 = MockResult.Builder().Bigint("v", 4, 5).Build().Batches[0];
            var batch3 = MockResult.Builder().Bigint("v", 6).Build().Batches[0];
            scenario.Stub.OnExecuteStatement = _ => new MockResult(schema, new[] { batch1, batch2, batch3 });

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT v FROM t";
            QueryResult result = await statement.ExecuteQueryAsync();
            using IArrowArrayStream stream = result.Stream!;

            var seen = new List<long>();
            while (true)
            {
                using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
                if (batch == null) break;
                var col = Assert.IsType<Int64Array>(batch.Column(0));
                for (int i = 0; i < col.Length; i++)
                {
                    var v = col.GetValue(i);
                    Assert.NotNull(v);
                    seen.Add(v!.Value);
                }
            }
            Assert.Equal(new long[] { 1, 2, 3, 4, 5, 6 }, seen);
        }

        [Fact]
        public async Task StatementText_IsForwardedToServer()
        {
            string? captured = null;
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = req =>
            {
                captured = req.Statement;
                return MockResult.SingleBigint(1);
            };

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT count(*) FROM events WHERE day = '2024-01-01'";
            await statement.ExecuteQueryAsync();
            Assert.Equal("SELECT count(*) FROM events WHERE day = '2024-01-01'", captured);
        }

        [Fact]
        public async Task ConnectionLifecycle_CanCreateAndDisposeMultipleStatements()
        {
            using var scenario = HiveMockServer.Create();
            for (int i = 0; i < 3; i++)
            {
                using AdbcStatement statement = scenario.NewStatement();
                statement.SqlQuery = $"SELECT {i}";
                var result = await statement.ExecuteQueryAsync();
                using var stream = result.Stream!;
                using var batch = await stream.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                Assert.Equal(1, batch.Length);
            }
        }
    }
}

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
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Drives the synchronous <c>ExecuteQuery</c>/<c>ExecuteUpdate</c>
    /// wrappers and the async <c>ExecuteUpdateAsync</c> path through the
    /// mock. These end up exercising <c>ExecuteUpdateAsyncInternal</c>,
    /// the affected-rows accumulation loop, and the <c>num_affected_rows</c>
    /// schema-lookup branches.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerStatementExecuteTests
    {
        [Fact]
        public void ExecuteQuery_Sync_ReturnsResult()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ => MockResult.SingleBigint(99);

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT 99";
            QueryResult result = statement.ExecuteQuery();
            using var stream = result.Stream!;
            // Sync ExecuteQuery wraps the async path via GetAwaiter().GetResult();
            // confirming we got a stream back is enough to lock in coverage of
            // lines 90-109.
            Assert.NotNull(stream);
        }

        [Fact]
        public async Task ExecuteUpdateAsync_NoAffectedRowsColumn_ReturnsMinusOne()
        {
            using var scenario = HiveMockServer.Create();
            // Result has no "num_affected_rows" column → the helper short-circuits
            // and returns -1 per the documented contract.
            scenario.Stub.OnExecuteStatement = _ => MockResult.SingleBigint(0, columnName: "ignored");

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "INSERT INTO t VALUES (1)";
            UpdateResult result = await statement.ExecuteUpdateAsync();
            Assert.Equal(-1, result.AffectedRows);
        }

        [Fact]
        public async Task ExecuteUpdateAsync_WithAffectedRowsColumn_ReturnsSum()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ =>
                MockResult.Builder().Bigint("num_affected_rows", 17L).Build();

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "UPDATE t SET x = 1";
            UpdateResult result = await statement.ExecuteUpdateAsync();
            Assert.Equal(17, result.AffectedRows);
        }

        [Fact]
        public async Task ExecuteUpdateAsync_NullAffectedRow_TreatedAsZero()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ =>
                MockResult.Builder().Bigint("num_affected_rows", (long?)null).Build();

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "DELETE FROM t";
            UpdateResult result = await statement.ExecuteUpdateAsync();
            Assert.Equal(0, result.AffectedRows);
        }

        [Fact]
        public void ExecuteUpdate_Sync_ReturnsAffectedRows()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ =>
                MockResult.Builder().Bigint("num_affected_rows", 42L).Build();

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "UPDATE t SET x = 1";
            UpdateResult result = statement.ExecuteUpdate();
            Assert.Equal(42, result.AffectedRows);
        }

        [Fact]
        public void Cancel_WithoutActiveOperation_IsSafe()
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            // No execution in flight — Cancel() should be a no-op rather
            // than crashing on a null token source.
            statement.Cancel();
        }

        [Fact]
        public async Task ExecuteQueryAsync_ThenCancel_IsSafe()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ => MockResult.SingleBigint(1);

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT 1";
            QueryResult result = await statement.ExecuteQueryAsync();
            using var stream = result.Stream!;
            // After completion the token source is disposed; Cancel() must
            // still be a no-op (CancelTokenSource handles the null case).
            statement.Cancel();
        }
    }
}

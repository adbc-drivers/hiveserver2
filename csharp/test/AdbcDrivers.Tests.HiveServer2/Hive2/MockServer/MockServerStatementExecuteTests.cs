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
using System.Diagnostics;
using System.Linq;
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

        /// <summary>
        /// Regression for adbc-drivers/databricks#484. When Thrift returns no
        /// <c>num_affected_rows</c> column (e.g. DDL paths), the
        /// <c>db.response.returned_rows</c> OTel tag must be OMITTED rather
        /// than emitted with the -1 sentinel — the sentinel clutters APM
        /// dashboards by sorting alongside legitimate row counts.
        /// </summary>
        [Fact]
        public async Task ExecuteUpdateAsync_NoAffectedRowsColumn_OmitsReturnedRowsTag()
        {
            var captured = new ConcurrentBag<Activity>();
            using var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "AdbcDrivers.HiveServer2",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = activity => captured.Add(activity),
            };
            ActivitySource.AddActivityListener(listener);

            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ => MockResult.SingleBigint(0, columnName: "ignored");

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "CREATE TABLE t (x INT)";
            UpdateResult result = await statement.ExecuteUpdateAsync();
            Assert.Equal(-1, result.AffectedRows);

            Activity? internalActivity = captured.FirstOrDefault(a => a.OperationName.EndsWith("ExecuteUpdateAsyncInternal"));
            Assert.NotNull(internalActivity);
            // The tag must be absent — backends interpret absent-tag as "unknown".
            Assert.DoesNotContain(internalActivity!.TagObjects, t => t.Key == "db.response.returned_rows");
        }

        /// <summary>
        /// Companion to <see cref="ExecuteUpdateAsync_NoAffectedRowsColumn_OmitsReturnedRowsTag"/>:
        /// when the affected-row count IS known and non-negative, the OTel
        /// tag must still be emitted with the real count.
        /// </summary>
        [Fact]
        public async Task ExecuteUpdateAsync_WithAffectedRowsColumn_EmitsReturnedRowsTag()
        {
            var captured = new ConcurrentBag<Activity>();
            using var listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "AdbcDrivers.HiveServer2",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
                ActivityStopped = activity => captured.Add(activity),
            };
            ActivitySource.AddActivityListener(listener);

            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ =>
                MockResult.Builder().Bigint("num_affected_rows", 17L).Build();

            using var statement = scenario.NewStatement();
            statement.SqlQuery = "UPDATE t SET x = 1";
            UpdateResult result = await statement.ExecuteUpdateAsync();
            Assert.Equal(17, result.AffectedRows);

            Activity? internalActivity = captured.FirstOrDefault(a => a.OperationName.EndsWith("ExecuteUpdateAsyncInternal"));
            Assert.NotNull(internalActivity);
            var tag = internalActivity!.TagObjects.FirstOrDefault(t => t.Key == "db.response.returned_rows");
            Assert.Equal("db.response.returned_rows", tag.Key);
            Assert.Equal(17L, tag.Value);
        }
    }
}

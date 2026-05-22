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
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Spark.MockServer
{
    /// <summary>
    /// Drive the Spark driver against the TCP + SASL PLAIN + framed-binary
    /// mock. Covers <c>SparkStandardConnection</c> + the shared
    /// <c>HiveServer2StandardConnection</c> + SASL plumbing.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class SparkStandardMockServerTests
    {
        [Fact]
        public async Task CanExecuteSimpleSelect()
        {
            using var scenario = SparkMockServer.CreateStandard();
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
        public async Task CanExecuteMultipleStatements()
        {
            using var scenario = SparkMockServer.CreateStandard();
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

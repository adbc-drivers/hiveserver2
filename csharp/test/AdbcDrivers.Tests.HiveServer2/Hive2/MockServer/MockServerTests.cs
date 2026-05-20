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
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// End-to-end tests that drive the real <see cref="HiveServer2Driver"/>
    /// against the in-process mock HiveServer2 in the
    /// <c>AdbcDrivers.HiveServer2.TestServer</c> project. These run with no
    /// live server.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerTests
    {
        [Fact]
        public async Task CanExecuteSimpleSelectAgainstMockServer()
        {
            using var server = new HiveServer2TestServer(new HiveServer2StubHandler());

            var parameters = new Dictionary<string, string>
            {
                { AdbcOptions.Uri, server.Uri.AbsoluteUri },
                { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Http },
                { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.Basic },
                { AdbcOptions.Username, "test-user" },
                { AdbcOptions.Password, "test-password" },
            };

            using var driver = new HiveServer2Driver();
            using AdbcDatabase database = driver.Open(parameters);
            using AdbcConnection connection = database.Connect(parameters);
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42";

            QueryResult result = await statement.ExecuteQueryAsync();
            using IArrowArrayStream stream = result.Stream!;
            Assert.NotNull(stream);
            Assert.Single(stream.Schema.FieldsList);
            Assert.Equal("_c0", stream.Schema.FieldsList[0].Name);

            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
            Assert.Equal(1, batch.ColumnCount);
            var column = Assert.IsType<Int64Array>(batch.Column(0));
            Assert.Equal(42L, column.GetValue(0));

            using RecordBatch? next = await stream.ReadNextRecordBatchAsync();
            Assert.Null(next);
        }
    }
}

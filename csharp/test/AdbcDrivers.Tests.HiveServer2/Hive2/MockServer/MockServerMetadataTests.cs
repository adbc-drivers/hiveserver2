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
    /// Drive the metadata RPCs (GetCatalogs / GetSchemas / GetTables /
    /// GetTableTypes / GetColumns) through the real driver against the mock
    /// fixture. Covers <c>HiveServer2Connection.GetObjects</c>,
    /// <c>GetObjectsResultBuilder</c>, <c>MetadataSchemaDefinitions</c>,
    /// and the underlying <c>Get*</c> helpers.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerMetadataTests
    {
        [Fact]
        public async Task GetTableTypes_ReturnsConfiguredTypes()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetTableTypes = _ => MockResult.Builder()
                .String("TABLE_TYPE", "TABLE", "VIEW", "SYSTEM_TABLE")
                .Build();

            using IArrowArrayStream stream = scenario.Connection.GetTableTypes();
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            var types = Assert.IsType<StringArray>(batch.Column(0));
            Assert.Equal(3, types.Length);
            Assert.Equal("TABLE", types.GetString(0));
            Assert.Equal("VIEW", types.GetString(1));
            Assert.Equal("SYSTEM_TABLE", types.GetString(2));
        }

        [Fact]
        public async Task GetObjects_Catalogs_ReturnsConfiguredCatalogs()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCatalogs = _ => MockResult.Builder()
                .String("TABLE_CAT", "main", "samples")
                .Build();

            using IArrowArrayStream stream = scenario.Connection.GetObjects(
                depth: AdbcConnection.GetObjectsDepth.Catalogs,
                catalogPattern: null, dbSchemaPattern: null, tableNamePattern: null,
                tableTypes: null, columnNamePattern: null);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            // The ADBC GetObjects result has a top-level catalog_name column.
            var catalogNames = Assert.IsType<StringArray>(batch.Column(0));
            Assert.Equal(2, catalogNames.Length);
            Assert.Equal("main", catalogNames.GetString(0));
            Assert.Equal("samples", catalogNames.GetString(1));
        }

        [Fact]
        public async Task GetObjects_DbSchemas_FetchesSchemas()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCatalogs = _ => MockResult.Builder()
                .String("TABLE_CAT", "main")
                .Build();
            scenario.Stub.OnGetSchemas = _ => MockResult.Builder()
                .String("TABLE_SCHEM", "public", "internal")
                .String("TABLE_CATALOG", "main", "main")
                .Build();

            using IArrowArrayStream stream = scenario.Connection.GetObjects(
                depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                catalogPattern: "main", dbSchemaPattern: null, tableNamePattern: null,
                tableTypes: null, columnNamePattern: null);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length); // one catalog
        }

        [Fact]
        public async Task GetObjects_Tables_ReturnsAllLevels()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCatalogs = _ => MockResult.Builder()
                .String("TABLE_CAT", "main")
                .Build();
            scenario.Stub.OnGetSchemas = _ => MockResult.Builder()
                .String("TABLE_SCHEM", "public")
                .String("TABLE_CATALOG", "main")
                .Build();
            scenario.Stub.OnGetTables = _ => MockResult.Builder()
                .String("TABLE_CAT", "main", "main")
                .String("TABLE_SCHEM", "public", "public")
                .String("TABLE_NAME", "users", "orders")
                .String("TABLE_TYPE", "TABLE", "TABLE")
                .String("REMARKS", "", "")
                .Build();

            using IArrowArrayStream stream = scenario.Connection.GetObjects(
                depth: AdbcConnection.GetObjectsDepth.Tables,
                catalogPattern: null, dbSchemaPattern: null, tableNamePattern: null,
                tableTypes: null, columnNamePattern: null);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0);
        }

        [Fact]
        public async Task GetObjects_DefaultEmptyStub_CompletesWithoutError()
        {
            // Default stub returns empty metadata for every RPC; GetObjects must
            // still complete without throwing. The exact shape of the resulting
            // batch isn't important for this test.
            using var scenario = HiveMockServer.Create();
            using IArrowArrayStream stream = scenario.Connection.GetObjects(
                depth: AdbcConnection.GetObjectsDepth.All,
                catalogPattern: null, dbSchemaPattern: null, tableNamePattern: null,
                tableTypes: null, columnNamePattern: null);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            // Either a zero-batch stream (null) or any populated batch — both indicate
            // the pipeline ran cleanly end to end.
            _ = batch;
        }
    }
}

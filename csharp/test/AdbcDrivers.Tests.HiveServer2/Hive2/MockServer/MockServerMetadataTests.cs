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
using Apache.Arrow.Adbc.Tests.Metadata;
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
        public async Task GetObjects_All_SkipsColumnsOfTablesNotReturnedByGetTables()
        {
            // GetTables honours the tableTypes filter but GetColumns does not, so
            // the server returns columns for base table "t" even though only view
            // "v" was listed. Those orphan columns must be dropped, not throw.
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCatalogs = _ => MockResult.Builder()
                .String("TABLE_CAT", "main")
                .Build();
            scenario.Stub.OnGetSchemas = _ => MockResult.Builder()
                .String("TABLE_SCHEM", "public")
                .String("TABLE_CATALOG", "main")
                .Build();
            scenario.Stub.OnGetTables = _ => MockResult.Builder()
                .String("TABLE_CAT", "main")
                .String("TABLE_SCHEM", "public")
                .String("TABLE_NAME", "v")
                .String("TABLE_TYPE", "VIEW")
                .String("REMARKS", "")
                .Build();
            scenario.Stub.OnGetColumns = _ => MockResult.Builder()
                .String("TABLE_CAT", "main", "main", "main")
                .String("TABLE_SCHEM", "public", "public", "public")
                .String("TABLE_NAME", "t", "v", "t")
                .String("COLUMN_NAME", "orphan_a", "id", "orphan_b")
                .Int("DATA_TYPE", BigintSqlType, BigintSqlType, BigintSqlType)
                .String("TYPE_NAME", "BIGINT", "BIGINT", "BIGINT")
                .Int("COLUMN_SIZE", 19, 19, 19)
                .Tinyint("BUFFER_LENGTH", 8, 8, 8)
                .Int("DECIMAL_DIGITS", 0, 0, 0)
                .Int("NUM_PREC_RADIX", 10, 10, 10)
                .Int("NULLABLE", 1, 1, 1)
                .String("REMARKS", "", "", "")
                .String("COLUMN_DEF", "", "", "")
                .Int("SQL_DATA_TYPE", 0, 0, 0)
                .Int("SQL_DATETIME_SUB", 0, 0, 0)
                .Int("CHAR_OCTET_LENGTH", 0, 0, 0)
                .Int("ORDINAL_POSITION", 1, 1, 2)
                .String("IS_NULLABLE", "YES", "YES", "YES")
                .String("SCOPE_CATALOG", "", "", "")
                .String("SCOPE_SCHEMA", "", "", "")
                .String("SCOPE_TABLE", "", "", "")
                .Smallint("SOURCE_DATA_TYPE", 0, 0, 0)
                .String("IS_AUTO_INCREMENT", "NO", "NO", "NO")
                .Build();

            using IArrowArrayStream stream = scenario.Connection.GetObjects(
                depth: AdbcConnection.GetObjectsDepth.All,
                catalogPattern: null, dbSchemaPattern: null, tableNamePattern: null,
                tableTypes: new[] { "VIEW" }, columnNamePattern: null);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            List<AdbcCatalog> catalogs = GetObjectsParser.ParseCatalog(batch, null);
            AdbcCatalog catalog = Assert.Single(catalogs);
            AdbcDbSchema schema = Assert.Single(catalog.DbSchemas!);
            AdbcTable table = Assert.Single(schema.Tables!);
            Assert.Equal("v", table.Name);
            AdbcColumn column = Assert.Single(table.Columns!);
            Assert.Equal("id", column.Name);
        }

        private const int BigintSqlType = -5;

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

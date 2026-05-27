/*
 * Copyright (c) 2026 ADBC Drivers Contributors
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
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Exercises top-level <see cref="AdbcConnection"/> APIs against the mock:
    /// <c>GetTableSchema</c>, <c>GetInfo</c>, <c>SetOption</c>, <c>GetObjects</c>.
    /// These hit large stretches of HiveServer2Connection that the
    /// statement-only tests don't reach.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerConnectionApiTests
    {
        [Fact]
        public void GetTableSchema_RoundTripsServerColumns()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetColumns = _ =>
                MockResult.Builder()
                    .String("TABLE_CAT", "main")
                    .String("TABLE_SCHEM", "public")
                    .String("TABLE_NAME", "events")
                    .String("COLUMN_NAME", "id")
                    .Int("DATA_TYPE", (int)TestServerColumnTypes.Bigint)
                    .String("TYPE_NAME", "BIGINT")
                    .Int("COLUMN_SIZE", 19)
                    .Tinyint("BUFFER_LENGTH", 8)
                    .Int("DECIMAL_DIGITS", 0)
                    .Int("NUM_PREC_RADIX", 10)
                    .Int("NULLABLE", 1)
                    .Build();

            Schema schema = scenario.Connection.GetTableSchema("main", "public", "events");
            Assert.Single(schema.FieldsList);
            Assert.Equal("id", schema.FieldsList[0].Name);
            Assert.IsType<Int64Type>(schema.FieldsList[0].DataType);
            Assert.True(schema.FieldsList[0].IsNullable);
        }

        [Fact]
        public async Task GetInfo_ReturnsExpectedDriverValues()
        {
            using var scenario = HiveMockServer.Create();
            using IArrowArrayStream stream = scenario.Connection.GetInfo(new[]
            {
                AdbcInfoCode.DriverName,
                AdbcInfoCode.DriverVersion,
                AdbcInfoCode.DriverArrowVersion,
                AdbcInfoCode.VendorName,
                AdbcInfoCode.VendorSql,
            });
            // We don't pin the exact strings (DriverVersion floats with assembly
            // version), only that GetInfo produces a non-empty info batch
            // covering each requested code without throwing.
            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.True(batch.Length >= 4);
        }

        [Fact]
        public async Task GetInfo_EmptyCodes_FallsBackToDefaults()
        {
            // With an empty codes list, GetInfo substitutes its internal
            // supported-codes set — exercise that branch.
            using var scenario = HiveMockServer.Create();
            using IArrowArrayStream stream = scenario.Connection.GetInfo(System.Array.Empty<AdbcInfoCode>());
            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
        }

        [Fact]
        public void SetOption_TraceParent_AcceptedAfterConnect()
        {
            using var scenario = HiveMockServer.Create();
            // TraceParent is one of the two paths that may be set even after
            // the session is open — exercise the early-return.
            scenario.Connection.SetOption(AdbcOptions.Telemetry.TraceParent, "00-1234-5678-01");
            scenario.Connection.SetOption(AdbcOptions.Telemetry.TraceParent, "");
        }

        [Fact]
        public void SetOption_NonTraceOption_AfterConnect_Throws()
        {
            using var scenario = HiveMockServer.Create();
            // Anything other than the TraceParent fast-path is rejected once
            // SessionHandle is populated.
            AdbcException ex = Assert.Throws<AdbcException>(
                () => scenario.Connection.SetOption("adbc.bogus.unknown", "x"));
            Assert.Equal(AdbcStatusCode.InvalidState, ex.Status);
        }

        [Fact]
        public async Task GetObjects_CatalogsDepth_ReturnsCatalogList()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCatalogs = _ =>
                MockResult.Builder()
                    .String("TABLE_CAT", "main", "secondary")
                    .Build();
            using IArrowArrayStream stream = scenario.Connection.GetObjects(
                depth: AdbcConnection.GetObjectsDepth.Catalogs,
                catalogPattern: null,
                dbSchemaPattern: null,
                tableNamePattern: null,
                tableTypes: null,
                columnNamePattern: null);
            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.True(batch.Length >= 1);
        }

        [Fact]
        public async Task GetObjects_DbSchemasDepth_RoutesToBothCatalogsAndSchemas()
        {
            bool calledGetCatalogs = false, calledGetSchemas = false;
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCatalogs = _ =>
            {
                calledGetCatalogs = true;
                return MockResult.Builder().String("TABLE_CAT", "main").Build();
            };
            scenario.Stub.OnGetSchemas = _ =>
            {
                calledGetSchemas = true;
                return MockResult.Builder()
                    .String("TABLE_SCHEM", "public")
                    .String("TABLE_CATALOG", "main")
                    .Build();
            };
            using IArrowArrayStream stream = scenario.Connection.GetObjects(
                depth: AdbcConnection.GetObjectsDepth.DbSchemas,
                catalogPattern: null,
                dbSchemaPattern: null,
                tableNamePattern: null,
                tableTypes: null,
                columnNamePattern: null);
            // Force materialization.
            while (true)
            {
                using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
                if (batch == null) break;
            }
            Assert.True(calledGetCatalogs);
            Assert.True(calledGetSchemas);
        }

        [Fact]
        public async Task GetObjects_TablesDepth_DrivesTablesBranch()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCatalogs = _ =>
                MockResult.Builder().String("TABLE_CAT", "main").Build();
            scenario.Stub.OnGetSchemas = _ =>
                MockResult.Builder()
                    .String("TABLE_SCHEM", "public")
                    .String("TABLE_CATALOG", "main")
                    .Build();
            scenario.Stub.OnGetTables = _ =>
                MockResult.Builder()
                    .String("TABLE_CAT", "main")
                    .String("TABLE_SCHEM", "public")
                    .String("TABLE_NAME", "events")
                    .String("TABLE_TYPE", "TABLE")
                    .String("REMARKS", "")
                    .Build();
            using IArrowArrayStream stream = scenario.Connection.GetObjects(
                depth: AdbcConnection.GetObjectsDepth.Tables,
                catalogPattern: null,
                dbSchemaPattern: null,
                tableNamePattern: null,
                tableTypes: null,
                columnNamePattern: null);
            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
        }

        [Fact]
        public async Task GetObjects_AllDepth_DrivesBuildColumnsPath()
        {
            // depth=All forces GetObjectsResultBuilder.BuildColumns to fire on
            // each table's column list, exercising the otherwise-dormant
            // ~80-line column-aggregation block.
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCatalogs = _ =>
                MockResult.Builder().String("TABLE_CAT", "main").Build();
            scenario.Stub.OnGetSchemas = _ =>
                MockResult.Builder()
                    .String("TABLE_SCHEM", "public")
                    .String("TABLE_CATALOG", "main")
                    .Build();
            scenario.Stub.OnGetTables = _ =>
                MockResult.Builder()
                    .String("TABLE_CAT", "main")
                    .String("TABLE_SCHEM", "public")
                    .String("TABLE_NAME", "events")
                    .String("TABLE_TYPE", "TABLE")
                    .String("REMARKS", "")
                    .Build();
            scenario.Stub.OnGetColumns = _ =>
                MockResult.Builder()
                    .String("TABLE_CAT", "main", "main")
                    .String("TABLE_SCHEM", "public", "public")
                    .String("TABLE_NAME", "events", "events")
                    .String("COLUMN_NAME", "id", "name")
                    .Int("DATA_TYPE", (int)TestServerColumnTypes.Bigint, (int)TestServerColumnTypes.Varchar)
                    .String("TYPE_NAME", "BIGINT", "STRING")
                    .Int("COLUMN_SIZE", 19, 255)
                    .Tinyint("BUFFER_LENGTH", 8, 1)
                    .Int("DECIMAL_DIGITS", 0, 0)
                    .Int("NUM_PREC_RADIX", 10, 0)
                    .Int("NULLABLE", 1, 1)
                    .String("REMARKS", "", "")
                    .String("COLUMN_DEF", "", "")
                    .Int("SQL_DATA_TYPE", 0, 0)
                    .Int("SQL_DATETIME_SUB", 0, 0)
                    .Int("CHAR_OCTET_LENGTH", 0, 255)
                    .Int("ORDINAL_POSITION", 1, 2)
                    .String("IS_NULLABLE", "YES", "YES")
                    .String("SCOPE_CATALOG", "", "")
                    .String("SCOPE_SCHEMA", "", "")
                    .String("SCOPE_TABLE", "", "")
                    .Smallint("SOURCE_DATA_TYPE", 0, 0)
                    .String("IS_AUTO_INCREMENT", "NO", "NO")
                    .Build();
            using IArrowArrayStream stream = scenario.Connection.GetObjects(
                depth: AdbcConnection.GetObjectsDepth.All,
                catalogPattern: null,
                dbSchemaPattern: null,
                tableNamePattern: null,
                tableTypes: null,
                columnNamePattern: null);
            // Drain so column-build runs to completion.
            while (true)
            {
                using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
                if (batch == null) break;
            }
        }

        // Local copy of the well-known java.sql.Types codes the driver uses to
        // map server column types — kept here (rather than reaching into the
        // internal HiveServer2Connection.ColumnTypeId enum) so this test class
        // stays public without pulling internal types into its public surface.
        private enum TestServerColumnTypes
        {
            Bigint = -5,
            Varchar = 12,
        }
    }
}

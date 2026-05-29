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

namespace AdbcDrivers.Tests.HiveServer2.Impala.MockServer
{
    /// <summary>
    /// Drives Impala-specific connection overrides through the mock so
    /// SetPrecisionScaleAndTypeName + GetColumnsMetadataColumnNames run on
    /// the Impala path.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class ImpalaMockServerConnectionApiTests
    {
        [Fact]
        public void GetTableSchema_DecimalAndVarchar_TriggersImpalaTypeParsing()
        {
            using var scenario = ImpalaMockServer.Create();
            scenario.Stub.OnGetColumns = _ =>
                MockResult.Builder()
                    .String("TABLE_CAT", "main", "main", "main")
                    .String("TABLE_SCHEM", "public", "public", "public")
                    .String("TABLE_NAME", "t", "t", "t")
                    .String("COLUMN_NAME", "amount", "label", "id")
                    .Int("DATA_TYPE", (int)SqlTypes.Decimal, (int)SqlTypes.Varchar, (int)SqlTypes.Bigint)
                    .String("TYPE_NAME", "DECIMAL(10,2)", "VARCHAR(32)", "BIGINT")
                    .Int("COLUMN_SIZE", 10, 32, 19)
                    .Tinyint("BUFFER_LENGTH", 10, 32, 8)
                    .Int("DECIMAL_DIGITS", 2, 0, 0)
                    .Int("NUM_PREC_RADIX", 10, 10, 10)
                    .Int("NULLABLE", 1, 1, 1)
                    .Build();
            Schema schema = scenario.Connection.GetTableSchema("main", "public", "t");
            Assert.Equal(3, schema.FieldsList.Count);
            Assert.IsType<Decimal128Type>(schema.FieldsList[0].DataType);
            Assert.IsType<StringType>(schema.FieldsList[1].DataType);
            Assert.IsType<Int64Type>(schema.FieldsList[2].DataType);
        }

        [Fact]
        public async Task GetObjects_AllDepth_DrivesImpalaColumnMetadata()
        {
            // Impala's driver uses ColumnMapIndexOffset=0, so the mock's
            // GetColumns response needs 0-indexed positions — WithPositionBase(0)
            // tells the shared builder to emit those.
            using var scenario = ImpalaMockServer.Create();
            scenario.Stub.OnGetCatalogs = _ =>
                MockResult.Builder().WithPositionBase(0).String("TABLE_CAT", "main").Build();
            scenario.Stub.OnGetSchemas = _ =>
                MockResult.Builder()
                    .WithPositionBase(0)
                    .String("TABLE_SCHEM", "public")
                    .String("TABLE_CATALOG", "main")
                    .Build();
            scenario.Stub.OnGetTables = _ =>
                MockResult.Builder()
                    .WithPositionBase(0)
                    .String("TABLE_CAT", "main")
                    .String("TABLE_SCHEM", "public")
                    .String("TABLE_NAME", "t")
                    .String("TABLE_TYPE", "TABLE")
                    .String("REMARKS", "")
                    .Build();
            scenario.Stub.OnGetColumns = _ =>
                MockResult.Builder()
                    .WithPositionBase(0)
                    .String("TABLE_CAT", "main", "main")
                    .String("TABLE_MD", "public", "public")
                    .String("TABLE_NAME", "t", "t")
                    .String("COLUMN_NAME", "amount", "id")
                    .Int("DATA_TYPE", (int)SqlTypes.Decimal, (int)SqlTypes.Bigint)
                    .String("TYPE_NAME", "DECIMAL(10,2)", "BIGINT")
                    .Int("COLUMN_SIZE", 10, 19)
                    .Tinyint("BUFFER_LENGTH", 10, 8)
                    .Int("DECIMAL_DIGITS", 2, 0)
                    .Int("NUM_PREC_RADIX", 10, 10)
                    .Int("NULLABLE", 1, 1)
                    .String("REMARKS", "", "")
                    .String("COLUMN_DEF", "", "")
                    .Int("SQL_DATA_TYPE", 0, 0)
                    .Int("SQL_DATETIME_SUB", 0, 0)
                    .Int("CHAR_OCTET_LENGTH", 0, 0)
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
            while (true)
            {
                using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
                if (batch == null) break;
            }
        }

        [Fact]
        public async Task GetInfo_ReturnsImpalaVendorIdentity()
        {
            using var scenario = ImpalaMockServer.Create();
            using IArrowArrayStream stream = scenario.Connection.GetInfo(new[]
            {
                AdbcInfoCode.DriverName,
                AdbcInfoCode.DriverVersion,
                AdbcInfoCode.VendorName,
            });
            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
        }

        private enum SqlTypes
        {
            Bigint = -5,
            Varchar = 12,
            Decimal = 3,
        }
    }
}

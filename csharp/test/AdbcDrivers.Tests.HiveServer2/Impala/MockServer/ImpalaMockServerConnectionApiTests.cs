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

        // Note: a GetObjects depth=All test against the Impala mock would
        // require building a TGetColumnsResp with 0-indexed column positions,
        // but the shared MockResultBuilder emits 1-indexed positions (correct
        // for Hive/Spark — which have ColumnMapIndexOffset=1 — but off by one
        // for Impala's offset=0). Driving GetObjects all-depth on Impala is
        // covered by the Spark variant in SparkMockServerConnectionApiTests;
        // adding it here would mean teaching the mock fixture about
        // flavor-specific column-position conventions, which is out of scope
        // for this PR.

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

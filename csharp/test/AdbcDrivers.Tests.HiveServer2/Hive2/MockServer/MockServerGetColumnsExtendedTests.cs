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
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Drives <c>HiveServer2Statement.GetColumnsExtendedAsync</c> end-to-end
    /// with realistic PK/FK fixtures so the
    /// <c>ProcessRelationshipDataSafe</c> body (the ~150-line PK/FK merge
    /// loop) actually runs — including the Int32 KEQ_SEQ branch and the
    /// string-fallback branch — instead of short-circuiting on empty data.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerGetColumnsExtendedTests
    {
        [Fact]
        public async Task GetColumnsExtended_WithPkAndFk_MergesIntoCombinedSchema()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetColumns = _ =>
                MockResult.Builder()
                    .String("TABLE_CAT", "main", "main", "main")
                    .String("TABLE_SCHEM", "public", "public", "public")
                    .String("TABLE_NAME", "orders", "orders", "orders")
                    .String("COLUMN_NAME", "id", "user_id", "amount")
                    .Int("DATA_TYPE", -5, -5, -5)
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
                    .Int("ORDINAL_POSITION", 1, 2, 3)
                    .String("IS_NULLABLE", "YES", "YES", "YES")
                    .String("SCOPE_CATALOG", "", "", "")
                    .String("SCOPE_SCHEMA", "", "", "")
                    .String("SCOPE_TABLE", "", "", "")
                    .Smallint("SOURCE_DATA_TYPE", 0, 0, 0)
                    .String("IS_AUTO_INCREMENT", "NO", "NO", "NO")
                    .Build();
            // "id" is the table's primary key.
            scenario.Stub.OnGetPrimaryKeys = _ =>
                MockResult.Builder()
                    .String("TABLE_CAT", "main")
                    .String("TABLE_SCHEM", "public")
                    .String("TABLE_NAME", "orders")
                    .String("COLUMN_NAME", "id")
                    .Int("KEQ_SEQ", 1)  // "KEQ_SEQ" — upstream Apache typo, preserved
                    .String("PK_NAME", "orders_pk")
                    .Build();
            // "user_id" references users.id; KEQ_SEQ=1 exercises the Int32
            // branch in ProcessRelationshipDataSafe's per-field array builder.
            scenario.Stub.OnGetCrossReference = _ =>
                MockResult.Builder()
                    .String("PKTABLE_CAT", "main")
                    .String("PKTABLE_SCHEM", "public")
                    .String("PKTABLE_NAME", "users")
                    .String("PKCOLUMN_NAME", "id")
                    .String("FKTABLE_CAT", "main")
                    .String("FKTABLE_SCHEM", "public")
                    .String("FKTABLE_NAME", "orders")
                    .String("FKCOLUMN_NAME", "user_id")
                    .Int("KEQ_SEQ", 1)
                    .Int("UPDATE_RULE", 0)
                    .Int("DELETE_RULE", 0)
                    .String("FK_NAME", "orders_user_fk")
                    .String("PK_NAME", "users_pk")
                    .Int("DEFERRABILITY", 0)
                    .Build();

            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, "main");
            statement.SetOption(ApacheParameters.SchemaName, "public");
            statement.SetOption(ApacheParameters.TableName, "orders");
            statement.SqlQuery = "getcolumnsextended";

            QueryResult result = await statement.ExecuteQueryAsync();
            using var stream = result.Stream!;
            using RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            // The combined schema includes the base columns plus PK_* and FK_*
            // prefixed fields. The exact column index is implementation
            // dependent, so just check the prefix-named fields exist.
            Assert.NotNull(stream.Schema.GetFieldByName("PK_COLUMN_NAME"));
            Assert.NotNull(stream.Schema.GetFieldByName("FK_FKCOLUMN_NAME"));
            Assert.NotNull(stream.Schema.GetFieldByName("FK_KEQ_SEQ"));
        }

        [Fact]
        public async Task GetColumnsExtended_WithEmptyColumns_ReturnsEmptyExtendedSchema()
        {
            // When GetColumns yields zero rows, the helper returns an empty
            // result with the *complete* extended schema — exercising
            // CreateEmptyExtendedColumnsResult's per-Arrow-type branches.
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetColumns = _ => MockResult.Empty(MockSchema.GetColumnsSchema);
            scenario.Stub.OnGetPrimaryKeys = _ => MockResult.Empty(MockSchema.GetPrimaryKeysSchema);
            scenario.Stub.OnGetCrossReference = _ => MockResult.Empty(MockSchema.GetCrossReferenceSchema);

            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, "main");
            statement.SetOption(ApacheParameters.SchemaName, "public");
            statement.SetOption(ApacheParameters.TableName, "missing");
            statement.SqlQuery = "getcolumnsextended";

            QueryResult result = await statement.ExecuteQueryAsync();
            using var stream = result.Stream!;
            Assert.NotNull(stream.Schema.GetFieldByName("PK_COLUMN_NAME"));
            Assert.NotNull(stream.Schema.GetFieldByName("FK_FKCOLUMN_NAME"));
        }
    }
}

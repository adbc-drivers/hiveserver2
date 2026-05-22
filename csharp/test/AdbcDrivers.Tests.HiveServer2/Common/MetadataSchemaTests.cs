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
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Common
{
    /// <summary>
    /// Pure-logic tests for the static schema / empty-result factories used
    /// by <c>HiveServer2Connection.GetObjects</c>. They run without the mock
    /// fixture and exercise <c>MetadataSchemaDefinitions</c> +
    /// <c>HiveInfoArrowStream</c>.
    /// </summary>
    public class MetadataSchemaTests
    {
        [Fact]
        public void CatalogsSchema_HasSingleStringColumn()
        {
            Schema schema = MetadataSchemaFactory.CreateCatalogsSchema();
            Assert.Single(schema.FieldsList);
            Assert.Equal("TABLE_CAT", schema.FieldsList[0].Name);
            Assert.Equal(ArrowTypeId.String, schema.FieldsList[0].DataType.TypeId);
        }

        [Fact]
        public void SchemasSchema_HasTwoStringColumns()
        {
            Schema schema = MetadataSchemaFactory.CreateSchemasSchema();
            Assert.Equal(2, schema.FieldsList.Count);
            Assert.Equal("TABLE_SCHEM", schema.FieldsList[0].Name);
            Assert.Equal("TABLE_CATALOG", schema.FieldsList[1].Name);
        }

        [Fact]
        public void TablesSchema_HasTenColumns()
        {
            Schema schema = MetadataSchemaFactory.CreateTablesSchema();
            Assert.Equal(10, schema.FieldsList.Count);
            Assert.Equal("TABLE_CAT", schema.FieldsList[0].Name);
            Assert.Equal("REMARKS", schema.FieldsList[4].Name);
        }

        [Fact]
        public void ColumnMetadataSchema_IncludesAllJdbcColumns()
        {
            Schema schema = MetadataSchemaFactory.CreateColumnMetadataSchema();
            Assert.Equal(24, schema.FieldsList.Count);
            var names = new HashSet<string>();
            foreach (var field in schema.FieldsList) names.Add(field.Name);
            Assert.Contains("DATA_TYPE", names);
            Assert.Contains("BASE_TYPE_NAME", names);
            Assert.Contains("IS_AUTO_INCREMENT", names);
        }

        [Fact]
        public void PrimaryKeysSchema_HasSixColumns()
        {
            Schema schema = MetadataSchemaFactory.CreatePrimaryKeysSchema();
            Assert.Equal(6, schema.FieldsList.Count);
            Assert.Equal("KEQ_SEQ", schema.FieldsList[4].Name);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[4].DataType.TypeId);
        }

        [Fact]
        public void CrossReferenceSchema_HasFourteenColumns()
        {
            Schema schema = MetadataSchemaFactory.CreateCrossReferenceSchema();
            Assert.Equal(14, schema.FieldsList.Count);
            Assert.Equal("DEFERRABILITY", schema.FieldsList[13].Name);
        }

        [Fact]
        public void EmptyCatalogsResult_HasZeroRows()
        {
            QueryResult result = MetadataSchemaFactory.CreateEmptyCatalogsResult();
            Assert.Equal(0, result.RowCount);
            using IArrowArrayStream stream = result.Stream!;
            Assert.Single(stream.Schema.FieldsList);
        }

        [Fact]
        public void EmptySchemasResult_HasZeroRows()
        {
            QueryResult result = MetadataSchemaFactory.CreateEmptySchemasResult();
            Assert.Equal(0, result.RowCount);
            using IArrowArrayStream stream = result.Stream!;
            Assert.Equal(2, stream.Schema.FieldsList.Count);
        }

        [Fact]
        public void EmptyTablesResult_HasZeroRows()
        {
            QueryResult result = MetadataSchemaFactory.CreateEmptyTablesResult();
            Assert.Equal(0, result.RowCount);
            using IArrowArrayStream stream = result.Stream!;
            Assert.Equal(10, stream.Schema.FieldsList.Count);
        }

        [Fact]
        public void EmptyPrimaryKeysResult_HasZeroRows()
        {
            QueryResult result = MetadataSchemaFactory.CreateEmptyPrimaryKeysResult();
            Assert.Equal(0, result.RowCount);
            using IArrowArrayStream stream = result.Stream!;
            Assert.Equal(6, stream.Schema.FieldsList.Count);
        }

        [Fact]
        public void EmptyCrossReferenceResult_HasZeroRows()
        {
            QueryResult result = MetadataSchemaFactory.CreateEmptyCrossReferenceResult();
            Assert.Equal(0, result.RowCount);
            using IArrowArrayStream stream = result.Stream!;
            Assert.Equal(14, stream.Schema.FieldsList.Count);
        }

        [Fact]
        public void BuildPrimaryKeysResult_PopulatesRows()
        {
            var keys = new[]
            {
                ("cat", "schema", "users", "id", 1, "users_pk"),
                ("cat", "schema", "users", "email", 2, "users_pk"),
            };
            QueryResult result = MetadataSchemaFactory.BuildPrimaryKeysResult(keys);
            Assert.Equal(2, result.RowCount);
        }

        [Fact]
        public void BuildCrossReferenceResult_PopulatesRows()
        {
            var refs = new (string, string, string, string, string, string, string, string, int, int, int, string, string?, int)[]
            {
                ("cat", "schema", "users", "id", "cat", "schema", "orders", "user_id", 1, 0, 0, "fk1", "users_pk", 7),
            };
            QueryResult result = MetadataSchemaFactory.BuildCrossReferenceResult(refs);
            Assert.Equal(1, result.RowCount);
        }

        [Fact]
        public void MetadataColumnNames_AreStableConstants()
        {
            // Exercise the const initializer + sanity-check the well-known names.
            Assert.Equal("TABLE_CAT", MetadataColumnNames.TableCat);
            Assert.Equal("COLUMN_NAME", MetadataColumnNames.ColumnName);
            Assert.Equal("DATA_TYPE", MetadataColumnNames.DataType);
            Assert.NotEmpty(MetadataColumnNames.PrimaryKeyFields);
            Assert.NotEmpty(MetadataColumnNames.ForeignKeyFields);
        }
    }
}

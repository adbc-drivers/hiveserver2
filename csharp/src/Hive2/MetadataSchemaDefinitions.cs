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

using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;

namespace AdbcDrivers.HiveServer2.Hive2
{
    internal static class MetadataColumnNames
    {
        internal const string ColumnDef = "COLUMN_DEF";
        internal const string ColumnName = "COLUMN_NAME";
        internal const string DataType = "DATA_TYPE";
        internal const string IsAutoIncrement = "IS_AUTO_INCREMENT";
        internal const string IsNullable = "IS_NULLABLE";
        internal const string OrdinalPosition = "ORDINAL_POSITION";
        internal const string TableCat = "TABLE_CAT";
        internal const string TableCatalog = "TABLE_CATALOG";
        internal const string TableName = "TABLE_NAME";
        internal const string TableSchem = "TABLE_SCHEM";
        internal const string TableMd = "TABLE_MD";
        internal const string TableType = "TABLE_TYPE";
        internal const string TypeName = "TYPE_NAME";
        internal const string NullableColumn = "NULLABLE";
        internal const string ColumnSize = "COLUMN_SIZE";
        internal const string DecimalDigits = "DECIMAL_DIGITS";
        internal const string BufferLength = "BUFFER_LENGTH";
        internal const string BaseTypeName = "BASE_TYPE_NAME";

        internal const string PrimaryKeyPrefix = "PK_";
        internal const string ForeignKeyPrefix = "FK_";

        internal static readonly string[] PrimaryKeyFields = new[] { "COLUMN_NAME" };
        internal static readonly string[] ForeignKeyFields = new[] { "PKCOLUMN_NAME", "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "FKCOLUMN_NAME", "FK_NAME", "KEQ_SEQ" };
    }

    internal static class MetadataSchemaFactory
    {
        internal static Schema CreatePrimaryKeysSchema()
        {
            return new Schema(new[]
            {
                new Field("TABLE_CAT", StringType.Default, true),
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_NAME", StringType.Default, true),
                new Field("COLUMN_NAME", StringType.Default, true),
                new Field("KEQ_SEQ", Int32Type.Default, true),
                new Field("PK_NAME", StringType.Default, true)
            }, null);
        }

        internal static Schema CreateCrossReferenceSchema()
        {
            return new Schema(new[]
            {
                new Field("PKTABLE_CAT", StringType.Default, true),
                new Field("PKTABLE_SCHEM", StringType.Default, true),
                new Field("PKTABLE_NAME", StringType.Default, true),
                new Field("PKCOLUMN_NAME", StringType.Default, true),
                new Field("FKTABLE_CAT", StringType.Default, true),
                new Field("FKTABLE_SCHEM", StringType.Default, true),
                new Field("FKTABLE_NAME", StringType.Default, true),
                new Field("FKCOLUMN_NAME", StringType.Default, true),
                new Field("KEQ_SEQ", Int32Type.Default, true),
                new Field("UPDATE_RULE", Int32Type.Default, true),
                new Field("DELETE_RULE", Int32Type.Default, true),
                new Field("FK_NAME", StringType.Default, true),
                new Field("PK_NAME", StringType.Default, true),
                new Field("DEFERRABILITY", Int32Type.Default, true)
            }, null);
        }

        internal static QueryResult CreateEmptyPrimaryKeysResult()
        {
            var schema = CreatePrimaryKeysSchema();
            var arrays = new IArrowArray[]
            {
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build(),
                new StringArray.Builder().Build()
            };
            return new QueryResult(0, new HiveInfoArrowStream(schema, arrays));
        }

        internal static QueryResult CreateEmptyCrossReferenceResult()
        {
            var schema = CreateCrossReferenceSchema();
            var arrays = new IArrowArray[]
            {
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build(),
                new Int32Array.Builder().Build()
            };
            return new QueryResult(0, new HiveInfoArrowStream(schema, arrays));
        }
    }
}

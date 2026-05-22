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
using Apache.Hive.Service.Rpc.Thrift.Reference;

namespace AdbcDrivers.HiveServer2.TestServer
{
    /// <summary>
    /// Helpers for fabricating <see cref="TTableSchema"/> values, including
    /// the well-known schemas the HiveServer2 metadata RPCs (GetCatalogs,
    /// GetSchemas, GetTables, etc.) are required to return.
    /// </summary>
    public static class MockSchema
    {
        public static TTableSchema Of(params (string Name, TTypeId Type)[] columns)
        {
            var cols = new List<TColumnDesc>(columns.Length);
            for (int i = 0; i < columns.Length; i++)
            {
                cols.Add(SimpleColumn(columns[i].Name, columns[i].Type, position: i + 1));
            }
            return new TTableSchema(cols);
        }

        public static TColumnDesc SimpleColumn(string name, TTypeId type, int position)
        {
            var typeDesc = new TTypeDesc(new List<TTypeEntry>
            {
                new() { PrimitiveEntry = new TPrimitiveTypeEntry(type) },
            });
            return new TColumnDesc(name, typeDesc, position);
        }

        public static TColumnDesc DecimalColumn(string name, int precision, int scale, int position)
        {
            var qualifiers = new TTypeQualifiers(new Dictionary<string, TTypeQualifierValue>
            {
                ["precision"] = new() { I32Value = precision },
                ["scale"] = new() { I32Value = scale },
            });
            var primitive = new TPrimitiveTypeEntry(TTypeId.DECIMAL_TYPE) { TypeQualifiers = qualifiers };
            var typeDesc = new TTypeDesc(new List<TTypeEntry>
            {
                new() { PrimitiveEntry = primitive },
            });
            return new TColumnDesc(name, typeDesc, position);
        }

        // Well-known schemas defined by the HiveServer2 protocol for metadata RPCs.
        // The driver expects FetchResults on the corresponding operation handle to
        // return rows shaped like these.

        public static TTableSchema GetCatalogsSchema { get; } = Of(
            ("TABLE_CAT", TTypeId.STRING_TYPE));

        public static TTableSchema GetSchemasSchema { get; } = Of(
            ("TABLE_SCHEM", TTypeId.STRING_TYPE),
            ("TABLE_CATALOG", TTypeId.STRING_TYPE));

        public static TTableSchema GetTablesSchema { get; } = Of(
            ("TABLE_CAT", TTypeId.STRING_TYPE),
            ("TABLE_SCHEM", TTypeId.STRING_TYPE),
            ("TABLE_NAME", TTypeId.STRING_TYPE),
            ("TABLE_TYPE", TTypeId.STRING_TYPE),
            ("REMARKS", TTypeId.STRING_TYPE));

        public static TTableSchema GetTableTypesSchema { get; } = Of(
            ("TABLE_TYPE", TTypeId.STRING_TYPE));

        public static TTableSchema GetColumnsSchema { get; } = Of(
            ("TABLE_CAT", TTypeId.STRING_TYPE),
            ("TABLE_SCHEM", TTypeId.STRING_TYPE),
            ("TABLE_NAME", TTypeId.STRING_TYPE),
            ("COLUMN_NAME", TTypeId.STRING_TYPE),
            ("DATA_TYPE", TTypeId.INT_TYPE),
            ("TYPE_NAME", TTypeId.STRING_TYPE),
            ("COLUMN_SIZE", TTypeId.INT_TYPE),
            ("BUFFER_LENGTH", TTypeId.TINYINT_TYPE),
            ("DECIMAL_DIGITS", TTypeId.INT_TYPE),
            ("NUM_PREC_RADIX", TTypeId.INT_TYPE),
            ("NULLABLE", TTypeId.INT_TYPE),
            ("REMARKS", TTypeId.STRING_TYPE),
            ("COLUMN_DEF", TTypeId.STRING_TYPE),
            ("SQL_DATA_TYPE", TTypeId.INT_TYPE),
            ("SQL_DATETIME_SUB", TTypeId.INT_TYPE),
            ("CHAR_OCTET_LENGTH", TTypeId.INT_TYPE),
            ("ORDINAL_POSITION", TTypeId.INT_TYPE),
            ("IS_NULLABLE", TTypeId.STRING_TYPE),
            ("SCOPE_CATALOG", TTypeId.STRING_TYPE),
            ("SCOPE_SCHEMA", TTypeId.STRING_TYPE),
            ("SCOPE_TABLE", TTypeId.STRING_TYPE),
            ("SOURCE_DATA_TYPE", TTypeId.SMALLINT_TYPE),
            ("IS_AUTO_INCREMENT", TTypeId.STRING_TYPE));

        public static TTableSchema GetFunctionsSchema { get; } = Of(
            ("FUNCTION_CAT", TTypeId.STRING_TYPE),
            ("FUNCTION_SCHEM", TTypeId.STRING_TYPE),
            ("FUNCTION_NAME", TTypeId.STRING_TYPE),
            ("REMARKS", TTypeId.STRING_TYPE),
            ("FUNCTION_TYPE", TTypeId.INT_TYPE),
            ("SPECIFIC_NAME", TTypeId.STRING_TYPE));

        // Note: "KEQ_SEQ" is a misspelling of "KEY_SEQ" that originates in
        // upstream Apache Hive/Spark — see
        // https://apache.googlesource.com/spark/+/refs/heads/master/sql/hive-thriftserver/src/main/java/org/apache/hive/service/cli/operation/GetPrimaryKeysOperation.java
        // The driver's MetadataColumnNames.ForeignKeyFields looks up the
        // column under that same misspelling. We preserve it here so the mock
        // faithfully mirrors what real Hive/Spark/Impala servers send on the
        // wire; "fixing" the typo would make the mock diverge from reality
        // and break tests that drive GetPrimaryKeys / GetCrossReference
        // end-to-end through the driver.
        public static TTableSchema GetPrimaryKeysSchema { get; } = Of(
            ("TABLE_CAT", TTypeId.STRING_TYPE),
            ("TABLE_SCHEM", TTypeId.STRING_TYPE),
            ("TABLE_NAME", TTypeId.STRING_TYPE),
            ("COLUMN_NAME", TTypeId.STRING_TYPE),
            ("KEQ_SEQ", TTypeId.INT_TYPE),
            ("PK_NAME", TTypeId.STRING_TYPE));

        // "KEQ_SEQ" — see comment on GetPrimaryKeysSchema above.
        public static TTableSchema GetCrossReferenceSchema { get; } = Of(
            ("PKTABLE_CAT", TTypeId.STRING_TYPE),
            ("PKTABLE_SCHEM", TTypeId.STRING_TYPE),
            ("PKTABLE_NAME", TTypeId.STRING_TYPE),
            ("PKCOLUMN_NAME", TTypeId.STRING_TYPE),
            ("FKTABLE_CAT", TTypeId.STRING_TYPE),
            ("FKTABLE_SCHEM", TTypeId.STRING_TYPE),
            ("FKTABLE_NAME", TTypeId.STRING_TYPE),
            ("FKCOLUMN_NAME", TTypeId.STRING_TYPE),
            ("KEQ_SEQ", TTypeId.INT_TYPE),
            ("UPDATE_RULE", TTypeId.INT_TYPE),
            ("DELETE_RULE", TTypeId.INT_TYPE),
            ("FK_NAME", TTypeId.STRING_TYPE),
            ("PK_NAME", TTypeId.STRING_TYPE),
            ("DEFERRABILITY", TTypeId.INT_TYPE));
    }
}

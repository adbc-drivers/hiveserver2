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

using System;
using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
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
        internal const string Nullable = "NULLABLE";
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
        internal static Schema CreateCatalogsSchema()
        {
            return new Schema(new[]
            {
                new Field("TABLE_CAT", StringType.Default, true),
            }, null);
        }

        internal static QueryResult CreateEmptyCatalogsResult()
        {
            return new QueryResult(0, new HiveInfoArrowStream(CreateCatalogsSchema(), new IArrowArray[]
            {
                new StringArray.Builder().Build()
            }));
        }

        internal static Schema CreateSchemasSchema()
        {
            return new Schema(new[]
            {
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_CATALOG", StringType.Default, true),
            }, null);
        }

        internal static QueryResult CreateEmptySchemasResult()
        {
            return new QueryResult(0, new HiveInfoArrowStream(CreateSchemasSchema(), new IArrowArray[]
            {
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build()
            }));
        }

        internal static Schema CreateTablesSchema()
        {
            return new Schema(new[]
            {
                new Field("TABLE_CAT", StringType.Default, true),
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_NAME", StringType.Default, true),
                new Field("TABLE_TYPE", StringType.Default, true),
                new Field("REMARKS", StringType.Default, true),
                new Field("TYPE_CAT", StringType.Default, true),
                new Field("TYPE_SCHEM", StringType.Default, true),
                new Field("TYPE_NAME", StringType.Default, true),
                new Field("SELF_REFERENCING_COL_NAME", StringType.Default, true),
                new Field("REF_GENERATION", StringType.Default, true),
            }, null);
        }

        internal static QueryResult CreateEmptyTablesResult()
        {
            var schema = CreateTablesSchema();
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
                new StringArray.Builder().Build(),
                new StringArray.Builder().Build()
            };
            return new QueryResult(0, new HiveInfoArrowStream(schema, arrays));
        }

        internal static Schema CreateColumnMetadataSchema()
        {
            return new Schema(new[]
            {
                new Field("TABLE_CAT", StringType.Default, true),
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_NAME", StringType.Default, true),
                new Field("COLUMN_NAME", StringType.Default, true),
                new Field("DATA_TYPE", Int32Type.Default, true),
                new Field("TYPE_NAME", StringType.Default, true),
                new Field("COLUMN_SIZE", Int32Type.Default, true),
                new Field("BUFFER_LENGTH", Int32Type.Default, true),
                new Field("DECIMAL_DIGITS", Int32Type.Default, true),
                new Field("NUM_PREC_RADIX", Int32Type.Default, true),
                new Field("NULLABLE", Int32Type.Default, true),
                new Field("REMARKS", StringType.Default, true),
                new Field("COLUMN_DEF", StringType.Default, true),
                new Field("SQL_DATA_TYPE", Int32Type.Default, true),
                new Field("SQL_DATETIME_SUB", Int32Type.Default, true),
                new Field("CHAR_OCTET_LENGTH", Int32Type.Default, true),
                new Field("ORDINAL_POSITION", Int32Type.Default, true),
                new Field("IS_NULLABLE", StringType.Default, true),
                new Field("SCOPE_CATALOG", StringType.Default, true),
                new Field("SCOPE_SCHEMA", StringType.Default, true),
                new Field("SCOPE_TABLE", StringType.Default, true),
                new Field("SOURCE_DATA_TYPE", Int16Type.Default, true),
                new Field("IS_AUTO_INCREMENT", StringType.Default, true),
                new Field("BASE_TYPE_NAME", StringType.Default, true)
            }, null);
        }

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

        internal static QueryResult BuildPrimaryKeysResult(
            IEnumerable<(string catalog, string schema, string table, string column, int keySeq, string pkName)> keys)
        {
            var tableCatBuilder = new StringArray.Builder();
            var tableSchemaBuilder = new StringArray.Builder();
            var tableNameBuilder = new StringArray.Builder();
            var columnNameBuilder = new StringArray.Builder();
            var keySeqBuilder = new Int32Array.Builder();
            var pkNameBuilder = new StringArray.Builder();
            int count = 0;

            foreach (var (catalog, schema, table, column, keySeq, pkName) in keys)
            {
                tableCatBuilder.Append(catalog);
                tableSchemaBuilder.Append(schema);
                tableNameBuilder.Append(table);
                columnNameBuilder.Append(column);
                keySeqBuilder.Append(keySeq);
                pkNameBuilder.Append(pkName);
                count++;
            }

            var resultSchema = CreatePrimaryKeysSchema();
            return new QueryResult(count, new HiveInfoArrowStream(resultSchema, new IArrowArray[]
            {
                tableCatBuilder.Build(), tableSchemaBuilder.Build(), tableNameBuilder.Build(),
                columnNameBuilder.Build(), keySeqBuilder.Build(), pkNameBuilder.Build()
            }));
        }

        internal static QueryResult BuildCrossReferenceResult(
            IEnumerable<(string pkCatalog, string pkSchema, string pkTable, string pkColumn,
                string fkCatalog, string fkSchema, string fkTable, string fkColumn,
                int keySeq, int updateRule, int deleteRule, string fkName, string? pkName, int deferrability)> refs)
        {
            var pkTableCatBuilder = new StringArray.Builder();
            var pkTableSchemaBuilder = new StringArray.Builder();
            var pkTableNameBuilder = new StringArray.Builder();
            var pkColumnNameBuilder = new StringArray.Builder();
            var fkTableCatBuilder = new StringArray.Builder();
            var fkTableSchemaBuilder = new StringArray.Builder();
            var fkTableNameBuilder = new StringArray.Builder();
            var fkColumnNameBuilder = new StringArray.Builder();
            var keySeqBuilder = new Int32Array.Builder();
            var updateRuleBuilder = new Int32Array.Builder();
            var deleteRuleBuilder = new Int32Array.Builder();
            var fkNameBuilder = new StringArray.Builder();
            var pkNameBuilder = new StringArray.Builder();
            var deferrabilityBuilder = new Int32Array.Builder();
            int count = 0;

            foreach (var r in refs)
            {
                pkTableCatBuilder.Append(r.pkCatalog);
                pkTableSchemaBuilder.Append(r.pkSchema);
                pkTableNameBuilder.Append(r.pkTable);
                pkColumnNameBuilder.Append(r.pkColumn);
                fkTableCatBuilder.Append(r.fkCatalog);
                fkTableSchemaBuilder.Append(r.fkSchema);
                fkTableNameBuilder.Append(r.fkTable);
                fkColumnNameBuilder.Append(r.fkColumn);
                keySeqBuilder.Append(r.keySeq);
                updateRuleBuilder.Append(r.updateRule);
                deleteRuleBuilder.Append(r.deleteRule);
                fkNameBuilder.Append(r.fkName);
                if (r.pkName != null) pkNameBuilder.Append(r.pkName); else pkNameBuilder.AppendNull();
                deferrabilityBuilder.Append(r.deferrability);
                count++;
            }

            var resultSchema = CreateCrossReferenceSchema();
            return new QueryResult(count, new HiveInfoArrowStream(resultSchema, new IArrowArray[]
            {
                pkTableCatBuilder.Build(), pkTableSchemaBuilder.Build(), pkTableNameBuilder.Build(),
                pkColumnNameBuilder.Build(), fkTableCatBuilder.Build(), fkTableSchemaBuilder.Build(),
                fkTableNameBuilder.Build(), fkColumnNameBuilder.Build(), keySeqBuilder.Build(),
                updateRuleBuilder.Build(), deleteRuleBuilder.Build(), fkNameBuilder.Build(),
                pkNameBuilder.Build(), deferrabilityBuilder.Build()
            }));
        }

        /// <summary>
        /// Builds a GetInfo result from a dictionary of info code to value mappings.
        /// Values can be string or bool. Unsupported codes are returned as null.
        /// </summary>
        internal static IArrowArrayStream BuildGetInfoResult(
            IReadOnlyList<AdbcInfoCode> codes,
            IReadOnlyDictionary<AdbcInfoCode, object> values)
        {
            const int strValTypeId = 0;
            const int boolValTypeId = 1;

            var infoNameBuilder = new UInt32Array.Builder();
            var typeBuilder = new ArrowBuffer.Builder<byte>();
            var offsetBuilder = new ArrowBuffer.Builder<int>();
            var stringInfoBuilder = new StringArray.Builder();
            var booleanInfoBuilder = new BooleanArray.Builder();

            int nullCount = 0;
            int offset = 0;

            foreach (var code in codes)
            {
                infoNameBuilder.Append((uint)code);

                if (values.TryGetValue(code, out object? val) && val != null)
                {
                    if (val is bool boolVal)
                    {
                        typeBuilder.Append(boolValTypeId);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.AppendNull();
                        booleanInfoBuilder.Append(boolVal);
                    }
                    else
                    {
                        typeBuilder.Append(strValTypeId);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append(val.ToString());
                        booleanInfoBuilder.AppendNull();
                    }
                }
                else
                {
                    typeBuilder.Append(strValTypeId);
                    offsetBuilder.Append(offset++);
                    stringInfoBuilder.AppendNull();
                    booleanInfoBuilder.AppendNull();
                    nullCount++;
                }
            }

            var entryType = new StructType(new[]
            {
                new Field("key", Int32Type.Default, false),
                new Field("value", Int32Type.Default, true)
            });

            var entriesDataArray = new StructArray(entryType, 0,
                new IArrowArray[] { new Int32Array.Builder().Build(), new Int32Array.Builder().Build() },
                new ArrowBuffer.BitmapBuilder().Build());

            var infoUnionType = new UnionType(
                new[]
                {
                    new Field("string_value", StringType.Default, true),
                    new Field("bool_value", BooleanType.Default, true),
                    new Field("int64_value", Int64Type.Default, true),
                    new Field("int32_bitmask", Int32Type.Default, true),
                    new Field("string_list", new ListType(new Field("item", StringType.Default, true)), false),
                    new Field("int32_to_int32_list_map", new ListType(new Field("entries", entryType, false)), true)
                },
                new[] { 0, 1, 2, 3, 4, 5 },
                UnionMode.Dense);

            var childrenArrays = new IArrowArray[]
            {
                stringInfoBuilder.Build(),
                booleanInfoBuilder.Build(),
                new Int64Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new ListArray.Builder(StringType.Default).Build(),
                new List<IArrowArray?>() { entriesDataArray }.BuildListArrayForType(entryType)
            };

            var infoValue = new DenseUnionArray(
                infoUnionType, codes.Count, childrenArrays,
                typeBuilder.Build(), offsetBuilder.Build(), nullCount);

            var dataArrays = new IArrowArray[] { infoNameBuilder.Build(), infoValue };
            StandardSchemas.GetInfoSchema.Validate(dataArrays);

            return new HiveInfoArrowStream(StandardSchemas.GetInfoSchema, dataArrays);
        }
    }
}

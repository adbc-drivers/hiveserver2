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
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Types;
using static Apache.Arrow.Adbc.AdbcConnection;

namespace AdbcDrivers.HiveServer2.Hive2
{
    internal static class GetObjectsResultBuilder
    {
        internal static HiveInfoArrowStream BuildGetObjectsResult(
            IGetObjectsDataProvider provider,
            GetObjectsDepth depth,
            string? catalogPattern,
            string? schemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            var catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>>();

            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Catalogs)
            {
                foreach (string catalog in provider.GetCatalogs(catalogPattern))
                {
                    catalogMap[catalog] = new Dictionary<string, Dictionary<string, TableInfo>>();
                }
            }

            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.DbSchemas)
            {
                foreach (var (catalog, schema) in provider.GetSchemas(catalogPattern, schemaPattern))
                {
                    if (catalogMap.TryGetValue(catalog, out var schemaMap) && !schemaMap.ContainsKey(schema))
                    {
                        schemaMap.Add(schema, new Dictionary<string, TableInfo>());
                    }
                }
            }

            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Tables)
            {
                foreach (var (catalog, schema, table, tableType) in provider.GetTables(catalogPattern, schemaPattern, tableNamePattern, tableTypes))
                {
                    if (catalogMap.TryGetValue(catalog, out var schemaMap)
                        && schemaMap.TryGetValue(schema, out var tableMap)
                        && !tableMap.ContainsKey(table))
                    {
                        tableMap.Add(table, new TableInfo(tableType));
                    }
                }
            }

            if (depth == GetObjectsDepth.All)
            {
                provider.PopulateColumnInfo(catalogPattern, schemaPattern, tableNamePattern, columnNamePattern, catalogMap);
            }

            return BuildResult(depth, catalogMap);
        }

        internal static HiveInfoArrowStream BuildResult(
            GetObjectsDepth depth,
            Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogMap)
        {
            StringArray.Builder catalogNameBuilder = new StringArray.Builder();
            List<IArrowArray?> catalogDbSchemasValues = new List<IArrowArray?>();

            foreach (KeyValuePair<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogEntry in catalogMap)
            {
                catalogNameBuilder.Append(catalogEntry.Key);

                if (depth == GetObjectsDepth.Catalogs)
                {
                    catalogDbSchemasValues.Add(null);
                }
                else
                {
                    catalogDbSchemasValues.Add(BuildDbSchemas(
                                depth, catalogEntry.Value));
                }
            }

            Schema schema = StandardSchemas.GetObjectsSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    catalogNameBuilder.Build(),
                    catalogDbSchemasValues.BuildListArrayForType(new StructType(StandardSchemas.DbSchemaSchema)),
                });

            return new HiveInfoArrowStream(schema, dataArrays);
        }

        internal static StructArray BuildDbSchemas(
            GetObjectsDepth depth,
            Dictionary<string, Dictionary<string, TableInfo>> schemaMap)
        {
            StringArray.Builder dbSchemaNameBuilder = new StringArray.Builder();
            List<IArrowArray?> dbSchemaTablesValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            foreach (KeyValuePair<string, Dictionary<string, TableInfo>> schemaEntry in schemaMap)
            {
                dbSchemaNameBuilder.Append(schemaEntry.Key);
                length++;
                nullBitmapBuffer.Append(true);

                if (depth == GetObjectsDepth.DbSchemas)
                {
                    dbSchemaTablesValues.Add(null);
                }
                else
                {
                    dbSchemaTablesValues.Add(BuildTables(
                        depth, schemaEntry.Value));
                }
            }

            IReadOnlyList<Field> schema = StandardSchemas.DbSchemaSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    dbSchemaNameBuilder.Build(),
                    dbSchemaTablesValues.BuildListArrayForType(new StructType(StandardSchemas.TableSchema)),
                });

            return new StructArray(
                new StructType(schema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        internal static StructArray BuildTables(
            GetObjectsDepth depth,
            Dictionary<string, TableInfo> tableMap)
        {
            StringArray.Builder tableNameBuilder = new StringArray.Builder();
            StringArray.Builder tableTypeBuilder = new StringArray.Builder();
            List<IArrowArray?> tableColumnsValues = new List<IArrowArray?>();
            List<IArrowArray?> tableConstraintsValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            foreach (KeyValuePair<string, TableInfo> tableEntry in tableMap)
            {
                tableNameBuilder.Append(tableEntry.Key);
                tableTypeBuilder.Append(tableEntry.Value.Type);
                nullBitmapBuffer.Append(true);
                length++;

                tableConstraintsValues.Add(null);

                if (depth == GetObjectsDepth.Tables)
                {
                    tableColumnsValues.Add(null);
                }
                else
                {
                    tableColumnsValues.Add(BuildColumns(tableEntry.Value));
                }
            }

            IReadOnlyList<Field> schema = StandardSchemas.TableSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    tableNameBuilder.Build(),
                    tableTypeBuilder.Build(),
                    tableColumnsValues.BuildListArrayForType(new StructType(StandardSchemas.ColumnSchema)),
                    tableConstraintsValues.BuildListArrayForType( new StructType(StandardSchemas.ConstraintSchema))
                });

            return new StructArray(
                new StructType(schema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        internal static QueryResult BuildFlatColumnsResult(
            IEnumerable<(string catalog, string schema, string table, TableInfo info)> tables)
        {
            var tableCatBuilder = new StringArray.Builder();
            var tableSchemaBuilder = new StringArray.Builder();
            var tableNameBuilder = new StringArray.Builder();
            var columnNameBuilder = new StringArray.Builder();
            var dataTypeBuilder = new Int32Array.Builder();
            var typeNameBuilder = new StringArray.Builder();
            var columnSizeBuilder = new Int32Array.Builder();
            var bufferLengthBuilder = new Int32Array.Builder();
            var decimalDigitsBuilder = new Int32Array.Builder();
            var numPrecRadixBuilder = new Int32Array.Builder();
            var nullableBuilder = new Int32Array.Builder();
            var remarksBuilder = new StringArray.Builder();
            var columnDefBuilder = new StringArray.Builder();
            var sqlDataTypeBuilder = new Int32Array.Builder();
            var sqlDatetimeSubBuilder = new Int32Array.Builder();
            var charOctetLengthBuilder = new Int32Array.Builder();
            var ordinalPositionBuilder = new Int32Array.Builder();
            var isNullableBuilder = new StringArray.Builder();
            var scopeCatalogBuilder = new StringArray.Builder();
            var scopeSchemaBuilder = new StringArray.Builder();
            var scopeTableBuilder = new StringArray.Builder();
            var sourceDataTypeBuilder = new Int16Array.Builder();
            var isAutoIncrementBuilder = new StringArray.Builder();
            var baseTypeNameBuilder = new StringArray.Builder();
            int totalRows = 0;

            foreach (var (catalog, schema, table, info) in tables)
            {
                for (int i = 0; i < info.ColumnName.Count; i++)
                {
                    tableCatBuilder.Append(catalog);
                    tableSchemaBuilder.Append(schema);
                    tableNameBuilder.Append(table);
                    columnNameBuilder.Append(info.ColumnName[i]);
                    dataTypeBuilder.Append(info.ColType[i]);
                    typeNameBuilder.Append(info.TypeName[i]);

                    if (info.Precision[i].HasValue) columnSizeBuilder.Append(info.Precision[i]!.Value); else columnSizeBuilder.AppendNull();

                    int? bufLen = ColumnMetadataHelper.GetBufferLength(info.TypeName[i]);
                    if (bufLen.HasValue) bufferLengthBuilder.Append(bufLen.Value); else bufferLengthBuilder.AppendNull();

                    if (info.Scale[i].HasValue) decimalDigitsBuilder.Append(info.Scale[i]!.Value); else decimalDigitsBuilder.AppendNull();

                    short? radix = ColumnMetadataHelper.GetNumPrecRadix(info.TypeName[i]);
                    if (radix.HasValue) numPrecRadixBuilder.Append(radix.Value); else numPrecRadixBuilder.AppendNull();

                    nullableBuilder.Append(info.Nullable[i]);
                    remarksBuilder.Append(info.TypeName[i]);
                    columnDefBuilder.Append(info.ColumnDefault[i]);

                    sqlDataTypeBuilder.Append(info.ColType[i]);

                    short? dtSub = ColumnMetadataHelper.GetSqlDatetimeSub(info.TypeName[i]);
                    if (dtSub.HasValue) sqlDatetimeSubBuilder.Append(dtSub.Value); else sqlDatetimeSubBuilder.AppendNull();

                    int? charOctet = ColumnMetadataHelper.GetCharOctetLength(info.TypeName[i]);
                    if (charOctet.HasValue) charOctetLengthBuilder.Append(charOctet.Value); else charOctetLengthBuilder.AppendNull();

                    ordinalPositionBuilder.Append(info.OrdinalPosition[i]);
                    isNullableBuilder.Append(info.IsNullable[i]);
                    scopeCatalogBuilder.AppendNull();
                    scopeSchemaBuilder.AppendNull();
                    scopeTableBuilder.AppendNull();
                    sourceDataTypeBuilder.AppendNull();
                    isAutoIncrementBuilder.Append(info.IsAutoIncrement[i] ? "YES" : "NO");
                    baseTypeNameBuilder.Append(info.BaseTypeName[i]);
                    totalRows++;
                }
            }

            var resultSchema = MetadataSchemaFactory.CreateColumnMetadataSchema();

            var dataArrays = new IArrowArray[]
            {
                tableCatBuilder.Build(),
                tableSchemaBuilder.Build(),
                tableNameBuilder.Build(),
                columnNameBuilder.Build(),
                dataTypeBuilder.Build(),
                typeNameBuilder.Build(),
                columnSizeBuilder.Build(),
                bufferLengthBuilder.Build(),
                decimalDigitsBuilder.Build(),
                numPrecRadixBuilder.Build(),
                nullableBuilder.Build(),
                remarksBuilder.Build(),
                columnDefBuilder.Build(),
                sqlDataTypeBuilder.Build(),
                sqlDatetimeSubBuilder.Build(),
                charOctetLengthBuilder.Build(),
                ordinalPositionBuilder.Build(),
                isNullableBuilder.Build(),
                scopeCatalogBuilder.Build(),
                scopeSchemaBuilder.Build(),
                scopeTableBuilder.Build(),
                sourceDataTypeBuilder.Build(),
                isAutoIncrementBuilder.Build(),
                baseTypeNameBuilder.Build()
            };

            return new QueryResult(totalRows, new HiveInfoArrowStream(resultSchema, dataArrays));
        }

        internal static StructArray BuildColumns(TableInfo tableInfo)
        {
            StringArray.Builder columnNameBuilder = new StringArray.Builder();
            Int32Array.Builder ordinalPositionBuilder = new Int32Array.Builder();
            StringArray.Builder remarksBuilder = new StringArray.Builder();
            Int16Array.Builder xdbcDataTypeBuilder = new Int16Array.Builder();
            StringArray.Builder xdbcTypeNameBuilder = new StringArray.Builder();
            Int32Array.Builder xdbcColumnSizeBuilder = new Int32Array.Builder();
            Int16Array.Builder xdbcDecimalDigitsBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcNumPrecRadixBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcNullableBuilder = new Int16Array.Builder();
            StringArray.Builder xdbcColumnDefBuilder = new StringArray.Builder();
            Int16Array.Builder xdbcSqlDataTypeBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcDatetimeSubBuilder = new Int16Array.Builder();
            Int32Array.Builder xdbcCharOctetLengthBuilder = new Int32Array.Builder();
            StringArray.Builder xdbcIsNullableBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeCatalogBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeSchemaBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeTableBuilder = new StringArray.Builder();
            BooleanArray.Builder xdbcIsAutoincrementBuilder = new BooleanArray.Builder();
            BooleanArray.Builder xdbcIsGeneratedcolumnBuilder = new BooleanArray.Builder();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            for (int i = 0; i < tableInfo.ColumnName.Count; i++)
            {
                columnNameBuilder.Append(tableInfo.ColumnName[i]);
                ordinalPositionBuilder.Append(tableInfo.OrdinalPosition[i]);
                // Use the "remarks" field to store the original type name value
                remarksBuilder.Append(tableInfo.TypeName[i]);
                xdbcColumnSizeBuilder.Append(tableInfo.Precision[i]);
                xdbcDecimalDigitsBuilder.Append(tableInfo.Scale[i]);
                xdbcDataTypeBuilder.Append(tableInfo.ColType[i]);
                // Just the base type name without precision or scale clause
                xdbcTypeNameBuilder.Append(tableInfo.BaseTypeName[i]);
                short? numPrecRadix = ColumnMetadataHelper.GetNumPrecRadix(tableInfo.TypeName[i]);
                if (numPrecRadix.HasValue) xdbcNumPrecRadixBuilder.Append(numPrecRadix.Value); else xdbcNumPrecRadixBuilder.AppendNull();
                xdbcNullableBuilder.Append(tableInfo.Nullable[i]);
                xdbcColumnDefBuilder.Append(tableInfo.ColumnDefault[i]);
                xdbcSqlDataTypeBuilder.Append(tableInfo.ColType[i]);
                short? datetimeSub = ColumnMetadataHelper.GetSqlDatetimeSub(tableInfo.TypeName[i]);
                if (datetimeSub.HasValue) xdbcDatetimeSubBuilder.Append(datetimeSub.Value); else xdbcDatetimeSubBuilder.AppendNull();
                int? charOctetLength = ColumnMetadataHelper.GetCharOctetLength(tableInfo.TypeName[i]);
                if (charOctetLength.HasValue) xdbcCharOctetLengthBuilder.Append(charOctetLength.Value); else xdbcCharOctetLengthBuilder.AppendNull();
                xdbcIsNullableBuilder.Append(tableInfo.IsNullable[i]);
                xdbcScopeCatalogBuilder.AppendNull();
                xdbcScopeSchemaBuilder.AppendNull();
                xdbcScopeTableBuilder.AppendNull();
                xdbcIsAutoincrementBuilder.Append(tableInfo.IsAutoIncrement[i]);
                xdbcIsGeneratedcolumnBuilder.Append(true);
                nullBitmapBuffer.Append(true);
                length++;
            }

            IReadOnlyList<Field> schema = StandardSchemas.ColumnSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    columnNameBuilder.Build(),
                    ordinalPositionBuilder.Build(),
                    remarksBuilder.Build(),
                    xdbcDataTypeBuilder.Build(),
                    xdbcTypeNameBuilder.Build(),
                    xdbcColumnSizeBuilder.Build(),
                    xdbcDecimalDigitsBuilder.Build(),
                    xdbcNumPrecRadixBuilder.Build(),
                    xdbcNullableBuilder.Build(),
                    xdbcColumnDefBuilder.Build(),
                    xdbcSqlDataTypeBuilder.Build(),
                    xdbcDatetimeSubBuilder.Build(),
                    xdbcCharOctetLengthBuilder.Build(),
                    xdbcIsNullableBuilder.Build(),
                    xdbcScopeCatalogBuilder.Build(),
                    xdbcScopeSchemaBuilder.Build(),
                    xdbcScopeTableBuilder.Build(),
                    xdbcIsAutoincrementBuilder.Build(),
                    xdbcIsGeneratedcolumnBuilder.Build()
                });

            return new StructArray(
                new StructType(schema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }
    }
}

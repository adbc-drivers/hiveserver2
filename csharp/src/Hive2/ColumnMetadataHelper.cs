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
using System.Text.RegularExpressions;
using static AdbcDrivers.HiveServer2.Hive2.HiveServer2Connection;

namespace AdbcDrivers.HiveServer2.Hive2
{
    internal static class ColumnMetadataHelper
    {
        private static readonly Regex s_parameterSuffix = new(
            @"\s*[\(<].*$",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        private static readonly Dictionary<string, short> s_dataTypeCodeMap = new(StringComparer.OrdinalIgnoreCase)
        {
            { "BOOLEAN", (short)ColumnTypeId.BOOLEAN },
            { "TINYINT", (short)ColumnTypeId.TINYINT },
            { "BYTE", (short)ColumnTypeId.TINYINT },
            { "SMALLINT", (short)ColumnTypeId.SMALLINT },
            { "SHORT", (short)ColumnTypeId.SMALLINT },
            { "INT", (short)ColumnTypeId.INTEGER },
            { "INTEGER", (short)ColumnTypeId.INTEGER },
            { "BIGINT", (short)ColumnTypeId.BIGINT },
            { "LONG", (short)ColumnTypeId.BIGINT },
            { "FLOAT", (short)ColumnTypeId.FLOAT },
            { "REAL", (short)ColumnTypeId.REAL },
            { "DOUBLE", (short)ColumnTypeId.DOUBLE },
            { "DECIMAL", (short)ColumnTypeId.DECIMAL },
            { "DEC", (short)ColumnTypeId.DECIMAL },
            { "NUMERIC", (short)ColumnTypeId.NUMERIC },
            { "CHAR", (short)ColumnTypeId.CHAR },
            { "NCHAR", (short)ColumnTypeId.NCHAR },
            { "STRING", (short)ColumnTypeId.VARCHAR },
            { "VARCHAR", (short)ColumnTypeId.VARCHAR },
            { "NVARCHAR", (short)ColumnTypeId.NVARCHAR },
            { "LONGVARCHAR", (short)ColumnTypeId.LONGVARCHAR },
            { "LONGNVARCHAR", (short)ColumnTypeId.LONGNVARCHAR },
            { "BINARY", (short)ColumnTypeId.BINARY },
            { "VARBINARY", (short)ColumnTypeId.VARBINARY },
            { "DATE", (short)ColumnTypeId.DATE },
            { "TIMESTAMP", (short)ColumnTypeId.TIMESTAMP },
            { "TIMESTAMP_LTZ", (short)ColumnTypeId.TIMESTAMP },
            { "TIMESTAMP_NTZ", (short)ColumnTypeId.TIMESTAMP },
            { "ARRAY", (short)ColumnTypeId.ARRAY },
            { "MAP", (short)ColumnTypeId.JAVA_OBJECT },
            { "STRUCT", (short)ColumnTypeId.STRUCT },
            { "VOID", (short)ColumnTypeId.NULL },
            { "NULL", (short)ColumnTypeId.NULL },
        };

        private static readonly HashSet<string> s_numericTypes = new(StringComparer.OrdinalIgnoreCase)
        {
            "TINYINT", "BYTE", "SMALLINT", "SHORT", "INT", "INTEGER",
            "BIGINT", "LONG", "FLOAT", "REAL", "DOUBLE",
            "DECIMAL", "DEC", "NUMERIC"
        };

        private static readonly HashSet<string> s_charTypes = new(StringComparer.OrdinalIgnoreCase)
        {
            "STRING", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR",
            "LONGVARCHAR", "LONGNVARCHAR"
        };

        /// <summary>
        /// Maps a type name to its XDBC data type code.
        /// </summary>
        internal static short GetDataTypeCode(string typeName)
        {
            string normalized = NormalizeTypeName(typeName);

            if (s_dataTypeCodeMap.TryGetValue(normalized, out short code))
                return code;

            // INTERVAL types have qualifiers (e.g., "INTERVAL YEAR TO MONTH")
            if (normalized.StartsWith("INTERVAL", StringComparison.OrdinalIgnoreCase))
                return (short)ColumnTypeId.OTHER;

            return (short)ColumnTypeId.OTHER;
        }

        /// <summary>
        /// Strips parameter clauses and normalizes type name aliases.
        /// </summary>
        internal static string GetBaseTypeName(string typeName)
        {
            string normalized = NormalizeTypeName(typeName);

            return normalized.ToUpperInvariant() switch
            {
                "INT" => "INTEGER",
                "DEC" => "DECIMAL",
                "TIMESTAMP_NTZ" or "TIMESTAMP_LTZ" => "TIMESTAMP",
                "BYTE" => "TINYINT",
                "SHORT" => "SMALLINT",
                "LONG" => "BIGINT",
                _ => normalized.ToUpperInvariant()
            };
        }

        /// <summary>
        /// Returns the default column size for the given type.
        /// For DECIMAL, returns the parsed precision. For VARCHAR/CHAR, returns the parsed length.
        /// </summary>
        internal static int? GetColumnSizeDefault(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);

            switch (baseName)
            {
                case "BOOLEAN":
                case "TINYINT":
                    return 1;
                case "SMALLINT":
                    return 2;
                case "INTEGER":
                case "FLOAT":
                case "DATE":
                    return 4;
                case "BIGINT":
                case "DOUBLE":
                case "TIMESTAMP":
                    return 8;
                case "DECIMAL":
                case "NUMERIC":
                    return TryParseDecimalPrecision(typeName) ?? SqlDecimalTypeParser.DecimalPrecisionDefault;
                case "VARCHAR":
                case "LONGVARCHAR":
                case "LONGNVARCHAR":
                case "NVARCHAR":
                    return TryParseCharLength(typeName) ?? SqlVarcharTypeParser.VarcharColumnSizeDefault;
                case "STRING":
                    return int.MaxValue;
                case "CHAR":
                case "NCHAR":
                    return TryParseCharLength(typeName) ?? 255;
                case "BINARY":
                case "VARBINARY":
                    return int.MaxValue;
                case "NULL":
                    return 1;
                case "REAL":
                    return 4;
                default:
                    if (baseName.StartsWith("INTERVAL", StringComparison.OrdinalIgnoreCase))
                        return GetIntervalSize(typeName);
                    return 0;
            }
        }

        /// <summary>
        /// Returns the default decimal digits (scale) for the given type.
        /// </summary>
        internal static int? GetDecimalDigitsDefault(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);

            return baseName switch
            {
                "DECIMAL" or "NUMERIC" => TryParseDecimalScale(typeName) ?? SqlDecimalTypeParser.DecimalScaleDefault,
                "FLOAT" or "REAL" => 7,
                "DOUBLE" => 15,
                "TIMESTAMP" => 6,
                _ => 0
            };
        }

        /// <summary>
        /// Returns the buffer length for binary representation of the given type.
        /// </summary>
        internal static int? GetBufferLength(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);

            switch (baseName)
            {
                case "BOOLEAN":
                case "TINYINT":
                    return 1;
                case "SMALLINT":
                    return 2;
                case "INTEGER":
                case "FLOAT":
                case "REAL":
                    return 4;
                case "BIGINT":
                case "DOUBLE":
                case "TIMESTAMP":
                case "DATE":
                    return 8;
                case "DECIMAL":
                case "NUMERIC":
                    int precision = TryParseDecimalPrecision(typeName) ?? SqlDecimalTypeParser.DecimalPrecisionDefault;
                    return ((precision + 8) / 9) * 5 + 1;
                default:
                    return null;
            }
        }

        /// <summary>
        /// Returns 10 for numeric types, null otherwise.
        /// </summary>
        internal static short? GetNumPrecRadix(string typeName)
        {
            string normalized = NormalizeTypeName(typeName);
            return s_numericTypes.Contains(normalized) ? (short)10 : null;
        }

        /// <summary>
        /// Returns the character octet length for character types, null otherwise.
        /// </summary>
        internal static int? GetCharOctetLength(string typeName)
        {
            string normalized = NormalizeTypeName(typeName);
            return s_charTypes.Contains(normalized) ? GetColumnSizeDefault(typeName) : null;
        }

        internal static short? GetSqlDatetimeSub(string typeName)
        {
            string baseName = GetBaseTypeName(typeName);
            return baseName switch
            {
                "DATE" => 1,
                "TIMESTAMP" => 3,
                _ => null
            };
        }

        internal static void PopulateTableInfoFromTypeName(
            TableInfo tableInfo,
            string columnName,
            string typeName,
            int ordinalPosition,
            bool isNullable = true,
            string? comment = null,
            string? columnDefault = null)
        {
            tableInfo.ColumnName.Add(columnName);
            tableInfo.TypeName.Add(typeName);
            tableInfo.ColType.Add(GetDataTypeCode(typeName));
            tableInfo.BaseTypeName.Add(GetBaseTypeName(typeName));
            tableInfo.Precision.Add(GetColumnSizeDefault(typeName));
            int? scale = GetDecimalDigitsDefault(typeName);
            tableInfo.Scale.Add(scale.HasValue ? (short)scale.Value : null);
            tableInfo.OrdinalPosition.Add(ordinalPosition);
            tableInfo.Nullable.Add(isNullable ? (short)1 : (short)0);
            tableInfo.IsNullable.Add(isNullable ? "YES" : "NO");
            tableInfo.IsAutoIncrement.Add(false);
            tableInfo.ColumnDefault.Add(columnDefault ?? "");
        }

        private static string NormalizeTypeName(string typeName)
        {
            string trimmed = typeName.Trim();
            return s_parameterSuffix.Replace(trimmed, "").Trim();
        }

        private static int? TryParseDecimalPrecision(string typeName)
        {
            if (SqlTypeNameParser<SqlDecimalParserResult>.TryParse(typeName, out SqlTypeNameParserResult? result, (int)ColumnTypeId.DECIMAL)
                && result is SqlDecimalParserResult decimalResult)
            {
                return decimalResult.Precision;
            }
            return null;
        }

        private static int? TryParseDecimalScale(string typeName)
        {
            if (SqlTypeNameParser<SqlDecimalParserResult>.TryParse(typeName, out SqlTypeNameParserResult? result, (int)ColumnTypeId.DECIMAL)
                && result is SqlDecimalParserResult decimalResult)
            {
                return decimalResult.Scale;
            }
            return null;
        }

        private static int? TryParseCharLength(string typeName)
        {
            if (SqlTypeNameParser<SqlCharVarcharParserResult>.TryParse(typeName, out SqlTypeNameParserResult? result, (int)ColumnTypeId.VARCHAR)
                && result is SqlCharVarcharParserResult charResult)
            {
                return charResult.ColumnSize;
            }
            return null;
        }

        private static int GetIntervalSize(string typeName)
        {
            string upper = typeName.Trim().ToUpperInvariant();
            if (upper.Contains("YEAR") || upper.Contains("MONTH"))
                return 4;
            if (upper.Contains("DAY") || upper.Contains("HOUR") || upper.Contains("MINUTE") || upper.Contains("SECOND"))
                return 8;
            return 4;
        }
    }
}

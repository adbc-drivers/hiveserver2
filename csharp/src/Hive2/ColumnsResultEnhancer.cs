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
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;

namespace AdbcDrivers.HiveServer2.Hive2
{
    internal static class ColumnsResultEnhancer
    {
        internal delegate void SetPrecisionScaleDelegate(short colType, string typeName, TableInfo? tableInfo, int columnSize, int decimalDigits);

        internal static QueryResult Enhance(
            Schema originalSchema,
            IReadOnlyList<IArrowArray> originalData,
            int rowCount,
            int typeNameIndex,
            int columnSizeIndex,
            int decimalDigitsIndex,
            ReadOnlySpan<int> rawDataTypes,
            SetPrecisionScaleDelegate setPrecisionScaleAndTypeName)
        {
            StringArray typeNames = (StringArray)originalData[typeNameIndex];
            Int32Array originalColumnSizes = (Int32Array)originalData[columnSizeIndex];
            Int32Array originalDecimalDigits = (Int32Array)originalData[decimalDigitsIndex];

            var enhancedFields = originalSchema.FieldsList.ToList();
            enhancedFields.Add(new Field(MetadataColumnNames.BaseTypeName, StringType.Default, true));
            Schema enhancedSchema = new Schema(enhancedFields, originalSchema.Metadata);

            int length = typeNames.Length;
            var baseTypeNames = new List<string>(length);
            var columnSizeValues = new List<int>(length);
            var decimalDigitsValues = new List<int>(length);

            for (int i = 0; i < length; i++)
            {
                string typeName = typeNames.GetString(i) ?? string.Empty;
                short colType = (short)rawDataTypes[i];
                int columnSize = originalColumnSizes.GetValue(i).GetValueOrDefault();
                int decimalDigits = originalDecimalDigits.GetValue(i).GetValueOrDefault();

                var tableInfo = new TableInfo(string.Empty);
                setPrecisionScaleAndTypeName(colType, typeName, tableInfo, columnSize, decimalDigits);

                baseTypeNames.Add(
                    tableInfo.BaseTypeName.Count > 0
                        ? tableInfo.BaseTypeName[0] ?? typeName
                        : typeName);

                columnSizeValues.Add(
                    tableInfo.Precision.Count > 0
                        ? tableInfo.Precision[0].GetValueOrDefault(columnSize)
                        : columnSize);

                decimalDigitsValues.Add(
                    tableInfo.Scale.Count > 0
                        ? tableInfo.Scale[0].GetValueOrDefault((short)decimalDigits)
                        : decimalDigits);
            }

            StringArray baseTypeNameArray = new StringArray.Builder().AppendRange(baseTypeNames).Build();
            Int32Array columnSizeArray = new Int32Array.Builder().AppendRange(columnSizeValues).Build();
            Int32Array decimalDigitsArray = new Int32Array.Builder().AppendRange(decimalDigitsValues).Build();

            var enhancedData = new List<IArrowArray>(originalData);
            enhancedData[columnSizeIndex] = columnSizeArray;
            enhancedData[decimalDigitsIndex] = decimalDigitsArray;
            enhancedData.Add(baseTypeNameArray);

            return new QueryResult(rowCount, new HiveInfoArrowStream(enhancedSchema, enhancedData));
        }
    }
}

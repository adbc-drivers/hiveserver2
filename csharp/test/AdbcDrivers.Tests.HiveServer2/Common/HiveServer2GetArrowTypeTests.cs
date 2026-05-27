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

using System;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow.Types;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Common
{
    /// <summary>
    /// Direct tests for the <c>HiveServer2Connection.GetArrowType</c> switch.
    /// Static, side-effect-free — drives every case branch including the
    /// DECIMAL precision/scale variants and the unsupported-type throw.
    /// </summary>
    public class HiveServer2GetArrowTypeTests
    {
        private static IArrowType ResolveType(HiveServer2Connection.ColumnTypeId type, string typeName = "") =>
            HiveServer2Connection.GetArrowType((int)type, typeName, isColumnSizeValid: false, columnSize: null, decimalDigits: null);

        [Fact]
        public void Boolean_MapsToBooleanType() =>
            Assert.IsType<BooleanType>(ResolveType(HiveServer2Connection.ColumnTypeId.BOOLEAN));

        [Fact]
        public void Tinyint_MapsToInt8() =>
            Assert.IsType<Int8Type>(ResolveType(HiveServer2Connection.ColumnTypeId.TINYINT));

        [Fact]
        public void Smallint_MapsToInt16() =>
            Assert.IsType<Int16Type>(ResolveType(HiveServer2Connection.ColumnTypeId.SMALLINT));

        [Fact]
        public void Integer_MapsToInt32() =>
            Assert.IsType<Int32Type>(ResolveType(HiveServer2Connection.ColumnTypeId.INTEGER));

        [Fact]
        public void Bigint_MapsToInt64() =>
            Assert.IsType<Int64Type>(ResolveType(HiveServer2Connection.ColumnTypeId.BIGINT));

        [Theory]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.FLOAT)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.REAL)]
        public void FloatAndReal_MapToFloatType(int typeId) =>
            Assert.IsType<FloatType>(HiveServer2Connection.GetArrowType(typeId, "", false, null, null));

        [Fact]
        public void Double_MapsToDoubleType() =>
            Assert.IsType<DoubleType>(ResolveType(HiveServer2Connection.ColumnTypeId.DOUBLE));

        [Theory]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.VARCHAR)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.NVARCHAR)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.LONGVARCHAR)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.LONGNVARCHAR)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.CHAR)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.NCHAR)]
        public void StringFamily_MapsToStringType(int typeId) =>
            Assert.IsType<StringType>(HiveServer2Connection.GetArrowType(typeId, "", false, null, null));

        [Fact]
        public void Timestamp_MapsToMicrosecondTimestampType()
        {
            IArrowType t = ResolveType(HiveServer2Connection.ColumnTypeId.TIMESTAMP);
            var ts = Assert.IsType<TimestampType>(t);
            Assert.Equal(TimeUnit.Microsecond, ts.Unit);
            Assert.Null(ts.Timezone);
        }

        [Theory]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.BINARY)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.VARBINARY)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.LONGVARBINARY)]
        public void BinaryFamily_MapsToBinaryType(int typeId) =>
            Assert.IsType<BinaryType>(HiveServer2Connection.GetArrowType(typeId, "", false, null, null));

        [Fact]
        public void Date_MapsToDate32() =>
            Assert.IsType<Date32Type>(ResolveType(HiveServer2Connection.ColumnTypeId.DATE));

        [Theory]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.DECIMAL)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.NUMERIC)]
        public void Decimal_WithServerProvidedPrecisionScale_UsesServerValues(int typeId)
        {
            // When isColumnSizeValid + columnSize + decimalDigits are all
            // present, the conversion trusts the server's metadata directly.
            IArrowType t = HiveServer2Connection.GetArrowType(
                typeId, "DECIMAL(38,9)", isColumnSizeValid: true, columnSize: 38, decimalDigits: 9);
            var dec = Assert.IsType<Decimal128Type>(t);
            Assert.Equal(38, dec.Precision);
            Assert.Equal(9, dec.Scale);
        }

        [Theory]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.DECIMAL)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.NUMERIC)]
        public void Decimal_WithoutColumnSize_FallsBackToTypeNameParse(int typeId)
        {
            // When the server didn't populate column_size/decimal_digits we
            // parse precision/scale out of the type name string instead.
            IArrowType t = HiveServer2Connection.GetArrowType(
                typeId, "DECIMAL(12,4)", isColumnSizeValid: false, columnSize: null, decimalDigits: null);
            var dec = Assert.IsType<Decimal128Type>(t);
            Assert.Equal(12, dec.Precision);
            Assert.Equal(4, dec.Scale);
        }

        [Fact]
        public void Null_MapsToNullType() =>
            Assert.IsType<NullType>(ResolveType(HiveServer2Connection.ColumnTypeId.NULL));

        [Theory]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.ARRAY)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.JAVA_OBJECT)]
        [InlineData((int)HiveServer2Connection.ColumnTypeId.STRUCT)]
        public void ComplexFamily_MapsToStringType(int typeId)
        {
            // Complex types are passed to ADBC clients as JSON-encoded strings.
            Assert.IsType<StringType>(HiveServer2Connection.GetArrowType(typeId, "", false, null, null));
        }

        [Fact]
        public void UnknownType_Throws()
        {
            // Some made-up column-type ID that isn't in java.sql.Types.
            Assert.Throws<NotImplementedException>(
                () => HiveServer2Connection.GetArrowType(9999, "MYSTERY", false, null, null));
        }
    }
}

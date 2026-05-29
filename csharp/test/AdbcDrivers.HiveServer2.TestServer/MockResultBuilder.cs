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
using System.Globalization;
using System.Linq;
using Apache.Hive.Service.Rpc.Thrift.Reference;

namespace AdbcDrivers.HiveServer2.TestServer
{
    /// <summary>
    /// Fluent builder for a single-batch <see cref="MockResult"/>. Add columns
    /// in the order they should appear; each column-adding method enforces
    /// row-count consistency. The default DATE and TIMESTAMP formatters match
    /// what the HiveServer2 protocol uses on the wire.
    /// </summary>
    public sealed class MockResultBuilder
    {
        private readonly List<TColumnDesc> _schemaColumns = new();
        private readonly List<TColumn> _columns = new();
        private int _rowCount = -1;
        private int _positionBase = 1;

        /// <summary>
        /// Override the base index for column positions in the emitted
        /// schema. Defaults to 1 (matches Hive/Spark, whose drivers use
        /// <c>ColumnMapIndexOffset=1</c>). Set to 0 for Impala fixtures —
        /// the Impala driver uses <c>ColumnMapIndexOffset=0</c>, so a
        /// 1-indexed schema would yield off-by-one lookups when
        /// PopulateColumnInfoAsync walks the column map.
        /// </summary>
        public MockResultBuilder WithPositionBase(int positionBase)
        {
            if (_schemaColumns.Count > 0)
                throw new InvalidOperationException("WithPositionBase must be called before adding any columns.");
            _positionBase = positionBase;
            return this;
        }

        public MockResultBuilder Bool(string name, params bool?[] values)
        {
            AssertRowCount(values.Length);
            _schemaColumns.Add(MockSchema.SimpleColumn(name, TTypeId.BOOLEAN_TYPE, position: _schemaColumns.Count + _positionBase));
            _columns.Add(new TColumn
            {
                BoolVal = new TBoolColumn(
                    values.Select(v => v ?? default).ToList(),
                    MockRowSet.NullsBitmap(values.Select(v => v is null).ToList())),
            });
            return this;
        }

        public MockResultBuilder Tinyint(string name, params sbyte?[] values)
        {
            AssertRowCount(values.Length);
            _schemaColumns.Add(MockSchema.SimpleColumn(name, TTypeId.TINYINT_TYPE, position: _schemaColumns.Count + _positionBase));
            _columns.Add(new TColumn
            {
                ByteVal = new TByteColumn(
                    values.Select(v => v ?? default).ToList(),
                    MockRowSet.NullsBitmap(values.Select(v => v is null).ToList())),
            });
            return this;
        }

        public MockResultBuilder Smallint(string name, params short?[] values)
        {
            AssertRowCount(values.Length);
            _schemaColumns.Add(MockSchema.SimpleColumn(name, TTypeId.SMALLINT_TYPE, position: _schemaColumns.Count + _positionBase));
            _columns.Add(new TColumn
            {
                I16Val = new TI16Column(
                    values.Select(v => v ?? default).ToList(),
                    MockRowSet.NullsBitmap(values.Select(v => v is null).ToList())),
            });
            return this;
        }

        public MockResultBuilder Int(string name, params int?[] values)
        {
            AssertRowCount(values.Length);
            _schemaColumns.Add(MockSchema.SimpleColumn(name, TTypeId.INT_TYPE, position: _schemaColumns.Count + _positionBase));
            _columns.Add(new TColumn
            {
                I32Val = new TI32Column(
                    values.Select(v => v ?? default).ToList(),
                    MockRowSet.NullsBitmap(values.Select(v => v is null).ToList())),
            });
            return this;
        }

        public MockResultBuilder Bigint(string name, params long?[] values)
        {
            AssertRowCount(values.Length);
            _schemaColumns.Add(MockSchema.SimpleColumn(name, TTypeId.BIGINT_TYPE, position: _schemaColumns.Count + _positionBase));
            _columns.Add(new TColumn
            {
                I64Val = new TI64Column(
                    values.Select(v => v ?? default).ToList(),
                    MockRowSet.NullsBitmap(values.Select(v => v is null).ToList())),
            });
            return this;
        }

        public MockResultBuilder Float(string name, params float?[] values)
        {
            AssertRowCount(values.Length);
            _schemaColumns.Add(MockSchema.SimpleColumn(name, TTypeId.FLOAT_TYPE, position: _schemaColumns.Count + _positionBase));
            _columns.Add(new TColumn
            {
                DoubleVal = new TDoubleColumn(
                    values.Select(v => v.HasValue ? (double)v.Value : 0d).ToList(),
                    MockRowSet.NullsBitmap(values.Select(v => v is null).ToList())),
            });
            return this;
        }

        public MockResultBuilder Double(string name, params double?[] values)
        {
            AssertRowCount(values.Length);
            _schemaColumns.Add(MockSchema.SimpleColumn(name, TTypeId.DOUBLE_TYPE, position: _schemaColumns.Count + _positionBase));
            _columns.Add(new TColumn
            {
                DoubleVal = new TDoubleColumn(
                    values.Select(v => v ?? 0d).ToList(),
                    MockRowSet.NullsBitmap(values.Select(v => v is null).ToList())),
            });
            return this;
        }

        public MockResultBuilder String(string name, params string?[] values) =>
            AddStringWireColumn(name, TTypeId.STRING_TYPE, values, _ => null);

        public MockResultBuilder Varchar(string name, params string?[] values) =>
            AddStringWireColumn(name, TTypeId.VARCHAR_TYPE, values, _ => null);

        public MockResultBuilder Char(string name, int length, params string?[] values) =>
            AddStringWireColumn(
                name,
                TTypeId.CHAR_TYPE,
                values,
                _ => new TTypeQualifiers(new Dictionary<string, TTypeQualifierValue>
                {
                    ["characterMaximumLength"] = new() { I32Value = length },
                }));

        public MockResultBuilder Date(string name, params DateTime?[] values)
        {
            var encoded = values.Select(v => v?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)).ToArray();
            return AddStringWireColumn(name, TTypeId.DATE_TYPE, encoded, _ => null);
        }

        public MockResultBuilder Timestamp(string name, params DateTime?[] values)
        {
            var encoded = values
                .Select(v => v?.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture))
                .ToArray();
            return AddStringWireColumn(name, TTypeId.TIMESTAMP_TYPE, encoded, _ => null);
        }

        public MockResultBuilder Decimal(string name, int precision, int scale, params decimal?[] values)
        {
            var encoded = values
                .Select(v => v?.ToString(CultureInfo.InvariantCulture))
                .ToArray();
            AssertRowCount(encoded.Length);
            _schemaColumns.Add(MockSchema.DecimalColumn(name, precision, scale, position: _schemaColumns.Count + _positionBase));
            _columns.Add(new TColumn
            {
                StringVal = new TStringColumn(
                    encoded.Select(v => v ?? string.Empty).ToList(),
                    MockRowSet.NullsBitmap(encoded.Select(v => v is null).ToList())),
            });
            return this;
        }

        public MockResultBuilder Binary(string name, params byte[]?[] values)
        {
            AssertRowCount(values.Length);
            _schemaColumns.Add(MockSchema.SimpleColumn(name, TTypeId.BINARY_TYPE, position: _schemaColumns.Count + _positionBase));
            _columns.Add(new TColumn
            {
                BinaryVal = new TBinaryColumn(
                    values.Select(v => v ?? Array.Empty<byte>()).ToList(),
                    MockRowSet.NullsBitmap(values.Select(v => v is null).ToList())),
            });
            return this;
        }

        public MockResult Build()
        {
            var schema = new TTableSchema(new List<TColumnDesc>(_schemaColumns));
            var rowSet = new TRowSet(0L, new List<TRow>()) { Columns = new List<TColumn>(_columns) };
            return new MockResult(schema, new[] { rowSet });
        }

        private MockResultBuilder AddStringWireColumn(
            string name,
            TTypeId schemaType,
            string?[] values,
            Func<int, TTypeQualifiers?> qualifiersFor)
        {
            AssertRowCount(values.Length);
            var primitive = new TPrimitiveTypeEntry(schemaType);
            var qualifiers = qualifiersFor(values.Length);
            if (qualifiers != null) primitive.TypeQualifiers = qualifiers;
            var typeDesc = new TTypeDesc(new List<TTypeEntry>
            {
                new() { PrimitiveEntry = primitive },
            });
            _schemaColumns.Add(new TColumnDesc(name, typeDesc, _schemaColumns.Count + _positionBase));
            _columns.Add(new TColumn
            {
                StringVal = new TStringColumn(
                    values.Select(v => v ?? string.Empty).ToList(),
                    MockRowSet.NullsBitmap(values.Select(v => v is null).ToList())),
            });
            return this;
        }

        private void AssertRowCount(int count)
        {
            if (_rowCount < 0) _rowCount = count;
            else if (_rowCount != count)
                throw new ArgumentException(
                    $"All columns in a MockResult must have the same row count; existing columns have {_rowCount} rows, new column has {count}.");
        }
    }
}

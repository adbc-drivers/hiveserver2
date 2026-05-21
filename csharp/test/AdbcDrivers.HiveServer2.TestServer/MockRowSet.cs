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
using Apache.Hive.Service.Rpc.Thrift.Reference;

namespace AdbcDrivers.HiveServer2.TestServer
{
    /// <summary>
    /// Helpers for fabricating <see cref="TRowSet"/> values in the column-
    /// oriented wire format used by HiveServer2 protocol V6 and later.
    /// </summary>
    public static class MockRowSet
    {
        /// <summary>An empty row set whose columns match the given schema.</summary>
        public static TRowSet EmptyFor(TTableSchema schema)
        {
            var cols = new List<TColumn>(schema.Columns.Count);
            for (int i = 0; i < schema.Columns.Count; i++)
            {
                cols.Add(EmptyColumnFor(schema.Columns[i].TypeDesc.Types[0].PrimitiveEntry.Type));
            }
            return new TRowSet(0L, new List<TRow>()) { Columns = cols };
        }

        /// <summary>Pack a per-row null indicator into the bitmap Thrift expects.</summary>
        /// <remarks>
        /// HiveServer2's nulls bitmap is little-endian within each byte: bit
        /// <c>i &amp; 7</c> of byte <c>i / 8</c> controls row <c>i</c>. A set
        /// bit means null. The array length is <c>ceil(rows / 8)</c>.
        /// </remarks>
        public static byte[] NullsBitmap(IReadOnlyList<bool> isNull)
        {
            int count = isNull.Count;
            if (count == 0) return Array.Empty<byte>();
            var bytes = new byte[(count + 7) / 8];
            for (int i = 0; i < count; i++)
            {
                if (isNull[i]) bytes[i / 8] |= (byte)(1 << (i & 7));
            }
            return bytes;
        }

        private static TColumn EmptyColumnFor(TTypeId type) => type switch
        {
            TTypeId.BOOLEAN_TYPE => new() { BoolVal = new TBoolColumn(new List<bool>(), Array.Empty<byte>()) },
            TTypeId.TINYINT_TYPE => new() { ByteVal = new TByteColumn(new List<sbyte>(), Array.Empty<byte>()) },
            TTypeId.SMALLINT_TYPE => new() { I16Val = new TI16Column(new List<short>(), Array.Empty<byte>()) },
            TTypeId.INT_TYPE => new() { I32Val = new TI32Column(new List<int>(), Array.Empty<byte>()) },
            TTypeId.BIGINT_TYPE => new() { I64Val = new TI64Column(new List<long>(), Array.Empty<byte>()) },
            TTypeId.FLOAT_TYPE or TTypeId.DOUBLE_TYPE =>
                new() { DoubleVal = new TDoubleColumn(new List<double>(), Array.Empty<byte>()) },
            TTypeId.BINARY_TYPE => new() { BinaryVal = new TBinaryColumn(new List<byte[]>(), Array.Empty<byte>()) },
            // STRING/VARCHAR/CHAR/DATE/TIMESTAMP/INTERVAL/DECIMAL/NULL all ride on TStringColumn
            // in the columnar wire format.
            _ => new() { StringVal = new TStringColumn(new List<string>(), Array.Empty<byte>()) },
        };
    }
}

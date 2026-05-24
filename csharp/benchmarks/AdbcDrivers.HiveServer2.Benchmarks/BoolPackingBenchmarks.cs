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
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;

namespace AdbcDrivers.HiveServer2.Benchmarks
{
    /// <summary>
    /// Converting a wire-format boolean column (one byte per value, 1 == true)
    /// into an Arrow bit-packed bitmap. Compares the original per-bit
    /// <see cref="ArrowBuffer.BitmapBuilder"/> loop against direct scalar
    /// packing and the SIMD packing now used by the driver on .NET 8+.
    /// </summary>
    [MemoryDiagnoser]
    public class BoolPackingBenchmarks
    {
        [Params(1_000, 100_000, 1_000_000)]
        public int RowCount;

        private byte[] _values = System.Array.Empty<byte>();

        [GlobalSetup]
        public void Setup()
        {
            _values = new byte[RowCount];
            var rng = new Random(42);
            for (int i = 0; i < _values.Length; i++)
            {
                _values[i] = (byte)rng.Next(2);
            }
        }

        [Benchmark(Baseline = true)]
        public ArrowBuffer BitmapBuilder_Append()
        {
            var builder = new ArrowBuffer.BitmapBuilder(_values.Length);
            for (int i = 0; i < _values.Length; i++)
            {
                builder.Append(_values[i] == 1);
            }
            return builder.Build();
        }

        [Benchmark]
        public byte[] Scalar_Pack()
        {
            ReadOnlySpan<byte> values = _values;
            byte[] packed = new byte[(values.Length + 7) / 8];
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] == 1)
                {
                    packed[i >> 3] |= (byte)(1 << (i & 7));
                }
            }
            return packed;
        }

        [Benchmark]
        public byte[] Vectorized_Pack()
        {
            ReadOnlySpan<byte> values = _values;
            int length = values.Length;
            byte[] packed = new byte[(length + 7) / 8];
            int i = 0;
            ref byte src = ref MemoryMarshal.GetReference(values);
            if (Vector256.IsHardwareAccelerated)
            {
                Vector256<byte> ones = Vector256.Create((byte)1);
                for (; i + Vector256<byte>.Count <= length; i += Vector256<byte>.Count)
                {
                    uint mask = Vector256.Equals(Vector256.LoadUnsafe(ref src, (nuint)i), ones).ExtractMostSignificantBits();
                    BinaryPrimitives.WriteUInt32LittleEndian(packed.AsSpan(i / 8), mask);
                }
            }
            if (Vector128.IsHardwareAccelerated)
            {
                Vector128<byte> ones = Vector128.Create((byte)1);
                for (; i + Vector128<byte>.Count <= length; i += Vector128<byte>.Count)
                {
                    uint mask = Vector128.Equals(Vector128.LoadUnsafe(ref src, (nuint)i), ones).ExtractMostSignificantBits();
                    BinaryPrimitives.WriteUInt16LittleEndian(packed.AsSpan(i / 8), (ushort)mask);
                }
            }
            for (; i < length; i++)
            {
                if (values[i] == 1)
                {
                    packed[i >> 3] |= (byte)(1 << (i & 7));
                }
            }
            return packed;
        }
    }
}

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
using System.Numerics;
using BenchmarkDotNet.Attributes;

namespace AdbcDrivers.HiveServer2.Benchmarks
{
    /// <summary>
    /// Compares the per-byte bitwise NOT used to turn a wire-format "nulls"
    /// bitmap into an Arrow "validity" bitmap against the SIMD XOR-with-ones
    /// path used by <c>BitmapUtilities.GetValidityBitmapBuffer</c> on .NET 8+.
    /// The bitmap is one bit per row, so the byte count is <c>RowCount / 8</c>.
    /// </summary>
    [MemoryDiagnoser]
    public class BitmapInversionBenchmarks
    {
        [Params(1_000, 100_000, 1_000_000)]
        public int RowCount;

        private byte[] _bitmap = Array.Empty<byte>();

        [GlobalSetup]
        public void Setup()
        {
            _bitmap = new byte[RowCount / 8];
            new Random(42).NextBytes(_bitmap);
        }

        [Benchmark(Baseline = true)]
        public void Scalar_Invert()
        {
            Span<byte> bytes = _bitmap;
            for (int i = 0; i < bytes.Length; i++)
            {
                bytes[i] = (byte)~bytes[i];
            }
        }

        [Benchmark]
        public void Vectorized_Invert()
        {
            Span<byte> bytes = _bitmap;
            int i = 0;
            if (Vector.IsHardwareAccelerated)
            {
                Vector<byte> ones = Vector<byte>.AllBitsSet;
                int width = Vector<byte>.Count;
                for (; i <= bytes.Length - width; i += width)
                {
                    Span<byte> block = bytes.Slice(i, width);
                    (new Vector<byte>(block) ^ ones).CopyTo(block);
                }
            }
            for (; i < bytes.Length; i++)
            {
                bytes[i] = (byte)~bytes[i];
            }
        }
    }
}

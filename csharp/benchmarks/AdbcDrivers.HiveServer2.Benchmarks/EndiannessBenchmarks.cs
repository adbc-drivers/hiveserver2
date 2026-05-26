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
using BenchmarkDotNet.Attributes;

namespace AdbcDrivers.HiveServer2.Benchmarks
{
    /// <summary>
    /// Compares the per-element scalar byte swap (what the driver does on
    /// down-level targets) against the SIMD-accelerated span overload used on
    /// .NET 8+ for the numeric Thrift column readers (TI16/TI32/TI64/TDouble).
    ///
    /// The buffers mirror the real code: a raw <c>byte[]</c> read off the wire,
    /// reinterpreted via <see cref="MemoryMarshal.Cast{TFrom, TTo}"/> and
    /// byte-swapped in place. <c>RowCount</c> is the number of column values.
    /// </summary>
    [MemoryDiagnoser]
    public class EndiannessBenchmarks
    {
        // Representative HiveServer2 fetch batch sizes: a small page, a typical
        // batch, and a large single fetch.
        [Params(1_000, 100_000, 1_000_000)]
        public int RowCount;

        private byte[] _shortBytes = Array.Empty<byte>();
        private byte[] _intBytes = Array.Empty<byte>();
        private byte[] _longBytes = Array.Empty<byte>();

        [GlobalSetup]
        public void Setup()
        {
            var rng = new Random(42);
            _shortBytes = new byte[RowCount * sizeof(short)];
            _intBytes = new byte[RowCount * sizeof(int)];
            _longBytes = new byte[RowCount * sizeof(long)];
            rng.NextBytes(_shortBytes);
            rng.NextBytes(_intBytes);
            rng.NextBytes(_longBytes);
        }

        [Benchmark(Baseline = true)]
        [BenchmarkCategory("Int16")]
        public void Scalar_Int16()
        {
            var values = MemoryMarshal.Cast<byte, short>(_shortBytes.AsSpan());
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = BinaryPrimitives.ReverseEndianness(values[i]);
            }
        }

        [Benchmark]
        [BenchmarkCategory("Int16")]
        public void Vectorized_Int16()
        {
            var values = MemoryMarshal.Cast<byte, short>(_shortBytes.AsSpan());
            BinaryPrimitives.ReverseEndianness(values, values);
        }

        [Benchmark]
        [BenchmarkCategory("Int32")]
        public void Scalar_Int32()
        {
            var values = MemoryMarshal.Cast<byte, int>(_intBytes.AsSpan());
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = BinaryPrimitives.ReverseEndianness(values[i]);
            }
        }

        [Benchmark]
        [BenchmarkCategory("Int32")]
        public void Vectorized_Int32()
        {
            var values = MemoryMarshal.Cast<byte, int>(_intBytes.AsSpan());
            BinaryPrimitives.ReverseEndianness(values, values);
        }

        [Benchmark]
        [BenchmarkCategory("Int64")]
        public void Scalar_Int64()
        {
            var values = MemoryMarshal.Cast<byte, long>(_longBytes.AsSpan());
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = BinaryPrimitives.ReverseEndianness(values[i]);
            }
        }

        [Benchmark]
        [BenchmarkCategory("Int64")]
        public void Vectorized_Int64()
        {
            var values = MemoryMarshal.Cast<byte, long>(_longBytes.AsSpan());
            BinaryPrimitives.ReverseEndianness(values, values);
        }
    }
}

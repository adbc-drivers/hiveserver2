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
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;

namespace AdbcDrivers.HiveServer2.Benchmarks
{
    /// <summary>
    /// HiveServer2 sends FLOAT columns as doubles; the driver narrows them to a
    /// FloatArray. Today that is a FloatArray.Builder with a per-element
    /// Append((float?)array.GetValue(i)) loop. This compares it against
    /// narrowing the value buffer directly (reusing the source validity bitmap
    /// and building the array in one shot) - scalar and SIMD (Vector256.Narrow).
    /// Null slots are narrowed too; the validity bitmap masks them, so their
    /// float value is don't-care (matching how the column decoders work).
    /// </summary>
    [MemoryDiagnoser]
    public class FloatConversionBenchmarks
    {
        [Params(1_000, 100_000, 1_000_000)]
        public int Rows;

        private DoubleArray _doubles = null!;

        [GlobalSetup]
        public void Setup()
        {
            var rng = new Random(7);
            var builder = new DoubleArray.Builder();
            builder.Reserve(Rows);
            for (int i = 0; i < Rows; i++)
            {
                if (i % 17 == 0)
                {
                    builder.AppendNull();
                }
                else
                {
                    builder.Append(rng.NextDouble() * 1_000_000.0);
                }
            }
            _doubles = builder.Build();
        }

        [Benchmark(Baseline = true)]
        public FloatArray Current_BuilderAppend()
        {
            int length = _doubles.Length;
            var builder = new FloatArray.Builder();
            builder.Reserve(length);
            for (int i = 0; i < length; i++)
            {
                builder.Append((float?)_doubles.GetValue(i));
            }
            return builder.Build();
        }

        [Benchmark]
        public FloatArray DirectScalarNarrow()
        {
            int length = _doubles.Length;
            ReadOnlySpan<double> src = _doubles.Values;
            byte[] buffer = new byte[length * sizeof(float)];
            Span<float> dst = MemoryMarshal.Cast<byte, float>(buffer.AsSpan());
            for (int i = 0; i < length; i++)
            {
                dst[i] = (float)src[i];
            }
            return new FloatArray(new ArrowBuffer(buffer), _doubles.NullBitmapBuffer, length, _doubles.NullCount, 0);
        }

        [Benchmark]
        public FloatArray SimdNarrow()
        {
            int length = _doubles.Length;
            ReadOnlySpan<double> src = _doubles.Values;
            byte[] buffer = new byte[length * sizeof(float)];
            Span<float> dst = MemoryMarshal.Cast<byte, float>(buffer.AsSpan());
            int i = 0;
            if (Vector256.IsHardwareAccelerated)
            {
                ref double s = ref MemoryMarshal.GetReference(src);
                ref float d = ref MemoryMarshal.GetReference(dst);
                int step = Vector256<float>.Count; // 8 floats per pass, from 8 doubles
                for (; i + step <= length; i += step)
                {
                    Vector256<double> lower = Vector256.LoadUnsafe(ref s, (nuint)i);
                    Vector256<double> upper = Vector256.LoadUnsafe(ref s, (nuint)(i + 4));
                    Vector256.Narrow(lower, upper).StoreUnsafe(ref d, (nuint)i);
                }
            }
            for (; i < length; i++)
            {
                dst[i] = (float)src[i];
            }
            return new FloatArray(new ArrowBuffer(buffer), _doubles.NullBitmapBuffer, length, _doubles.NullCount, 0);
        }
    }
}

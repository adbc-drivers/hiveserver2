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
using Apache.Arrow;
using BenchmarkDotNet.Attributes;

namespace AdbcDrivers.HiveServer2.Benchmarks
{
    /// <summary>
    /// Models the value-buffer accumulation done by TStringColumn/TBinaryColumn
    /// (the per-element await reads are unchanged and excluded). "Source"
    /// stands in for the transport's already-buffered bytes.
    ///
    /// <list type="bullet">
    /// <item><b>Current</b>: copy each element into a reused temp buffer (as the
    /// real <c>ReadExactlyAsync</c> does), then <c>ArrowBuffer.Builder&lt;byte&gt;.Append</c>
    /// it (a second copy), then <c>Build()</c>.</item>
    /// <item><b>DirectGrow</b>: copy each element straight into a self-managed
    /// byte[] that doubles on demand, then wrap an exact-length slice in an
    /// ArrowBuffer (no per-element second copy, no final copy).</item>
    /// </list>
    /// </summary>
    [MemoryDiagnoser]
    public class VarLenAccumulationBenchmarks
    {
        [Params(10_000, 100_000)]
        public int ElementCount;

        [Params(16, 256)]
        public int ElementSize;

        private byte[] _source = System.Array.Empty<byte>();
        private byte[] _tmp = System.Array.Empty<byte>();

        [GlobalSetup]
        public void Setup()
        {
            _source = new byte[ElementCount * ElementSize];
            new Random(42).NextBytes(_source);
            _tmp = new byte[65536]; // matches the driver's preAllocatedBuffer
        }

        [Benchmark(Baseline = true)]
        public ArrowBuffer Current_TmpThenAppend()
        {
            var values = new ArrowBuffer.Builder<byte>();
            int srcOffset = 0;
            for (int i = 0; i < ElementCount; i++)
            {
                // Emulates ReadExactlyAsync copying transport bytes into tmp.
                _source.AsSpan(srcOffset, ElementSize).CopyTo(_tmp);
                values.Append(_tmp.AsSpan(0, ElementSize));
                srcOffset += ElementSize;
            }
            return values.Build();
        }

        [Benchmark]
        public ArrowBuffer DirectGrow()
        {
            byte[] valueBuffer = new byte[Math.Max(64, ElementSize)];
            int valueLength = 0;
            int srcOffset = 0;
            for (int i = 0; i < ElementCount; i++)
            {
                if (valueLength + ElementSize > valueBuffer.Length)
                {
                    int newCapacity = valueBuffer.Length * 2;
                    while (newCapacity < valueLength + ElementSize)
                    {
                        newCapacity *= 2;
                    }
                    System.Array.Resize(ref valueBuffer, newCapacity);
                }
                // Emulates ReadExactlyAsync copying transport bytes straight in.
                _source.AsSpan(srcOffset, ElementSize).CopyTo(valueBuffer.AsSpan(valueLength));
                valueLength += ElementSize;
                srcOffset += ElementSize;
            }
            return new ArrowBuffer(valueBuffer.AsMemory(0, valueLength));
        }
    }
}

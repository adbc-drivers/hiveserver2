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
using AdbcDrivers.HiveServer2.Thrift;
using Apache.Arrow;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Common
{
    /// <summary>
    /// Direct coverage for <see cref="BitmapUtilities.GetValidityBitmapBuffer"/>,
    /// which inverts the wire-format "nulls" bitmap into an Arrow "validity"
    /// bitmap. The lengths deliberately straddle SIMD vector widths (16/32/64
    /// bytes) and byte boundaries to exercise the vectorized body, the scalar
    /// remainder, and the partial-last-byte mask.
    /// </summary>
    public class BitmapUtilitiesTests
    {
        [Theory]
        // Sub-byte and small lengths
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(8)]
        [InlineData(9)]
        // Around a 16-byte (128-bit) vector
        [InlineData(127)]
        [InlineData(128)]
        [InlineData(129)]
        // Around a 32-byte (256-bit) vector
        [InlineData(255)]
        [InlineData(256)]
        [InlineData(257)]
        // Around a 64-byte (512-bit) vector
        [InlineData(511)]
        [InlineData(512)]
        [InlineData(513)]
        // Larger sizes with awkward tails
        [InlineData(1000)]
        [InlineData(1023)]
        [InlineData(1024)]
        [InlineData(1025)]
        public void GetValidityBitmapBuffer_InvertsInRangeBitsAndMasksTail(int length)
        {
            int byteLength = (length + 7) / 8;
            byte[] nulls = new byte[byteLength];
            new Random(length + 1).NextBytes(nulls);

            // A well-formed wire bitmap leaves bits beyond 'length' clear.
            int remainingBits = length % 8;
            if (remainingBits > 0)
            {
                nulls[byteLength - 1] &= (byte)((1 << remainingBits) - 1);
            }

            byte[] original = (byte[])nulls.Clone();

            ArrowBuffer buffer = BitmapUtilities.GetValidityBitmapBuffer(ref nulls, length, out int nullCount);
            ReadOnlySpan<byte> validity = buffer.Span;

            // Every in-range bit is the inverse of the incoming null bit.
            for (int i = 0; i < length; i++)
            {
                Assert.Equal(!BitUtility.GetBit(original, i), BitUtility.GetBit(validity, i));
            }

            // Bits past the logical length must be masked to zero so they don't
            // inflate Arrow's null count.
            for (int i = length; i < byteLength * 8; i++)
            {
                Assert.False(BitUtility.GetBit(validity, i), $"bit {i} beyond length {length} should be 0");
            }

            // nullCount reflects the number of set ("null") bits in the input.
            Assert.Equal(BitUtility.CountBits(original), nullCount);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(8)]
        [InlineData(15)]
        [InlineData(16)]
        [InlineData(17)]
        [InlineData(31)]
        [InlineData(32)]
        [InlineData(33)]
        [InlineData(47)]
        [InlineData(63)]
        [InlineData(64)]
        [InlineData(65)]
        [InlineData(100)]
        [InlineData(256)]
        [InlineData(257)]
        [InlineData(1000)]
        public void PackBooleanValues_PacksOneBitPerValue(int length)
        {
            var rng = new Random(length + 7);
            byte[] values = new byte[length];
            for (int i = 0; i < length; i++)
            {
                values[i] = (byte)(rng.Next(2)); // 0 or 1
            }

            byte[] packed = BitmapUtilities.PackBooleanValues(values);

            Assert.Equal((length + 7) / 8, packed.Length);
            for (int i = 0; i < length; i++)
            {
                Assert.Equal(values[i] == 1, BitUtility.GetBit(packed, i));
            }
        }

        [Fact]
        public void PackBooleanValues_TreatsOnlyOneAsTrue()
        {
            // The wire format uses 1 for true; any other byte is false. Use a
            // length past the 32-byte vector width so both the SIMD and scalar
            // paths see non-1 bytes.
            byte[] values = new byte[40];
            values[0] = 1;   // true
            values[1] = 2;   // not 1 -> false
            values[5] = 255; // not 1 -> false
            values[33] = 1;  // true (scalar tail)

            byte[] packed = BitmapUtilities.PackBooleanValues(values);

            Assert.True(BitUtility.GetBit(packed, 0));
            Assert.False(BitUtility.GetBit(packed, 1));
            Assert.False(BitUtility.GetBit(packed, 5));
            Assert.True(BitUtility.GetBit(packed, 33));
        }

        [Fact]
        public void GetValidityBitmapBuffer_ExtendsShortNullsBuffer()
        {
            // Spark may send a nulls buffer shorter than the column length;
            // the missing trailing rows are treated as non-null (valid).
            const int length = 24; // 3 bytes required
            byte[] nulls = new byte[] { 0b0000_0001 }; // only row 0 is null; buffer is 1 byte

            ArrowBuffer buffer = BitmapUtilities.GetValidityBitmapBuffer(ref nulls, length, out int nullCount);
            ReadOnlySpan<byte> validity = buffer.Span;

            Assert.Equal(1, nullCount);
            Assert.False(BitUtility.GetBit(validity, 0)); // null -> invalid
            for (int i = 1; i < length; i++)
            {
                Assert.True(BitUtility.GetBit(validity, i), $"row {i} should be valid");
            }
        }
    }
}

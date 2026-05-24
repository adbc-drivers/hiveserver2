/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * This file has been modified from its original version, which is
 * under the Apache License:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
#if NET8_0_OR_GREATER
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
#endif
using Apache.Arrow;

namespace AdbcDrivers.HiveServer2.Thrift
{
    internal static class BitmapUtilities
    {
        private static readonly byte[] s_bitMasks = [0, 0b00000001, 0b00000011, 0b00000111, 0b00001111, 0b00011111, 0b00111111, 0b01111111, 0b11111111];

        /// <summary>
        /// Gets the "validity" bitmap buffer from a 'nulls' bitmap.
        /// </summary>
        /// <param name="nulls">The bitmap of rows where the value is a null value (i.e., "invalid")</param>
        /// <param name="arrayLength">The length of the array.</param>
        /// <param name="nullCount">Returns the number of bits set in the bitmap.</param>
        /// <returns>A <see cref="ArrowBuffer"/> bitmap of "valid" rows (i.e., not null values).</returns>
        /// <remarks>Inverts the bits in the incoming bitmap to reverse the null to valid indicators.</remarks>
        internal static ArrowBuffer GetValidityBitmapBuffer(ref byte[] nulls, int arrayLength, out int nullCount)
        {
            nullCount = BitUtility.CountBits(nulls);

            int fullBytes = arrayLength / 8;
            int remainingBits = arrayLength % 8;
            int requiredBytes = fullBytes + (remainingBits == 0 ? 0 : 1);
            if (nulls.Length < requiredBytes)
            {
                // Note: Spark may return a nulls bitmap buffer that is shorter than required - implying that missing bits indicate non-null.
                // However, since we need to invert the bits and return a "validity" bitmap, we need to have a full length bitmap.
                byte[] temp = new byte[requiredBytes];
                nulls.CopyTo(temp, 0);
                nulls = temp;
            }

            // Invert every fully-populated byte so "null" bits become "valid" bits.
            InvertBytes(nulls.AsSpan(0, fullBytes));
            // Handle remaining bits
            if (remainingBits > 0)
            {
                int lastByteIndex = requiredBytes - 1;
                nulls[lastByteIndex] = (byte)(s_bitMasks[remainingBits] & (byte)~nulls[lastByteIndex]);
            }
            return new ArrowBuffer(nulls);
        }

        /// <summary>
        /// Inverts every bit of each byte in <paramref name="bytes"/> in place
        /// (a bitwise NOT). On .NET 8+ this XORs SIMD-width blocks with an
        /// all-ones vector, letting the JIT pick the widest register the CPU
        /// supports; the trailing bytes (and every down-level target) fall back
        /// to the per-byte loop.
        /// </summary>
        private static void InvertBytes(Span<byte> bytes)
        {
            int i = 0;
#if NET8_0_OR_GREATER
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
#endif
            for (; i < bytes.Length; i++)
            {
                bytes[i] = (byte)~bytes[i];
            }
        }

        /// <summary>
        /// Packs a wire-format boolean column - one byte per value, where the
        /// byte 1 means true - into an Arrow (LSB-first) bit-packed bitmap, one
        /// bit per value.
        /// </summary>
        /// <remarks>
        /// On .NET 8+ this collapses 32 value-bytes into a 4-byte mask per pass
        /// using <see cref="Vector256{T}.ExtractMostSignificantBits"/> (with a
        /// 128-bit path for narrower hardware such as Arm NEON), then a scalar
        /// loop handles the remainder. The masks land directly in Arrow's
        /// LSB-first order: lane <c>k</c> of the vector becomes bit <c>k</c> of
        /// the little-endian mask, i.e. value <c>i</c> -> byte <c>i / 8</c>, bit
        /// <c>i % 8</c>. Down-level targets pack a bit at a time.
        /// </remarks>
        internal static byte[] PackBooleanValues(ReadOnlySpan<byte> values)
        {
            int length = values.Length;
            byte[] packed = new byte[(length + 7) / 8];
            int i = 0;
#if NET8_0_OR_GREATER
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
#endif
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

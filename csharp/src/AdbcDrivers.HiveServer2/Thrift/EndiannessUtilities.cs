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
using System.Runtime.CompilerServices;

namespace AdbcDrivers.HiveServer2.Thrift
{
    /// <summary>
    /// Helpers for converting the big-endian numeric payloads carried by the
    /// Thrift wire format into the host's (little-endian) layout.
    /// </summary>
    /// <remarks>
    /// Every numeric column in every result batch is byte-swapped here, so this
    /// is one of the hottest paths in the driver. On .NET 8+ we hand the whole
    /// span to <see cref="BinaryPrimitives"/>, whose span overloads are
    /// SIMD-accelerated (Vector128/256/512 byte shuffles) and process many
    /// elements per instruction. On the down-level targets (netstandard2.0,
    /// net472) those overloads don't exist, so we fall back to the scalar loop.
    ///
    /// The swap is unconditional, matching the driver's existing assumption that
    /// the host is little-endian; this preserves behavior exactly while letting
    /// the JIT pick the widest vector path available on the running CPU.
    /// </remarks>
    internal static class EndiannessUtilities
    {
        /// <summary>Reverses the byte order of each 16-bit element in place.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ReverseEndianness(Span<short> values)
        {
#if NET8_0_OR_GREATER
            BinaryPrimitives.ReverseEndianness(values, values);
#else
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = BinaryPrimitives.ReverseEndianness(values[i]);
            }
#endif
        }

        /// <summary>Reverses the byte order of each 32-bit element in place.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ReverseEndianness(Span<int> values)
        {
#if NET8_0_OR_GREATER
            BinaryPrimitives.ReverseEndianness(values, values);
#else
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = BinaryPrimitives.ReverseEndianness(values[i]);
            }
#endif
        }

        /// <summary>
        /// Reverses the byte order of each 64-bit element in place. Also used for
        /// <see cref="double"/> payloads, which are swapped as 64-bit integers.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ReverseEndianness(Span<long> values)
        {
#if NET8_0_OR_GREATER
            BinaryPrimitives.ReverseEndianness(values, values);
#else
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = BinaryPrimitives.ReverseEndianness(values[i]);
            }
#endif
        }
    }
}

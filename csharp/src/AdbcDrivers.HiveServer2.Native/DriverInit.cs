/*
 * Copyright (c) 2026 ADBC Drivers Contributors
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

using System.Runtime.InteropServices;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.C;
using Apache.Arrow.C;

namespace AdbcDrivers.HiveServer2.Native
{
    /// <summary>
    /// Native entry point exposed by this AOT-published shared library.
    ///
    /// <para>The ADBC driver manager looks up a symbol named <c>AdbcDriverInit</c>
    /// (the default; callers may override via <c>entrypoint=</c>). When called,
    /// this method delegates to <see cref="CAdbcDriverExporter.AdbcDriverInit"/>
    /// wrapped around a fresh <see cref="HiveServer2Driver"/> — the same managed
    /// driver type the in-process tests exercise, but reached here across the
    /// C ABI from a NativeAOT-compiled binary.</para>
    ///
    /// <para>One <see cref="HiveServer2Driver"/> instance is created per load; the
    /// exporter takes ownership via a GCHandle stored on the CAdbcDriver and
    /// disposes it when the driver is released.</para>
    /// </summary>
    public static unsafe class DriverInit
    {
        [UnmanagedCallersOnly(EntryPoint = "AdbcDriverInit")]
        public static AdbcStatusCode AdbcDriverInit(int version, CAdbcDriver* driver, CAdbcError* error)
        {
            // The HiveServer2 column decoders read wire bytes straight into managed
            // byte[] and wrap them as Arrow buffers (zero-copy). Apache.Arrow's C
            // data interface exporter normally only exports native (off-heap)
            // buffers; opting into managed-memory export lets it pin those managed
            // buffers for the duration of the export instead, so result columns can
            // cross the C ABI without the driver having to re-pack into native
            // memory. This is set on the export side — inside this AOT-published
            // library, which carries its own copy of Apache.Arrow.
            CArrowArrayExporter.EnableManagedMemoryExport = true;

            return CAdbcDriverExporter.AdbcDriverInit(version, driver, error, new HiveServer2Driver());
        }
    }
}

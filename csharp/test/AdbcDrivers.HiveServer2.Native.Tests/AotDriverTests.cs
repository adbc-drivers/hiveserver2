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

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.C;
using Apache.Arrow.Ipc;
using Xunit;

namespace AdbcDrivers.HiveServer2.Native.Tests
{
    /// <summary>
    /// Loads the AOT-published <c>AdbcDrivers.HiveServer2.Native</c> shared
    /// library through <see cref="CAdbcDriverImporter"/> and drives the real
    /// HiveServer2 driver against the in-process mock HiveServer2
    /// (<see cref="HiveServer2TestServer"/>) end-to-end over the C ABI. This is
    /// the AOT analogue of the managed mock-server tests: the same driver code,
    /// but reached as a NativeAOT-compiled binary through the importer rather
    /// than instantiated in-process. It acts as a canary for AOT regressions
    /// (trim-unsafe reflection, missing exports, broken C-ABI / Arrow C data
    /// interface marshaling) that a plain managed test run can't catch.
    ///
    /// <para>The path to the published library is taken from the
    /// <c>ADBC_TEST_AOT_FIXTURE_PATH</c> environment variable. When unset (or
    /// pointing at a missing file) the tests skip rather than fail, so the suite
    /// stays green in local builds that haven't run <c>dotnet publish</c>
    /// against the native project.</para>
    /// </summary>
    public class AotDriverTests
    {
        private const string FixturePathEnvVar = "ADBC_TEST_AOT_FIXTURE_PATH";

        /// <summary>
        /// Returns the path to the AOT-published native library, or skips the
        /// test when it isn't available.
        /// </summary>
        private static string RequireFixture()
        {
            string? path = Environment.GetEnvironmentVariable(FixturePathEnvVar);
            bool available = !string.IsNullOrEmpty(path) && File.Exists(path);
            Skip.IfNot(
                available,
                $"Set {FixturePathEnvVar} to the AOT-published AdbcDrivers.HiveServer2.Native shared library to run this test.");
            return path!;
        }

        /// <summary>
        /// Default HTTP + basic-auth parameters pointing at the in-process mock.
        /// Mirrors the managed mock-server tests' parameter set.
        /// </summary>
        private static Dictionary<string, string> HttpParameters(Uri serverUri) => new Dictionary<string, string>
        {
            { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Http },
            { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.Basic },
            { AdbcOptions.Username, "mock-user" },
            { AdbcOptions.Password, "mock-password" },
            { AdbcOptions.Uri, serverUri.AbsoluteUri },
        };

        [SkippableFact]
        public void DriverLoadsFromAotPublishedLibrary()
        {
            string fixturePath = RequireFixture();
            using AdbcDriver driver = CAdbcDriverImporter.Load(fixturePath);
            Assert.NotNull(driver);
        }

        [SkippableFact]
        public async Task SimpleSelectRoundTripsThroughAotDriver()
        {
            string fixturePath = RequireFixture();

            // Default stub answers any ExecuteStatement with a single BIGINT 42.
            var stub = new HiveServer2StubHandler();
            using var server = new HiveServer2TestServer(stub);

            using AdbcDriver driver = CAdbcDriverImporter.Load(fixturePath);
            using AdbcDatabase database = driver.Open(HttpParameters(server.Uri));
            using AdbcConnection connection = database.Connect(HttpParameters(server.Uri));
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 42";

            QueryResult result = statement.ExecuteQuery();
            using IArrowArrayStream stream = result.Stream!;
            Assert.NotNull(stream);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            var column = Assert.IsType<Int64Array>(batch!.Column(0));
            Assert.Equal(42L, column.GetValue(0));

            // A second read drains the stream — proves end-of-stream crosses the
            // ABI cleanly rather than looping or faulting.
            Assert.Null(await stream.ReadNextRecordBatchAsync());
        }

        [SkippableFact]
        public async Task DataTypeRoundTripThroughAotDriver()
        {
            string fixturePath = RequireFixture();

            // A mixed string/bigint result with nulls exercises the column
            // decoders and the Arrow C data interface export/import across the
            // AOT boundary, not just the control-flow of a single column.
            var stub = new HiveServer2StubHandler
            {
                OnExecuteStatement = _ => MockResult.Builder()
                    .String("city", "berlin", "tokyo", null)
                    .Bigint("pop", 3_700_000L, 13_960_000L, null)
                    .Build(),
            };
            using var server = new HiveServer2TestServer(stub);

            using AdbcDriver driver = CAdbcDriverImporter.Load(fixturePath);
            using AdbcDatabase database = driver.Open(HttpParameters(server.Uri));
            using AdbcConnection connection = database.Connect(HttpParameters(server.Uri));
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT city, pop FROM cities";

            QueryResult result = statement.ExecuteQuery();
            using IArrowArrayStream stream = result.Stream!;
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);

            var cities = Assert.IsType<StringArray>(batch!.Column(0));
            var pops = Assert.IsType<Int64Array>(batch.Column(1));
            Assert.Equal("berlin", cities.GetString(0));
            Assert.Equal(3_700_000L, pops.GetValue(0));
            Assert.True(cities.IsNull(2));
            Assert.Null(pops.GetValue(2));
        }
    }
}

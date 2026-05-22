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
using System.Collections.Generic;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.Tests.HiveServer2.MockServer
{
    /// <summary>
    /// Boilerplate-collapsing setup for a single mock-server test: holds an
    /// in-process test server (HTTP or TCP+SASL), an <see cref="AdbcConnection"/>
    /// against it, and the underlying <see cref="HiveServer2StubHandler"/>
    /// so the test can configure per-RPC behavior. Flavor-agnostic; flavor-
    /// and transport-specific entry points (e.g. <c>HiveMockServer.Create()</c>
    /// / <c>HiveMockServer.CreateStandard()</c>) live alongside each flavor's
    /// tests.
    /// </summary>
    internal sealed class MockServerScenario : IDisposable
    {
        private readonly IDisposable _server;

        public MockServerScenario(
            AdbcDriver driver,
            IDisposable server,
            IReadOnlyDictionary<string, string> connectionParameters,
            HiveServer2StubHandler stub)
        {
            Stub = stub;
            Driver = driver;
            _server = server;

            Database = Driver.Open(connectionParameters);
            Connection = Database.Connect(connectionParameters);
        }

        public HiveServer2StubHandler Stub { get; }
        public AdbcDriver Driver { get; }
        public AdbcDatabase Database { get; }
        public AdbcConnection Connection { get; }

        public AdbcStatement NewStatement() => Connection.CreateStatement();

        /// <summary>
        /// Copy an <see cref="IReadOnlyDictionary{TKey, TValue}"/> into a
        /// mutable <see cref="Dictionary{TKey, TValue}"/>. Used by per-flavor
        /// factories to lay down a fresh parameter dict they can inject
        /// transport-specific keys into without mutating the caller's input.
        /// </summary>
        /// <remarks>
        /// The <c>Dictionary(IReadOnlyDictionary)</c> copy constructor only
        /// exists from net6.0 onward; this helper works on net472 too.
        /// </remarks>
        public static Dictionary<string, string> CopyParameters(IReadOnlyDictionary<string, string> source)
        {
            var result = new Dictionary<string, string>(source.Count);
            foreach (var kvp in source) result[kvp.Key] = kvp.Value;
            return result;
        }

        public void Dispose()
        {
            // Disposal order matters: tear down the ADBC stack first (which
            // closes the open session against the server) before stopping the
            // server itself. Exceptions are allowed to propagate so a teardown
            // bug surfaces as a real test failure instead of a silent leak;
            // the in-process listener is bound to an ephemeral loopback port,
            // so the OS reclaims it when the test process exits anyway.
            Connection.Dispose();
            Database.Dispose();
            Driver.Dispose();
            _server.Dispose();
        }
    }
}

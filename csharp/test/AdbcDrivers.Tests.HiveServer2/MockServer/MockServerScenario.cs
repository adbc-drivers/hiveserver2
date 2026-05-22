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
    /// Boilerplate-collapsing setup for a single mock-server test: spins up
    /// the in-process HiveServer2 mock, builds an <see cref="AdbcConnection"/>
    /// against it using the supplied driver + parameters, and exposes the
    /// underlying <see cref="HiveServer2StubHandler"/> so the test can
    /// configure per-RPC behavior. Flavor-agnostic; flavor-specific entry
    /// points (e.g. <c>HiveMockServer.Create()</c>) live alongside each
    /// flavor's tests.
    /// </summary>
    internal sealed class MockServerScenario : IDisposable
    {
        private readonly HiveServer2TestServer _server;

        public MockServerScenario(
            AdbcDriver driver,
            IReadOnlyDictionary<string, string> connectionParameters,
            HiveServer2StubHandler stub)
        {
            Stub = stub;
            Driver = driver;
            _server = new HiveServer2TestServer(stub);

            // Inject the mock server's URI into a fresh dict so the caller's
            // parameter map stays untouched. (Constructed from an IEnumerable
            // rather than a dictionary copy ctor — the IReadOnlyDictionary
            // overload doesn't exist on net472.)
            var parameters = new Dictionary<string, string>();
            foreach (var kvp in connectionParameters) parameters[kvp.Key] = kvp.Value;
            parameters[AdbcOptions.Uri] = _server.Uri.AbsoluteUri;

            Database = Driver.Open(parameters);
            Connection = Database.Connect(parameters);
        }

        public HiveServer2StubHandler Stub { get; }
        public AdbcDriver Driver { get; }
        public AdbcDatabase Database { get; }
        public AdbcConnection Connection { get; }

        public AdbcStatement NewStatement() => Connection.CreateStatement();

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

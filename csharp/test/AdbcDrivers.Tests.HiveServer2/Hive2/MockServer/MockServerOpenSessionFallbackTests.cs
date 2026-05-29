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

using System.Collections.Generic;
using System.Net;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Drives <c>HiveServer2Connection.TryOpenSessionWithFallbackAsync</c>
    /// + <c>ResetConnection</c> by pointing the driver at a test server
    /// configured to return non-200 HTTP status codes during OpenSession.
    /// The driver-side <c>THttpTransport</c> wraps each non-OK response as a
    /// <c>TTransportException</c>; the fallback path catches it, rebuilds
    /// the transport, and retries on the next FallbackProtocolVersion.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerOpenSessionFallbackTests
    {
        private static Dictionary<string, string> HttpParams(HiveServer2TestServer server) => new()
        {
            { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Http },
            { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.Basic },
            { AdbcOptions.Username, "mock-user" },
            { AdbcOptions.Password, "mock-password" },
            { AdbcOptions.Uri, server.Uri.AbsoluteUri },
        };

        [Fact]
        public void Unauthorized_FirstResponse_ThrowsAdbcUnauthorized()
        {
            // 401 on the first call → ApacheUtility.ContainsException finds the
            // HttpRequestException inner and IsUnauthorized returns true, so
            // the catch turns it into a HiveServer2Exception with status
            // AdbcStatusCode.Unauthorized.
            var stub = new HiveServer2StubHandler();
            using var server = new HiveServer2TestServer(stub)
            {
                StatusCodeOverride = _ => HttpStatusCode.Unauthorized,
            };
            using var driver = new HiveServer2Driver();
            using var database = driver.Open(HttpParams(server));
            HiveServer2Exception ex = Assert.Throws<HiveServer2Exception>(
                () => database.Connect(HttpParams(server)));
            Assert.Equal(AdbcStatusCode.Unauthorized, ex.Status);
        }

        [Fact]
        public void AllAttemptsFail_ThrowsAfterExhaustingFallbacks()
        {
            // 500 on every call (not Unauthorized) → loop walks through every
            // FallbackProtocolVersion, lastException is set each iteration,
            // and the final throw re-raises it. We don't pin the inner type
            // since it depends on Thrift transport plumbing; only that the
            // session never opened.
            var stub = new HiveServer2StubHandler();
            using var server = new HiveServer2TestServer(stub)
            {
                StatusCodeOverride = _ => HttpStatusCode.InternalServerError,
            };
            using var driver = new HiveServer2Driver();
            using var database = driver.Open(HttpParams(server));
            Assert.ThrowsAny<System.Exception>(() => database.Connect(HttpParams(server)));
        }

        [Fact]
        public void FirstAttemptFails_FallbackSucceeds_ConnectsCleanly()
        {
            // 500 only on call #1; subsequent calls dispatch normally and the
            // session opens on the next FallbackProtocolVersion iteration.
            var stub = new HiveServer2StubHandler();
            using var server = new HiveServer2TestServer(stub)
            {
                StatusCodeOverride = count => count == 1 ? HttpStatusCode.InternalServerError : HttpStatusCode.OK,
            };
            using var driver = new HiveServer2Driver();
            using var database = driver.Open(HttpParams(server));
            using AdbcConnection connection = database.Connect(HttpParams(server));
            // Sanity: the recovered connection is usable.
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1";
            using var result = statement.ExecuteQuery().Stream!;
        }
    }
}

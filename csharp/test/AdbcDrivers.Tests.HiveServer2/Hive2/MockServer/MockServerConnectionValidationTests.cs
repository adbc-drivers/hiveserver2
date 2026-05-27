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
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Drives the parameter-validation paths on HiveServer2HttpConnection
    /// and HiveServer2StandardConnection — ValidateConnection,
    /// ValidateAuthentication, ValidateOptions — by handing the driver bad
    /// (or atypical) parameter bags and asserting it rejects them. The
    /// validation runs inside the Connection constructor, which fires from
    /// database.Connect().
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerConnectionValidationTests
    {
        private static void OpenAndConnect(AdbcDriver driver, IReadOnlyDictionary<string, string> props)
        {
            using var database = driver.Open(props);
            using var connection = database.Connect(props);
        }

        private static Dictionary<string, string> HttpParams(string authType, string? username = null, string? password = null)
        {
            var d = new Dictionary<string, string>
            {
                { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Http },
                { HiveServer2Parameters.AuthType, authType },
                { AdbcOptions.Uri, "http://127.0.0.1:1/" },
            };
            if (username != null) d[AdbcOptions.Username] = username;
            if (password != null) d[AdbcOptions.Password] = password;
            return d;
        }

        [Fact]
        public void Http_UnknownAuthType_Throws()
        {
            using var driver = new HiveServer2Driver();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => OpenAndConnect(driver, HttpParams("nonsense")));
        }

        [Fact]
        public void Http_Basic_MissingPassword_Throws()
        {
            using var driver = new HiveServer2Driver();
            Assert.Throws<ArgumentException>(
                () => OpenAndConnect(driver, HttpParams(HiveServer2AuthTypeConstants.Basic, username: "u")));
        }

        [Fact]
        public void Http_UsernameOnly_MissingUsername_Throws()
        {
            using var driver = new HiveServer2Driver();
            Assert.Throws<ArgumentException>(
                () => OpenAndConnect(driver, HttpParams(HiveServer2AuthTypeConstants.UsernameOnly)));
        }

        [Fact]
        public void Http_MissingHostAndUri_Throws()
        {
            using var driver = new HiveServer2Driver();
            var props = new Dictionary<string, string>
            {
                { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Http },
                { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.None },
            };
            Assert.Throws<ArgumentException>(() => OpenAndConnect(driver, props));
        }

        [Fact]
        public void Http_NegativeConnectTimeout_Throws()
        {
            using var driver = new HiveServer2Driver();
            var props = HttpParams(HiveServer2AuthTypeConstants.None);
            props[HiveServer2Parameters.ConnectTimeoutMilliseconds] = "-5";
            Assert.Throws<ArgumentOutOfRangeException>(() => OpenAndConnect(driver, props));
        }

        [Fact]
        public void Http_NonNumericConnectTimeout_Throws()
        {
            using var driver = new HiveServer2Driver();
            var props = HttpParams(HiveServer2AuthTypeConstants.None);
            props[HiveServer2Parameters.ConnectTimeoutMilliseconds] = "soon";
            Assert.Throws<ArgumentOutOfRangeException>(() => OpenAndConnect(driver, props));
        }

        private static Dictionary<string, string> StandardParams(string authType, string? username = null, string? password = null)
        {
            var d = new Dictionary<string, string>
            {
                { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Standard },
                { HiveServer2Parameters.AuthType, authType },
                { HiveServer2Parameters.HostName, "127.0.0.1" },
                { HiveServer2Parameters.Port, "65535" },
                { StandardTlsOptions.IsTlsEnabled, "false" },
            };
            if (username != null) d[AdbcOptions.Username] = username;
            if (password != null) d[AdbcOptions.Password] = password;
            return d;
        }

        [Fact]
        public void Standard_UnknownAuthType_Throws()
        {
            using var driver = new HiveServer2Driver();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => OpenAndConnect(driver, StandardParams("nonsense")));
        }

        [Fact]
        public void Standard_Basic_MissingPassword_Throws()
        {
            using var driver = new HiveServer2Driver();
            Assert.Throws<ArgumentException>(
                () => OpenAndConnect(driver, StandardParams(HiveServer2AuthTypeConstants.Basic, username: "u")));
        }

        [Fact]
        public void Standard_MissingHost_Throws()
        {
            using var driver = new HiveServer2Driver();
            var props = new Dictionary<string, string>
            {
                { HiveServer2Parameters.TransportType, HiveServer2TransportTypeConstants.Standard },
                { HiveServer2Parameters.AuthType, HiveServer2AuthTypeConstants.None },
                { HiveServer2Parameters.Port, "10000" },
            };
            Assert.Throws<ArgumentException>(() => OpenAndConnect(driver, props));
        }
    }
}

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
using AdbcDrivers.HiveServer2.Impala;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Impala.MockServer
{
    /// <summary>
    /// Drives ValidateConnection / ValidateAuthentication / ValidateOptions
    /// branches on ImpalaHttpConnection + ImpalaStandardConnection.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class ImpalaMockServerValidationTests
    {
        private static void OpenAndConnect(AdbcDriver driver, IReadOnlyDictionary<string, string> props)
        {
            using var database = driver.Open(props);
            using var connection = database.Connect(props);
        }

        private static Dictionary<string, string> HttpParams(string authType)
        {
            return new Dictionary<string, string>
            {
                { ImpalaParameters.Type, ImpalaServerTypeConstants.Http },
                { ImpalaParameters.AuthType, authType },
                { AdbcOptions.Uri, "http://127.0.0.1:1/" },
            };
        }

        [Fact]
        public void Http_UnknownAuthType_Throws()
        {
            using var driver = new ImpalaDriver();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => OpenAndConnect(driver, HttpParams("nonsense")));
        }

        [Fact]
        public void Http_Basic_MissingPassword_Throws()
        {
            using var driver = new ImpalaDriver();
            var props = HttpParams(ImpalaAuthTypeConstants.Basic);
            props[AdbcOptions.Username] = "u";
            Assert.Throws<ArgumentException>(() => OpenAndConnect(driver, props));
        }

        [Fact]
        public void Http_UsernameOnly_MissingUsername_Throws()
        {
            using var driver = new ImpalaDriver();
            Assert.Throws<ArgumentException>(
                () => OpenAndConnect(driver, HttpParams(ImpalaAuthTypeConstants.UsernameOnly)));
        }

        [Fact]
        public void Standard_UnknownAuthType_Throws()
        {
            using var driver = new ImpalaDriver();
            var props = new Dictionary<string, string>
            {
                { ImpalaParameters.Type, ImpalaServerTypeConstants.Standard },
                { ImpalaParameters.AuthType, "nonsense" },
                { ImpalaParameters.HostName, "127.0.0.1" },
                { ImpalaParameters.Port, "65535" },
                { StandardTlsOptions.IsTlsEnabled, "false" },
            };
            Assert.Throws<ArgumentOutOfRangeException>(() => OpenAndConnect(driver, props));
        }

        [Fact]
        public void Standard_Basic_MissingPassword_Throws()
        {
            using var driver = new ImpalaDriver();
            var props = new Dictionary<string, string>
            {
                { ImpalaParameters.Type, ImpalaServerTypeConstants.Standard },
                { ImpalaParameters.AuthType, ImpalaAuthTypeConstants.Basic },
                { ImpalaParameters.HostName, "127.0.0.1" },
                { ImpalaParameters.Port, "65535" },
                { StandardTlsOptions.IsTlsEnabled, "false" },
                { AdbcOptions.Username, "u" },
            };
            Assert.Throws<ArgumentException>(() => OpenAndConnect(driver, props));
        }
    }
}

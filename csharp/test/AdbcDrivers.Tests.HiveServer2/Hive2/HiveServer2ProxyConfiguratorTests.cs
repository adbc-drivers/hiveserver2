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
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using AdbcDrivers.HiveServer2.Hive2;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2
{
    public class HiveServer2ProxyConfiguratorTests
    {
        [Fact]
        public void ConfigureProxy_NoProxySettings_DisablesProxy()
        {
            // Arrange
            var properties = new Dictionary<string, string>();
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            // Act
            configurator.ConfigureProxy(handler);

            // Assert
            Assert.False(handler.UseProxy);
        }

        [Fact]
        public void ConfigureProxy_UseProxyWithInvalidPort_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "true" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "99999" } // Invalid port
            };

            // Act & Assert
            var ex = Assert.Throws<ArgumentOutOfRangeException>(() => HiveServer2ProxyConfigurator.FromProperties(properties));
            Assert.Equal(HttpProxyOptions.ProxyPort, ex.ParamName);
        }

        [Fact]
        public void ConfigureProxy_ValidProxySettings_ConfiguresProxy()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "true" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "8080" }
            };
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            // Act
            configurator.ConfigureProxy(handler);

            // Assert
            Assert.True(handler.UseProxy);
            Assert.NotNull(handler.Proxy);
            Assert.IsType<WebProxy>(handler.Proxy);
        }

        [Fact]
        public void ConfigureProxy_ProxyWithAuthentication_ConfiguresProxyCredentials()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "true" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "8080" },
                { HttpProxyOptions.ProxyAuth, "true" },
                { HttpProxyOptions.ProxyUID, "username" },
                { HttpProxyOptions.ProxyPWD, "password" }
            };
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            // Act
            configurator.ConfigureProxy(handler);

            // Assert
            Assert.True(handler.UseProxy);
            Assert.NotNull(handler.Proxy);
            Assert.IsType<WebProxy>(handler.Proxy);

            var proxy = (WebProxy)handler.Proxy;
            var credentials = proxy.Credentials;
            Assert.NotNull(credentials);
            Assert.IsType<NetworkCredential>(credentials);

            var networkCredential = (NetworkCredential)credentials;
            Assert.Equal("username", networkCredential.UserName);
            Assert.Equal("password", networkCredential.Password);
        }

        [Fact]
        public void ConfigureProxy_ProxyWithBypassList_PopulatesBypassList()
        {
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "true" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "8080" },
                { HttpProxyOptions.ProxyIgnoreList, "localhost,127.0.0.1,*.internal.domain.com" }
            };
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            configurator.ConfigureProxy(handler);

            var proxy = Assert.IsType<WebProxy>(handler.Proxy);
            Assert.NotNull(proxy.BypassList);
            Assert.NotEmpty(proxy.BypassList);
        }

        [Theory]
        // Wildcard at start — both schemes and any port should bypass.
        [InlineData("*.databricks.com", "https://x.cloud.databricks.com/sql", true)]
        [InlineData("*.databricks.com", "http://x.cloud.databricks.com/sql", true)]
        [InlineData("*.databricks.com", "https://x.cloud.databricks.com:8443/sql", true)]
        [InlineData("*.databricks.com", "https://x.example.com", false)]
        // Wildcard in the middle (literal prefix) — used to fail before the fix.
        [InlineData("te-*.cloud.databricks.com", "https://te-eda-qa.cloud.databricks.com", true)]
        [InlineData("te-*.cloud.databricks.com", "https://te-eda-qa.cloud.databricks.com:8443", true)]
        [InlineData("te-*.cloud.databricks.com", "https://other-eda-qa.cloud.databricks.com", false)]
        // Bare hostname (no wildcard) — used to fail before the fix.
        [InlineData("host.example.com", "https://host.example.com", true)]
        [InlineData("host.example.com", "https://host.example.com:9443", true)]
        [InlineData("host.example.com", "https://other.example.com", false)]
        public void ConfigureProxy_BypassList_IsBypassed_MatchesExpected(string pattern, string url, bool expectedBypass)
        {
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "true" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "8080" },
                { HttpProxyOptions.ProxyIgnoreList, pattern }
            };
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            configurator.ConfigureProxy(handler);

            var proxy = Assert.IsType<WebProxy>(handler.Proxy);
            Assert.Equal(expectedBypass, proxy.IsBypassed(new Uri(url)));
        }

        [Fact]
        public void ConfigureProxy_BypassList_LoopbackStillBypassed()
        {
            // localhost / 127.0.0.1 bypass via Uri.IsLoopback regardless of pattern, but the
            // pattern should not produce a regex that breaks WebProxy when set.
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "true" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "8080" },
                { HttpProxyOptions.ProxyIgnoreList, "localhost,127.0.0.1" }
            };
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            configurator.ConfigureProxy(handler);

            var proxy = Assert.IsType<WebProxy>(handler.Proxy);
            Assert.True(proxy.IsBypassed(new Uri("https://localhost/sql")));
            Assert.True(proxy.IsBypassed(new Uri("https://127.0.0.1/sql")));
        }

        [Fact]
        public void ConfigureProxy_BypassList_TolerantOfEmptyEntries()
        {
            // Trailing / repeated commas should not produce malformed bypass entries.
            var properties = new Dictionary<string, string>
            {
                { HttpProxyOptions.UseProxy, "true" },
                { HttpProxyOptions.ProxyHost, "proxy.example.com" },
                { HttpProxyOptions.ProxyPort, "8080" },
                { HttpProxyOptions.ProxyIgnoreList, ",*.databricks.com,, ,host.example.com," }
            };
            var configurator = HiveServer2ProxyConfigurator.FromProperties(properties);
            var handler = new HttpClientHandler();

            configurator.ConfigureProxy(handler);

            var proxy = Assert.IsType<WebProxy>(handler.Proxy);
            Assert.True(proxy.IsBypassed(new Uri("https://x.databricks.com")));
            Assert.True(proxy.IsBypassed(new Uri("https://host.example.com:9443")));
            Assert.False(proxy.IsBypassed(new Uri("https://other.example.com")));
        }
    }
}

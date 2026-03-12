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
using System.Reflection;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.Spark;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2
{
    /// <summary>
    /// Unit tests for HiveServer2Connection.GetBaseAddress, focusing on correct
    /// handling of HTTP paths that contain query parameters (e.g. ?o=orgId).
    /// </summary>
    public class HiveServer2ConnectionGetBaseAddressTest
    {
        /// <summary>
        /// Verifies that a path containing a query parameter (e.g. Databricks warehouse
        /// paths with ?o=orgId) is not percent-encoded. The '?' must remain a literal
        /// query separator and not become %3F in the resulting URI.
        /// </summary>
        [Fact]
        public void GetBaseAddress_PathWithQueryParam_DoesNotPercentEncodeQuestionMark()
        {
            Uri result = InvokeGetBaseAddress(
                uri: null,
                hostName: "test.server.com",
                path: "/sql/1.0/warehouses/abc123?o=999888777",
                port: "443",
                isTlsEnabled: true);

            Assert.DoesNotContain("%3F", result.ToString());
            Assert.Equal("/sql/1.0/warehouses/abc123", result.AbsolutePath);
            Assert.Equal("o=999888777", result.Query.TrimStart('?'));
            Assert.Equal("https://test.server.com/sql/1.0/warehouses/abc123?o=999888777", result.ToString());
        }

        /// <summary>
        /// Verifies that a plain path without a query parameter is unchanged.
        /// </summary>
        [Fact]
        public void GetBaseAddress_PathWithoutQueryParam_ReturnsExpectedUri()
        {
            Uri result = InvokeGetBaseAddress(
                uri: null,
                hostName: "test.server.com",
                path: "/sql/1.0/warehouses/abc123",
                port: "443",
                isTlsEnabled: true);

            Assert.Equal("/sql/1.0/warehouses/abc123", result.AbsolutePath);
            Assert.Equal(string.Empty, result.Query);
            Assert.Equal("https://test.server.com/sql/1.0/warehouses/abc123", result.ToString());
        }

        /// <summary>
        /// Verifies that when a full URI is provided it is returned as-is, bypassing
        /// the UriBuilder path entirely.
        /// </summary>
        [Fact]
        public void GetBaseAddress_FullUri_ReturnsUriDirectly()
        {
            string fullUri = "https://test.server.com/sql/1.0/warehouses/abc123";

            Uri result = InvokeGetBaseAddress(
                uri: fullUri,
                hostName: null,
                path: null,
                port: null,
                isTlsEnabled: true);

            Assert.Equal(new Uri(fullUri), result);
        }

        private static Uri InvokeGetBaseAddress(string? uri, string? hostName, string? path, string? port, bool isTlsEnabled)
        {
            // Reflect directly on the declaring type to avoid instantiating a connection
            // (which would trigger auth validation) and to correctly find the protected
            // static method without needing FlattenHierarchy.
            MethodInfo? method = typeof(HiveServer2Connection).GetMethod(
                "GetBaseAddress",
                BindingFlags.NonPublic | BindingFlags.Static,
                null,
                new[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(bool) },
                null);

            if (method == null)
                throw new InvalidOperationException("GetBaseAddress method not found");

            return (Uri)method.Invoke(null, new object?[] { uri, hostName, path, port, SparkParameters.HostName, isTlsEnabled })!;
        }
    }
}

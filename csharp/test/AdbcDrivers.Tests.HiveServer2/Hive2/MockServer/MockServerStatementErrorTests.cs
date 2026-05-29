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
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.TestServer;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Drives the <c>catch (Exception ex) when (ex is not HiveServer2Exception)</c>
    /// wrapping branches in HiveServer2Statement's execute and metadata
    /// methods by making the corresponding RPC throw. Each test asserts
    /// the specific <see cref="HiveServer2Exception"/> wrapper — if a
    /// future change ever lets the raw inner exception escape, that
    /// catch arm would regress silently against <c>ThrowsAny</c>.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerStatementErrorTests
    {
        [Fact]
        public async Task ExecuteQueryAsync_RpcThrows_WrapsAsHiveServer2Exception()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ => throw new InvalidOperationException("boom");
            using var statement = scenario.NewStatement();
            statement.SqlQuery = "SELECT 1";
            await Assert.ThrowsAsync<HiveServer2Exception>(() => statement.ExecuteQueryAsync().AsTask());
        }

        [Fact]
        public async Task ExecuteUpdateAsync_RpcThrows_WrapsAsHiveServer2Exception()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnExecuteStatement = _ => throw new InvalidOperationException("boom");
            using var statement = scenario.NewStatement();
            statement.SqlQuery = "DELETE FROM t";
            await Assert.ThrowsAsync<HiveServer2Exception>(() => statement.ExecuteUpdateAsync());
        }

        [Fact]
        public async Task GetCatalogs_RpcThrows_WrapsAsHiveServer2Exception()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCatalogs = _ => throw new InvalidOperationException("boom");
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "getcatalogs";
            await Assert.ThrowsAsync<HiveServer2Exception>(() => statement.ExecuteQueryAsync().AsTask());
        }

        [Fact]
        public async Task GetSchemas_RpcThrows_WrapsAsHiveServer2Exception()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetSchemas = _ => throw new InvalidOperationException("boom");
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "getschemas";
            await Assert.ThrowsAsync<HiveServer2Exception>(() => statement.ExecuteQueryAsync().AsTask());
        }

        [Fact]
        public async Task GetTables_RpcThrows_WrapsAsHiveServer2Exception()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetTables = _ => throw new InvalidOperationException("boom");
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "gettables";
            await Assert.ThrowsAsync<HiveServer2Exception>(() => statement.ExecuteQueryAsync().AsTask());
        }

        [Fact]
        public async Task GetColumns_RpcThrows_WrapsAsHiveServer2Exception()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetColumns = _ => throw new InvalidOperationException("boom");
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SqlQuery = "getcolumns";
            await Assert.ThrowsAsync<HiveServer2Exception>(() => statement.ExecuteQueryAsync().AsTask());
        }

        [Fact]
        public async Task GetPrimaryKeys_RpcThrows_WrapsAsHiveServer2Exception()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetPrimaryKeys = _ => throw new InvalidOperationException("boom");
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, "main");
            statement.SetOption(ApacheParameters.SchemaName, "public");
            statement.SetOption(ApacheParameters.TableName, "t");
            statement.SqlQuery = "getprimarykeys";
            await Assert.ThrowsAsync<HiveServer2Exception>(() => statement.ExecuteQueryAsync().AsTask());
        }

        [Fact]
        public async Task GetCrossReference_RpcThrows_WrapsAsHiveServer2Exception()
        {
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCrossReference = _ => throw new InvalidOperationException("boom");
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            statement.SetOption(ApacheParameters.CatalogName, "main");
            statement.SetOption(ApacheParameters.SchemaName, "public");
            statement.SetOption(ApacheParameters.TableName, "t");
            statement.SqlQuery = "getcrossreference";
            await Assert.ThrowsAsync<HiveServer2Exception>(() => statement.ExecuteQueryAsync().AsTask());
        }

        [Fact]
        public void Connection_GetTableSchema_RpcThrows_WrapsAsHiveServer2Exception()
        {
            // Connection.GetTableSchema has its own catch wrapper around
            // Client.GetColumns — separate from the Statement path.
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetColumns = _ => throw new InvalidOperationException("boom");
            Assert.Throws<HiveServer2Exception>(() => scenario.Connection.GetTableSchema("main", "public", "t"));
        }
    }
}

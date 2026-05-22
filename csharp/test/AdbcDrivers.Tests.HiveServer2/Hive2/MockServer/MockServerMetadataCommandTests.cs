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
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2;
using AdbcDrivers.HiveServer2.TestServer;
using AdbcDrivers.Tests.HiveServer2.MockServer;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Drives the metadata-command routing inside
    /// <c>HiveServer2Statement.ExecuteMetadataCommandQuery</c>: each
    /// well-known SqlQuery string maps to a specific Get*Async helper, which
    /// in turn issues the matching Get* RPC. We assert both the routing
    /// (correct RPC fires on the wire) and the error branches (null/empty,
    /// unknown command).
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerMetadataCommandTests
    {
        private static AdbcStatement MetadataStatement(MockServerScenario scenario)
        {
            var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, "true");
            return statement;
        }

        [Fact]
        public async Task GetCatalogs_RoutesToGetCatalogsRpc()
        {
            using var scenario = HiveMockServer.Create();
            bool called = false;
            scenario.Stub.OnGetCatalogs = _ => { called = true; return MockResult.Empty(MockSchema.GetCatalogsSchema); };

            using var statement = MetadataStatement(scenario);
            statement.SqlQuery = "getcatalogs";
            QueryResult result = await statement.ExecuteQueryAsync();
            result.Stream?.Dispose();
            Assert.True(called);
        }

        [Fact]
        public async Task GetSchemas_RoutesToGetSchemasRpc()
        {
            using var scenario = HiveMockServer.Create();
            bool called = false;
            scenario.Stub.OnGetSchemas = _ => { called = true; return MockResult.Empty(MockSchema.GetSchemasSchema); };

            using var statement = MetadataStatement(scenario);
            statement.SqlQuery = "getschemas";
            QueryResult result = await statement.ExecuteQueryAsync();
            result.Stream?.Dispose();
            Assert.True(called);
        }

        [Fact]
        public async Task GetTables_RoutesToGetTablesRpc()
        {
            using var scenario = HiveMockServer.Create();
            bool called = false;
            scenario.Stub.OnGetTables = _ => { called = true; return MockResult.Empty(MockSchema.GetTablesSchema); };

            using var statement = MetadataStatement(scenario);
            statement.SqlQuery = "gettables";
            QueryResult result = await statement.ExecuteQueryAsync();
            result.Stream?.Dispose();
            Assert.True(called);
        }

        [Fact]
        public async Task GetColumns_RoutesToGetColumnsRpc()
        {
            using var scenario = HiveMockServer.Create();
            bool called = false;
            scenario.Stub.OnGetColumns = _ => { called = true; return MockResult.Empty(MockSchema.GetColumnsSchema); };

            using var statement = MetadataStatement(scenario);
            statement.SqlQuery = "getcolumns";
            QueryResult result = await statement.ExecuteQueryAsync();
            result.Stream?.Dispose();
            Assert.True(called);
        }

        [Fact]
        public async Task GetPrimaryKeys_RoutesAndForwardsTableIdentifiers()
        {
            string? catalog = null, schema = null, table = null;
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetPrimaryKeys = req =>
            {
                catalog = req.CatalogName;
                schema = req.SchemaName;
                table = req.TableName;
                return MockResult.Empty(MockSchema.GetPrimaryKeysSchema);
            };

            using var statement = MetadataStatement(scenario);
            statement.SetOption(ApacheParameters.CatalogName, "main");
            statement.SetOption(ApacheParameters.SchemaName, "public");
            statement.SetOption(ApacheParameters.TableName, "events");
            statement.SqlQuery = "getprimarykeys";
            QueryResult result = await statement.ExecuteQueryAsync();
            result.Stream?.Dispose();

            Assert.Equal("main", catalog);
            Assert.Equal("public", schema);
            Assert.Equal("events", table);
        }

        [Fact]
        public async Task GetCrossReference_RoutesAndForwardsBothSides()
        {
            string? pkCat = null, pkSchema = null, pkTable = null;
            string? fkCat = null, fkSchema = null, fkTable = null;
            using var scenario = HiveMockServer.Create();
            scenario.Stub.OnGetCrossReference = req =>
            {
                pkCat = req.ParentCatalogName;
                pkSchema = req.ParentSchemaName;
                pkTable = req.ParentTableName;
                fkCat = req.ForeignCatalogName;
                fkSchema = req.ForeignSchemaName;
                fkTable = req.ForeignTableName;
                return MockResult.Empty(MockSchema.GetCrossReferenceSchema);
            };

            using var statement = MetadataStatement(scenario);
            statement.SetOption(ApacheParameters.CatalogName, "main");
            statement.SetOption(ApacheParameters.SchemaName, "public");
            statement.SetOption(ApacheParameters.TableName, "users");
            statement.SetOption(ApacheParameters.ForeignCatalogName, "main");
            statement.SetOption(ApacheParameters.ForeignSchemaName, "public");
            statement.SetOption(ApacheParameters.ForeignTableName, "orders");
            statement.SqlQuery = "getcrossreference";
            QueryResult result = await statement.ExecuteQueryAsync();
            result.Stream?.Dispose();

            Assert.Equal("main", pkCat);
            Assert.Equal("public", pkSchema);
            Assert.Equal("users", pkTable);
            Assert.Equal("main", fkCat);
            Assert.Equal("public", fkSchema);
            Assert.Equal("orders", fkTable);
        }

        [Fact]
        public async Task GetColumnsExtended_Routes()
        {
            using var scenario = HiveMockServer.Create();
            bool columnsCalled = false;
            scenario.Stub.OnGetColumns = _ => { columnsCalled = true; return MockResult.Empty(MockSchema.GetColumnsSchema); };
            scenario.Stub.OnGetPrimaryKeys = _ => MockResult.Empty(MockSchema.GetPrimaryKeysSchema);
            scenario.Stub.OnGetCrossReference = _ => MockResult.Empty(MockSchema.GetCrossReferenceSchema);

            using var statement = MetadataStatement(scenario);
            statement.SetOption(ApacheParameters.CatalogName, "main");
            statement.SetOption(ApacheParameters.SchemaName, "public");
            statement.SetOption(ApacheParameters.TableName, "events");
            statement.SqlQuery = "getcolumnsextended";
            QueryResult result = await statement.ExecuteQueryAsync();
            result.Stream?.Dispose();

            // GetColumnsExtended composes GetColumns + GetPrimaryKeys +
            // GetCrossReference. We only assert GetColumns fired because the
            // other two are optional and the extended-merge logic short-circuits
            // when there's no PK/FK data — that's a separate test surface.
            Assert.True(columnsCalled);
        }

        [Theory]
        [InlineData("GETCATALOGS")]
        [InlineData("GetCatalogs")]
        [InlineData("gEtCaTaLoGs")]
        public async Task MetadataCommandLookup_IsCaseInsensitive(string command)
        {
            using var scenario = HiveMockServer.Create();
            bool called = false;
            scenario.Stub.OnGetCatalogs = _ => { called = true; return MockResult.Empty(MockSchema.GetCatalogsSchema); };

            using var statement = MetadataStatement(scenario);
            statement.SqlQuery = command;
            QueryResult result = await statement.ExecuteQueryAsync();
            result.Stream?.Dispose();
            Assert.True(called);
        }

        [Fact]
        public async Task EmptySqlQuery_OnMetadataCommand_Throws()
        {
            using var scenario = HiveMockServer.Create();
            using var statement = MetadataStatement(scenario);
            statement.SqlQuery = "";
            // Wrapped by ExecuteQueryAsync's generic catch — the underlying
            // ArgumentNullException is what ExecuteMetadataCommandQuery throws.
            await Assert.ThrowsAnyAsync<Exception>(() => statement.ExecuteQueryAsync().AsTask());
        }

        [Fact]
        public async Task UnknownMetadataCommand_Throws()
        {
            using var scenario = HiveMockServer.Create();
            using var statement = MetadataStatement(scenario);
            statement.SqlQuery = "getmagicstuff";
            await Assert.ThrowsAnyAsync<Exception>(() => statement.ExecuteQueryAsync().AsTask());
        }
    }
}

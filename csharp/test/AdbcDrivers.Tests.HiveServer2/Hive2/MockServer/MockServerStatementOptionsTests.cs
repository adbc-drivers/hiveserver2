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
using AdbcDrivers.HiveServer2;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Hive2.MockServer
{
    /// <summary>
    /// Drives the per-key branches of <c>HiveServer2Statement.SetOption</c>.
    /// Hits each <c>case</c> of the switch plus its <c>default</c>; covers the
    /// validation helpers (<c>UpdatePollTimeIfValid</c>,
    /// <c>UpdateBatchSizeIfValid</c>, <c>ApacheUtility.BooleanIsValid</c>,
    /// <c>ApacheUtility.QueryTimeoutIsValid</c>) on both happy and failure paths.
    /// </summary>
    [Trait("Category", "MockServer")]
    public class MockServerStatementOptionsTests
    {
        [Theory]
        [InlineData("0")]
        [InlineData("1000")]
        [InlineData("2147483647")]
        public void PollTimeMilliseconds_ValidValues_Accepted(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.PollTimeMilliseconds, value);
        }

        [Theory]
        [InlineData("-1")]
        [InlineData("not-a-number")]
        [InlineData("")]
        [InlineData("2147483648")]
        public void PollTimeMilliseconds_InvalidValues_Throw(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => statement.SetOption(ApacheParameters.PollTimeMilliseconds, value));
        }

        [Theory]
        [InlineData("1")]
        [InlineData("100")]
        [InlineData("9223372036854775807")]
        public void BatchSize_ValidValues_Accepted(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.BatchSize, value);
        }

        [Theory]
        [InlineData("0")]
        [InlineData("-1")]
        [InlineData("not-a-number")]
        [InlineData("")]
        public void BatchSize_InvalidValues_Throw(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => statement.SetOption(ApacheParameters.BatchSize, value));
        }

        [Theory]
        [InlineData("true")]
        [InlineData("false")]
        [InlineData("TRUE")]
        [InlineData("False")]
        public void BatchSizeStopCondition_BooleanValues_Accepted(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.BatchSizeStopCondition, value);
        }

        [Theory]
        [InlineData("not-a-bool")]
        [InlineData("yes")]
        public void BatchSizeStopCondition_InvalidValues_Throw(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => statement.SetOption(ApacheParameters.BatchSizeStopCondition, value));
        }

        [Theory]
        [InlineData("0")]
        [InlineData("60")]
        [InlineData("3600")]
        public void QueryTimeoutSeconds_ValidValues_Accepted(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.QueryTimeoutSeconds, value);
        }

        [Theory]
        [InlineData("-1")]
        [InlineData("not-a-number")]
        public void QueryTimeoutSeconds_InvalidValues_Throw(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => statement.SetOption(ApacheParameters.QueryTimeoutSeconds, value));
        }

        [Theory]
        [InlineData("true")]
        [InlineData("false")]
        public void IsMetadataCommand_BooleanValues_Accepted(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.IsMetadataCommand, value);
        }

        [Fact]
        public void IsMetadataCommand_InvalidValue_Throws()
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => statement.SetOption(ApacheParameters.IsMetadataCommand, "not-a-bool"));
        }

        [Theory]
        [InlineData(ApacheParameters.CatalogName, "my_catalog")]
        [InlineData(ApacheParameters.SchemaName, "my_schema")]
        [InlineData(ApacheParameters.TableName, "my_table")]
        [InlineData(ApacheParameters.TableTypes, "TABLE,VIEW")]
        [InlineData(ApacheParameters.ColumnName, "my_column")]
        [InlineData(ApacheParameters.ForeignCatalogName, "fk_catalog")]
        [InlineData(ApacheParameters.ForeignSchemaName, "fk_schema")]
        [InlineData(ApacheParameters.ForeignTableName, "fk_table")]
        public void StringOptions_AreAcceptedAsIs(string key, string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            statement.SetOption(key, value);
        }

        [Fact]
        public void StringOptions_AllowEmpty()
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.CatalogName, string.Empty);
            statement.SetOption(ApacheParameters.SchemaName, string.Empty);
        }

        [Theory]
        [InlineData("true")]
        [InlineData("false")]
        public void EscapePatternWildcards_BooleanValues_Accepted(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            statement.SetOption(ApacheParameters.EscapePatternWildcards, value);
        }

        [Fact]
        public void EscapePatternWildcards_InvalidValue_Throws()
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            Assert.Throws<ArgumentOutOfRangeException>(
                () => statement.SetOption(ApacheParameters.EscapePatternWildcards, "maybe"));
        }

        [Theory]
        [InlineData("00-0123456789abcdef0123456789abcdef-fedcba9876543210-01")]
        [InlineData("")]
        [InlineData("   ")]
        public void TraceParent_AnyValue_Accepted(string value)
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            statement.SetOption(AdbcOptions.Telemetry.TraceParent, value);
        }

        [Fact]
        public void UnknownOption_ThrowsNotImplemented()
        {
            using var scenario = HiveMockServer.Create();
            using var statement = scenario.NewStatement();
            // Hits the `default:` branch of SetOption — the wrapping NotImplemented
            // throw is what AdbcException.NotImplemented produces.
            Assert.Throws<AdbcException>(
                () => statement.SetOption("adbc.bogus.unknown_option", "anything"));
        }
    }
}

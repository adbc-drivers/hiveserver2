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
using System.Threading;
using static AdbcDrivers.HiveServer2.Hive2.MetadataColumnNames;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.Thrift;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace AdbcDrivers.HiveServer2.Hive2
{
    internal abstract class HiveServer2ExtendedConnection : HiveServer2Connection
    {
        private const string ProductVersionDefault = "1.0.0";
        private const string DriverName = "ADBC Hive Driver";
        private const string ArrowVersion = "1.0.0";
        private const string BasicAuthenticationScheme = "Basic";
        private readonly Lazy<string> _productVersion;
        internal static readonly string s_userAgent = $"{DriverName.Replace(" ", "")}/{ProductVersionDefault}";

        protected override string GetProductVersionDefault() => ProductVersionDefault;

        protected override string ProductVersion => _productVersion.Value;

        public HiveServer2ExtendedConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
            ValidateProperties();
            _productVersion = new Lazy<string>(() => GetProductVersion(), LazyThreadSafetyMode.PublicationOnly);
        }

        private void ValidateProperties()
        {
            ValidateAuthentication();
            ValidateConnection();
            ValidateOptions();
        }

        protected abstract void ValidateAuthentication();

        protected abstract void ValidateConnection();

        protected abstract void ValidateOptions();

        public override AdbcStatement CreateStatement()
        {
            return new HiveServer2Statement(this);
        }

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema, IResponse response, TGetResultSetMetadataResp? metadataResp = null) =>
            new HiveServer2Reader(
                statement,
                schema,
                response,
                dataTypeConversion: statement.Connection.DataTypeConversion);

        internal override void SetPrecisionScaleAndTypeName(
            short colType,
            string typeName,
            TableInfo? tableInfo,
            int columnSize,
            int decimalDigits)
        {
            tableInfo?.TypeName.Add(typeName);
            tableInfo?.BaseTypeName.Add(ColumnMetadataHelper.GetBaseTypeName(typeName));
            switch (colType)
            {
                case (short)ColumnTypeId.DECIMAL:
                case (short)ColumnTypeId.NUMERIC:
                    {
                        tableInfo?.Precision.Add(columnSize);
                        tableInfo?.Scale.Add((short)decimalDigits);
                        break;
                    }

                case (short)ColumnTypeId.CHAR:
                case (short)ColumnTypeId.NCHAR:
                case (short)ColumnTypeId.VARCHAR:
                case (short)ColumnTypeId.LONGVARCHAR:
                case (short)ColumnTypeId.LONGNVARCHAR:
                case (short)ColumnTypeId.NVARCHAR:
                    {
                        tableInfo?.Precision.Add(columnSize);
                        tableInfo?.Scale.Add(null);
                        break;
                    }

                default:
                    {
                        tableInfo?.Precision.Add(null);
                        tableInfo?.Scale.Add(null);
                        break;
                    }
            }
        }

        protected override ColumnsMetadataColumnNames GetColumnsMetadataColumnNames()
        {
            return new ColumnsMetadataColumnNames()
            {
                TableCatalog = TableCat,
                TableSchema = TableSchem,
                TableName = TableName,
                ColumnName = ColumnName,
                DataType = DataType,
                TypeName = TypeName,
                Nullable = NullableColumn,
                ColumnDef = ColumnDef,
                OrdinalPosition = OrdinalPosition,
                IsNullable = IsNullable,
                IsAutoIncrement = IsAutoIncrement,
                ColumnSize = ColumnSize,
                DecimalDigits = DecimalDigits,
            };
        }

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(IResponse response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle!, Client, cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(IResponse response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle!, cancellationToken: cancellationToken);

        protected internal override int PositionRequiredOffset => 0;

        protected override string InfoDriverName => DriverName;

        protected override string InfoDriverArrowVersion => ArrowVersion;

        protected override bool IsColumnSizeValidForDecimal => false;

        protected override bool GetObjectsPatternsRequireLowerCase => false;

        internal override SchemaParser SchemaParser => new HiveServer2SchemaParser();

        protected abstract HiveServer2TransportType Type { get; }

        protected override int ColumnMapIndexOffset => 1;
    }
}

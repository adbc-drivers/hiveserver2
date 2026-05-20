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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Hive.Service.Rpc.Thrift.Reference;

namespace AdbcDrivers.HiveServer2.TestServer
{
    /// <summary>
    /// Minimal Hive-flavored implementation of <see cref="TCLIService.IAsync"/>.
    /// Hard-codes a single-column BIGINT result set for every ExecuteStatement
    /// call so the driver can complete a real session/execute/fetch/close
    /// cycle without a live HiveServer2 backend.
    /// </summary>
    public sealed class HiveServer2StubHandler : TCLIService.IAsync
    {
        private const string VendorName = "Test Hive";
        private const string VendorVersion = "0.0.0-test";
        private const long CannedResultValue = 42L;
        private const string CannedResultColumnName = "_c0";

        private readonly ConcurrentDictionary<Guid, OperationState> _operations = new();

        public Task<TOpenSessionResp> OpenSession(TOpenSessionReq req, CancellationToken cancellationToken = default)
        {
            // Echo the client's protocol back so version negotiation appears successful.
            var resp = new TOpenSessionResp(Ok(), req.Client_protocol)
            {
                SessionHandle = new TSessionHandle(NewHandleId()),
            };
            return Task.FromResult(resp);
        }

        public Task<TCloseSessionResp> CloseSession(TCloseSessionReq req, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new TCloseSessionResp(Ok()));
        }

        public Task<TGetInfoResp> GetInfo(TGetInfoReq req, CancellationToken cancellationToken = default)
        {
            var value = new TGetInfoValue();
            switch (req.InfoType)
            {
                case TGetInfoType.CLI_DBMS_NAME:
                    value.StringValue = VendorName;
                    break;
                case TGetInfoType.CLI_DBMS_VER:
                    value.StringValue = VendorVersion;
                    break;
                default:
                    value.StringValue = string.Empty;
                    break;
            }
            return Task.FromResult(new TGetInfoResp(Ok(), value));
        }

        public Task<TExecuteStatementResp> ExecuteStatement(TExecuteStatementReq req, CancellationToken cancellationToken = default)
        {
            TOperationHandle handle = NewOperationHandle(TOperationType.EXECUTE_STATEMENT, hasResultSet: true);
            _operations[GuidOf(handle)] = new OperationState();
            return Task.FromResult(new TExecuteStatementResp(Ok()) { OperationHandle = handle });
        }

        public Task<TGetOperationStatusResp> GetOperationStatus(TGetOperationStatusReq req, CancellationToken cancellationToken = default)
        {
            var resp = new TGetOperationStatusResp(Ok())
            {
                OperationState = TOperationState.FINISHED_STATE,
                HasResultSet = true,
            };
            return Task.FromResult(resp);
        }

        public Task<TGetResultSetMetadataResp> GetResultSetMetadata(TGetResultSetMetadataReq req, CancellationToken cancellationToken = default)
        {
            var resp = new TGetResultSetMetadataResp(Ok())
            {
                Schema = BuildSingleBigintSchema(),
            };
            return Task.FromResult(resp);
        }

        public Task<TFetchResultsResp> FetchResults(TFetchResultsReq req, CancellationToken cancellationToken = default)
        {
            var op = _operations.GetOrAdd(GuidOf(req.OperationHandle), _ => new OperationState());

            TRowSet rowSet;
            bool hasMore;
            if (op.FetchedOnce)
            {
                rowSet = new TRowSet(0L, new List<TRow>())
                {
                    Columns = new List<TColumn>
                    {
                        new() { I64Val = new TI64Column(new List<long>(), Array.Empty<byte>()) },
                    },
                };
                hasMore = false;
            }
            else
            {
                rowSet = new TRowSet(0L, new List<TRow>())
                {
                    Columns = new List<TColumn>
                    {
                        new() { I64Val = new TI64Column(new List<long> { CannedResultValue }, new byte[] { 0x00 }) },
                    },
                };
                op.FetchedOnce = true;
                hasMore = false;
            }

            return Task.FromResult(new TFetchResultsResp(Ok()) { HasMoreRows = hasMore, Results = rowSet });
        }

        public Task<TCloseOperationResp> CloseOperation(TCloseOperationReq req, CancellationToken cancellationToken = default)
        {
            _operations.TryRemove(GuidOf(req.OperationHandle), out _);
            return Task.FromResult(new TCloseOperationResp(Ok()));
        }

        // The remaining IAsync methods are not exercised by the initial end-to-end test.
        // Throwing here surfaces as a TApplicationException on the client, which is what
        // we want — any test that needs these should override or extend the handler.

        public Task<TGetTypeInfoResp> GetTypeInfo(TGetTypeInfoReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TGetCatalogsResp> GetCatalogs(TGetCatalogsReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TGetSchemasResp> GetSchemas(TGetSchemasReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TGetTablesResp> GetTables(TGetTablesReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TGetTableTypesResp> GetTableTypes(TGetTableTypesReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TGetColumnsResp> GetColumns(TGetColumnsReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TGetFunctionsResp> GetFunctions(TGetFunctionsReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TGetPrimaryKeysResp> GetPrimaryKeys(TGetPrimaryKeysReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TGetCrossReferenceResp> GetCrossReference(TGetCrossReferenceReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TCancelOperationResp> CancelOperation(TCancelOperationReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TGetDelegationTokenResp> GetDelegationToken(TGetDelegationTokenReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TCancelDelegationTokenResp> CancelDelegationToken(TCancelDelegationTokenReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TRenewDelegationTokenResp> RenewDelegationToken(TRenewDelegationTokenReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TGetQueryIdResp> GetQueryId(TGetQueryIdReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TSetClientInfoResp> SetClientInfo(TSetClientInfoReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TUploadDataResp> UploadData(TUploadDataReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<TDownloadDataResp> DownloadData(TDownloadDataReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();

        private static TStatus Ok() => new(TStatusCode.SUCCESS_STATUS);

        private static THandleIdentifier NewHandleId()
        {
            byte[] guid = Guid.NewGuid().ToByteArray();
            byte[] secret = Guid.NewGuid().ToByteArray();
            return new THandleIdentifier(guid, secret);
        }

        private static TOperationHandle NewOperationHandle(TOperationType type, bool hasResultSet)
        {
            return new TOperationHandle(NewHandleId(), type, hasResultSet);
        }

        private static Guid GuidOf(TOperationHandle handle) => new(handle.OperationId.Guid);

        private static TTableSchema BuildSingleBigintSchema()
        {
            var typeDesc = new TTypeDesc(new List<TTypeEntry>
            {
                new() { PrimitiveEntry = new TPrimitiveTypeEntry(TTypeId.BIGINT_TYPE) },
            });
            return new TTableSchema(new List<TColumnDesc>
            {
                new(CannedResultColumnName, typeDesc, position: 1),
            });
        }

        private sealed class OperationState
        {
            public bool FetchedOnce;
        }
    }
}

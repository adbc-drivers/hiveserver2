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
using System.Collections.Generic;
using Apache.Hive.Service.Rpc.Thrift.Reference;

namespace AdbcDrivers.HiveServer2.TestServer
{
    /// <summary>
    /// Logical result set returned from a HiveServer2 RPC: the column schema,
    /// zero or more <see cref="TRowSet"/> batches (each delivered by a
    /// successive FetchResults call), and an optional non-success status
    /// reported back via <c>GetOperationStatus</c>.
    /// </summary>
    public sealed class MockResult
    {
        public MockResult(TTableSchema schema, IReadOnlyList<TRowSet> batches, TStatus? operationStatus = null)
        {
            Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            Batches = batches ?? throw new ArgumentNullException(nameof(batches));
            OperationStatus = operationStatus ?? new TStatus(TStatusCode.SUCCESS_STATUS);
        }

        public TTableSchema Schema { get; }
        public IReadOnlyList<TRowSet> Batches { get; }
        public TStatus OperationStatus { get; }

        /// <summary>An empty result set conforming to <paramref name="schema"/>.</summary>
        public static MockResult Empty(TTableSchema schema) =>
            new(schema, new[] { MockRowSet.EmptyFor(schema) });

        /// <summary>Operation that succeeded at the RPC layer but reports an error via GetOperationStatus.</summary>
        public static MockResult OperationError(TTableSchema schema, string message, string? sqlState = null, int errorCode = 0)
        {
            var status = new TStatus(TStatusCode.ERROR_STATUS) { ErrorMessage = message, ErrorCode = errorCode };
            if (sqlState != null) status.SqlState = sqlState;
            return new MockResult(schema, Array.Empty<TRowSet>(), status);
        }

        /// <summary>Begin building a multi-column result set.</summary>
        public static MockResultBuilder Builder() => new();

        /// <summary>A one-row BIGINT result, useful as a default for ExecuteStatement.</summary>
        public static MockResult SingleBigint(long value, string columnName = "_c0") =>
            Builder().Bigint(columnName, value).Build();
    }
}

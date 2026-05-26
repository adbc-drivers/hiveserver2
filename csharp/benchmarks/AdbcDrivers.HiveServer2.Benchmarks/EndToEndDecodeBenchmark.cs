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
using System.Linq;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.Hive2;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Ipc;
using BenchmarkDotNet.Attributes;

namespace AdbcDrivers.HiveServer2.Benchmarks
{
    /// <summary>
    /// End-to-end client decode throughput: drives a large multi-batch,
    /// mixed-type result through the real driver against the in-process HTTP
    /// mock server over loopback. Unlike the operation-level microbenchmarks,
    /// this captures the full client path - Thrift framing, the per-column
    /// decoders this PR optimizes (endianness, validity bitmap, boolean packing,
    /// variable-length accumulation), and Arrow RecordBatch assembly.
    /// </summary>
    /// <remarks>
    /// Because it runs over loopback, it excludes real network latency and
    /// includes the (unoptimized, constant) mock server's serialization on the
    /// same machine, so the absolute numbers are not a remote-query prediction.
    /// Its purpose is regression tracking and an honest, full-pipeline view of
    /// where the per-operation speedups land once diluted by surrounding work.
    /// </remarks>
    [MemoryDiagnoser]
    public class EndToEndDecodeBenchmark
    {
        [Params(10_000)]
        public int RowsPerBatch;

        [Params(20)]
        public int Batches;

        private HiveServer2TestServer _server = null!;
        private AdbcDriver _driver = null!;
        private AdbcDatabase _database = null!;
        private AdbcConnection _connection = null!;

        [GlobalSetup]
        public void Setup()
        {
            // Deterministic mixed-type batch exercising every optimized decoder:
            // int/bigint/double (endianness), interleaved nulls (validity bitmap),
            // bool (bit packing), and string (variable-length accumulation).
            var rng = new Random(1234);
            var ints = new int?[RowsPerBatch];
            var longs = new long?[RowsPerBatch];
            var dbls = new double?[RowsPerBatch];
            var bools = new bool?[RowsPerBatch];
            var strs = new string?[RowsPerBatch];
            for (int i = 0; i < RowsPerBatch; i++)
            {
                bool isNull = (i % 17) == 0;
                ints[i] = isNull ? (int?)null : rng.Next();
                longs[i] = isNull ? (long?)null : (((long)rng.Next()) << 20) ^ rng.Next();
                dbls[i] = isNull ? (double?)null : rng.NextDouble() * 1_000_000.0;
                bools[i] = isNull ? (bool?)null : (i % 2 == 0);
                strs[i] = isNull ? null : ("row-" + i + "-" + rng.Next(100_000));
            }

            MockResult one = MockResult.Builder()
                .Int("i", ints)
                .Bigint("l", longs)
                .Double("d", dbls)
                .Bool("b", bools)
                .String("s", strs)
                .Build();

            // Reuse the single batch across `Batches` fetches (one FetchResults RPC each).
            var result = new MockResult(one.Schema, Enumerable.Repeat(one.Batches[0], Batches).ToList());

            var stub = new HiveServer2StubHandler { OnExecuteStatement = _ => result };
            _server = new HiveServer2TestServer(stub);

            var parameters = new Dictionary<string, string>
            {
                [HiveServer2Parameters.TransportType] = HiveServer2TransportTypeConstants.Http,
                [HiveServer2Parameters.AuthType] = HiveServer2AuthTypeConstants.Basic,
                [AdbcOptions.Username] = "mock-user",
                [AdbcOptions.Password] = "mock-password",
                [AdbcOptions.Uri] = _server.Uri.AbsoluteUri,
            };

            _driver = new HiveServer2Driver();
            _database = _driver.Open(parameters);
            _connection = _database.Connect(parameters);
        }

        [Benchmark]
        public async Task<long> ReadAllBatches()
        {
            using AdbcStatement statement = _connection.CreateStatement();
            statement.SqlQuery = "SELECT * FROM t";
            QueryResult queryResult = await statement.ExecuteQueryAsync();
            using IArrowArrayStream stream = queryResult.Stream!;

            long rows = 0;
            while (true)
            {
                using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
                if (batch == null) break;
                rows += batch.Length;
            }
            return rows;
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _connection?.Dispose();
            _database?.Dispose();
            _driver?.Dispose();
            _server?.Dispose();
        }
    }
}

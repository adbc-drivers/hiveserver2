/*
 * Copyright (c) 2025 ADBC Drivers Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.TestServer;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Thrift
{
    [Trait("Category", "MockServer")]
    public class TCLIServiceClientTests
    {
        [Fact]
        public async Task ProtocolFactoryIsCalledForEachRpcWithDistinctTransports()
        {
            using var server = new HiveServer2TestServer(new HiveServer2StubHandler());
            using var transport = new THttpTransport(server.Uri, new TConfiguration(), null, null, null);
            await transport.OpenAsync(CancellationToken.None);
            var factory = new TrackingProtocolFactory();
            var client = new TCLIService.Client(factory, transport);

            TOpenSessionResp session = await client.OpenSession(new TOpenSessionReq());
            await client.CloseSession(new TCloseSessionReq(session.SessionHandle));

            Assert.Equal(2, factory.Transports.Count);
            Assert.NotSame(factory.Transports[0], factory.Transports[1]);
        }

        [Fact]
        public async Task SuccessfulRpcDisposesPerCallTransportAndProtocol()
        {
            using var server = new HiveServer2TestServer(new HiveServer2StubHandler());
            using var transport = new THttpTransport(server.Uri, new TConfiguration(), null, null, null);
            await transport.OpenAsync(CancellationToken.None);
            var factory = new TrackingProtocolFactory();
            var client = new TCLIService.Client(factory, transport);

            await client.OpenSession(new TOpenSessionReq());

            Assert.Single(factory.Protocols);
            Assert.False(factory.Transports[0].IsOpen);
        }

        [Fact]
        public async Task FailedResponseDisposesPerCallTransportAndProtocol()
        {
            using var server = new HiveServer2TestServer(new HiveServer2StubHandler());
            server.StatusCodeOverride = _ => HttpStatusCode.InternalServerError;
            using var transport = new THttpTransport(server.Uri, new TConfiguration(), null, null, null);
            await transport.OpenAsync(CancellationToken.None);
            var factory = new TrackingProtocolFactory();
            var client = new TCLIService.Client(factory, transport);

            await Assert.ThrowsAnyAsync<Exception>(() => client.OpenSession(new TOpenSessionReq()));

            Assert.Single(factory.Protocols);
            Assert.False(factory.Transports[0].IsOpen);
        }

        [Fact]
        public async Task CallerOwnedNonProviderTransportRemainsUsableAcrossCalls()
        {
            using var server = new HiveServer2TestServer(new HiveServer2StubHandler());
            using var httpTransport = new THttpTransport(server.Uri, new TConfiguration(), null, null, null);
            await httpTransport.OpenAsync(CancellationToken.None);
            using var transport = new SharedTransport(httpTransport);
            var factory = new TrackingProtocolFactory();
            var client = new TCLIService.Client(factory, transport);

            TOpenSessionResp session = await client.OpenSession(new TOpenSessionReq());
            await client.CloseSession(new TCloseSessionReq(session.SessionHandle));

            Assert.Equal(2, factory.Transports.Count);
            Assert.Same(transport, factory.Transports[0]);
            Assert.Same(transport, factory.Transports[1]);
            Assert.True(transport.IsOpen);
        }

        [Fact]
        public async Task LegacySingleProtocolConstructorSupportsMultipleCalls()
        {
            using var server = new HiveServer2TestServer(new HiveServer2StubHandler());
            using var transport = new THttpTransport(server.Uri, new TConfiguration(), null, null, null);
            await transport.OpenAsync(CancellationToken.None);
            using var protocol = new TBinaryProtocol(transport);
            var client = new TCLIService.Client(protocol);

            TOpenSessionResp session = await client.OpenSession(new TOpenSessionReq());
            await client.CloseSession(new TCloseSessionReq(session.SessionHandle));
        }

        [Fact]
        public async Task LegacyInputOutputProtocolConstructorSupportsMultipleCalls()
        {
            using var server = new HiveServer2TestServer(new HiveServer2StubHandler());
            using var transport = new THttpTransport(server.Uri, new TConfiguration(), null, null, null);
            await transport.OpenAsync(CancellationToken.None);
            using var inputProtocol = new TBinaryProtocol(transport);
            using var outputProtocol = new TBinaryProtocol(transport);
            var client = new TCLIService.Client(inputProtocol, outputProtocol);

            TOpenSessionResp session = await client.OpenSession(new TOpenSessionReq());
            await client.CloseSession(new TCloseSessionReq(session.SessionHandle));
        }

        private sealed class TrackingProtocolFactory : TProtocolFactory
        {
            public List<TTransport> Transports { get; } = new();
            public List<TProtocol> Protocols { get; } = new();

            public override TProtocol GetProtocol(TTransport transport)
            {
                Transports.Add(transport);
                var protocol = new TBinaryProtocol(transport);
                Protocols.Add(protocol);
                return protocol;
            }
        }

        private sealed class SharedTransport : TTransport
        {
            private readonly TTransport _inner;

            public SharedTransport(TTransport inner) : base() => _inner = inner;
            public override bool IsOpen => _inner.IsOpen;
            public override TConfiguration Configuration => _inner.Configuration;
            public override Task OpenAsync(CancellationToken cancellationToken = default) => _inner.OpenAsync(cancellationToken);
            public override void Close() => _inner.Close();
            public override ValueTask<int> ReadAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default) => _inner.ReadAsync(buffer, offset, length, cancellationToken);
            public override Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken = default) => _inner.WriteAsync(buffer, offset, length, cancellationToken);
            public override Task FlushAsync(CancellationToken cancellationToken = default) => _inner.FlushAsync(cancellationToken);
            public override void UpdateKnownMessageSize(long size) => _inner.UpdateKnownMessageSize(size);
            public override void CheckReadBytesAvailable(long size) => _inner.CheckReadBytesAvailable(size);
            public override void ResetMessageSizeAndConsumedBytes(long newMessageSize) => _inner.ResetMessageSizeAndConsumedBytes(newMessageSize);
            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _inner.Dispose();
                }
            }
        }
    }
}

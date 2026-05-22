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
using System.Buffers.Binary;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Hive.Service.Rpc.Thrift.Reference;
using Thrift;
using Thrift.Processor;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace AdbcDrivers.HiveServer2.TestServer
{
    /// <summary>
    /// In-process HiveServer2-flavored Thrift server speaking the
    /// "Standard" (TCP + SASL PLAIN + framed binary) transport. Companion
    /// to <see cref="HiveServer2TestServer"/> (which speaks HTTP).
    ///
    /// <para>Wire protocol mirrors what real Hive/Spark/Impala servers do
    /// when the client opens a SASL+framed connection:</para>
    /// <list type="number">
    ///   <item>SASL handshake: client → 5-byte header (status + length) + payload, twice (Start + Ok); server → one Complete reply.</item>
    ///   <item>Post-handshake: framed Thrift binary — <c>&lt;4-byte big-endian length&gt;&lt;thrift-binary&gt;</c> per message in each direction.</item>
    /// </list>
    /// </summary>
    public sealed class HiveServer2StandardTestServer : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _shutdown = new();
        private readonly Task _acceptLoop;
        private readonly ITAsyncProcessor _processor;

        public HiveServer2StandardTestServer(TCLIService.IAsync handler)
        {
            _processor = new TCLIService.AsyncProcessor(handler);
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            HostName = "127.0.0.1";
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _acceptLoop = Task.Run(AcceptLoopAsync);
        }

        public string HostName { get; }
        public int Port { get; }

        public void Dispose()
        {
            _shutdown.Cancel();
            try { _listener.Stop(); } catch (ObjectDisposedException) { }
            try { _acceptLoop.Wait(TimeSpan.FromSeconds(5)); } catch (AggregateException) { }
            _shutdown.Dispose();
        }

        private async Task AcceptLoopAsync()
        {
            while (!_shutdown.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                }
                catch (ObjectDisposedException) { return; }
                catch (SocketException) when (_shutdown.IsCancellationRequested) { return; }
                catch (InvalidOperationException) when (_shutdown.IsCancellationRequested) { return; }

                _ = Task.Run(() => HandleClientAsync(client));
            }
        }

        private async Task HandleClientAsync(TcpClient client)
        {
            try
            {
                using TcpClient owned = client;
                using NetworkStream stream = owned.GetStream();

                if (!await TryNegotiateSaslAsync(stream).ConfigureAwait(false))
                {
                    return;
                }

                await ProcessFramedThriftAsync(stream).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Connection-level failure — let the test fail at the assertion
                // layer rather than crashing the accept loop.
            }
        }

        /// <summary>
        /// Read the client's two SASL messages (Start + mechanism name, then
        /// Ok + auth payload), send one Complete reply. PLAIN is single-step
        /// and we don't validate credentials — the test fixture is mock —
        /// but we DO assert the mechanism name is "PLAIN" so a future driver
        /// change that switches mechanism doesn't silently negotiate
        /// successfully against this fixture.
        /// </summary>
        private static async Task<bool> TryNegotiateSaslAsync(NetworkStream stream)
        {
            try
            {
                // SASL message 1: status=Start, payload=mechanism name.
                var (status1, mechanismBytes) = await ReadSaslMessageAsync(stream).ConfigureAwait(false);
                if (status1 != SaslStatusStart) return false;
                if (Encoding.UTF8.GetString(mechanismBytes) != "PLAIN") return false;

                // SASL message 2: status=Ok, payload=PLAIN auth "\0username\0password"
                var (status2, _) = await ReadSaslMessageAsync(stream).ConfigureAwait(false);
                if (status2 != SaslStatusOk) return false;

                // Server reply: status=Complete, empty payload
                await WriteSaslMessageAsync(stream, SaslStatusComplete, Array.Empty<byte>()).ConfigureAwait(false);
                return true;
            }
            catch (EndOfStreamException) { return false; }
            catch (IOException) { return false; }
            catch (InvalidDataException) { return false; }
        }

        /// <summary>
        /// Read length-prefixed Thrift binary messages until the client
        /// closes the stream. Each request → one TCLIService.Processor
        /// dispatch → one length-prefixed reply.
        /// </summary>
        private async Task ProcessFramedThriftAsync(NetworkStream stream)
        {
            while (!_shutdown.IsCancellationRequested)
            {
                byte[]? frame;
                try
                {
                    frame = await ReadFrameAsync(stream).ConfigureAwait(false);
                }
                catch (EndOfStreamException) { return; }
                catch (IOException) { return; }

                if (frame == null) return;

                using var requestBuffer = new MemoryStream(frame, writable: false);
                using var responseBuffer = new MemoryStream();
                var config = new TConfiguration();
                using var transport = new TStreamTransport(requestBuffer, responseBuffer, config);
                var input = new TBinaryProtocol(transport);
                var output = new TBinaryProtocol(transport);

                await _processor.ProcessAsync(input, output, _shutdown.Token).ConfigureAwait(false);

                byte[] payload = responseBuffer.ToArray();
                await WriteFrameAsync(stream, payload).ConfigureAwait(false);
            }
        }

        private const byte SaslStatusStart = 0x01;
        private const byte SaslStatusOk = 0x02;
        private const byte SaslStatusComplete = 0x05;

        // Generous upper bounds — the real handshake/frame sizes are tiny.
        // Hard caps turn a corrupted-length-from-the-wire (e.g. a misframed
        // payload or an unexpected TLS ClientHello whose first byte happens
        // to match a SASL status) into a clean negotiation failure instead
        // of an OOM or a multi-minute hang trying to read bytes that will
        // never arrive.
        private const int MaxSaslPayloadBytes = 64 * 1024;        // SASL PLAIN auth is well under 1 KiB
        private const int MaxFramedPayloadBytes = 16 * 1024 * 1024; // 16 MiB matches typical Thrift defaults

        private static async Task<(byte status, byte[] payload)> ReadSaslMessageAsync(NetworkStream stream)
        {
            byte[] header = new byte[5];
            await ReadExactAsync(stream, header, 0, 5).ConfigureAwait(false);
            byte status = header[0];
            int length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1, 4));
            if (length < 0 || length > MaxSaslPayloadBytes)
            {
                throw new InvalidDataException(
                    $"SASL payload length {length} out of range [0, {MaxSaslPayloadBytes}].");
            }
            byte[] payload = new byte[length];
            if (length > 0) await ReadExactAsync(stream, payload, 0, length).ConfigureAwait(false);
            return (status, payload);
        }

        private static async Task WriteSaslMessageAsync(NetworkStream stream, byte status, byte[] payload)
        {
            byte[] header = new byte[5];
            header[0] = status;
            BinaryPrimitives.WriteInt32BigEndian(header.AsSpan(1, 4), payload.Length);
            await stream.WriteAsync(header, 0, header.Length).ConfigureAwait(false);
            if (payload.Length > 0) await stream.WriteAsync(payload, 0, payload.Length).ConfigureAwait(false);
            await stream.FlushAsync().ConfigureAwait(false);
        }

        private static async Task<byte[]?> ReadFrameAsync(NetworkStream stream)
        {
            byte[] lengthBytes = new byte[4];
            int read = 0;
            while (read < 4)
            {
                int n = await stream.ReadAsync(lengthBytes, read, 4 - read).ConfigureAwait(false);
                if (n == 0)
                {
                    // Clean EOF before any of the length bytes arrived → client closed.
                    if (read == 0) return null;
                    throw new EndOfStreamException("Truncated frame length.");
                }
                read += n;
            }
            int length = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);
            if (length <= 0 || length > MaxFramedPayloadBytes)
            {
                throw new IOException(
                    $"Frame length {length} out of range (1, {MaxFramedPayloadBytes}].");
            }
            byte[] payload = new byte[length];
            await ReadExactAsync(stream, payload, 0, length).ConfigureAwait(false);
            return payload;
        }

        private static async Task WriteFrameAsync(NetworkStream stream, byte[] payload)
        {
            byte[] lengthBytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(lengthBytes, payload.Length);
            await stream.WriteAsync(lengthBytes, 0, 4).ConfigureAwait(false);
            await stream.WriteAsync(payload, 0, payload.Length).ConfigureAwait(false);
            await stream.FlushAsync().ConfigureAwait(false);
        }

        private static async Task ReadExactAsync(NetworkStream stream, byte[] buffer, int offset, int count)
        {
            int read = 0;
            while (read < count)
            {
                int n = await stream.ReadAsync(buffer, offset + read, count - read).ConfigureAwait(false);
                if (n == 0) throw new EndOfStreamException("Unexpected end of stream.");
                read += n;
            }
        }
    }
}

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

#if NET8_0_OR_GREATER
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using AdbcDrivers.HiveServer2.Hive2;
using Xunit;
using Xunit.Abstractions;

namespace AdbcDrivers.Tests.HiveServer2.Common
{
    /// <summary>
    /// Exercises <see cref="TlsProperties.AcceptUndeterminedRevocation"/> against a real
    /// CA → leaf chain whose CRL distribution point is unreachable, served, or revoking.
    /// Uses the <c>TrustedCertificatePath</c> branch so the test CA does not have to be
    /// installed in the system trust store.
    /// </summary>
    public class HiveServer2TlsImplRevocationTests
    {
        // TEST-NET-1 (RFC 5737): never routed, so packets are silently dropped.
        private const string BlackholedCrlUrl = "http://192.0.2.1/test.crl";

        private readonly ITestOutputHelper _output;

        public HiveServer2TlsImplRevocationTests(ITestOutputHelper output) => _output = output;

        [Fact]
        public void CrlRefused_Default_Throws()
        {
            using var pki = new TestPki(RefusedCrlUrl());
            var ex = Assert.Throws<AuthenticationException>(() => pki.Validate(acceptUndeterminedRevocation: false));
            _output.WriteLine(ex.Message);
            Assert.Contains(HttpTlsOptions.AcceptUndeterminedRevocation, ex.Message);
        }

        [Fact]
        public void CrlRefused_AcceptUndeterminedRevocation_ReturnsTrue()
        {
            using var pki = new TestPki(RefusedCrlUrl());
            Assert.True(pki.Validate(acceptUndeterminedRevocation: true));
        }

        [Fact]
        public void CrlBlackholed_AcceptUndeterminedRevocation_ReturnsTrue()
        {
            using var pki = new TestPki(BlackholedCrlUrl);
            var sw = Stopwatch.StartNew();
            Assert.True(pki.Validate(acceptUndeterminedRevocation: true));
            _output.WriteLine($"Validation with blackholed CRL took {sw.Elapsed.TotalSeconds:F1}s");
        }

        [SkippableFact]
        public async Task CrlServed_NotRevoked_ReturnsTrue()
        {
            await using var server = new CrlServer();
            using var pki = new TestPki(server.Url);
            server.Crl = pki.BuildCrl(revokeLeaf: false);
            Exception? error = Record.Exception(() => Assert.True(pki.Validate(acceptUndeterminedRevocation: false)));
            SkipIfCrlNotFetched(server);
            Assert.Null(error);
        }

        [SkippableFact]
        public async Task CrlServed_Revoked_AcceptUndeterminedRevocation_StillThrows()
        {
            await using var server = new CrlServer();
            using var pki = new TestPki(server.Url);
            server.Crl = pki.BuildCrl(revokeLeaf: true);
            Exception? error = Record.Exception(() => pki.Validate(acceptUndeterminedRevocation: true));
            SkipIfCrlNotFetched(server);
            var ex = Assert.IsType<AuthenticationException>(error);
            _output.WriteLine(ex.Message);
            Assert.Contains("revoked", ex.Message);
        }

        // Some chain engines never download CRLs for a root that is only supplied via ExtraStore
        // (macOS never fetches CRLs; Windows skips the fetch for an untrusted root) and report
        // RevocationStatusUnknown instead, so a served CRL can't be observed there.
        private static void SkipIfCrlNotFetched(CrlServer server) =>
            Skip.If(server.RequestCount == 0, "Platform X509Chain did not fetch the CRL for an ExtraStore root");

        private static string RefusedCrlUrl()
        {
            // Bind and immediately release a port so nothing is listening on it.
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return $"http://127.0.0.1:{port}/test.crl";
        }

        private sealed class TestPki : IDisposable
        {
            private readonly X509Certificate2 _root;
            private readonly X509Certificate2 _leaf;
            private readonly string _rootPath;

            public TestPki(string crlUrl)
            {
                var now = DateTimeOffset.UtcNow;
                using (RSA rootKey = RSA.Create(2048))
                {
                    var rootReq = new CertificateRequest("CN=adbc-test-root", rootKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                    rootReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
                    rootReq.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
                    rootReq.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(rootReq.PublicKey, false));
                    _root = rootReq.CreateSelfSigned(now.AddMinutes(-5), now.AddDays(1));
                }

                using (RSA leafKey = RSA.Create(2048))
                {
                    var leafReq = new CertificateRequest("CN=adbc-test-server", leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                    leafReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
                    leafReq.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
                    leafReq.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(_root, true, false));
                    leafReq.CertificateExtensions.Add(CertificateRevocationListBuilder.BuildCrlDistributionPointExtension(new[] { crlUrl }));
                    byte[] serial = new byte[8];
                    RandomNumberGenerator.Fill(serial);
                    serial[0] &= 0x7F;
                    _leaf = leafReq.Create(_root, now.AddMinutes(-1), now.AddHours(12), serial);
                }

                _rootPath = Path.Combine(Path.GetTempPath(), $"adbc-tls-root-{Guid.NewGuid():N}.cer");
                File.WriteAllBytes(_rootPath, _root.Export(X509ContentType.Cert));
            }

            public byte[] BuildCrl(bool revokeLeaf)
            {
                var builder = new CertificateRevocationListBuilder();
                if (revokeLeaf)
                    builder.AddEntry(_leaf, DateTimeOffset.UtcNow.AddMinutes(-1), X509RevocationReason.KeyCompromise);
                var now = DateTimeOffset.UtcNow;
                return builder.Build(_root, 1, now.AddHours(1), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1, now.AddMinutes(-1));
            }

            public bool Validate(bool acceptUndeterminedRevocation) =>
                HiveServer2TlsImpl.ValidateCertificate(
                    _leaf,
                    SslPolicyErrors.RemoteCertificateChainErrors,
                    new TlsProperties
                    {
                        TrustedCertificatePath = _rootPath,
                        RevocationMode = X509RevocationMode.Online,
                        AcceptUndeterminedRevocation = acceptUndeterminedRevocation,
                    });

            public void Dispose()
            {
                File.Delete(_rootPath);
                _leaf.Dispose();
                _root.Dispose();
            }
        }

        /// <summary>Minimal HTTP server that serves a single CRL.</summary>
        private sealed class CrlServer : IAsyncDisposable
        {
            private readonly TcpListener _listener;
            private readonly Task _loop;
            private int _requestCount;

            public CrlServer()
            {
                _listener = new TcpListener(IPAddress.Loopback, 0);
                _listener.Start();
                // A unique path per server avoids hitting a CRL cached by an earlier test.
                Url = $"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/{Guid.NewGuid():N}.crl";
                _loop = Task.Run(ServeAsync);
            }

            public string Url { get; }
            public byte[] Crl { get; set; } = Array.Empty<byte>();
            public int RequestCount => _requestCount;

            private async Task ServeAsync()
            {
                while (true)
                {
                    TcpClient client;
                    try { client = await _listener.AcceptTcpClientAsync(); }
                    catch (Exception) { return; }

                    using (client)
                    {
                        NetworkStream stream = client.GetStream();
                        // Drain the request headers; the body of a GET is empty.
                        var request = new System.Text.StringBuilder();
                        var buffer = new byte[4096];
                        while (!request.ToString().Contains("\r\n\r\n"))
                        {
                            int read = await stream.ReadAsync(buffer.AsMemory());
                            if (read == 0) break;
                            request.Append(System.Text.Encoding.ASCII.GetString(buffer, 0, read));
                        }
                        System.Threading.Interlocked.Increment(ref _requestCount);
                        byte[] body = Crl;
                        byte[] header = System.Text.Encoding.ASCII.GetBytes(
                            $"HTTP/1.1 200 OK\r\nContent-Type: application/pkix-crl\r\nContent-Length: {body.Length}\r\nConnection: close\r\n\r\n");
                        await stream.WriteAsync(header);
                        await stream.WriteAsync(body);
                    }
                }
            }

            public async ValueTask DisposeAsync()
            {
                _listener.Stop();
                await _loop;
            }
        }
    }
}
#endif

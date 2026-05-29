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
using System.IO;
using System.Net;
using System.Net.Sockets;
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
    /// In-process HiveServer2-flavored Thrift server backed by HttpListener.
    /// Decodes each POST body as a single Thrift binary request, dispatches it
    /// through a generated <c>TCLIService.AsyncProcessor</c> wired to the
    /// supplied <see cref="TCLIService.IAsync"/> handler, and writes the
    /// generated reply back as the HTTP response body.
    /// </summary>
    public sealed class HiveServer2TestServer : IDisposable
    {
        private const string CliServicePath = "cliservice";

        private readonly HttpListener _listener;
        private readonly CancellationTokenSource _shutdown = new();
        private readonly Task _acceptLoop;
        private readonly ITAsyncProcessor _processor;
        private int _requestCount;

        /// <summary>
        /// Optional override that lets tests force a specific HTTP status on
        /// individual requests (1-indexed). Returning <see cref="HttpStatusCode.OK"/>
        /// (or returning <c>null</c>, i.e. leaving this property unset) means
        /// "dispatch the Thrift request normally." Anything else short-circuits
        /// the handler and writes the chosen status with an empty body — the
        /// driver-side <c>THttpTransport</c> surfaces that as a
        /// <c>TTransportException</c> wrapping an <c>HttpRequestException</c>,
        /// which is what the OpenSession-fallback path looks for.
        /// </summary>
        public Func<int, HttpStatusCode>? StatusCodeOverride { get; set; }

        public HiveServer2TestServer(TCLIService.IAsync handler)
        {
            _processor = new TCLIService.AsyncProcessor(handler);

            int port = ReserveLoopbackPort();
            Uri = new Uri($"http://127.0.0.1:{port}/{CliServicePath}");

            _listener = new HttpListener();
            // HttpListener prefixes must end in '/'.
            _listener.Prefixes.Add(Uri.AbsoluteUri + "/");
            _listener.Start();
            _acceptLoop = Task.Run(AcceptLoopAsync);
        }

        public Uri Uri { get; }

        public void Dispose()
        {
            _shutdown.Cancel();
            try { _listener.Stop(); } catch (ObjectDisposedException) { }
            try { _acceptLoop.Wait(TimeSpan.FromSeconds(5)); } catch (AggregateException) { }
            _listener.Close();
            _shutdown.Dispose();
        }

        private static int ReserveLoopbackPort()
        {
            // Bind a TCP socket to port 0, grab the kernel-assigned port, release it
            // immediately. There is a short race window where another process could
            // grab the same port before HttpListener binds, but for local test runs
            // this is acceptable.
            var probe = new TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            int port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            return port;
        }

        private async Task AcceptLoopAsync()
        {
            while (!_shutdown.IsCancellationRequested)
            {
                HttpListenerContext context;
                try
                {
                    context = await _listener.GetContextAsync().ConfigureAwait(false);
                }
                catch (HttpListenerException) { return; }
                catch (ObjectDisposedException) { return; }
                catch (InvalidOperationException) { return; }

                _ = Task.Run(() => HandleAsync(context));
            }
        }

        private async Task HandleAsync(HttpListenerContext context)
        {
            HttpListenerRequest request = context.Request;
            HttpListenerResponse response = context.Response;

            try
            {
                if (!string.Equals(request.HttpMethod, "POST", StringComparison.OrdinalIgnoreCase))
                {
                    response.StatusCode = (int)HttpStatusCode.MethodNotAllowed;
                    return;
                }

                int count = Interlocked.Increment(ref _requestCount);
                if (StatusCodeOverride != null)
                {
                    HttpStatusCode overrideCode = StatusCodeOverride(count);
                    if (overrideCode != HttpStatusCode.OK)
                    {
                        response.StatusCode = (int)overrideCode;
                        return;
                    }
                }

                using var requestBuffer = new MemoryStream();
                await request.InputStream.CopyToAsync(requestBuffer, 81920, _shutdown.Token).ConfigureAwait(false);
                requestBuffer.Position = 0;

                using var responseBuffer = new MemoryStream();
                var config = new TConfiguration();
                using var transport = new TStreamTransport(requestBuffer, responseBuffer, config);
                var inputProtocol = new TBinaryProtocol(transport);
                var outputProtocol = new TBinaryProtocol(transport);

                await _processor.ProcessAsync(inputProtocol, outputProtocol, _shutdown.Token).ConfigureAwait(false);

                byte[] payload = responseBuffer.ToArray();
                response.StatusCode = (int)HttpStatusCode.OK;
                response.ContentType = "application/x-thrift";
                response.ContentLength64 = payload.Length;
                await response.OutputStream.WriteAsync(payload, 0, payload.Length, _shutdown.Token).ConfigureAwait(false);
            }
            catch (Exception)
            {
                try { response.StatusCode = (int)HttpStatusCode.InternalServerError; }
                catch (ObjectDisposedException) { }
            }
            finally
            {
                try { response.OutputStream.Close(); } catch (ObjectDisposedException) { }
                try { response.Close(); } catch (ObjectDisposedException) { }
            }
        }
    }
}

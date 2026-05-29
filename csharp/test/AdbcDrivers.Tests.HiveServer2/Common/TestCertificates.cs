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

#if NET5_0_OR_GREATER
using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace AdbcDrivers.Tests.HiveServer2.Common
{
    /// <summary>
    /// In-memory X.509 cert generation for TLS-validation tests. Builds a
    /// self-signed cert via <see cref="CertificateRequest"/> — public API
    /// from net5.0 onward, so this helper is conditionally compiled.
    /// </summary>
    internal static class TestCertificates
    {
        /// <summary>
        /// Self-signed cert valid for one day starting now, common name
        /// <c>CN=mock-server</c>. The private key isn't persisted because
        /// these tests only ever validate the public cert.
        /// </summary>
        public static X509Certificate2 SelfSigned(string commonName = "mock-server") =>
            BuildSelfSigned(commonName, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(1));

        /// <summary>Self-signed cert whose validity window ended yesterday.</summary>
        public static X509Certificate2 SelfSignedExpired(string commonName = "mock-server-expired") =>
            BuildSelfSigned(commonName, DateTimeOffset.UtcNow.AddDays(-30), DateTimeOffset.UtcNow.AddDays(-1));

        /// <summary>
        /// Writes a self-signed cert to a temp <c>.cer</c> file (DER-encoded
        /// public cert, no private key) and returns the path; the caller is
        /// responsible for deleting it. Used to drive the
        /// <c>TrustedCertificatePath</c> branch of ValidateCertificate.
        /// </summary>
        public static string SelfSignedAsTempFile(string commonName = "trusted-root")
        {
            using X509Certificate2 cert = SelfSigned(commonName);
            string path = Path.Combine(Path.GetTempPath(), $"adbc-tls-{Guid.NewGuid():N}.cer");
            File.WriteAllBytes(path, cert.Export(X509ContentType.Cert));
            return path;
        }

        private static X509Certificate2 BuildSelfSigned(string commonName, DateTimeOffset notBefore, DateTimeOffset notAfter)
        {
            using RSA rsa = RSA.Create(2048);
            var req = new CertificateRequest(
                $"CN={commonName}",
                rsa,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            return req.CreateSelfSigned(notBefore, notAfter);
        }
    }
}
#endif

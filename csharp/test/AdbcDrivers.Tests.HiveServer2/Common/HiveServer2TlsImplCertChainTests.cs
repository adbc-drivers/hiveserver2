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
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using AdbcDrivers.HiveServer2.Hive2;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Common
{
    /// <summary>
    /// Drives <see cref="HiveServer2TlsImpl.ValidateCertificate"/> through
    /// every branch and exercises the <c>UntrustedRoot</c> arm of
    /// <c>ThrowDetailedCertificateError</c> (the one ChainStatus flag a
    /// self-signed cert reliably produces). The other ChainStatus arms
    /// (Revoked, PartialChain, NotTimeValid, etc.) need a fabricated chain
    /// with a trusted root, which isn't portable from a unit test — those
    /// branches stay uncovered in this PR.
    /// </summary>
    public class HiveServer2TlsImplCertChainTests
    {
        [Fact]
        public void NoPolicyErrors_ReturnsTrue()
        {
            // Early-return path — never inspects the cert.
            Assert.True(HiveServer2TlsImpl.ValidateCertificate(
                cert: null,
                policyErrors: SslPolicyErrors.None,
                tlsProperties: new TlsProperties()));
        }

        [Fact]
        public void DisableServerCertificateValidation_BypassesChecks()
        {
            // The escape hatch — accepts anything, including a null cert.
            Assert.True(HiveServer2TlsImpl.ValidateCertificate(
                cert: null,
                policyErrors: SslPolicyErrors.RemoteCertificateChainErrors,
                tlsProperties: new TlsProperties { DisableServerCertificateValidation = true }));
        }

        [Fact]
        public void NullCert_WithChainErrors_ReturnsFalse()
        {
            Assert.False(HiveServer2TlsImpl.ValidateCertificate(
                cert: null,
                policyErrors: SslPolicyErrors.RemoteCertificateChainErrors,
                tlsProperties: new TlsProperties()));
        }

        [Fact]
        public void NameMismatch_NotAllowed_ReturnsFalse()
        {
            using X509Certificate2 cert = TestCertificates.SelfSigned();
            Assert.False(HiveServer2TlsImpl.ValidateCertificate(
                cert,
                SslPolicyErrors.RemoteCertificateNameMismatch,
                new TlsProperties { AllowHostnameMismatch = false }));
        }

        [Fact]
        public void NameMismatch_Allowed_FallsThroughToChainCheck()
        {
            using X509Certificate2 cert = TestCertificates.SelfSigned();
            // No chain-error flag, no trusted-cert path → returns true at
            // the "no chain errors" branch.
            Assert.True(HiveServer2TlsImpl.ValidateCertificate(
                cert,
                SslPolicyErrors.RemoteCertificateNameMismatch,
                new TlsProperties { AllowHostnameMismatch = true }));
        }

        [Fact]
        public void SelfSigned_WithChainErrors_AllowSelfSigned_ReturnsTrue()
        {
            using X509Certificate2 cert = TestCertificates.SelfSigned();
            Assert.True(HiveServer2TlsImpl.ValidateCertificate(
                cert,
                SslPolicyErrors.RemoteCertificateChainErrors,
                new TlsProperties { AllowSelfSigned = true, RevocationMode = X509RevocationMode.NoCheck }));
        }

        [Fact]
        public void SelfSigned_WithChainErrors_NoAllowSelfSigned_ThrowsAuthentication()
        {
            using X509Certificate2 cert = TestCertificates.SelfSigned();
            // ThrowDetailedCertificateError fires here — for a self-signed
            // cert the chain reports UntrustedRoot, hitting the first arm of
            // the per-flag switch.
            Assert.Throws<AuthenticationException>(() => HiveServer2TlsImpl.ValidateCertificate(
                cert,
                SslPolicyErrors.RemoteCertificateChainErrors,
                new TlsProperties { AllowSelfSigned = false, RevocationMode = X509RevocationMode.NoCheck }));
        }

        [Fact]
        public void TrustedCertificatePath_AllowSelfSigned_ReturnsTrue()
        {
            using X509Certificate2 cert = TestCertificates.SelfSigned();
            string trustedPath = TestCertificates.SelfSignedAsTempFile();
            try
            {
                Assert.True(HiveServer2TlsImpl.ValidateCertificate(
                    cert,
                    SslPolicyErrors.RemoteCertificateChainErrors,
                    new TlsProperties
                    {
                        AllowSelfSigned = true,
                        TrustedCertificatePath = trustedPath,
                        RevocationMode = X509RevocationMode.NoCheck,
                    }));
            }
            finally
            {
                File.Delete(trustedPath);
            }
        }

        [Fact]
        public void TrustedCertificatePath_ExpiredCert_NoAllowSelfSigned_Throws()
        {
            // ChainPolicy uses AllowUnknownCertificateAuthority so an unknown
            // root doesn't fail the chain — but NotTimeValid on an expired
            // cert does, regardless of flags. That's enough to fail
            // customChain.Build and fire ThrowDetailedCertificateError.
            using X509Certificate2 cert = TestCertificates.SelfSignedExpired("server-cert");
            string trustedPath = TestCertificates.SelfSignedAsTempFile("root-cert");
            try
            {
                Assert.Throws<AuthenticationException>(() => HiveServer2TlsImpl.ValidateCertificate(
                    cert,
                    SslPolicyErrors.RemoteCertificateChainErrors,
                    new TlsProperties
                    {
                        AllowSelfSigned = false,
                        TrustedCertificatePath = trustedPath,
                        RevocationMode = X509RevocationMode.NoCheck,
                    }));
            }
            finally
            {
                File.Delete(trustedPath);
            }
        }
    }
}
#endif

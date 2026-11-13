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
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Apache.Arrow.Adbc;

namespace AdbcDrivers.HiveServer2.Hive2
{
    class TlsProperties
    {
        public bool IsTlsEnabled { get; set; } = true;
        public X509RevocationMode RevocationMode { get; set; } = X509RevocationMode.Online;
        public bool AcceptUndeterminedRevocation { get; set; }
        public bool DisableServerCertificateValidation { get; set; }
        public bool AllowHostnameMismatch { get; set; }
        public bool AllowSelfSigned { get; set; }
        public string? TrustedCertificatePath { get; set; }
    }

    static class HiveServer2TlsImpl
    {
        /// <summary>
        /// Chain status flags that mean "revocation status could not be determined" (e.g. CRL/OCSP
        /// endpoints unreachable). Accepted when <see cref="TlsProperties.AcceptUndeterminedRevocation"/> is set.
        /// A positive <see cref="X509ChainStatusFlags.Revoked"/> result is never accepted.
        /// </summary>
        private const X509ChainStatusFlags UndeterminedRevocationFlags =
            X509ChainStatusFlags.RevocationStatusUnknown | X509ChainStatusFlags.OfflineRevocation;

        private const X509VerificationFlags IgnoreUndeterminedRevocationFlags =
            X509VerificationFlags.IgnoreEndRevocationUnknown |
            X509VerificationFlags.IgnoreCertificateAuthorityRevocationUnknown |
            X509VerificationFlags.IgnoreRootRevocationUnknown;

        private static void ApplyRevocationPolicy(X509ChainPolicy policy, TlsProperties tlsProperties)
        {
            policy.RevocationMode = tlsProperties.RevocationMode;
            if (tlsProperties.AcceptUndeterminedRevocation)
            {
                policy.VerificationFlags |= IgnoreUndeterminedRevocationFlags;
            }
        }

        /// <summary>
        /// Chain status flags that the chain policy's verification flags tell Build() to tolerate.
        /// </summary>
        private static X509ChainStatusFlags IgnoredStatusFlags(X509ChainPolicy policy)
        {
            X509ChainStatusFlags ignored = X509ChainStatusFlags.NoError;
            if (policy.VerificationFlags.HasFlag(X509VerificationFlags.AllowUnknownCertificateAuthority))
                ignored |= X509ChainStatusFlags.UntrustedRoot;
            if (policy.VerificationFlags.HasFlag(IgnoreUndeterminedRevocationFlags))
                ignored |= UndeterminedRevocationFlags;
            return ignored;
        }

        /// <summary>
        /// True when the chain's only problems are undetermined revocation status.
        /// </summary>
        private static bool HasOnlyUndeterminedRevocationErrors(X509Chain chain)
        {
            bool hasUndetermined = false;
            foreach (var status in chain.ChainStatus)
            {
                if ((status.Status & ~UndeterminedRevocationFlags) != X509ChainStatusFlags.NoError)
                    return false;
                hasUndetermined |= status.Status != X509ChainStatusFlags.NoError;
            }
            return hasUndetermined;
        }

        /// <summary>
        /// Parses a revocation mode string into the corresponding X509RevocationMode enum value.
        /// </summary>
        /// <param name="value">The revocation mode string: "0" (NoCheck), "1" (Online), or "2" (Offline)</param>
        /// <returns>The parsed X509RevocationMode</returns>
        /// <exception cref="ArgumentException">Thrown when the value is not a valid revocation mode</exception>
        private static X509RevocationMode ParseRevocationMode(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return X509RevocationMode.Online;
            }

            var trimmedValue = value!.Trim();
            if (int.TryParse(trimmedValue, out int intValue) && Enum.IsDefined(typeof(X509RevocationMode), intValue))
            {
                return (X509RevocationMode)intValue;
            }

            throw new ArgumentException(
                $"Invalid revocation mode: '{value}'. Valid values are {RevocationModeConstants.NoCheck} (NoCheck), {RevocationModeConstants.Online} (Online), or {RevocationModeConstants.Offline} (Offline).",
                nameof(value));
        }

        static internal TlsProperties GetHttpTlsOptions(IReadOnlyDictionary<string, string> properties)
        {
            TlsProperties tlsProperties = new();
            if (properties.TryGetValue(AdbcOptions.Uri, out string? uri) && !string.IsNullOrWhiteSpace(uri))
            {
                var uriValue = new Uri(uri);
                tlsProperties.IsTlsEnabled = uriValue.Scheme == Uri.UriSchemeHttps || !properties.TryGetValue(HttpTlsOptions.IsTlsEnabled, out string? isTlsEnabled) || !bool.TryParse(isTlsEnabled, out bool isTlsEnabledBool) || isTlsEnabledBool;
            }
            else if (!properties.TryGetValue(HttpTlsOptions.IsTlsEnabled, out string? isTlsEnabled) || !bool.TryParse(isTlsEnabled, out bool isTlsEnabledBool))
            {
                tlsProperties.IsTlsEnabled = true;
            }
            else
            {
                tlsProperties.IsTlsEnabled = isTlsEnabledBool;
            }
            if (!tlsProperties.IsTlsEnabled)
            {
                return tlsProperties;
            }

            if (properties.TryGetValue(HttpTlsOptions.DisableServerCertificateValidation, out string? disableServerCertificateValidation) && bool.TryParse(disableServerCertificateValidation, out bool disableServerCertificateValidationBool) && disableServerCertificateValidationBool)
            {
                tlsProperties.DisableServerCertificateValidation = true;
                return tlsProperties;
            }
            tlsProperties.DisableServerCertificateValidation = false;
            tlsProperties.AllowHostnameMismatch = properties.TryGetValue(HttpTlsOptions.AllowHostnameMismatch, out string? allowHostnameMismatch) && bool.TryParse(allowHostnameMismatch, out bool allowHostnameMismatchBool) && allowHostnameMismatchBool;
            tlsProperties.AllowSelfSigned = properties.TryGetValue(HttpTlsOptions.AllowSelfSigned, out string? allowSelfSigned) && bool.TryParse(allowSelfSigned, out bool allowSelfSignedBool) && allowSelfSignedBool;

            // Parse revocation mode if specified
            if (properties.TryGetValue(HttpTlsOptions.RevocationMode, out string? revocationMode))
            {
                tlsProperties.RevocationMode = ParseRevocationMode(revocationMode);
            }
            tlsProperties.AcceptUndeterminedRevocation = properties.TryGetValue(HttpTlsOptions.AcceptUndeterminedRevocation, out string? acceptUndeterminedRevocation) && bool.TryParse(acceptUndeterminedRevocation, out bool acceptUndeterminedRevocationBool) && acceptUndeterminedRevocationBool;

            if (!properties.TryGetValue(HttpTlsOptions.TrustedCertificatePath, out string? trustedCertificatePath)) return tlsProperties;
            tlsProperties.TrustedCertificatePath = trustedCertificatePath != "" && File.Exists(trustedCertificatePath) ? trustedCertificatePath : throw new FileNotFoundException("Trusted certificate path is invalid or file does not exist.");
            return tlsProperties;
        }

        static internal HttpClientHandler NewHttpClientHandler(TlsProperties tlsProperties, HiveServer2ProxyConfigurator proxyConfigurator)
        {
            HttpClientHandler httpClientHandler = new();
            if (tlsProperties.IsTlsEnabled)
            {
                httpClientHandler.ServerCertificateCustomValidationCallback = (request, cert, chain, errors) => ValidateCertificate(cert, errors, tlsProperties);
            }
            proxyConfigurator.ConfigureProxy(httpClientHandler);
            httpClientHandler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
            return httpClientHandler;
        }

        static private bool IsSelfSigned(X509Certificate2 cert)
        {
            return cert.Subject == cert.Issuer && IsSignedBy(cert, cert);
        }

        static private bool IsSignedBy(X509Certificate2 cert, X509Certificate2 issuer)
        {
            try
            {
                using (var chain = new X509Chain())
                {
                    chain.ChainPolicy.ExtraStore.Add(issuer);
                    chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
                    // Note: NoCheck is intentional here - this helper only validates cryptographic signatures,
                    // not full certificate chain validation. Revocation checking happens in ValidateCertificate()
                    // which uses the configurable tlsProperties.RevocationMode.
                    chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;

                    return chain.Build(cert)
                        && chain.ChainElements.Count == 1
                        && chain.ChainElements[0].Certificate.Thumbprint == issuer.Thumbprint;
                }
            }
            catch
            {
                return false;
            }
        }

        static internal TlsProperties GetStandardTlsOptions(IReadOnlyDictionary<string, string> properties)
        {
            TlsProperties tlsProperties = new();
            // tls is enabled by default
            if (!properties.TryGetValue(StandardTlsOptions.IsTlsEnabled, out string? isTlsEnabled) || !bool.TryParse(isTlsEnabled, out bool isTlsEnabledBool))
            {
                tlsProperties.IsTlsEnabled = true;
            }
            else
            {
                tlsProperties.IsTlsEnabled = isTlsEnabledBool;
            }
            if (!tlsProperties.IsTlsEnabled)
            {
                return tlsProperties;
            }

            if (properties.TryGetValue(StandardTlsOptions.DisableServerCertificateValidation, out string? disableServerCertificateValidation) && bool.TryParse(disableServerCertificateValidation, out bool disableServerCertificateValidationBool) && disableServerCertificateValidationBool)
            {
                tlsProperties.DisableServerCertificateValidation = true;
                return tlsProperties;
            }
            tlsProperties.DisableServerCertificateValidation = false;
            tlsProperties.AllowHostnameMismatch = properties.TryGetValue(StandardTlsOptions.AllowHostnameMismatch, out string? allowHostnameMismatch) && bool.TryParse(allowHostnameMismatch, out bool allowHostnameMismatchBool) && allowHostnameMismatchBool;
            tlsProperties.AllowSelfSigned = properties.TryGetValue(StandardTlsOptions.AllowSelfSigned, out string? allowSelfSigned) && bool.TryParse(allowSelfSigned, out bool allowSelfSignedBool) && allowSelfSignedBool;

            // Parse revocation mode if specified
            if (properties.TryGetValue(StandardTlsOptions.RevocationMode, out string? revocationMode))
            {
                tlsProperties.RevocationMode = ParseRevocationMode(revocationMode);
            }
            tlsProperties.AcceptUndeterminedRevocation = properties.TryGetValue(StandardTlsOptions.AcceptUndeterminedRevocation, out string? acceptUndeterminedRevocation) && bool.TryParse(acceptUndeterminedRevocation, out bool acceptUndeterminedRevocationBool) && acceptUndeterminedRevocationBool;

            if (!properties.TryGetValue(StandardTlsOptions.TrustedCertificatePath, out string? trustedCertificatePath)) return tlsProperties;
            tlsProperties.TrustedCertificatePath = trustedCertificatePath != "" && File.Exists(trustedCertificatePath) ? trustedCertificatePath : throw new FileNotFoundException("Trusted certificate path is invalid or file does not exist.");
            return tlsProperties;
        }

        /// <summary>
        /// Analyzes certificate chain validation errors and throws descriptive exceptions with actionable guidance.
        /// </summary>
        private static void ThrowDetailedCertificateError(X509Chain chain, TlsProperties tlsProperties)
        {
            X509ChainStatusFlags ignored = IgnoredStatusFlags(chain.ChainPolicy);
            foreach (var status in chain.ChainStatus)
            {
                // Skip flags the chain policy already tolerates, so they don't mask the real failure
                X509ChainStatusFlags flags = status.Status & ~ignored;
                if (flags == X509ChainStatusFlags.NoError)
                    continue;

                // Check for specific flags (Status is a flags enum, multiple flags can be set)
                if (flags.HasFlag(X509ChainStatusFlags.UntrustedRoot))
                {
                    throw new AuthenticationException(
                        $"Certificate validation failed: The root certificate is not trusted. " +
                        "This occurs when the Certificate Authority (CA) that signed the server certificate is not in your system's trusted root store. " +
                        $"To resolve this: (1) Install the CA certificate in your system's trusted root store, or " +
                        $"(2) Specify the trusted certificate using '{HttpTlsOptions.TrustedCertificatePath}', or " +
                        $"(3) For testing only, set '{HttpTlsOptions.DisableServerCertificateValidation}=true' (NOT recommended for production).");
                }

                if (flags.HasFlag(X509ChainStatusFlags.PartialChain))
                {
                    throw new AuthenticationException(
                        "Certificate validation failed: The certificate chain is incomplete. " +
                        "This occurs when one or more intermediate certificates are missing from the chain. " +
                        "The server should send all intermediate certificates, but some servers are misconfigured. " +
                        "To resolve this: (1) Contact the server administrator to fix the certificate chain configuration, or " +
                        "(2) Install the missing intermediate certificates in your system's certificate store, or " +
                        $"(3) Specify the complete certificate chain using '{HttpTlsOptions.TrustedCertificatePath}'.");
                }

                if (flags.HasFlag(X509ChainStatusFlags.RevocationStatusUnknown))
                {
                    throw new AuthenticationException(
                        "Certificate validation failed: Unable to determine the revocation status of the certificate. " +
                        "This typically occurs when the Certificate Revocation List (CRL) or Online Certificate Status Protocol (OCSP) servers are unreachable. " +
                        "Common causes: AWS PrivateLink, corporate firewalls blocking port 80, or network restrictions. " +
                        $"Current revocation mode: {tlsProperties.RevocationMode}. " +
                        $"To resolve this: Set '{HttpTlsOptions.AcceptUndeterminedRevocation}=true' to continue when revocation status cannot be determined (revoked certificates are still rejected), " +
                        $"set '{HttpTlsOptions.RevocationMode}={RevocationModeConstants.NoCheck}' to skip revocation checking (safe in isolated networks), " +
                        $"or set '{HttpTlsOptions.RevocationMode}={RevocationModeConstants.Offline}' to use only cached CRL data.");
                }

                if (flags.HasFlag(X509ChainStatusFlags.OfflineRevocation))
                {
                    throw new AuthenticationException(
                        "Certificate validation failed: The revocation server is offline or unreachable. " +
                        "The certificate contains revocation URLs (CRL/OCSP) that require outbound HTTP (port 80) access, but the connection failed. " +
                        "This is common in AWS PrivateLink environments or networks with restrictive egress rules. " +
                        $"To resolve this: Set '{HttpTlsOptions.AcceptUndeterminedRevocation}=true' to continue when revocation status cannot be determined (revoked certificates are still rejected), " +
                        $"set '{HttpTlsOptions.RevocationMode}={RevocationModeConstants.NoCheck}' to disable revocation checking, " +
                        $"or set '{HttpTlsOptions.RevocationMode}={RevocationModeConstants.Offline}' to use only locally cached revocation data.");
                }

                if (flags.HasFlag(X509ChainStatusFlags.Revoked))
                {
                    throw new AuthenticationException(
                        "Certificate validation failed: The certificate has been revoked by the Certificate Authority. " +
                        "This is a critical security issue - the certificate is no longer valid and should not be trusted. " +
                        "To resolve this: Contact the server administrator to install a new, valid certificate. " +
                        "Do NOT disable certificate validation to work around this error.");
                }

                if (flags.HasFlag(X509ChainStatusFlags.NotTimeValid))
                {
                    throw new AuthenticationException(
                        "Certificate validation failed: The certificate is not valid for the current date/time. " +
                        "The certificate may be expired or not yet valid. " +
                        $"Certificate details: {status.StatusInformation}. " +
                        "To resolve this: (1) Verify your system clock is correct, or " +
                        "(2) Contact the server administrator to renew the expired certificate.");
                }

                if (flags.HasFlag(X509ChainStatusFlags.NotSignatureValid))
                {
                    throw new AuthenticationException(
                        "Certificate validation failed: The certificate signature is invalid. " +
                        "This indicates the certificate may be corrupted, tampered with, or improperly signed. " +
                        "This is a critical security issue. " +
                        "To resolve this: Contact the server administrator - the certificate needs to be replaced.");
                }

                if (flags.HasFlag(X509ChainStatusFlags.InvalidNameConstraints) ||
                    flags.HasFlag(X509ChainStatusFlags.HasNotPermittedNameConstraint) ||
                    flags.HasFlag(X509ChainStatusFlags.HasExcludedNameConstraint))
                {
                    throw new AuthenticationException(
                        "Certificate validation failed: Certificate name constraints violation. " +
                        "The certificate is not permitted to be used for this hostname or domain. " +
                        $"Details: {status.StatusInformation}. " +
                        "To resolve this: Verify you are connecting to the correct hostname, or contact the server administrator.");
                }

                if (flags.HasFlag(X509ChainStatusFlags.InvalidBasicConstraints))
                {
                    throw new AuthenticationException(
                        "Certificate validation failed: Invalid basic constraints. " +
                        "The certificate chain violates basic constraints (e.g., a non-CA certificate is being used as a CA). " +
                        "This indicates a misconfigured certificate hierarchy. " +
                        "To resolve this: Contact the server administrator to fix the certificate chain.");
                }

                // For any other error flags not explicitly handled above
                if (flags != X509ChainStatusFlags.NoError)
                {
                    throw new AuthenticationException(
                        $"Certificate validation failed: {flags}. " +
                        $"Details: {status.StatusInformation}. " +
                        "This may indicate a certificate configuration issue. Please contact your system administrator.");
                }
            }

            // Callers rely on this method never returning, even if every status was tolerated by policy
            throw new AuthenticationException(
                "Certificate validation failed: The certificate chain could not be validated. " +
                "Please contact your system administrator.");
        }

        static internal bool ValidateCertificate(X509Certificate? cert, SslPolicyErrors policyErrors, TlsProperties tlsProperties)
        {
            if (policyErrors == SslPolicyErrors.None || tlsProperties.DisableServerCertificateValidation)
                return true;

            if (cert == null || !(cert is X509Certificate2 cert2))
                return false;

            bool isNameMismatchError = policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch) && !tlsProperties.AllowHostnameMismatch;

            if (isNameMismatchError) return false;

            if (string.IsNullOrEmpty(tlsProperties.TrustedCertificatePath))
            {
                // If chain errors exist, provide detailed error information
                if (policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateChainErrors))
                {
                    if (tlsProperties.AllowSelfSigned && IsSelfSigned(cert2))
                        return true;

                    // Build a chain to get detailed error information
                    using (X509Chain defaultChain = new X509Chain())
                    {
                        ApplyRevocationPolicy(defaultChain.ChainPolicy, tlsProperties);
                        defaultChain.Build(cert2);

                        if (tlsProperties.AcceptUndeterminedRevocation && HasOnlyUndeterminedRevocationErrors(defaultChain))
                            return true;

                        // Throw detailed error with actionable guidance (never returns)
                        ThrowDetailedCertificateError(defaultChain, tlsProperties);
                    }

                    // Unreachable - ThrowDetailedCertificateError always throws
                    throw new InvalidOperationException("ThrowDetailedCertificateError should have thrown an exception");
                }
                return true;
            }

#if NET9_0_OR_GREATER
            X509Certificate2 trustedRoot = X509CertificateLoader.LoadCertificateFromFile(tlsProperties.TrustedCertificatePath);
#else
            X509Certificate2 trustedRoot = new X509Certificate2(tlsProperties.TrustedCertificatePath);
#endif
            X509Chain customChain = new();
            customChain.ChainPolicy.ExtraStore.Add(trustedRoot);
            // "tell the X509Chain class that I do trust this root certs and it should check just the certs in the chain and nothing else"
            customChain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
            ApplyRevocationPolicy(customChain.ChainPolicy, tlsProperties);

            bool chainValid = customChain.Build(cert2);

            // If self-signed is allowed and this is self-signed, accept it
            if (tlsProperties.AllowSelfSigned && IsSelfSigned(cert2))
                return true;

            // If validation failed, throw detailed error (never returns)
            if (!chainValid)
                ThrowDetailedCertificateError(customChain, tlsProperties);

            return true;
        }
    }
}

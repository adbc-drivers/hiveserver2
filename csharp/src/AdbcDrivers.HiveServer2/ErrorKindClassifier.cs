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
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using AdbcDrivers.HiveServer2.Hive2;
using Thrift.Protocol;
using Thrift.Transport;
using static AdbcDrivers.HiveServer2.ActivityKeys.Db;

namespace AdbcDrivers.HiveServer2
{
    /// <summary>
    /// Maps a caught exception to one of the <see cref="ActivityKeys.Db.ErrorKindValues"/>
    /// taxonomy values, and writes it onto the supplied (or current) Activity as
    /// <c>db.error.kind</c>. Used at the catch sites in
    /// <see cref="HiveServer2Connection"/> / <see cref="HiveServer2Statement"/>
    /// so OTel consumers can group <c>Status=Error</c> spans by category
    /// without parsing <c>exception.type</c>/<c>exception.message</c>.
    ///
    /// See adbc-drivers/databricks#481 for motivation.
    /// </summary>
    internal static class ErrorKindClassifier
    {
        /// <summary>
        /// Classify <paramref name="exception"/> into one of the
        /// <see cref="ActivityKeys.Db.ErrorKindValues"/> taxonomy values, or
        /// return <c>null</c> when the classification is ambiguous.
        ///
        /// <para>
        /// Per the parent issue we intentionally do NOT distinguish
        /// <c>user_cancel</c> from <c>query_timeout</c> here — the latter
        /// requires cross-layer state (whether the parent driver called
        /// <c>Cancel()</c> vs. the local CTS fired its CancelAfter timer),
        /// and is left for a follow-up. Callers that have local knowledge
        /// of "the CTS in scope was created with a timer" should pass
        /// <paramref name="cancellationLooksLikeTimeout"/> = <c>true</c>.
        /// </para>
        /// </summary>
        public static string? Classify(Exception exception, bool cancellationLooksLikeTimeout = false)
        {
            // Most specific first.
            //
            // Auth: an HTTP 401/403 from the server. We use the .NET 5+
            // typed StatusCode where available; on net472 / netstandard2.0
            // we fall back to a tolerant message scan because the HttpRequestException
            // lacks a typed status code.
            if (ApacheUtility.ContainsException(exception, out HttpRequestException? httpEx) && httpEx != null)
            {
                if (IsHttpAuthFailure(httpEx)) return ErrorKindValues.AuthFailed;
                if (IsHttpServerError(httpEx)) return ErrorKindValues.ServerError;
                // HttpRequestException without a status code (no response from
                // server) classifies as transport-level.
                if (!HasHttpStatus(httpEx)) return ErrorKindValues.Network;
            }

            // Protocol-level: Thrift framing / encoding.
            if (ApacheUtility.ContainsException(exception, out TProtocolException? _))
            {
                return ErrorKindValues.ProtocolError;
            }

            // Server-thrown via HandleThriftResponse(). HiveServer2Exception is
            // the wrapper used for every TStatus != SUCCESS path and for the
            // operation-poll error state in PollForResponseAsync.
            if (exception is HiveServer2Exception)
            {
                return ErrorKindValues.ServerError;
            }

            // Transport-level: explicit socket or IO failures.
            if (ApacheUtility.ContainsException(exception, out SocketException? _)
                || ApacheUtility.ContainsException(exception, out IOException? _))
            {
                return ErrorKindValues.Network;
            }

            // TTransportException is what the Thrift client wraps almost
            // every wire failure in. If we get here, the inner exception
            // chain didn't carry a more specific HttpRequestException /
            // SocketException / IOException, so treat it as transport-level.
            if (ApacheUtility.ContainsException(exception, out TTransportException? _))
            {
                return ErrorKindValues.Network;
            }

            // Cancellation: only classify when the caller can attest that the
            // local CTS was created with a CancelAfter timer (i.e. a timeout
            // is the *only* way this could fire from the driver's side).
            // Otherwise we'd risk mislabeling a parent-Cancel() as a timeout,
            // which is exactly the ambiguity #481 calls out. The user_cancel
            // disambiguation requires parent-driver state and is deferred.
            if (cancellationLooksLikeTimeout
                && ApacheUtility.ContainsException(exception, out OperationCanceledException? _))
            {
                return ErrorKindValues.QueryTimeout;
            }

            return null;
        }

        /// <summary>
        /// Apply <see cref="Classify"/> to <paramref name="exception"/> and tag
        /// the resulting kind on <paramref name="activity"/> (or
        /// <see cref="Activity.Current"/>). No-op when the classification is
        /// ambiguous, when the activity is null, or when the tag is already
        /// set by an inner catch site (innermost wins).
        /// </summary>
        public static void Tag(Activity? activity, Exception exception, bool cancellationLooksLikeTimeout = false)
        {
            string? kind = Classify(exception, cancellationLooksLikeTimeout);
            if (kind != null) Tag(activity, kind);
        }

        /// <summary>
        /// Tag a known classification onto the supplied activity (or current).
        /// Idempotent: an existing <c>db.error.kind</c> tag wins, so the
        /// innermost throwing site keeps its classification when outer
        /// catch sites also call this.
        /// </summary>
        public static void Tag(Activity? activity, string kind)
        {
            Activity? target = activity ?? Activity.Current;
            if (target == null) return;
            foreach (var existing in target.TagObjects)
            {
                if (existing.Key == ErrorKind) return;
            }
            target.AddTag(ErrorKind, kind);
        }

        private static bool IsHttpAuthFailure(HttpRequestException ex)
        {
#if NET5_0_OR_GREATER
            if (ex.StatusCode == System.Net.HttpStatusCode.Unauthorized) return true;
            if (ex.StatusCode == System.Net.HttpStatusCode.Forbidden) return true;
            // Fall through to message scan only when no typed code is present —
            // useful for handlers that surface 401 via re-thrown exceptions
            // that lost the typed status.
            if (ex.StatusCode != null) return false;
#endif
            return ex.Message.IndexOf("unauthorized", StringComparison.OrdinalIgnoreCase) >= 0
                || ex.Message.IndexOf("authenticat", StringComparison.OrdinalIgnoreCase) >= 0
                || ex.Message.IndexOf("forbidden", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static bool IsHttpServerError(HttpRequestException ex)
        {
#if NET5_0_OR_GREATER
            if (ex.StatusCode is { } code)
            {
                int n = (int)code;
                return n >= 500 && n < 600;
            }
#endif
            return false;
        }

        private static bool HasHttpStatus(HttpRequestException ex)
        {
#if NET5_0_OR_GREATER
            return ex.StatusCode != null;
#else
            return false;
#endif
        }
    }
}

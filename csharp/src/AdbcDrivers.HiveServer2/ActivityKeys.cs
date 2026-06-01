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

namespace AdbcDrivers.HiveServer2
{
    internal static class ActivityKeys
    {
        public const string AuthType = "auth_type";
        public const string Encrypted = "encrypted";
        public const string TransportType = "transport_type";
        public const string Host = "host";
        public const string Port = "port";

        internal static class Http
        {
            public const string Key = "http";
            public const string UserAgent = Key + ".user_agent";
            public const string Uri = Key + ".uri";
            public const string AuthScheme = Key + ".auth_scheme";
        }

        internal static class Thrift
        {
            public const string Key = "thrift";
            public const string MaxMessageSize = Key + ".max_message_size";
            public const string MaxFrameSize = Key + ".max_frame_size";
        }

        /// <summary>
        /// OTel-compatible classification of a failure on a Status=Error span.
        /// See adbc-drivers/databricks#481. Dashboards group/filter on this
        /// instead of parsing <c>exception.type</c>/<c>exception.message</c>.
        /// </summary>
        internal static class Db
        {
            public const string ErrorKind = "db.error.kind";

            internal static class ErrorKindValues
            {
                /// <summary>Cancellation triggered by a CancelAfter timer on
                /// the operation's CTS (driver-internal timeout fired).</summary>
                public const string QueryTimeout = "query_timeout";

                /// <summary>Transport-level: socket / IO / HTTP without an HTTP
                /// status code / TTransportException with no Thrift status.</summary>
                public const string Network = "network";

                /// <summary>Thrift response carried <c>TStatus != SUCCESS</c>,
                /// or HTTP 5xx.</summary>
                public const string ServerError = "server_error";

                /// <summary>HTTP 401 / 403.</summary>
                public const string AuthFailed = "auth_failed";

                /// <summary>Thrift framing exceptions, Arrow parsing errors,
                /// schema mismatches.</summary>
                public const string ProtocolError = "protocol_error";
            }
        }
    }
}

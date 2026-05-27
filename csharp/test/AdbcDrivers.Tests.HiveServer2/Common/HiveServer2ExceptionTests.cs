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

using System;
using AdbcDrivers.HiveServer2.Hive2;
using Apache.Arrow.Adbc;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Common
{
    /// <summary>
    /// Construction + setter coverage for HiveServer2Exception. Driver code
    /// uses several different constructor overloads in error-wrapping paths
    /// that aren't otherwise reached by the mock fixture.
    /// </summary>
    public class HiveServer2ExceptionTests
    {
        [Fact]
        public void Parameterless_Constructor_Works()
        {
            var ex = new HiveServer2Exception();
            Assert.NotNull(ex);
        }

        [Fact]
        public void Message_Constructor_PreservesMessage()
        {
            var ex = new HiveServer2Exception("boom");
            Assert.Equal("boom", ex.Message);
        }

        [Fact]
        public void MessageStatus_Constructor_PreservesStatusCode()
        {
            var ex = new HiveServer2Exception("boom", AdbcStatusCode.Unauthorized);
            Assert.Equal("boom", ex.Message);
            Assert.Equal(AdbcStatusCode.Unauthorized, ex.Status);
        }

        [Fact]
        public void MessageInner_Constructor_PreservesInner()
        {
            var inner = new InvalidOperationException("inner");
            var ex = new HiveServer2Exception("outer", inner);
            Assert.Same(inner, ex.InnerException);
        }

        [Fact]
        public void MessageStatusInner_Constructor_PreservesAll()
        {
            var inner = new InvalidOperationException("inner");
            var ex = new HiveServer2Exception("outer", AdbcStatusCode.InvalidArgument, inner);
            Assert.Equal("outer", ex.Message);
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
            Assert.Same(inner, ex.InnerException);
        }

        [Fact]
        public void SetSqlState_ExposesViaProperty()
        {
            // SetSqlState is internal — usable from tests via InternalsVisibleTo.
            HiveServer2Exception ex = new HiveServer2Exception("boom").SetSqlState("42S02");
            Assert.Equal("42S02", ex.SqlState);
        }

        [Fact]
        public void SetNativeError_ExposesViaProperty()
        {
            HiveServer2Exception ex = new HiveServer2Exception("boom").SetNativeError(1234);
            Assert.Equal(1234, ex.NativeError);
        }
    }
}

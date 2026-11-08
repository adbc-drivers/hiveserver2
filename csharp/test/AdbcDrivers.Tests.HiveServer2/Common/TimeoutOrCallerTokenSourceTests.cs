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
using System.Threading;
using AdbcDrivers.HiveServer2;
using Xunit;

namespace AdbcDrivers.Tests.HiveServer2.Common
{
    public class TimeoutOrCallerTokenSourceTests
    {
        private static readonly TimeSpan s_long = TimeSpan.FromMinutes(5);

        [Fact]
        public void CallerCancel_IsReportedAsCaller()
        {
            using var caller = new CancellationTokenSource();
            using var source = new TimeoutOrCallerTokenSource(s_long, caller.Token);

            caller.Cancel();

            Assert.True(source.Token.IsCancellationRequested);
            Assert.True(source.IsCancelledByCaller);
        }

        [Fact]
        public void Timeout_IsNotReportedAsCaller()
        {
            using var caller = new CancellationTokenSource();
            using var source = new TimeoutOrCallerTokenSource(TimeSpan.FromMilliseconds(1), caller.Token);

            Assert.True(SpinWait.SpinUntil(() => source.Token.IsCancellationRequested, TimeSpan.FromSeconds(10)));
            Assert.False(source.IsCancelledByCaller);
        }

        [Fact]
        public void CallerCancelAfterTimeout_StillReportedAsTimeout()
        {
            using var caller = new CancellationTokenSource();
            using var source = new TimeoutOrCallerTokenSource(TimeSpan.FromMilliseconds(1), caller.Token);
            Assert.True(SpinWait.SpinUntil(() => source.Token.IsCancellationRequested, TimeSpan.FromSeconds(10)));

            // The caller cancelling while the timed-out operation is still unwinding
            // must not change the recorded cause.
            caller.Cancel();

            Assert.False(source.IsCancelledByCaller);
        }

        [Fact]
        public void PreCancelledCaller_IsReportedAsCallerImmediately()
        {
            using var caller = new CancellationTokenSource();
            caller.Cancel();

            using var source = new TimeoutOrCallerTokenSource(s_long, caller.Token);

            Assert.True(source.Token.IsCancellationRequested);
            Assert.True(source.IsCancelledByCaller);
        }

        [Fact]
        public void CauseIsVisibleToAnythingObservingTheToken()
        {
            using var caller = new CancellationTokenSource();
            using var source = new TimeoutOrCallerTokenSource(s_long, caller.Token);
            bool? causeSeenByObserver = null;
            using CancellationTokenRegistration _ = source.Token.Register(() => causeSeenByObserver = source.IsCancelledByCaller);

            caller.Cancel();

            Assert.True(causeSeenByObserver);
        }

        [Fact]
        public void UncancellableCaller_OnlyTimeoutCanFire()
        {
            using var source = new TimeoutOrCallerTokenSource(s_long, CancellationToken.None);

            Assert.True(source.Token.CanBeCanceled);
            Assert.False(source.Token.IsCancellationRequested);
            Assert.False(source.IsCancelledByCaller);
        }

        [Fact]
        public void CallerCancelAfterDispose_DoesNotThrow()
        {
            using var caller = new CancellationTokenSource();
            var source = new TimeoutOrCallerTokenSource(s_long, caller.Token);
            source.Dispose();

            caller.Cancel();
        }
    }
}

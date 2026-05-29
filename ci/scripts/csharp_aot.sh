#!/usr/bin/env bash
#
# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Publishes the HiveServer2 driver as a NativeAOT shared library and runs the
# interop tests against it. The native artifact only exists after `dotnet
# publish`, so these tests can't run in the normal `dotnet test` pass — they
# need the published library's path in ADBC_TEST_AOT_FIXTURE_PATH.
#
# Usage: csharp_aot.sh <workspace> <rid>
#   <rid>  e.g. win-x64, linux-x64, osx-arm64

set -ex

workspace=${1}
rid=${2}

native_project=${workspace}/csharp/src/AdbcDrivers.HiveServer2.Native/AdbcDrivers.HiveServer2.Native.csproj
tests_project=${workspace}/csharp/test/AdbcDrivers.HiveServer2.Native.Tests/AdbcDrivers.HiveServer2.Native.Tests.csproj
publish_dir=${workspace}/csharp/artifacts/aot-fixture

# 1. AOT-publish the native shared library for this RID.
dotnet publish "${native_project}" -c Release -r "${rid}" -o "${publish_dir}"

# 2. Locate the published shared library. NativeAOT names it
#    <assembly>.{so,dylib,dll} with no "lib" prefix by default; that prefix is
#    opt-in ($(_UseNativeLibPrefix)) and a future SDK could flip the default, so
#    discover the file rather than hardcoding its name. A NativeAOT publish
#    output holds exactly one shared library.
fixture=$(find "${publish_dir}" -maxdepth 1 -type f \( -name '*.so' -o -name '*.dylib' -o -name '*.dll' \) | head -n 1)
if [ -z "${fixture}" ]; then
  echo "No NativeAOT shared library (*.so/*.dylib/*.dll) found under ${publish_dir}" >&2
  exit 1
fi

# 3. Run the interop tests against the freshly published library.
ADBC_TEST_AOT_FIXTURE_PATH="${fixture}" \
  dotnet test "${tests_project}" -c Release

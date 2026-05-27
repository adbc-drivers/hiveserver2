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

# Assembles the multi-RID NativeAOT driver package. Each RID's shared library is
# built on its own OS runner by csharp_aot.sh and uploaded as an artifact named
# aot-native-<rid>; this script stages those libraries under
# runtimes/<rid>/native/ and runs `dotnet pack -p:IsPackagingPipeline=true` to
# produce a single .nupkg. NativeAOT can't cross-compile, which is why the
# libraries are gathered here rather than built in one pass.
#
# Usage: csharp_aot_pack.sh <workspace> <artifacts-dir> <out-dir>
#   <artifacts-dir>  holds the downloaded artifacts, one dir per RID:
#                    <artifacts-dir>/aot-native-<rid>/<lib>
#   <out-dir>        where the .nupkg is written

set -ex

workspace=${1}
artifacts_dir=${2}
out_dir=${3}

project_dir=${workspace}/csharp/src/AdbcDrivers.HiveServer2.Native
project=${project_dir}/AdbcDrivers.HiveServer2.Native.csproj
runtimes_dir=${project_dir}/runtimes

# Stage each downloaded native library at runtimes/<rid>/native/<lib>, the
# layout the project's pack step (and NuGet) expects. Start clean so a rerun
# doesn't carry stale RIDs into the package.
rm -rf "${runtimes_dir}"
for dir in "${artifacts_dir}"/aot-native-*; do
  [ -d "${dir}" ] || continue
  rid=${dir##*/aot-native-}
  mkdir -p "${runtimes_dir}/${rid}/native"
  cp "${dir}"/* "${runtimes_dir}/${rid}/native/"
done

dotnet pack "${project}" -c Release -p:IsPackagingPipeline=true -o "${out_dir}"

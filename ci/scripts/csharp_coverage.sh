#!/usr/bin/env bash
#
# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Runs the same CI-eligible test bucket as csharp_test.sh but with code
# coverage collection enabled (coverlet.collector + coverlet.runsettings).
# Writes the resulting cobertura XML to $1/coverage/coverage.cobertura.xml
# and prints a one-line line/branch summary suitable for $GITHUB_STEP_SUMMARY.
#
# The coverage metric is intentionally report-only: this script never fails
# based on a threshold.

set -ex

workspace="${1}"
source_dir="${workspace}/csharp/test/AdbcDrivers.Tests.HiveServer2"
coverage_out_dir="${workspace}/coverage"

mkdir -p "${coverage_out_dir}"

pushd "${source_dir}"
# Pin to a single TFM (net8.0) for coverage so the cobertura output is one
# file. coverlet.collector also doesn't play cleanly with net472 — runs only
# on Windows anyway, so it's not an option in this Linux-hosted job.
dotnet test \
  -f net8.0 \
  --filter "FullyQualifiedName~AdbcDrivers.Tests.HiveServer2.Common|Category=MockServer" \
  --collect:"XPlat Code Coverage" \
  --settings coverlet.runsettings \
  --results-directory "${coverage_out_dir}"
popd

# coverlet writes coverage.cobertura.xml under a guid-named subdirectory; pick
# the newest one by modification time and copy it to a stable path so the
# artifact-upload step doesn't have to guess the directory name. Today we only
# run one TFM (single XML); the mtime sort keeps this honest if that ever
# changes.
latest_xml=$(find "${coverage_out_dir}" -name 'coverage.cobertura.xml' -printf '%T@ %p\n' \
  | sort -nr \
  | head -n 1 \
  | cut -d' ' -f2-)
if [ -z "${latest_xml}" ]; then
  echo "No coverage.cobertura.xml produced." >&2
  exit 1
fi
cp "${latest_xml}" "${coverage_out_dir}/coverage.cobertura.xml"

# Pretty-print top-level numbers. Awk parses the line/branch rate attributes
# from the <coverage> root element without pulling in xmllint.
summary=$(awk '
  /<coverage / {
    if (match($0, /line-rate="[^"]*"/))    { lr  = substr($0, RSTART+11, RLENGTH-12) }
    if (match($0, /branch-rate="[^"]*"/))  { br  = substr($0, RSTART+13, RLENGTH-14) }
    if (match($0, /lines-covered="[^"]*"/))  { lc = substr($0, RSTART+15, RLENGTH-16) }
    if (match($0, /lines-valid="[^"]*"/))    { lv = substr($0, RSTART+13, RLENGTH-14) }
    if (match($0, /branches-covered="[^"]*"/)) { bc = substr($0, RSTART+18, RLENGTH-19) }
    if (match($0, /branches-valid="[^"]*"/))   { bv = substr($0, RSTART+16, RLENGTH-17) }
    printf "Lines: %s/%s (%.1f%%)  Branches: %s/%s (%.1f%%)\n", lc, lv, lr*100, bc, bv, br*100
    exit
  }
' "${coverage_out_dir}/coverage.cobertura.xml")

echo "${summary}"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### Driver coverage (\`AdbcDrivers.HiveServer2\`, excluding generated Thrift bindings)"
    echo ""
    echo "\`\`\`"
    echo "${summary}"
    echo "\`\`\`"
  } >> "${GITHUB_STEP_SUMMARY}"
fi

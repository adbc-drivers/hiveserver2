#!/usr/bin/env bash
#
# Copyright (c) 2025 ADBC Drivers Contributors
#
# This file has been modified from its original version, which is
# under the Apache License:
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

# Run HiveServer2 driver unit tests
source_dir=${1}/csharp/test/AdbcDrivers.Tests.HiveServer2

pushd ${source_dir}
# Run two buckets of tests in CI: the Common helpers (pure-logic), plus any
# test class tagged [Trait("Category", "MockServer")] (driven by the
# in-process mock HiveServer2 in AdbcDrivers.HiveServer2.TestServer). Tests
# that need a live Hive/Spark/Impala backend are gated behind environment
# variables and aren't picked up by either filter.
dotnet test --filter "FullyQualifiedName~AdbcDrivers.Tests.HiveServer2.Common|Category=MockServer"
popd

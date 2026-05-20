<!--
Copyright (c) 2025 ADBC Drivers Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Tooling

Local-only build helpers. The contents of this directory (other than this README) are excluded from version control via the repo `.gitignore` (`*.exe`).

## thrift.exe

Apache Thrift compiler used to regenerate the reference (unmodified) C# bindings under `csharp/test/AdbcDrivers.HiveServer2.TestServer/Thrift/gen/`. Tested with version 0.23.0.

Acquire from <https://thrift.apache.org/download> and drop the Windows binary in this directory as `thrift.exe`.

To regenerate the bindings:

```powershell
cd csharp\test\AdbcDrivers.HiveServer2.TestServer\Thrift
..\..\..\.tools\thrift.exe --gen netstd -out gen TCLIService.thrift
```

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

# AdbcDrivers.HiveServer2.TestServer

In-process HiveServer2-flavored Thrift server, intended for consumption by
ADBC HiveServer2 test projects so driver-level tests can exercise real
session/execute/fetch flows in CI without standing up an actual Hive, Impala,
or Spark server.

## Layout

```
AdbcDrivers.HiveServer2.TestServer/
├── AdbcDrivers.HiveServer2.TestServer.csproj
├── HiveServer2TestServer.cs   # HttpListener-based host wrapping a TCLIService.Processor
├── HiveServer2StubHandler.cs  # TCLIService.IAsync implementation with canned responses
└── Thrift/
    ├── TCLIService.thrift     # Vendored copy of apache/hive master, with a single-line
    │                           # `namespace netstd` directive added so the unmodified
    │                           # generated types coexist with the driver's hand-edited copies.
    └── gen/                   # Output of `thrift --gen netstd`; checked in.
```

The reference bindings live under namespace
`Apache.Hive.Service.Rpc.Thrift.Reference`, so they cannot be mistaken for —
or collide with — the driver's hand-edited copies in
`csharp/src/AdbcDrivers.HiveServer2/Thrift/Service/Rpc/Thrift/` (namespace
`Apache.Hive.Service.Rpc.Thrift`). Exercising the driver against the unmodified
generator output is one of the points of this fixture: if the driver's
hand-edits ever drift from the wire format, the tests should notice.

## Regenerating the bindings

The Apache Thrift compiler is not on PATH on most machines. Drop a Windows
build of `thrift.exe` (tested with 0.23.0) into `csharp/.tools/` — the
directory is gitignored — then from this project's `Thrift/` directory:

```powershell
..\..\..\.tools\thrift.exe --gen netstd -out gen TCLIService.thrift
```

The two warnings the generator prints about `byte` vs `i8` and
`list<byte>` vs `binary` come from upstream IDL conventions and are safe to
ignore.

The netstd generator emits trailing whitespace on many lines (notably after
`default:` in switch statements). The repo's `trailing-whitespace`
pre-commit hook will fail on those, so after regenerating, strip trailing
whitespace from the `gen/` tree before committing — e.g.:

```powershell
Get-ChildItem gen -Recurse -Filter '*.cs' | ForEach-Object {
    $c = [System.IO.File]::ReadAllText($_.FullName)
    $cleaned = ($c -split "(`r?`n)" | ForEach-Object {
        if ($_ -match "^[`r`n]*$") { $_ } else { $_ -replace '[ \t]+$','' }
    }) -join ''
    if ($cleaned -ne $c) { [System.IO.File]::WriteAllText($_.FullName, $cleaned) }
}
```

The driver's hand-edited Thrift tree had the same cleanup applied, so this
keeps the two trees consistent.

## Why this project defines NETSTANDARD2_0_OR_GREATER

The netstd-generator output is guarded by:

```csharp
#if (! NETSTANDARD2_0_OR_GREATER)
#error Unexpected target platform.
#endif
```

None of our actual target frameworks (`net472`, `net8.0`, `net10.0`) defines
`NETSTANDARD2_0_OR_GREATER` automatically, even though they all implement the
netstandard 2.0 surface. The driver project sidesteps this by hand-patching
its generated files; this project keeps the generator output pristine and
defines the symbol via `<DefineConstants>` instead. See the csproj for the
comment.

## Stub behavior

`HiveServer2StubHandler` implements just enough of `TCLIService.IAsync` to let
the driver complete an open / execute / fetch / close round-trip:

| RPC                       | Behavior |
| ------------------------- | -------- |
| OpenSession               | Echoes the requested protocol version, returns a fresh session handle. |
| CloseSession              | SUCCESS. |
| GetInfo                   | Returns canned `Test Hive` / `0.0.0-test` for `CLI_DBMS_NAME` / `CLI_DBMS_VER`. |
| ExecuteStatement          | Always returns an operation handle with `HasResultSet = true`. The query text is ignored. |
| GetOperationStatus        | Always `FINISHED_STATE`. |
| GetResultSetMetadata      | One column `_c0 BIGINT`. |
| FetchResults              | First call returns `[42]`; subsequent calls return an empty row set. |
| CloseOperation            | SUCCESS. |
| Everything else           | Throws `NotImplementedException`, which the processor turns into a `TApplicationException` on the wire. |

Tests that need richer behavior should subclass or extend `HiveServer2StubHandler`.

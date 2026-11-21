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

using System.Collections.Generic;

namespace AdbcDrivers.HiveServer2.Hive2
{
    // TableInfo is a reference type (not a struct) on purpose.
    //
    // GetObjects(depth: All) builds `catalogMap` from GetTables, then joins each
    // GetColumns row to it by (catalog, schema, table). GetTables honors the caller's
    // tableTypes filter, but the Thrift GetColumns request has no such filter, so
    // GetColumns can return columns for tables GetTables excluded, which are therefore
    // absent from the map.
    //
    // Example: GetObjects(depth: All, tableTypes: ["VIEW"]) on a schema with a view `v`
    // and a base table `t`. GetTables returns only `v`; GetColumns still returns t.*, so
    // the lookup for `t` must come back null and be skipped. (An empty tableTypes `[]`,
    // which adbc-drivers/hiveserver2#83 made mean "match none", does this for every table.)
    //
    // As a struct, that missing-key lookup (Dictionary.GetValueOrDefault) returned
    // default(TableInfo): a non-null value whose list properties are null (the `= new()`
    // initializers never ran), so `tableInfo?.ColumnName.Add(...)` threw a
    // NullReferenceException. As a reference type the missing key yields null and the
    // existing `tableInfo?.` guards correctly skip the orphan column.
    internal class TableInfo(string type)
    {
        public string Type { get; } = type;

        public List<string> ColumnName { get; } = new();

        public List<short> ColType { get; } = new();

        public List<string> BaseTypeName { get; } = new();

        public List<string> TypeName { get; } = new();

        public List<short> Nullable { get; } = new();

        public List<int?> Precision { get; } = new();

        public List<short?> Scale { get; } = new();

        public List<int> OrdinalPosition { get; } = new();

        public List<string> ColumnDefault { get; } = new();

        public List<string> IsNullable { get; } = new();

        public List<bool> IsAutoIncrement { get; } = new();
    }
}

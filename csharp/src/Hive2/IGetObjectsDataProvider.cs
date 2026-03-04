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
    internal interface IGetObjectsDataProvider
    {
        IReadOnlyList<string> GetCatalogs(string? catalogPattern);

        IReadOnlyList<(string catalog, string schema)> GetSchemas(string? catalogPattern, string? schemaPattern);

        IReadOnlyList<(string catalog, string schema, string table, string tableType)> GetTables(
            string? catalogPattern, string? schemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes);

        /// <summary>
        /// Populates column metadata into existing TableInfo entries in the catalog map.
        /// Implementers should look up matching entries by catalog, schema, and table name,
        /// then add column information (name, type, nullability, etc.) to the TableInfo lists.
        /// </summary>
        /// <param name="catalogMap">Pre-populated map of catalog -> schema -> table -> TableInfo.
        /// Entries are created by prior GetCatalogs/GetSchemas/GetTables calls.</param>
        void PopulateColumnInfo(string? catalogPattern, string? schemaPattern,
            string? tablePattern, string? columnPattern,
            Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogMap);
    }
}

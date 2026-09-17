# File-scan binding baseline

This is the source-level baseline for #1796. It records the paths observed at
`f16c7f82` plus the initial Sirius-owned Parquet bind work. It is deliberately
not a claim that a source has one bind: the re-plan branches called out below
are the gaps the `BoundScan` registry must close.

## Common transparent lifecycle

For a transparent `SELECT`, DuckDB binds the table function and optimizes its
logical plan. `sirius_optimizer_hook` copies that plan into the connection
state. `SiriusContext::OnFinalizePrepare` consumes the copy for validation and
replaces DuckDB's physical plan with `PhysicalSiriusExecution` when GPU planning
succeeds. On its first `GetData`, that operator copies its logical plan again;
if a `LogicalGet` cannot be copied, it parses, binds, and optimizes the SQL
again before constructing the Sirius physical plan.

`OnExecutePrepared` requests a fresh DuckDB bind on every subsequent prepared
statement execution when the prior physical root is `PhysicalSiriusExecution`.
That is the existing per-execute boundary, but it is not yet a #1796 scan
generation: no file-scan handle is retained across the optimizer, finalize, and
execution copies.

| Source | Bind and metadata source | Sirius physical planning | Known replay / repeat point |
| --- | --- | --- | --- |
| `seq_scan` | DuckDB catalog bind and table storage metadata | `wrap_table_scan_source` selects the DuckDB-native ingestible | Existing native plan copy/rebind behavior; out of #1796 file binding scope |
| `read_parquet` / `parquet_scan` | DuckDB Parquet `MultiFileBindData`; DuckDB expands files and reads metadata | `resolve_parquet_scan_file_paths` reads the bind file list, then the Parquet ingestible may fetch/parse footer metadata | Any non-copyable `LogicalGet` follows the transparent SQL re-plan branch |
| `iceberg_scan` | DuckDB Iceberg bind resolves to `MultiFileBindData`; Sirius's Iceberg planner also discovers metadata/delete state | Parquet ingestible plus Iceberg delete metadata | `LogicalGet` is currently non-serializable in relevant cases; finalize/execution can rebind/re-discover |
| `sirius_read_parquet` | `SiriusReadParquetBind` calls `sirius_scan_manager::describe_parquet` | `SiriusReadParquetBindData` supplies the fixed URI, footer, and row count | Its `FunctionData::Copy` preserves the footer pointer, but no query registry / serialization exists |

## Sirius-owned single-file Parquet trace

1. `SiriusReadParquetBind` validates the one URI and calls
   `sirius_scan_manager::describe_parquet`.
2. `describe_parquet` opens a datasource with the footer-probe hint, obtains or
   parses `parquet_metadata`, extracts the schema and row count, and returns a
   shared `FileMetaData` object.
3. `SiriusReadParquetBindData` retains the URI, row count, and that shared
   footer object. Its cardinality callback exposes the exact footer row count
   to DuckDB's optimizer.
4. `populate_parquet_table_info` consumes the URI and footer from that bind
   object. It does not derive the file identity from `LogicalGet` parameters.
5. `parquet_gpu_ingestible::build_file_scan_info` receives the bound footer.
   It opens the datasource with the generic hint and skips the footer probe and
   metadata-store fallback. The resulting row-group slices retain the same
   parsed footer object.

This closes only the single-file, Sirius-owned scan-to-ingestible handoff. It
does not provide version evidence, multi-file binding, serialization, query
registry ownership, or an Iceberg snapshot binding contract.

## Reproduction anchors

The following tests provide the starting coverage matrix. They should gain
counters as #1796 introduces generation-scoped instrumentation.

| Case | Existing test anchor |
| --- | --- |
| Sirius-owned S3 bind, cardinality, and footer cache reuse | `test/cpp/integration/test_s3_sql_surface.cpp`, `test/cpp/scan_manager/test_describe_parquet_s3.cpp` |
| Sirius-owned single-file Parquet physical plan | `test/cpp/planner/test_sirius_read_parquet_scan.cpp` |
| Local/multi-file/hive Parquet and Iceberg GPU behavior | `test/cpp/integration/test_gpu_execution_multi_format.cpp` |
| Prepared statement / lifecycle rebind behavior | `test/cpp/integration/test_query_lifecycle_slot.cpp` |

The required follow-up is runtime instrumentation for bind, footer/manifest
fetch, `LogicalGet::Copy`, finalize, execution re-plan, and CPU replay. Source
inspection alone cannot establish the required same-generation fetch count.

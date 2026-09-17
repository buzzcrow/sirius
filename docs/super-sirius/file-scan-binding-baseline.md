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
3. `SiriusReadParquetBindData` retains the URI, object size, row count, and
   that shared footer object. For local files it also retains the bind fd's
   size/mtime_ns evidence; its cardinality callback exposes the exact footer
   row count to DuckDB's optimizer.
4. `populate_parquet_table_info` consumes the URI and footer from that bind
   object. It does not derive the file identity from `LogicalGet` parameters.
5. `parquet_gpu_ingestible::build_file_scan_info` receives the bound footer and
   object size. It opens the datasource with the known size, skipping both the
   footer probe and S3's size-discovery HEAD; it also skips the metadata-store
   fallback. A local scan compares the evidence from its newly opened fd before
   using the bound footer. The resulting row-group slices retain the same
   parsed footer object.

The target S3 policy is response-driven: the footer-probe Range GET supplies
the bind ETag and `Content-Range` supplies the full object size. Every later
range-read response must compare its ETag with that bound value. A missing
response ETag emits a WARN and falls back to the documented immutable-object
assumption; a differing ETag is a version conflict, never a reason to refresh
the footer in place. This comparison has no separate HEAD request. The
single-file `sirius_read_parquet` path implements this propagation and
comparison. Local files similarly compare bind and scan size/mtime_ns without
retaining a fd across that interval; absent scan evidence emits a WARN and
falls back to the immutable-file convention. Multi-file binding and cache
version isolation remain future work.

This closes only the single-file, Sirius-owned scan-to-ingestible handoff. It
does not provide multi-file binding, serialization, query registry ownership,
cache version isolation, or an Iceberg snapshot binding contract.

## A1 source trace: bind through execute

The following is the current source-level trace. “Rebind” means a new DuckDB
bind/optimize attempt, not merely a logical-plan copy.

| Source / shape | Bind-time metadata | Optimizer/finalize/execution behavior | CPU replay / identity risk |
| --- | --- | --- | --- |
| Local `read_parquet` / `parquet_scan` | DuckDB produces `MultiFileBindData`, including its expanded files; Sirius later resolves that list and may fetch/parse each footer in `build_file_scan_info`. | The optimizer captures a plan copy. If `LogicalGet::Copy` fails, finalize validates a re-planned logical plan and execution replans SQL again. | Runtime fallback replays the original SQL. File expansion/footer work can therefore repeat; source identity is the DuckDB bind-data file list, not a Sirius handle. |
| S3 `read_parquet` rewritten to `sirius_read_parquet` | Sirius bind does a footer Range GET and records URI, parsed footer, size and ETag. | Copy preserves this bind payload when DuckDB can copy it. The non-copyable path still reparses/rebinds SQL at execution. | S3 CPU fallback is intentionally rejected; a rebind can nevertheless repeat the footer probe. |
| `iceberg_scan` | DuckDB binds `MultiFileBindData`; Sirius additionally invokes Iceberg metadata/delete discovery and schema gates. | The same non-copyable `LogicalGet` finalize/execution replan branch applies. | A replay/redrive can rediscover table metadata, manifests and deletes; current source identity is path/bind data, not a fixed snapshot handle. |
| View over a file source | The view body is bound as part of the outer query and follows its underlying source row above. | Optimizer capture and any SQL replan bind the view body again. | The view name is not a stable file-source identity; the underlying table function/bind data is. |
| Prepared statement, including self-join | Prepare performs the initial bind/finalize. Every execution of a Sirius-backed prepared plan requests `ATTEMPT_TO_REBIND`, so it receives a fresh bind/optimize/finalize attempt. A self-join has two `LogicalGet` consumers and must eventually receive two consumption contracts, even where their source is identical. | Each execution is the current practical generation boundary, but no generation-scoped scan registry exists. | CPU-only prepared plans can remain cached; Sirius-backed ones deliberately rebind to re-evaluate eligibility. |

`sirius_optimizer_hook` records a copy stamped with the connection planning
generation. `OnFinalizePrepare` consumes that capture; if copying a logical get
is impossible, it validates a fresh SQL replan and leaves
`PhysicalSiriusExecution` with no logical plan. On first execution that operator
again parses/binds/optimizes the cached SQL before Sirius physical planning.
`OnExecutePrepared` asks DuckDB to repeat this cycle for a reused Sirius-backed
prepared statement. These are the precise seams where `BoundScan` must replace
path-based rediscovery.

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

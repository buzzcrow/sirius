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
| `sirius_read_parquet` / `sirius_parquet_scan` | `SiriusReadParquetBind` fixes one or more URIs (including bind-time glob expansion) and calls `sirius_scan_manager::describe_parquet` | `SiriusParquetBoundScan` owns the ordered footers/evidence; bind data, Copy and in-process logical-plan deserialization share it | The connection-local registry resolves only `(generation, scan_instance_id, fingerprint)` to the original object; no rebind, path lookup, listing or network I/O occurs during deserialization |

## Sirius-owned Parquet trace

1. `SiriusReadParquetBind` validates the one URI and calls
   `sirius_scan_manager::describe_parquet`.
2. `describe_parquet` opens a datasource with the footer-probe hint, obtains or
   parses `parquet_metadata`, extracts the schema and row count, and returns a
   shared `FileMetaData` object plus an immutable scalar footer summary. The
   summary contains output schema, each row group's original row interval and
   compressed/uncompressed byte totals, complete scalar null-count evidence,
   and exact PLAIN bounds for the conservative BOOLEAN/integer subset; it
   never requires an optimizer callback to parse the footer.
3. `SiriusParquetBoundScan` retains the ordered URI list, object sizes, total
   row count, shared footer objects and per-file ETag/local size+mtime_ns
   evidence. Its bind-data wrapper exposes the exact total footer row count to
   DuckDB's optimizer. Its statistics callback reports `NOT NULL` only when
   every bound file has complete scalar null-count coverage proving zero nulls.
   It also publishes min/max only for fully covered, exact BOOLEAN/integer
   PLAIN statistics with a lossless DuckDB conversion; decimal, temporal,
   floating-point, binary and nested values remain unavailable rather than
   guessed.
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
Sirius-owned Parquet path implements this propagation and comparison for every
bound file independently. Local files similarly compare bind and scan size/mtime_ns without
retaining a fd across that interval; absent scan evidence emits a WARN and
falls back to the immutable-file convention. Multi-file binding and cache
version isolation remain future work.

This closes the explicit Sirius-owned Parquet scan-to-ingestible handoff for
schema-identical local/S3 files, URI lists and globs. Its in-process plan-copy
registry is now implemented. It does not provide Hive/`union_by_name`, an
Iceberg snapshot binding contract, split ownership, or a transparent route for
DuckDB's original file functions.

## A1 source trace: bind through execute

The following is the current source-level trace. “Rebind” means a new DuckDB
bind/optimize attempt, not merely a logical-plan copy.

| Source / shape | Bind-time metadata | Optimizer/finalize/execution behavior | CPU replay / identity risk |
| --- | --- | --- | --- |
| Local `read_parquet` / `parquet_scan` | DuckDB produces `MultiFileBindData`, including its expanded files; Sirius later resolves that list and may fetch/parse each footer in `build_file_scan_info`. | The optimizer captures a plan copy. If `LogicalGet::Copy` fails, finalize validates a re-planned logical plan and execution replans SQL again. | Runtime fallback replays the original SQL. File expansion/footer work can therefore repeat; source identity is the DuckDB bind-data file list, not a Sirius handle. |
| S3 `read_parquet` rewritten to `sirius_read_parquet` / explicit `sirius_parquet_scan` | Sirius bind does footer Range GETs and records the resolved ordered URI list, parsed footers, sizes and ETags. | Table-function serialization carries only the generation/scan handle/fingerprint and resolves the same in-process payload. | S3 CPU fallback is intentionally rejected. A new prepared execution is a new bind generation and can repeat footer probes; a same-generation plan copy cannot. |
| `iceberg_scan` | DuckDB binds `MultiFileBindData`; Sirius additionally invokes Iceberg metadata/delete discovery and schema gates. | The same non-copyable `LogicalGet` finalize/execution replan branch applies. | A replay/redrive can rediscover table metadata, manifests and deletes; current source identity is path/bind data, not a fixed snapshot handle. |
| View over a file source | The view body is bound as part of the outer query and follows its underlying source row above. | Optimizer capture and any SQL replan bind the view body again. | The view name is not a stable file-source identity; the underlying table function/bind data is. |
| Prepared statement, including self-join | Prepare performs the initial bind/finalize. Every execution of a Sirius-backed prepared plan requests `ATTEMPT_TO_REBIND`, so it receives a fresh bind/optimize/finalize attempt. A self-join has two `LogicalGet` consumers and therefore receives two scan instance IDs, even where their source is identical. | Each execution is the practical generation boundary. The Sirius-owned Parquet registry resolves same-generation copies; Iceberg and DuckDB-bound file functions still have no registry. | CPU-only prepared plans can remain cached; Sirius-backed ones deliberately rebind to re-evaluate eligibility. |

`sirius_optimizer_hook` records a copy stamped with the connection planning
generation. `OnFinalizePrepare` consumes that capture; if copying a logical get
is impossible, it validates a fresh SQL replan and leaves
`PhysicalSiriusExecution` with no logical plan. On first execution that operator
again parses/binds/optimizes the cached SQL before Sirius physical planning.
`OnExecutePrepared` asks DuckDB to repeat this cycle for a reused Sirius-backed
prepared statement. These are the precise seams where `BoundScan` must replace
path-based rediscovery.

## A2 BoundScan interface contract

`BoundScan` is the immutable, generation-local result of a Sirius source bind.
It is not a cache key, a path lookup, or a serialized copy of file metadata.
The source binder creates it only after fixing the read view and reading all
metadata required for its selected files.  Every plan copy and every physical
consumer for that bind refers to the same object.

| Contract field | Meaning and ownership |
| --- | --- |
| `scan_instance_id` | A fresh opaque ID for one logical file-source occurrence. It distinguishes the two sides of a self-join even when their source locators are equal. It is never derived from a path, function name, or traversal order. |
| `generation` | The execution-generation ID that owns this binding. A new prepared-statement execution creates a new generation and may bind a new view; copies within one execution retain this value. |
| source identity | The source adapter's canonical locator and resolved source identity (for example, a Parquet URI/listing identity or Iceberg table UUID plus metadata location). This identifies what was bound; it is not used to find a newer binding. |
| read view | The immutable source-specific view selected by bind: expanded Parquet file order and options, or Iceberg snapshot/schema/manifest view. The common model holds an adapter-owned payload rather than source-specific fields. |
| output contract | Bound output names, logical types, ordinal order, and stable field references. A field reference is adapter-defined: Parquet may use a physical-column mapping while Iceberg uses field IDs in its payload. |
| semantic options | Canonicalized options that affect data meaning, schema construction, partition interpretation, or scan behavior. Presentation-only or execution-only settings do not mutate a bound scan. |
| file plans | One immutable `FilePlan` per selected input file. The `BoundScan` owns these plans and their `shared_ptr` parsed metadata; metadata-store entries are merely reusable backing cache entries. |
| object evidence | Per-file version evidence and its strength, such as S3 ETag/size or local size/mtime_ns. Missing evidence explicitly records the immutable-object convention; it must not become a path-only cross-generation cache hit. |
| optimizer summary | Bind-derived cardinality/statistics values with `exact`, `estimate`, or `unknown` provenance and coverage. Optimizer callbacks only read this snapshot. |
| format payload | An immutable adapter payload containing format details not valid for every source: Parquet physical schema/mappings, or Iceberg manifest/delete/snapshot data. No Iceberg-only member belongs in the common structure. |

`FilePlan` is the per-file half of this contract. It has a canonical file
identity, its bound version evidence, parsed footer/format metadata, physical
column mapping, row-group layout, and any adapter-owned delete information.
Its row-group slices preserve original file row offsets even when pruning later
selects only a subset. A later A3 contract will make the exact correspondence
to `scan_plan.hpp` and `parquet_split_info` executable.

### Lifetime, consumers, and invalidation

The query execution state owns a registry keyed by `(generation,
scan_instance_id)` whose value is `shared_ptr<const BoundScan>`. A physical
table-scan consumer additionally owns an immutable consumption contract:
consumer ID, optimized projection, static predicate, and its generation/scan
handle. Thus a self-join has two consumer contracts and two scan IDs; it does
not infer either side from a shared path.

The registry may release its reference when a newer generation is created or
when the statement finishes, but it cannot destroy a binding still held by a
logical-plan copy, physical plan, split, or active reader. New version evidence
creates a new `FilePlan`/`BoundScan`; an in-flight consumer never swaps to that
new object. A version mismatch during data reads is a source-version conflict,
not a request to refresh the footer in place. Cancellation and terminal bind
failure remove the registry's own references after their consumers are gone.

Copy/deserialize carries only `(generation, scan_instance_id)`, a binding
fingerprint, and the small consumer/optimizer descriptors needed by that copy.
It resolves the same registry entry in the same connection and process. It
must perform neither network I/O nor glob/listing expansion, and it cannot be
used to restore a plan after process restart. Failure is classified as one of:

- invalid or missing scan handle;
- generation or fingerprint mismatch;
- detected source-version conflict;
- capability/semantic decline before a GPU plan is committed; or
- ordinary source/bind failure (permissions, corruption, cancellation), which
  must not be disguised as a capability decline.

The common contract deliberately leaves source mechanics with adapters:
`BoundParquetPayload` may expose Parquet schema and row-group details, while
`BoundIcebergPayload` owns snapshot, manifest, and delete relationships. The
planner and scan manager consume only the common identity/lifetime contract
plus the selected adapter payload; they never reconstruct a file list from
table-function parameters.

## A3 FilePlan and row-group-slice consumption contract

`FilePlan` is immutable bind output; a split is immutable execution work
derived from it. The scan manager may prune row groups, coalesce work, choose
prefetch ranges, and attach a reader, but it may not replace a file's footer,
physical mapping, version evidence, or original row positions. Dynamic filters
are execution input: they can reduce selected work or rows, but cannot change
the bound file set.

| Required `FilePlan` / slice property | Existing carrier | Required rule for the BoundScan path |
| --- | --- | --- |
| Canonical file identity and source evidence | `parquet_ingestible_table_info::resolved_file_paths`; the current single-file fields carry footer, size, ETag, and local version. | One `FilePlan` owns the canonical file identity and its bind evidence. The temporary single-file fields become the one-file projection of this structure; multi-file scans must never recover identity from table-function arguments. |
| Parsed footer and physical schema mapping | `parquet_file_scan_info::file_metadata`, then `row_group_slice::file_metadata`; `scan_plan` maps DuckDB output to reader data columns. | The `FilePlan` holds the parsed footer and adapter mapping. `scan_plan` remains the optimized *consumer* layout; it does not become a substitute for per-file schema/field mapping. |
| Selected row groups and original row offsets | `row_group_slice::row_group_indices`; Iceberg `build_batch_layout` computes each selected group's file offset as the prefix sum over **all** footer row groups. | A slice names only selected row-group indexes, in file order. Its provenance includes each row group's original first row, or enough immutable footer information to derive it exactly; pruning must never renumber positions. |
| Read-byte and memory estimates | `parquet_file_scan_info::row_group_entry` records output, decode-working, compressed bytes; `parquet_split_info` sums them. | Estimates travel from the bound footer into a file plan and then the slice. They are planning/accounting values, not proof that a page can be read from a different version. |
| Physical read and prefetch | `row_group_slice::datasource`; `parquet_split_info::fadvise_entries()` derives projected column-chunk ranges using `reader_options`. | A slice may own a duplicated datasource/prefetch handle while sharing its `FilePlan` evidence and footer. This transient read handle must be version-validated by the backend and cannot be used as a cache identity by itself. |
| Projection, static predicate, and partition values | `scan_plan`, `parquet_reader_options`, `parquet_split_info::partition_values`, and `disable_filter_pushdown`. | These are immutable consumer-contract values after physical planning. Static predicates may prune from bound metadata; dynamic filters remain separate runtime input. Hive values must remain attached to the file/split that produced them. |
| Delete plan and row provenance | `iceberg_gpu_ingestible::build_batch_layout` turns slices into `batch_row_run` before its delete pipeline. | Iceberg's adapter payload attaches the applicable delete plan to each file/slice. It is applied using original file row positions before SQL row predicates or dynamic filtering. Parquet's common `FilePlan` has no Iceberg member. |
| Ownership and generation | Current `parquet_split_info` has no scan/generation owner fields. | Every emitted slice carries `(generation, scan_instance_id, consumer_id)` or an immutable owner object containing them. Decode rejects a slice whose owner does not match its active consumer, preventing cross-scan or old-generation work from entering a reader. |

The current flow is therefore: `parquet_ingestible_table_info` supplies bind
and DuckDB planning inputs; `parquet_gpu_ingestible::build_file_scan_info`
creates one `parquet_file_scan_info` per file; the batch coalescer converts its
pruned `row_group_entry` values to `row_group_slice`; and `parquet_split_info`
adds shared reader options and `scan_plan` before `materialize_metadata_to_table`
reads the slices. The BoundScan migration preserves that execution pipeline,
but changes its first input from paths plus optional single-file metadata to
bound `FilePlan` objects. The old DuckDB-bound path remains independent until
the transparent-route switch is accepted.

The all-pruned case is still a real execution split: the coalescer emits one
zero-row-group slice so completion and the schema-correct empty result are
preserved. It has the same owner contract as a nonempty slice and must not
cause a fallback metadata fetch.

## A4 GPU file-source capability matrix

This matrix freezes the *Sirius-owned binder* admission policy. “Supported”
means the binder must retain the corresponding semantic input in `BoundScan`
and the physical path has a defined consumer; it does not authorize a router
to silently drop an option. “Decline” means a recognized, legal invocation
returns a classified `SiriusBindDecline` before a GPU plan is committed, so an
original-function caller can perform a whole-query DuckDB rebind. Explicit
Sirius functions remain GPU-only and report the classified reason instead.

| Source feature | Sirius-owned binder policy | Bound view / enforcement |
| --- | --- | --- |
| Parquet, one local file | Supported. | Bind fixes URI, footer, schema/mapping, row-group layout and local size/mtime_ns evidence. The scan rechecks newly opened local evidence. |
| Parquet, one S3 file | Supported. | Bind footer Range GET fixes ETag/size; every later range response compares its ETag. No separate HEAD is added. |
| Parquet, multiple explicit files | Supported after C1, not yet implemented by the current single-file bind. | Bind fixes caller order, one `FilePlan` per file, and all schemas/footers before optimizer callbacks. |
| Parquet glob/listing | Supported after C1, with listing-as-observed semantics. | Bind records the one expanded ordered list; it does not promise an atomic directory snapshot and scan never expands again. |
| Parquet Hive partitioning | Supported when DuckDB-equivalent option parsing and per-file partition values are bound. | Partition values and types belong to file/consumer contracts; filters may prune files but are not passed to a Parquet reader as physical columns. |
| Parquet `union_by_name` | Supported after C1 only when all file mappings and type reconciliation are fully bound. | Binder constructs the output schema and per-file missing/physical-column mappings. Conflicting types, a corrupt footer, or an unsupported conversion fail as source/bind errors, never as silently ignored options. |
| Parquet S3 with unavailable version evidence | Supported with explicit weak-evidence record and WARN at the response boundary. | Cache reuse is limited to the open/generation; correctness relies on the immutable-object convention. |
| Iceberg `snapshot_from_id` | Supported only for the currently safe schema/deletion subset. | Bind fixes table UUID, metadata location, snapshot ID, schema ID, manifests, data/delete files and all file footers in one view. |
| Iceberg `current`, `snapshot_from_timestamp`, or `version` selector | Decline in this phase. | The existing GPU path cannot prove that independently discovered deletes match DuckDB's selected snapshot. A later binder may support these only by resolving and retaining one snapshot view. |
| Iceberg safe, unchanged schema | Supported. | Data-file field IDs, names, types and order must pass the existing conservative schema gate using the bound footers. |
| Iceberg schema evolution, name/default mapping, field-ID gaps, missing IDs, reordered columns, or type promotion | Decline in this phase. | These are valid Iceberg shapes, but current GPU mapping cannot prove equivalent semantics. They must not be admitted merely because a name-based Parquet read happens to work. |
| Iceberg positional deletes | Supported for the safe snapshot subset. | File plans retain original row positions; delete application precedes SQL row predicates and dynamic filtering. |
| Iceberg V3 deletion vectors | Supported for the safe snapshot subset, subject to existing Puffin validation. | The bound delete payload records the data-file relationship; malformed, retired, or misbound vectors are source failures/declines according to the validation boundary, never ignored. |
| Iceberg equality deletes | Decline in this phase. | Current GPU scan does not force-project/delete-match all equality key semantics. The binder must retain the rejection until a separately tested implementation covers IDs, sequence numbers, NULLs, types and ordering. |
| Iceberg `allow_moved_paths=true` | Decline in this phase. | Existing data-path rewriting does not share identity with manifest delete paths; accepting it could drop deletes. |

Capability checking occurs after normal parameter binding/overload selection,
so malformed arguments, permissions failures, cancellation, and corrupted
metadata remain ordinary errors. The route only converts a known, legal but
unsupported semantic shape into `SiriusBindDecline`. This makes CPU replay a
new complete DuckDB bind/execute attempt rather than a mixed plan that has
lost an option or partially reused Sirius metadata.

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

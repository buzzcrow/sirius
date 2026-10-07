# R3 implementation record

Baseline: `ca294edad88a991ee00ba4cd7f17a4985ccfc5d9` (R2b), branch `r3`.
Execution plan: `/nv/cpp/workshop/sirius-ssw/1796/r3.md`, v2, 2026-10-07.
Original constraints: shared framework v0.3 sections 11.6, 14, 15.1, 18.6 and phase 4 (#1838).

## S0: API and ownership audit

The existing metadata store is an ioctx member: an independently locked map from
path to shared metadata. All three footer producers use resolve_parquet_metadata.
The raw prefetch cache separately keys file entries by raw_file_cache_id; its
asynchronous users retain the opened io_object. Changing only the footer key
would leave stale raw-byte reuse possible.

The preparation unit already retains footer, approval and datasource references.
No new scan scheduler is required. Cache eviction must drop only its own reference.
The global 1 GiB limit requested for R3 covers retained metadata estimates, not
in-flight parsing, raw prefetch, decoded batches or all process RSS. All ioctx
stores must share this limit, while preserving separate access namespaces.

### Backend capabilities on the installed dependency pin

- REST HEAD/footer probe: size and ETag are already captured, without an extra
  validation request. known-size open has size only. Existing range requests do
  not attach If-Match; adding conditional reads is Sirius-owned work.
- io_uring: the buffered fd is owned and already fstat'ed for size; the same stat
  result can supply nanosecond mtime.
- kvikio local: libkvikio 26.08.00 (`5a77056e`) FileHandle::fd(bool=false) is public;
  Sirius can fstat it without changing the dependency.
- kvikio remote: RemoteHandle exposes nbytes but not response ETag or a general
  conditional-request parameter. It cannot claim verified cross-open reuse.
  No extra network request or third-party patch is assumed.

The module-context skill's generated libkvikio docs describe an older 26.02 pin
and incorrectly label the library unused for this branch. The actual installed
26.08 public headers are the API authority. cuDF I/O, cuCascade memory and the
Super Sirius architecture/scan/memory docs were also read.

### Verification environment and measurement scope

Local GPU: RTX 2080, 8192 MiB. Existing test binary is dated 2026-09-24 and is not
accepted as evidence for the R2b commit. At S0, Pixi was synchronizing the locked
CUDA 13 environment. Subsequent builds use the synchronized environment; runtime
qualification is recorded under S5.

The user removed the large-scale TPC-H/TPC-DS end-to-end performance matrix
from R3 scope. Local cache, real-file and request-count measurements remain;
the removed matrix is neither a pending task nor an acceptance gate.

### Decisions needed if capability gaps affect integration

Size-only remote handles must not match arbitrary previous opens. Scope cache
reuse to an owned open when no stronger identity exists; record any warm-scan
cost. ETag is an opaque validator, not a sortable version. Local size+mtime is
the user-selected best-effort change detector and does not detect writes that
preserve both. Parser profile and access namespace remain separate dimensions.

S0 code/API audit is recorded; baseline build and performance qualification are
pending. No claim of passed regression or benchmark is made by this commit.

## S1: backend identity

Added the type-specific identity value and per-open generation. REST inherits
ETag identity from its existing validation_tag; absent tags have open-local
identity. Local uring/kvikio capture size and nanosecond mtime from the owned fd.
No third-party source or network request is changed.

Host-only Catch2 verification: 2 cases, 19 assertions passed using `pixi run
--as-is` and the installed compiler. Coverage includes version/kind/size changes,
local same-size mtime changes, identity after unlink while the fd is retained,
and invalid-fd reporting. At S1, full backend compilation was pending environment
setup: cuda/std/iterator was missing during dependency synchronization. The
subsequent compilation evidence is recorded below.

## S2: cache integration and retention

Added process-shared metadata retention with per-store access namespaces, 1 GiB
estimated retained charge, LRU, 30-minute idle expiry and a 60-second maintenance
worker. Maintenance is interruptible and bounded to 256 removals per lock hold;
objects are destroyed outside the manager lock. Oversized/unaccounted entries
bypass retention. Candidates do not refresh idle time or authorize a hit.

Footer lookup checks backend identity and reader profile. Raw prefetch keys use
the same identity, and REST data-range requests attach the open's If-Match tag.
Known-size/remote-kvikio opens without a tag use open-local identity. Local uring
captures size and mtime together. Parser profiles conservatively include column
selection and reader options; custom nested column schemas bypass retention.
This can increase warm parses and remains a performance qualification item.

Host cache tests: 5 cases / 28 assertions passed, including namespaces, profile
and identity mismatch, replacement order, LRU, byte cap, oversized bypass,
idle expiry without new accesses, shared ownership and concurrent insertion.
Compilation checks using actual generated compiler flags passed the metadata
store, resolver, kvikio, REST, uring, prefetch and datasource translation units;
full build and integration execution remain in progress.

## S3: retained physical evidence

Prepared file approvals now own the metadata record and captured object identity;
consumption checks that identity against the datasource. Exported footer pointers
alias the complete record's owner, so even a footer-only consumer keeps schema
evidence and accounting alive after cache replacement/expiry. Live record charge
is distinct from cache retention and decremented only at final destruction; it
is an estimate, not an allocator-enforced total process budget.

Host tests: metadata cache 6 cases / 32 assertions passed, including live-charge
retention after eviction. A separate actual Parquet-record test verifies alias
ownership, dynamic-container charge and final release. Syntax checks passed the
read-view consumption and Parquet preparation translation units.

## S4: Puffin parser separation

Footer JSON/descriptor validation and deletion-vector CRC/Roaring decoding now
consume memory spans independently of file access. The outer local reader still
owns framing reads, bounded buffers and the charged allocator; public interfaces,
validation ordering, limits and error messages are preserved. No remote transport
or third-party change was introduced.

The existing Puffin corpus passed: 18 cases / 159 assertions, including malformed
inputs and charged-memory failure behavior. This executable was compiled from
current production parser, ledger, preparation and failure-classification sources
with the generated build flags; it does not rely on the stale baseline executable.

## S5: integration and qualification

S5 closes implementation gaps found during integration:

- Default bind, pin and scan options use one canonical, complete-footer parse.
  Query projections are applied by the consuming reader, avoiding separate cache
  records/parses solely for different selected columns. Non-default schema
  options keep distinct profiles; custom nested schemas still bypass retention.
- Entries with only open-local identity do not suppress the next open's suffix
  probe. They cannot produce a cross-open hit, so using them as a candidate would
  add an unnecessary HEAD before a fresh footer fetch.
- Shared records are charged once even when retained under multiple cache keys.
  Cache-node overhead remains per entry. Removal uses a stored candidate key and
  destroys payloads after unlocking.
- Duplicate same-identity publications advance the local generation fence so a
  later-finishing, older different-identity publication cannot replace them.
- Consumption requires nonempty identity and an owner matching the footer's
  shared ownership. The test-only private codec-injection copy receives its own
  retained record, preserving that invariant without mutating cached metadata.

### Host verification

Current-source executables, built through `pixi run --as-is`:

- Identity capture: 2 cases / 22 assertions, including reopening an unlinked fd.
- Actual Parquet record ownership plus metadata-store reopening/scope checks:
  2 cases / 15 assertions.
- Cache identity, LRU, shared capacity, expiry, background maintenance, concurrent
  publication, shared charge, reentrant destruction and 10,000-operation stress:
  10 cases / 10,049 assertions passed under ASan/UBSan, including the final
  duplicate-publication ordering regression.
- Existing Puffin corpus: 18 cases / 159 assertions on both the fixed R2b parser
  source and R3, using the same compiler/dependencies/corpus.
- Existing preparation completion, unit and ledger suites: 40 cases / 759 assertions.
- Existing preparation readiness/overlap/cancellation suite: 25 cases / 252 assertions.
- Production REST reactor with the checked-in local HTTP server: 1 parameterized
  case / 24 assertions. Both HEAD and footer-suffix identities authorize the
  unchanged object; a different server version produces HTTP 412 with exactly one
  data GET and no retry. The host harness uses verbatim helpers/test code from
  test_rest_ioctx_integration.cpp; it is not a full S3 service qualification.

The full build passed with `pixi run env CMAKE_BUILD_PARALLEL_LEVEL=4 make`,
followed by a successful final-source incremental build with parallel level 8.
Final-source integration test results are recorded below. These supersede earlier
per-step environment status, but do not substitute for an R2b end-to-end baseline.

### GPU integration verification

The built `sirius_unittest` executable passed:

- Identity/cache/retention/preparation/certificate selection: 142 cases / 16,384
  assertions, including stale raw fills and strict split ownership checks.
- Parquet profile and cache selection: 82 cases / 2,112,851 assertions.

The first wider scan/S3/Iceberg run explicitly selected hidden cases as well:
unconfigured SF10 and real-AWS cases failed their prerequisites. It also exposed
an old request-count expectation that allowed footer reuse without an ETag. The
updated test checks both capabilities: with ETag, the second describe uses HEAD
and reuses the footer; without ETag, it fetches a fresh suffix with no extra HEAD.
The replacement regular suite passed: **777 cases / 2,636,731 assertions**, using
the managed local SeaweedFS HTTP/TLS service in strict mode and seed
`2090273576`. Selection was scan/S3/Iceberg/io_uring, excluding hidden and
multi-GPU cases, with prepared-statement coverage separated for the dependency
issue below. It does not count unconfigured tests as passed. The final local
generation-fence change was subsequently checked with the cache sanitizer suite
and a final incremental build/test check; it does not change query execution.

The wider run completed with 823/835 cases passing. Of 12 failed cases, ten were
unconfigured opt-in SF10/real-AWS tests, one was the request-count expectation
above, and one was the abandoned-pending context issue below. The updated
request-count case passed both sections (with/without ETag).

The final incremental build passed. The final identity/cache/retention/prepared
selection ran 19 cases / 10,308 assertions: 18 cases passed and the same abandoned
PendingQuery case failed its two zero-retention assertions. The final cache-only
ASan/UBSan run passed 10 cases / 10,049 assertions. This is an explicitly recorded
acceptance failure, not an all-green qualification.

### Synthetic cache measurement

Intel i9-7960X (16 cores / 32 threads), 62 GiB host memory, RTX 2080; installed
GCC 15.3, `-O2`, 10,000 synthetic files with actual 4 KiB
record buffers; process cache policy defaults except background maintenance off.
The workload is checked in as the hidden `[metadata_cache_bench]` case. During a
concurrent four-worker full build, first insertion took 49.4 ms and retained
charge was 50,698,900 bytes. Each warm sample performs 100,000 hits in total:

| Workers | Three samples (ms)      |
| ------- | ----------------------- |
| 1       | 63.8, 63.8, 63.4         |
| 4       | 123.3, 105.6, 114.8      |
| 8       | 115.2, 116.9, 116.6      |

This exposes serialization in the shared LRU lock. These are manager-throughput
measurements under build contention, not per-query latency or a regression ratio.
The post-build repeat is recorded below. No percentage acceptance threshold has
been invented; these measurements support local overhead analysis only.

After the build finished, the same checked-in workload through the full release
test executable measured insertion at 35.5 ms with the same retained charge:

| Workers | Three samples (ms) |
| ------- | ------------------ |
| 1       | 55.9, 28.3, 30.7   |
| 4       | 92.7, 80.1, 92.3   |
| 8       | 90.1, 85.4, 86.5   |

The concurrent-throughput concern remains; these samples do not establish an
end-to-end regression against R2b.

### Real local footer measurement

The hidden `[parquet_footer_cache_bench]` case copies the 2,294-byte, 25-row nation
fixture to 10,000 distinct local files and uses production kvikio opens and
`resolve_parquet_metadata`. File-copy setup is excluded; both passes have warm OS
page cache, and raw prefetch is not initialized. All files have the same small
schema, so this does not estimate large-footer/TPC memory usage.

| Metadata state | Hits / files   | Time (ms) | Retained charge (bytes) |
| -------------- | -------------- | --------- | ----------------------- |
| Cold           | 0 / 10,000     | 2,003.4   | 59,977,777              |
| Warm           | 10,000 / 10,000 | 380.3     | 59,977,777              |

Destroying the ioctx returned retained charge to its initial value. This test,
the synthetic benchmark and the two-section REST describe case passed together:
3 cases / 20,049 assertions. Measurements are one local run, not an SF-scale
performance acceptance or RSS measurement.

### Abandoned PendingQuery dependency reproduction

The existing `GPU prepared statements own and renew deferred reservations` test
passed in the initial focused run but failed in the wider run and an isolated
repeat with seed `2090273576`. Its execute/rebind and next-statement-cleanup
sections pass; the section dropping the connection before pending/prepared
handles can retain one reservation (117–118 descriptor bytes in these runs).
The original test and zero-retention assertions have not been weakened.

`pending_context_repro.cpp` reproduces retained ClientContext ownership using
only DuckDB APIs: `SIRIUS_DISABLE=1`, `SELECT 42`, materialized `PendingQuery`, then
drop all three user handles. All five trials still retained the context after
100 ms; explicit `ClientContext::Destroy()` released it. It was compiled with
the current generated flags and linked with the same DuckDB static libraries.
No GPU scan/footer parser is executed in this reproduction. The pinned DuckDB
materialized collector's global state owns a shared ClientContext, while an
abandoned context owns its pending executor; initializer scheduling affects when
this retention appears. This needs separate upstream/lifecycle compatibility
work, not a test sleep or a claimed R3 cache fix. No third-party source changed.

### Outstanding acceptance issues

1. Remote kvikio lacks a public ETag/conditional-read interface on the installed
   pin; REST known-size opens also carry no validator. Their open-local fallback
   preserves cache isolation but reduces warm reuse and cannot prove that multiple
   requests observe a remotely mutable object. Ordinary REST footer-probe/HEAD
   opens do capture ETag; these paths must be measured separately.
2. Raw prefetch file entries own an append-only chunk arena and io_object until
   cache teardown. Version-aware keys prevent stale-byte reuse but can accumulate
   old-version index storage and handles; size-only repeated remote opens amplify
   this. Raw data buffers have their own eviction; the unreclaimed objects are
   file indexes/chunk descriptors and handles, not every historical byte buffer.
   Metadata's 1 GiB cap does not cover those indexes. This was raised for
   a separate lifecycle/retirement task, not silently declared bounded. Safely
   erasing entries requires protecting asynchronous chunk-pointer users.
3. Local size+mtime is best effort: concurrent in-place changes and preserved
   mtime are not immutable-read guarantees. Sirius io_uring reopens its buffered
   fd via /proc/self/fd for O_DIRECT, avoiding a second pathname-resolution race.
   Third-party backend-internal handle creation is outside Sirius control.
4. Live metadata charge estimates owned records. It is not a query allocation
   permit, allocator-exact heap measurement or cap on in-flight parsing/active
   splits. The record ownership and cache-budget bounds must not be presented as
   an RSS limit.
5. The abandoned PendingQuery lifecycle case above remains failing and blocks
   full cleanup acceptance. Separating its compatibility work was proposed to
   the user; deferral has not been assumed approved. Passing focused runs do not
   override the reproducible failure.

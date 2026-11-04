# Instrumentation without hidden work

Consider a benchmark that writes a key, flushes, reads it, then inspects the store.
If its inspector scans every table and those reads enter the same numerator as
`get`, the resulting read-amplification figure does not measure the lookup. This
implementation assigns each effect an explicit purpose and uses explicit windows.

## Capture ownership

A database handle selects `off` or `counters` at open. There is no global toggle,
pause context or reset. Operation owners guard direct increments of fixed slotted
counter records. The filesystem adapter counts completed physical primitives.
Snapshots copy those fixed fields into frozen records; they do no file I/O.
Hits and misses are the source of truth for point-call counts: snapshots derive
`gets = hits + misses`, avoiding a redundant increment on every get. Capture mode
is resolved once per handle, not parsed during each effect.

Python integer increments allocate integers. The low-overhead promise is narrower:
no event objects/containers, subscriber calls, logging or timing on the capture
path, and no counter updates or clocks in disabled mode. Actual overhead is measured
by the [counter-overhead assessment](benchmarking.md#counter-overhead).

## Logical and physical meaning

| Counter family | Definition |
| --- | --- |
| `puts`, `deletes` | Public calls that completed successfully, after triggered maintenance |
| `gets`, `hits`, `misses` | Completed point calls; empty values are hits; tombstones are misses |
| `scans`, `scan_rows` | Completed scans and emitted rows; reversed intervals count as empty scans |
| `mutation_bytes` | Key+value for successful puts; key for successful deletes |
| `returned_bytes` | Key+value bytes returned by successful gets and scans |
| maintenance | Completed stage counts, input/output entries and representation footprints |
| read cache | Successful block/metadata decodes and reuses; admissions, rejections, evictions and invalidations |
| physical I/O | Successful OS read/write primitives, actual bytes, fsyncs and removals |

Physical purposes are journal, flush, compaction, point read, scan, recovery,
metadata and inspection. Directory fsync is counted under its caller's purpose.
An OS call that partially executes then raises may leave effects not represented
as a completed primitive. These are measurement counters, not a transactional
audit log. A failed operation can leave physical work with no logical success.
Its ratios are diagnostic, not valid successful benchmark scores.

Maintenance input bytes are latest framed-record sizes at flush and complete
selected-table sizes at compaction. Outputs are complete table sizes in both cases.
The aggregate is processed source-representation footprint, not homogeneous user
payload bytes or physical bytes read. Input entries count the latest memtable
records or selected-table entries; output entries count produced table records.
Use purpose-tagged physical counters when comparing transferred I/O.

## Select a window

```python
before = database.snapshot()
value = database.get(b"apple")
after = database.snapshot()
window = after.since(before)

assert window.logical.gets == 1
ratio = window.point_read_calls_per_get
# ratio.numerator and ratio.denominator remain exact integers.
# ratio.value is None when the denominator is zero.
```

Snapshots carry an opaque handle origin. Cross-origin comparisons and negative
counter differences fail. Identical snapshots do not carry temporal ordering.
Off-mode windows carry `mode=off`;
all zeros mean capture disabled, not evidence that no work occurred. Engine state
and flush thresholds do not depend on counters.

The benchmark separates setup/recovery, timed operations, final flush and inspection.
Its correctness trace and post-timing persisted-map check are outside measured
operations. Result artifacts retain raw latency samples in nanoseconds; distributions
use nearest-rank quantiles. An operation family with no samples is the explicit
`empty` variant, not a fabricated zero percentile.

## Ratios are not interchangeable

- **All-write amplification:** all written storage bytes / logical mutation bytes.
- **Table-write amplification:** flush+compaction table bytes / logical mutation bytes.
- **Foreground-read amplification:** point+scan read bytes / returned logical bytes.
- **Point read calls/get:** point-read OS calls / completed gets, excluding scans.
- **Table space amplification:** committed table file bytes / live logical key+value bytes.
- **Total space amplification:** all recognized owned file bytes / live logical bytes.

Every ratio preserves numerator, denominator and an unavailable value for zero
denominators. A zero-byte key/value is valid, so undefined denominators are not
necessarily errors. No ratio silently substitutes one or reports fake zero.

Bytes transferred through OS calls are not storage-device bytes. A warm page-cache
read can still be a counted read syscall. Metadata and index loads are included in
the purpose that caused the reader to open; cached lookups do not re-decode indexes.

Space inspection is explicit and can read table contents. Its total includes LOCK,
MANIFEST, WAL and any recognized pending/orphan files present at inspection. It
computes live size without maintaining a second map containing every live key just
for metrics. Inspection does not enter foreground byte/call counters.

## Read-cache state and work

`window.read_cache` contains cumulative work differences, guarded by the same
capture mode as other counters. Decodes count successful complete admissions,
including those not retained when caching is off, an entry is oversized, or
compaction consumes a one-pass input. Reuse
counts exact-image hits. Admissions count inserted entries still retained after
eviction; rejections count budget admission attempts that could not be retained.
Disabled caching and one-pass nonadmission do not count as budget rejections. Evictions count budget removals;
invalidations count changed-image/context or table-reclamation removals. Close
clears retained memory, not cumulative observations. Failed parsing is not a
successful decode.

`database.read_cache_info()` separately reports current capacity, conservative
retained-object charges and entry count. These gauges may decrease and cannot be
subtracted as cumulative work. The cache decision and capacity do not depend on
metrics collection. Fresh physical reads are counted even on exact-image hits.
See the [cache contract](specification.md#validated-read-cache) for memory boundaries.

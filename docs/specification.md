# Store specification

This document defines the supported behavior of LSM Store. The
[format specification](format.md) defines its persistent bytes; the
[architecture](architecture.md) explains implementation ownership.

## 1. Purpose and scope

LSM Store is a sequential, persistent byte-key/value store with self-contained
storage algorithms. Pydantic validates external configuration and persistent
metadata; individual records use immutable domain types. Python 3.12 on Linux with
a local ext4 filesystem is the supported target. The storage contract requires
advisory locks, atomic same-directory replacement, file fsync and directory fsync.
Other operating systems and filesystems are not validated.

Included: byte keys/values, mutations, point lookup, inclusive ordered scan, flush,
reopen, checksummed WAL/tables/manifest, leveled/tiered policies, optional Bloom
filtering, TOML/CLI configuration, counters, deterministic benchmarks and profiles.
Excluded: transactions, data snapshots, batches, background threads, concurrent API calls,
replication, compression, retention policies, plugin discovery and native bindings.

## 2. Public semantics

An open database owns one directory and one advisory exclusive lock. A second
owner, including in the same process, fails with `DatabaseLockedError`. Calls are
sequential; crossing threads or using an inherited post-fork handle fails explicitly.
`close()` releases resources and is
idempotent. A context manager closes even when its body raises. Close does not need
to flush: acknowledged writes are already journaled durably.

- `Database.open(path: Path, config: StoreConfig | None = None, *,
  capture: CaptureMode | None = None, create: bool = True) -> Database` creates an
  empty directory or opens an existing store. Creation must not adopt an unrelated
  nonempty directory. Existing stores require their manifest and journal. Omitted
  config uses defaults at creation; reopening adopts persisted engine settings
  and current runtime defaults. Explicit config must match persisted engine settings. `capture` can independently override
  instrumentation without changing persistent semantics. `create=False` requires
  an initialized store and never creates an absent directory.
- `put(key: bytes, value: bytes) -> None` replaces the key's latest value.
- `delete(key: bytes) -> None` records a deletion, also for an absent key.
- `get(key: bytes) -> bytes | None` returns the latest value, or genuine absence.
- `scan(start: bytes, end: bytes) -> list[tuple[bytes, bytes]]` returns unique live
  keys in lexicographic order in the closed interval. Reversed intervals are empty.
- `flush() -> None` checkpoints the memtable and performs needed maintenance.
- `read_cache_info() -> ReadCacheInfo` reports capacity, retained-object charges and
  entry count without I/O or changing eviction order.
- `snapshot() -> MetricsSnapshot` returns an immutable counter snapshot without I/O.
- `inspect() -> StoreInspection` explicitly computes current layout/space state;
  it may scan data and its I/O is classified as inspection, not foreground reads.

Empty keys and empty values are valid and distinct from absence/deletion. Exact
`bytes` inputs are required (no implicit strings, bytearray or bool coercions).
Individual key/value sizes are bounded by configured `max_record_bytes` (default
16 MiB) over their combined length. Read keys and each scan endpoint obey the same
key-size bound and the separate fixed 64-KiB key limit. Values may still use the
full remaining combined payload budget. Rejected arguments have no storage effects.
Sequence numbers are positive unsigned 64-bit integers and increase strictly.
Exhaustion fails before effects. No implicit encoding belongs to the engine.

### Laws

For a sequential history, define state as a finite map from bytes to bytes.
`put(k,v)` assigns, `delete(k)` removes, and reads observe that map.

| Law | Requirement |
| --- | --- |
| S1 | Point lookup and scan agree with the map model and with each other. |
| S2 | Flush, compaction and reopen preserve the visible map. |
| S3 | A newer deletion never exposes an older value. |
| S4 | Replay selects mutations by sequence, not filename, key order, level or list position. File ID and sequence allocation survive reopen. |
| S5 | Under the crash-only fault model, every acknowledged mutation survives recovery unless superseded by a later recovered mutation. A final unacknowledged operation may be present or absent; recovery cannot invent an unrelated operation or a partial record. |
| S6 | Metadata references only complete, durably published tables. |
| S7 | Leveled L1+ table ranges are strictly disjoint and sorted. Tiered ranges may overlap. Both policies obey the same map semantics. |

## 3. Mutation and failure state machine

The handle is OPEN, FAULTED or CLOSED. Validation occurs before a mutation starts.
For an admitted mutation: allocate sequence, append a checksummed journal frame,
fsync the journal, apply to memtable, then synchronously flush/compact if needed.
Flush triggers when either resident latest-record bytes OR journal bytes since
checkpoint reach `memtable_bytes`, so hot-key overwrites cannot grow WAL indefinitely.
A successful return acknowledges that entire public operation. If any storage or
corruption error occurs after admission, poison the handle; only close is allowed.
Asynchronous exceptions such as KeyboardInterrupt during the mutation transition
and its completion bookkeeping also poison and immediately propagate; catching an
interrupt cannot permit a stale retry.
Reopen resolves uncertain state. A failed admitted mutation can have persisted;
errors must never promise rollback. Reads encountering corruption also poison the
handle. Pure caller validation errors do not poison it.

The store's parent directory must already exist. Newly created store directories
must have their creation synchronized through that parent
before acknowledging creation. Unexpected system OSError becomes StorageError with
operation/path context and original cause. Corruption, unsupported format,
configuration, locking and lifecycle failures remain distinguishable. No library
printing or catch-all error-to-success conversion is allowed.

## 4. Durable files and commit protocol

Owned names: `LOCK`, `MANIFEST`, `WAL`, and `table-<16 hex digits>.sst`; temporary
names use the reserved `.pending-` prefix. No user paths are accepted by internal
file APIs. Unknown files in an existing store cause an error, not deletion.
Recognized orphan tables and pending files can be removed only after a valid
manifest/journal establish the live set. Do not remove or recreate the lock file.
Symlinked owned files and symlinked store roots are rejected. A directory containing
only LOCK can initialize; missing MANIFEST with any other artifacts fails with an
incomplete-initialization error. Do not guess a manifest for interrupted creation.

The manifest is the sole authority for the live table set, checkpoint sequence,
next file ID, format version and persisted engine configuration. Reopen requires
equal engine configuration; runtime instrumentation and read-cache policy may change. IDs advance
beyond all recognized live/orphan IDs before new allocation. Persist an advanced
allocator before deleting orphans, including after reopen. Never overwrite an
existing table. A checksummed manifest envelope is atomically replaced and its
directory synchronized. Missing committed files and invalid metadata fail closed.

Flush protocol:

1. Build sorted latest-per-key tables, including tombstones; write and fsync each
   uniquely created table, then fsync the directory.
2. Atomically publish/fsync a manifest containing all outputs and the new checkpoint.
3. Install that version in memory and clear the checkpointed memtable.
4. Atomically replace/fsync WAL with an empty journal and reopen its append handle.

Crashes before manifest publication recover the old version and journal. Crashes
after publication recover the new version and ignore journal entries at/below its
checkpoint. Journal reset must never precede manifest durability.

Compaction writes/fsyncs outputs, publishes/fsyncs the replacing manifest, installs
the immutable version, then removes obsolete tables and fsyncs the directory.
Only the version owner performs publication. Policy selectors are pure. A failure
during cleanup faults the handle but cannot invalidate the published version.
Tombstones are retained in v1; safe reclamation is deliberately not implemented.

Recovery loads and validates manifest, table metadata/indexes and journal; verifies
layout constraints, IDs and sequence bounds; replays records beyond checkpoint;
then cleans known orphans. A complete journal frame with a bad checksum/tag/length
is corruption. Only an incomplete final frame is a recognized interrupted append:
recovery removes that tail durably before accepting a new append. Allocate the next
sequence above both checkpoint and replay maxima. This is a named
recovery rule, not a fallback for arbitrary corruption. Replay requires strictly
increasing journal sequence numbers, including checkpointed frames.

Checksums detect accidental corruption, not malicious tampering. Crash-prefix loss
of an unacknowledged append is in the fault model. Post-acknowledgment arbitrary
truncation, media loss, lying fsync and CRC collisions are not covered by the
survival law; an incomplete tail cannot prove its cause. The store assumes
exclusive control of its directory after opening; it is not an adversarial sandbox.

## 5. Encoding and algorithms

All binary numeric fields are unsigned big-endian; JSON metadata uses decimal text.
Formats have explicit magic/version,
bounded lengths and CRC32 checksums. Unsupported formats fail explicitly.

Mutation payload: sequence u64, tag u8 (1=put, 2=delete), key length u32, value length
u32, key, value. Delete requires zero value length. A record is `Put` or `Delete`,
not a tombstone flag plus optional value. Its fixed header is 17 bytes, so the maximum
payload is `17 + max_record_bytes`. WAL magic `LSMWAL01` precedes frames: length u32,
CRC32 of those four length bytes u32, CRC32 of payload u32, payload. Validate the
complete header before interpreting an incomplete body as a torn append. Frame
overhead is 12 bytes. Table layout and parse bounds are defined in [format.md](format.md).
No pickle or third-party Bloom algorithm.

Tables contain one latest record per key, strictly increasing keys, fixed-target
blocks, and a persisted index and Bloom filter. Targets are soft: a single allowed
record may exceed block/table target; no empty blocks/tables are written. Reader
indexes/filters are cached per open reader; no sort or index decode on each lookup.
Scan and compaction use the same streaming latest-sequence merge rule. Identical
key/sequence duplicates may collapse; conflicting equal-sequence records fail as
corruption. Scans may
materialize their output, but not all irrelevant input tables. A latest-only
memtable is sufficient because data snapshots are excluded.

Bloom hashing is deterministic BLAKE2b-128 double hashing with a fixed documented
domain and bit ordering defined in `format.md`. Bit/hash counts derive from expected
entries and false-positive probability in [1e-6, 0.5]; hash count is bounded to 32.
Metadata is bounded to 64 MiB. Impossible filter sizes fail explicitly. Disabled
filtering is an explicit variant.
No false negatives are allowed for inserted keys. Filters are persisted/checksummed;
invalid filters fail closed rather than silently switching to a slower path.
Open eagerly validates table envelopes/indexes/filters, while data blocks are
validated on access. Point-read metadata/descriptor caching is bounded to 32 tables;
scan/compaction resources are O(number of participating tables), released on every
exit. Large merge fan-in can exceed the operating system's descriptor limit and
fails explicitly.

### Validated read cache

Each version set owns one LRU for metadata and decoded blocks, shared by its point,
scan and compaction readers. The default `validated` policy retains at most 1 MiB
of charged objects; `off` retains no images. This runtime policy is not persisted
and may change on reopen without changing engine identity or durable format.

A block access always reads the current complete framed bytes. Reuse requires exact
byte equality with a completely admitted image, the same block index and sequence
bound, and the same decoding limits. A mismatch discards the entry and requires the
canonical decoder and all checksum, record, ordering, sequence and index checks
before exposing any record. CRC equality alone is insufficient. Metadata reuse
still reads and checks the magic, size, footer and payload checksum, and validates
the index against the current descriptor. Invalid bytes fail, never return stale data.

The byte budget conservatively charges images, immutable decoded objects, record
keys/values, tuples, cache keys, entries and incremental LRU container storage using
`sys.getsizeof`. Shared objects may be charged multiple times. Fixed empty-owner
overhead, reader-owned indexes, transient parsing allocations, results retained by
callers and process RSS are outside this cache-retention budget. Oversized entries
are served through the same full decoder without retention; pressure evicts oldest
entries. Compaction may reuse existing entries but does not admit one-pass input
images that its successful publication will retire. Reclamation invalidates the table's entries; handle close clears the cache.
No file descriptor is borrowed from the point-reader LRU during a merge.

Leveled selects the oldest L0 file plus transitive L0 range closure and overlaps in
L1. For over-budget L1+, choose the lowest-ID table and all overlaps in the next
level. Tiered merges all files in an over-budget level into the next; higher levels
may overlap. Lowest over-budget level is handled first until no threshold is exceeded.
L0 threshold is file count; L1+ byte budget is base*multiplier**(level-1).
Both use the same executor and newest-sequence merge. File target partitioning,
record limits, thresholds and ordering are invariant owners, not policy copies.

## 6. Instrumentation contract

Instrumentation mode is fixed per handle: `off` or `counters`. No mutable global
toggle, context-sensitive pause/reset, timing, callbacks, allocated event containers,
logging or Pydantic validation belongs on the instrumented operation path.
Disabled capture performs guarded updates only: no counter mutation or clocks.
Snapshots are immutable, O(number of fixed counters), and perform no I/O.
Snapshots from one handle support explicit differences; negative/cross-origin
differences fail. Snapshots carry opaque per-handle origin and mode;
`later.since(earlier)` returns a distinct immutable MetricsWindow. Off-mode windows
report disabled capture, not measured zero work. Collection never influences
sequence, thresholds or visible data.

Logical counters: completed puts/deletes/gets/scans, point-get hits/misses, scan rows,
key+value bytes admitted by completed mutations (delete counts key), and key+value
bytes returned. Logical counters increment only after public success, including any
triggered maintenance. Empty-value gets are hits; reversed scans are completed empty
scans. Failed windows may contain physical work without logical success and are
diagnostic only, never successful benchmark scores.
Maintenance counters: completed flushes/compactions and processed input/output
entries. Input bytes mean source-representation footprint: latest framed-record
bytes for flush, complete selected-table file bytes for compaction. Output bytes
are complete produced-table file sizes. These are not physical-I/O counters or
homogeneous user-payload bytes; use purpose-tagged I/O for transferred bytes.
Physical counters belong to the filesystem effect boundary and record completed
read/write calls, actual bytes, fsyncs and removals, grouped by purpose: journal,
flush, compaction, point read, scan, recovery, metadata, inspection. Failed primitive
calls may have partial effects and are not represented as completed work; counters
are not an audit log. Inspection cannot pollute foreground counters. All counters
are cumulative for a handle; callers select windows by snapshot differences.

Named ratios preserve integer numerator/denominator and expose `value=None` when
denominator is zero; JSON represents that as null, never fake zero or division by
one. Write amplification is all written storage bytes / logical mutation bytes;
table rewrite amplification separately counts flush+compaction table writes.
Read amplification is point+scan physical bytes / returned logical bytes;
point-read calls/get is a distinct ratio excluding scans. Ratios refer to one explicit
measurement window, not mismatched setup/timing counters. Space inspection reports committed table
bytes, journal bytes, total owned bytes, live logical key+value bytes and explicit
table/live and total/live ratios. Total owned bytes includes all recognized files,
including LOCK, MANIFEST, WAL and pending/orphan files at inspection. Committed table
bytes counts only manifest references. Bytes requested from OS are not device I/O or
hardware write amplification. Logical size is computed on demand, not maintained
by a duplicate full-key map solely for metrics.

Latency collection is a benchmark client concern. Exact per-operation nanoseconds
are retained only there; no samples produce an explicit unavailable distribution,
not zero latency. Quantiles use nearest rank, `ceil(p*n)-1`, in nanoseconds.
A correctness replay is separate from timed replay. Timing includes
synchronous maintenance triggered by operations and reports final flush separately.
Metrics-off and counters runs must produce identical visible contents.

## 7. Configuration, CLI and errors

Operational errors derive from `StoreError` and preserve their category:

| Error | Meaning |
| --- | --- |
| `ArgumentError` | Invalid public key, value or operation argument; no storage effects |
| `ConfigurationError` | Invalid or incompatible configuration |
| `StorageError` | Filesystem failure, with operation/path context and a chained cause |
| `CorruptionError` | Invalid persistent bytes or metadata |
| `UnsupportedFormatError` | Recognized but unsupported format; a subtype of `CorruptionError` |
| `DatabaseLockedError` | Another owner holds the store lock |
| `LifecycleError` | Closed, faulted or incorrectly owned handle |

Missing point keys return `None`; they are not operational errors. The CLI maps
absence and error categories to the exit codes defined below.

One strict/frozen Pydantic schema owns admitted configuration. Unknown fields,
bool-as-int, numeric strings, nonfinite probabilities, invalid fractions and
unsupported variants fail at the boundary. Internal values are typed dataclasses;
generic kwargs registries and dynamically imported implementations are prohibited.

`StoreConfig`: memtable_bytes (default 1 MiB), table_bytes (4 MiB), block_bytes
(16 KiB), max_record_bytes (16 MiB), compaction tagged `leveled`/`tiered` with
max_l0_tables=4, level_bytes=16 MiB, multiplier=10 (>1), Bloom tagged `off` or
`bloom` with false_positive_rate=0.01, instrumentation `off`/`counters` (default
counters), and runtime `read_cache` tagged `off` or `validated` with
capacity_bytes=1 MiB. Positive byte/count fields; record length is the combined key/value
limit. Byte budgets are at most 1 GiB; max_record_bytes is at most 16 MiB; count
thresholds and multipliers are at most 1024. Filter probability is in [1e-6, 0.5].
Resource limits reject oversized requests, never silently weaken them.

Resolution: schema defaults, optional TOML, explicit CLI overrides, one final
validation. No environment overrides. TOML contains engine/workload settings, not
operational paths; explicit CLI paths resolve against cwd. Commands never silently create a
database while inspecting it. Invalid configuration is rejected before store
effects. `check-config` can print resolved JSON and exported JSON schema.

CLI commands: `lsm-store check-config`, `put`, `get`, `delete`, `scan`, `flush`,
`inspect`. User keys/values are UTF-8 at this adapter only; output values/keys are
hex encoded in JSON so arbitrary bytes remain representable. Diagnostic stderr is
concise by default; `--debug` exposes tracebacks. Exit 0=success, 1=store/I/O failure,
2=usage/config/argument error, 3=missing key. Stable categories and context matter;
exact prose is not an API. CLI parsing and presentation never own engine rules.

Benchmarks are installed as a separate client package in the distribution, with
`lsm-bench run`, `suite`, `report` and `profile`; they cannot be imported by core code.
The [benchmark contract](benchmarking.md) owns prepared states, warmup, unsampled
throughput, per-operation latency, repeated suites and artifact schemas.
Workload variants: uniform, hot-set and bounded Zipf over a fixed integer keyspace,
using a local seeded RNG. Config validates operation probabilities and key encoding
capacity. Every run uses a fresh directory; never delete user-chosen existing data.
Results include resolved config, seed, source identity, Python/platform, durability
mode, operation counts, counters, elapsed ns, separate finalization ns, latency
distributions and correctness status. Failed runs do not become successful output.

## 8. Ownership and extension rules

Core domain rules depend on stdlib and owned records/config only. OS handles and
effects remain in storage adapters; table encoding remains in table modules.
Compaction selectors see immutable descriptors, return plans and cannot import
Database or filesystem. Database coordinates lifecycle and delegates replay,
publication, reads and maintenance to their real owners. No public mutable levels.
CLI/benchmarks depend on the engine, never vice versa. Local architecture checks
enforce imports and the folder grammar.

Adding a policy changes its config variant, selector and tests, not publication.
Replacing Bloom hashing or a codec requires an explicit format change. Any
replacement must satisfy the existing byte vectors and semantic tests.

## 9. Validation requirements

1. Strict lint, formatting and types cover source, benchmark clients and tests.
2. Model/stateful tests cover mutation/read/scan/flush/compaction/reopen histories.
3. Tests cover sequence precedence, recovery preservation and file ID allocation.
4. Failure tests interrupt table/manifest/WAL publication, replay and cleanup;
   subprocess termination tests retain acknowledged writes without graceful close.
5. Malformed formats, incomplete final append, invalid config, missing files,
   directory ownership, locks, lifecycle and schema/CLI round trips are checked.
6. Independent counter expectations and enabled/off equivalence are tested.
7. Wheel/sdist build and clean wheel installation exercise CLI and imports outside
   the source tree, including installed typing metadata.
8. Native Betterleaks scans tracked content/history with redaction and credential
   validation disabled. Local `prek.toml` runs the canonical project checks.
9. The bounded [benchmark assessment](benchmarking.md) measures durable workloads
   and counter overhead, with correctness checks and profiling outside timing.
   Results preserve raw observations, profiles, source identity and configuration.

Performance comparisons require successful correctness checks and matching
durability settings. Measurements apply to their recorded workload and environment.

## 10. Boundary references

The implementation uses the following platform and library interfaces:

- [Linux fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html): file sync
  alone does not persist the containing directory entry; directory sync is explicit.
- [Python 3.12 os](https://docs.python.org/3.12/library/os.html): native positioned
  I/O, file/directory synchronization and atomic same-filesystem replacement APIs.
- [Python 3.12 tomllib](https://docs.python.org/3.12/library/tomllib.html): TOML parsing
  at the configuration boundary.
- [Pydantic strict mode](https://docs.pydantic.dev/latest/concepts/strict_mode/):
  strict admission still needs format-specific boundary tests; annotations alone
  are not runtime validation.

Canonical JSON in this format means Python 3.12 `json.dumps` with sort_keys=True,
separators=(",", ":"), allow_nan=False and the normal ASCII string encoding.
Manifest finite floats use that encoder's shortest round-trip representation.
Any writer must match the format vectors and these bytes; another JSON encoder
is compatible only if it produces the same encoding.

# Walkthrough: one value, two versions, one crash boundary

How can `put(b"a", b"new")` return successfully
without rewriting every old copy of `b"a"`, and still survive process death?

An LSM tree separates three responsibilities: append mutations durably, buffer the
latest memory state, and reorganize sorted immutable files later. The following
trace follows each responsibility through its owning module.

Consider a new store with a memtable large enough to hold these small records:

| Operation | Durable representation | Visible value for `b"a"` |
| --- | --- | --- |
| `put(b"a", b"old")` | Journal record at sequence 1 | `b"old"` |
| `put(b"a", b"new")` | Journal records at sequences 1 and 2 | `b"new"` |
| `flush()` | Table containing sequence 2; manifest checkpoint 2; empty journal | `b"new"` |
| `delete(b"a")` | Existing table plus journal deletion at sequence 3 | `None` |
| Close and reopen | Table plus replayed sequence-3 deletion | `None` |

The memtable keeps only the latest record for each key. The journal retains each
mutation until its replacement table has been durably published.

## 1. A mutation has meaning before it has bytes

Read `record.py`. A `Put` carries key, sequence and value; a `Delete` carries key and
sequence but cannot carry an accidental value. Empty `b""` is a real value. These
are frozen domain records, not per-operation Pydantic models.

`Database.put` validates exact byte inputs and the size bound before any effect.
It assigns the next positive sequence. If the prior value had sequence 7, a later
put has a higher sequence; filenames and level numbers do not define which wins.

## 2. The journal establishes mutation durability

Read `Journal.append` and then `framing.py`. A frame protects its length separately
from its payload. A corrupted length cannot masquerade as an incomplete body just
because it points beyond EOF.

The journal writer completes short writes and synchronizes the file. Only then is
the mutation applied to the latest-only memtable. If an interrupt or error occurs
once mutation work begins, the handle is faulted and the exception propagates.
There is no rollback promise: an unacknowledged final operation might have persisted.

The memtable replaces its old record for the same key. That saves space because
historical data snapshots are not supported. To keep overwrites from growing an
unbounded journal, maintenance triggers on either resident bytes or uncheckpointed
journal bytes.

## 3. A flush publishes a new durable version

Read `maintenance.flush_memtable`, `table.writer.write_tables`, and `VersionSet.install`.

1. Sort the latest memtable keys and partition them through the one table writer.
2. Write indexed blocks and a persisted Bloom filter, synchronize each output,
   and synchronize its directory entry.
3. Write a new manifest naming those outputs, replace the manifest atomically,
   and synchronize the directory.
4. Install that version in memory and clear the checkpointed memtable.
5. Replace the journal with an empty initialized journal only after the manifest
   is durable.

Before step 3, the old manifest plus journal still recovers the value.
After step 3, the new table recovers it even if the old journal remains. Replayed
mutations at or below the manifest checkpoint are ignored after their framing and
sequence order have been validated.

## 4. Reads separate candidates from precedence

Read `VersionSet.get` and `TableReader.get`. A key-range index chooses candidate
files; Bloom filters may rule out absent keys. A validated block index narrows the
read to one block. That block's checksum and record ordering are checked when read.

Across candidates, the greatest sequence wins. Key order and file placement do not
establish mutation order.
Leveled L1+ ranges are disjoint and can be searched with binary search; L0 and tiered
levels can overlap. An on-disk tombstone suppresses older puts.

Read `merge_latest` next. Range scans, inspection and compaction share this
streaming newest-version rule. They do not each invent their own deduplication.
The final public scan omits tombstones and materializes only its emitted rows.

## 5. Compaction is selection plus one executor

Read `compaction/plan.py` and `compaction/leveled.py` before `maintenance.py`.
Selectors receive immutable table descriptors and return an immutable plan.
They do not receive the database or delete files.

The executor merges selected streams, writes replacement tables, publishes the new
manifest, and only then reclaims old inputs. A crash after publication can leave
orphans; recovery removes only recognized files not referenced by a valid live set.
The ID allocator is advanced durably before orphan cleanup.

Tiered selection merges an over-budget level without requiring disjoint ranges.
Leveled selection also includes overlaps in the destination. Both use the same
writer, sequence rule and publication path. Tombstones are retained; safe removal
requires proving that no older value can remain outside the selected inputs.

## 6. Observe without redefining the operation

Read `metrics/counters.py` and `metrics/snapshot.py`, then the benchmark runner.
The engine increments named counters but never times operations. A snapshot copies
fixed fields and performs no I/O. An explicit inspection may scan files and is
classified separately. No metric controls compaction or memory state.

The benchmark first checks a deterministic trace against a simple independent map,
then times a fresh store and validates the persisted final map afterward. A second
map in a test is an independent oracle; a second full-key map inside the engine
solely for accounting would be unnecessary state.

## Validation limits

The tests include independently constructed format bytes, generated operation
histories, malformed data, interrupted publication, short I/O, process death,
lock ownership and deliberately broken benchmark controls.

These are bounded checks, not proof of every storage stack. The crash-survival law
assumes truthful fsync and atomic replacement on the supported local filesystem.
It does not repair arbitrary post-acknowledgment media truncation or authenticate
against an attacker modifying a checksummed file.

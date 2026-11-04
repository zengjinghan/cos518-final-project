# Durable format version 1

This document defines the persistent byte format used by LSM Store.

All binary integer fields are unsigned big-endian; JSON integers use decimal text.
CRC means the unsigned result of standard
IEEE CRC32 (`zlib.crc32(data) & 0xffffffff`). Checksums detect accidental damage;
they neither authenticate data nor prevent malicious rewriting. Exact byte vectors
and corruption tests accompany the implementation. Unsupported formats fail explicitly.

## Mutation payload

| Field | Width | Meaning |
| --- | --- | --- |
| sequence | 8 bytes | Positive u64 mutation sequence |
| tag | 1 byte | 1 = put, 2 = delete |
| key length | 4 bytes | Key bytes following the header |
| value length | 4 bytes | Value bytes following the key |
| key | key length | Arbitrary bytes; empty is valid |
| value | value length | Arbitrary bytes; empty is valid for put |

The fixed payload header is 17 bytes. Keys are at most 64 KiB; values retain the
combined configured record bound. Delete requires value length zero. A decoded
payload must consume exactly its declared bytes, use a known tag and nonzero
sequence, and satisfy `key_length + value_length <= max_record_bytes`. No padding
or trailing bytes are admitted. Its maximum length is `17 + max_record_bytes`,
which must fit u32. Bytes supplied to a public mutation are validated before encoding.

## Protected frame

| Field | Width | Meaning |
| --- | --- | --- |
| payload length | 4 bytes | Number of payload bytes |
| length CRC | 4 bytes | CRC of the preceding four length bytes |
| payload CRC | 4 bytes | CRC of the entire payload |
| payload | payload length | Contents defined by the enclosing file |

The frame overhead is 12 bytes. Validate the complete 12-byte header's length CRC
and allowed length range before using that length for a read or allocation. A
payload checksum mismatch is corruption, not an interrupted append.

## Write-ahead log

`WAL` starts with the eight bytes `LSMWAL01`. Each following protected frame holds
exactly one mutation payload. Allowed frame payload lengths are
`[17, 17 + max_record_bytes]`. Sequences increase strictly, including records at or
below the manifest checkpoint. A full unsupported eight-byte magic is a format
error. A missing/truncated magic is corruption, including in an empty journal.

During recovery only, EOF after a valid magic with fewer than 12 remaining header
bytes, or EOF during the payload of a valid complete header, is an interrupted
final append. Remove that entire frame's partial tail durably before another append.
A complete header with bad length CRC or inadmissible length is corruption even
when its payload is absent. Complete malformed records are never skipped.

The acknowledged-write guarantee assumes a crash can interrupt the final append,
not arbitrarily truncate previously synchronized bytes. A checksum cannot identify
arbitrary truncation as distinct from an interrupted final operation. This limit is
part of the failure model, not a promise to repair damaged acknowledged history.

## Sorted table

Each `table-<16 lowercase hex digits>.sst` contains:

```text
LSMSST01 | protected block frames... | canonical metadata JSON | footer
```

The first eight bytes are `LSMSST01`. At least one nonempty block follows. A block
payload concatenates mutation payloads without individual frame headers. Each
mutation's key/value lengths establish its end. All block payload bytes must be
consumed. The table contains one latest mutation per key, including tombstones,
and keys increase strictly within and between blocks. Block targets are soft: one
admitted record may occupy an oversized block. A writer must reject unsorted or
duplicate-key inputs, not silently reorder or drop them.

The maximum block payload is `max(block_bytes, 17 + max_record_bytes)`; a record
that would exceed a nonempty block's target starts the next block. Thus the sum
never exceeds this bound. Writers partition tables by estimated encoded record
bytes with a soft `table_bytes` target; table metadata/filter overhead is additional
and a single record may exceed that target. A second conservative partition budget
starts at 4096 bytes and adds `8 * len(key) + 1024` per record. It bounds repeated
hex endpoints, block index fields and Bloom metadata even with one record per
block. Before either nonempty chunk would exceed its budget, start a new table;
the metadata budget may not exceed 64 MiB. The 64-KiB key limit ensures one record
always fits that budget. Empty tables are not written.

### Metadata JSON

The metadata is UTF-8 JSON with object keys sorted lexicographically, separators
`,` and `:`, no whitespace, no nonfinite numbers, and no non-ASCII escapes needed.
All byte strings are even-length, lowercase hexadecimal strings. Unknown fields,
duplicate keys, noncanonical encodings and strict-schema violations are rejected.
Metadata is limited to 64 MiB and validated against the actual physical file size
before it is read. It is decoded/validated once per opened table, not per record.

The exact schema is:

```text
{
  "version": 1,
  "table_id": positive u64,
  "level": integer in [0,64],
  "min_key": hex bytes,
  "max_key": hex bytes,
  "max_sequence": positive u64,
  "entry_count": positive integer,
  "blocks": [
    {
      "min_key": hex bytes,
      "max_key": hex bytes,
      "offset": integer >= 8,
      "length": integer >= 29,
      "entry_count": positive integer
    }, ...
  ],
  "bloom": {"kind": "off"}
       or {"kind": "bloom", "bit_count": integer >= 64,
           "hash_count": integer in [1,32], "bits": hex bytes}
}
```

The notation above describes types; it is not literal JSON. The table descriptor's
ID, level, ranges, sequence, entry count and physical size must agree with the
manifest. Block lengths include their 12-byte frame headers. Offsets are absolute
from file start; frames are contiguous starting at offset 8 and ending exactly at
metadata start. Each block's key range is nonempty, ordered, and strictly below
the next block's range. Entry counts sum to the table count. Table endpoints equal
the first/last block endpoints. A block entry count cannot exceed its payload
length divided by 17. These checks precede any block lookup. The reader caches the
validated immutable index, minimum-key search array and filter.

### Footer

The final 20 bytes are:

| Field | Width | Meaning |
| --- | --- | --- |
| metadata length | 4 bytes | Metadata JSON bytes |
| metadata CRC | 4 bytes | CRC of metadata JSON |
| footer CRC | 4 bytes | CRC of metadata length + metadata CRC + footer magic |
| footer magic | 8 bytes | `LSMEND01` |

Validate fixed footer magic and CRC before using metadata length. Metadata start
is `file_size - 20 - metadata_length`; it must leave the initial magic and at
least one minimum-sized block frame. Validate metadata CRC before parsing JSON.
No trailing bytes are admitted. A table is created exclusively, synchronized, and
published through the manifest protocol; the table writer never publishes metadata.

Block payload CRC and record/range/count/ordering constraints are validated when
that block is accessed. Opening a table validates the file envelope, metadata,
index and filter, not every data block. Short required block reads are corruption;
a missing committed file is a contextual StorageError. Neither is an absent-key
result. Lazy validation is not a full-file integrity claim.

## Bloom contract

For `n >= 1` distinct table keys and configured probability `p` in `[1e-6, 0.5]`:

1. Compute `raw = ceil(-n * ln(p) / (ln(2) ** 2))`.
2. Set `m = 8 * ceil(max(64, raw) / 8)` bits.
3. Set `k = min(32, max(1, floor((m / n) * ln(2) + 0.5)))` hashes.
4. Reject a filter larger than 16 MiB before allocation; do not weaken its
   requested probability or substitute an exact set.

The persisted `m`, `k` and bits, not floating-point recomputation, define a filter
for readers. `m` is a multiple of eight and equals `8 * len(bits)`. Empty filters
cannot be attached to nonempty tables. Disabled filtering is exactly `{"kind":"off"}`.

For a key, compute
`blake2b(key, digest_size=16, person=b"lsm-store-bloom")`. BLAKE2's standard
personalization zero-padding applies. Interpret digest bytes `[0:8]` as big-endian
`h1`, and bytes `[8:16]` as big-endian `h2`, then set `h2 = h2 | 1`.
Hash indices are `(h1 + i*h2) % m` for `i` from zero through `k-1`, using integer
arithmetic without u64 wraparound. Byte index is `index // 8`; bit mask is
`1 << (index % 8)` (least significant bit first). Insertion ORs all masks;
membership requires every mask. Readers reject malformed dimensions and rely on
the metadata checksum before using a filter to exclude a key.

These formulas choose a conventional approximation, not a proven exact finite-set
false-positive bound. Correct construction has no false negatives for inserted
keys. Golden bit vectors bind hashing/endian/bit conventions independently of a
particular writer. Probability experiments are evidence, not a correctness proof.

## Owned interfaces

`TableInfo` is an immutable dataclass with `table_id`, `level`, `min_key`, `max_key`,
`max_sequence`, `entry_count`, `size_bytes`. It describes one committed immutable
table; filesystem names derive exclusively from its ID.

`TableReader(store, info, config, *, purpose=RECOVERY)` owns one reader's validated metadata
and exposes `get(key, purpose)`, `iter_range(start, end, purpose)`,
`iter_all(purpose)`, and `close()`. It does not choose sequence precedence across
tables or mutate the version. The storage adapter owns OS handles/effects and
purpose-tagged I/O accounting. Every block access reads current bytes. An optional
version-owned exact-image cache reuses complete validation only for identical bytes
and admission context; it does not alter the durable format or skip physical reads.
A shared merge owner resolves cross-table versions.

`write_tables(store, records, level, allocate_id, config, *, purpose=FLUSH)` returns an immutable tuple
of `TableInfo`. Input is the shared merge's strictly increasing, latest-per-key
stream. Partitioning belongs here, so flush and compaction cannot disagree. The
writer creates/synchronizes table files but never publishes a manifest or deletes
its inputs. Failed writes may leave recognized orphan files for recovery.

## Manifest envelope

`MANIFEST` starts with eight bytes `LSMMAN01`, followed by exactly one protected
frame. Its payload is canonical compact sorted-key UTF-8 JSON, bounded to 64 MiB.
The full file has no trailing bytes. The schema is exactly:

```text
{
  "config": <StoreConfig engine fields, excluding instrumentation and read_cache>,
  "tables": [
    {"table_id": positive u64, "level": integer in [0,64],
     "min_key": lowercase hex, "max_key": lowercase hex,
     "max_sequence": positive u64, "entry_count": positive integer,
     "size_bytes": positive integer}, ...
  ],
  "checkpoint_sequence": u64,
  "next_file_id": integer in [1, 2**64]
}
```

Table IDs are unique and lower than next_file_id; 2**64 is only an exhausted
allocator sentinel, never a usable file ID. Key ranges are ordered and obey the
64-KiB key bound. The checkpoint equals the greatest persisted table sequence
(or zero for an empty live set); tombstones are retained, so the greatest committed
mutation remains represented. The version owner separately checks policy layout,
actual table sizes/envelopes and table sequence maxima.

Manifest encoding uses the same protected frame owner as journal and table blocks.
Decode validates exact file size, magic, header protection/bounds, payload checksum,
canonical JSON and strict schema before constructing owned values. Missing required
files are contextual StorageError; malformed bytes are CorruptionError. Both fail
closed. Metadata checksums are not authentication.

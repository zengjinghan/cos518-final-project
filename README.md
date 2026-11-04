# LSM Store

A persistent byte-key/value store for learning how a log-structured merge tree
works. It implements a write-ahead log, indexed sorted tables, Bloom filters,
version resolution, and leveled or tiered compaction.

The engine is sequential and self-contained. Configuration and persistent metadata
use strict Pydantic models; storage records use immutable domain types.

## Install

Python **3.12** on Linux with a local ext4 filesystem is the supported target.
Install from the source directory with native uv:

```console
uv sync --locked --python 3.12
```

Pydantic is the only runtime dependency. The lockfile also pins the development
and validation tools. See [Development](docs/development.md) for checks and builds.

## Use the Python API

The parent directory must exist. A store owns its directory exclusively; a new
store requires an absent or empty directory.

```python
from pathlib import Path

from lsm_store import Database

with Database.open(Path("demo-store")) as database:
    database.put(b"apple", b"red")
    database.put(b"banana", b"")  # An empty value is present, not missing.
    start = database.snapshot()

    assert database.get(b"apple") == b"red"
    assert database.scan(b"a", b"z") == [(b"apple", b"red"), (b"banana", b"")]
    database.delete(b"apple")
    window = database.snapshot().since(start)
    assert window.logical.gets == 1

# Acknowledged mutations are durable without an explicit flush.
with Database.open(Path("demo-store"), create=False) as database:
    assert database.get(b"apple") is None
    assert database.get(b"banana") == b""
    database.flush()
    assert database.inspect().live_keys == 1
```

`get` returns `None` for absence. `scan` returns sorted live entries within inclusive
bounds. Empty keys and values are valid. `snapshot` copies counters without I/O;
`inspect` explicitly scans logical contents and reports storage occupancy.

A handle belongs to its opening process and thread. A failure or interruption
during a mutation faults the handle: close it and reopen to recover. The mutation
may already be durable despite the exception; failure does not promise rollback.
Corruption is an error, not a missing key.

## Configure and run commands

```console
uv run --locked lsm-store check-config --config examples/teaching.toml
uv run --locked lsm-store check-config --schema
uv run --locked lsm-store put cli-store apple red --config examples/teaching.toml
uv run --locked lsm-store get cli-store apple --capture off
uv run --locked lsm-store scan cli-store a z
uv run --locked lsm-store flush cli-store
uv run --locked lsm-store inspect cli-store
```

The CLI example uses a separate directory from the Python example. Configuration
resolution is schema defaults, TOML, explicit CLI overrides, then strict validation.
Unknown fields and implicit coercions are rejected.

| Setting | Default | Meaning |
| --- | --- | --- |
| `memtable_bytes` | 1 MiB | Flush threshold for resident records or uncheckpointed journal bytes |
| `table_bytes` | 4 MiB | Soft encoded-record target per table |
| `block_bytes` | 16 KiB | Soft payload target per table block |
| `max_record_bytes` | 16 MiB | Maximum combined key/value length |
| `compaction.kind` | `leveled` | `leveled` or `tiered` selection |
| `bloom.kind` | `bloom` | Persisted Bloom filter or `off` |
| `instrumentation` | `counters` | Fixed per-handle `counters` or `off` capture |

See [examples/teaching.toml](examples/teaching.toml) for nested policy/filter settings
and the [specification](docs/specification.md) for all bounds. On reopen, omitted
engine settings adopt the persisted configuration. Explicit settings must match;
`--capture` can change independently. `--memtable-bytes` overrides that engine field.
There are no environment-based overrides.

CLI keys and values are UTF-8. JSON output encodes stored bytes as hexadecimal.
Library callers use bytes directly. Commands other than `put` do not create stores.
Errors go to stderr; `--debug` includes a traceback.

| Exit code | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Storage or lifecycle failure |
| 2 | Invalid arguments or configuration |
| 3 | Missing point key |

## Measure a workload

Create the result parent directory if needed. Existing result files and suite
directories are not overwritten.

```console
mkdir -p results
uv run --locked lsm-bench run --config examples/benchmark.toml --output results/run.json
uv run --locked lsm-bench suite --config examples/suite.toml --output results/speed
uv run --locked lsm-bench report results/speed/summary.json
```

Benchmarks have explicit load, warmup, measurement, final-flush and validation
phases. Throughput mode uses one clock pair for the trace; latency mode captures
puts, deletes, gets and scans separately. Read-hit scenarios preload data before
timing. Repeated suites retain raw runs, source, environment and robust variation
summaries. A separate oracle checks reference responses, measured logical counters
and the persisted final map. See [Benchmarking](docs/benchmarking.md) for workload definitions,
counter-overhead measurement, profiling, and interpretation.

## Documentation

| Document | Purpose |
| --- | --- |
| [Walkthrough](docs/walkthrough.md) | Follow a mutation through append, flush, lookup and recovery |
| [Specification](docs/specification.md) | Public semantics, invariants, failures and supported scope |
| [Persistent format](docs/format.md) | Exact bytes, checksums and validation order |
| [Architecture](docs/architecture.md) | Module responsibilities and dependency direction |
| [Metrics](docs/metrics.md) | Counter units, windows and amplification ratios |
| [Benchmarking](docs/benchmarking.md) | Reproducible measurements and their limits |
| [Development](docs/development.md) | Local checks, tests and distribution verification |

## Limits

Durability assumes truthful file/directory fsync and atomic replacement. Checks
cover interruption and process death, not arbitrary hardware failure or media loss.
Other platforms and filesystems are not validated.

Keys are limited to 64 KiB. Combined key/value bytes are limited by
`max_record_bytes`, at most 16 MiB. Single records may exceed soft block/table targets.
The point-reader cache holds at most 32 tables. A separate version-owned validated
read cache defaults to a 1-MiB retained-object budget, configurable or explicitly
off through `[read_cache]` in store TOML. It still reads current bytes on every access. Scan and compaction descriptors
scale with participating tables and can reach operating-system resource limits.

Tombstones are retained. Data snapshots, transactions, background maintenance,
concurrent API calls, replication, compression and native bindings are not supported.

## License

[MIT](LICENSE.txt).

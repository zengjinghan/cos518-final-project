# Benchmarking

Benchmarking is a supported client of the public database API. The commands are
`lsm-bench run`, `suite`, `report`, `profile` and `check-config`. Configuration and result artifacts
are strict, versioned schemas. Run and suite artifacts use format version 2.
Unknown fields and unsupported variants fail; there is no compatibility reader
for other result formats.

## Measurement contract

Every run has six distinct phases:

1. **Prepare:** create a private store and load deterministic records if requested.
   `empty` loads nothing; `resident` retains the load in the journal/memtable;
   `table` flushes the load. Close and reopen before measurement.
2. **Warm up:** replay a separate deterministic trace outside measurement.
3. **Measure:** execute a fixed, pregenerated trace sequentially.
4. **Finalize:** flush remaining records, with separate time and counters.
5. **Inspect:** report physical layout and live contents outside measurement.
6. **Verify:** reopen and compare the persisted final map with an independent oracle.

A separate same-configuration reference replay checks every operation before the
measurement run. It includes the same load and warmup. When counters are enabled,
measured logical counts, hits, misses, returned bytes and scan rows must also match
the oracle. Timed responses are not individually oracle-checked inside the loop.
All mutations retain journal fsync; neither measurement mode weakens durability.

| Mode | Timing | Output |
| --- | --- | --- |
| `throughput` | One wall-clock pair and one process-CPU pair for the whole trace | Operations/second, CPU time and counters; no latency samples |
| `latency` | Separate per-operation wall clocks, plus whole-trace wall/CPU clocks | Raw samples and nearest-rank p50/p95/p99 for puts, deletes, gets and scans |

Throughput includes dispatch and operation-triggered synchronous maintenance.
Preparation, warmup, explicit final flush, inspection and validation are excluded.
Throughput including final flush is reported separately. Latency-mode throughput
includes sampling overhead and must not be presented as unsampled throughput.
Scans count as one operation; scan rows and bytes are reported separately.
Elapsed minus CPU time is not an I/O-time measurement.

## Configure one run

```console
mkdir -p results
uv run --locked lsm-bench check-config --config examples/benchmark.toml
uv run --locked lsm-bench run --config examples/benchmark.toml --output results/run.json --scratch /tmp
uv run --locked lsm-bench report results/run.json
```

Use `check-config --kind suite` for a suite, or add `--schema` to export its JSON schema.
The output parent and scratch parent must exist. Outputs are never overwritten.
Scratch uses Linux/local ext4; its actual filesystem, CPU model, affinity, Python,
dependency versions, clock resolution and GC state are recorded without hostnames
or user paths. Temporary stores are owned and removed by the benchmark.

Top-level run settings select `initial_state`, `warmup_operations` and `measurement`.
`store` uses the engine schema; `workload` defines counts, key/value sizes, seed,
operation probabilities, scan span and access distribution. The remaining
probability after puts, gets and scans selects deletes.

The load has `keys` records with numeric keys `index * key_stride`. Measured keys
are `key_offset + index * key_stride`, encoded big-endian in `key_bytes` bytes.
Thus stride 2 and offset 0 give populated hits; offset 1 gives interleaved missing
keys, not an entirely out-of-range miss test. Scans span at most `scan_length`
keyspace positions, clipped at the end. Values and traces are deterministic.
The measured seed is configured directly; warmup uses seed+1 and load uses seed+2,
modulo 2**64. A trace digest identifies the exact load/warmup/measurement sequence.

A resident workload must be read-only and its complete framed load must fit below
the memtable/journal flush threshold. Oversized loads fail configuration validation,
not silently become table reads. Table hit cases warm indexes, OS pages and the configured validated read cache.
The resolved runtime policy is retained in each result. This protocol does not drop operating-system caches
or claim cold-device or sustained beyond-cache performance.

## Repeated suites

```console
uv run --locked lsm-bench suite --config examples/suite.toml --output results/speed --scratch /tmp
uv run --locked lsm-bench report results/speed/summary.json
```

Each run records `read_cache` capacity, retained-object charges and entry count
immediately after measurement, before final flush or inspection. Its counter windows
also retain successful block/metadata decodes and reuses, admissions, rejections,
evictions and invalidations. These are work counters; occupancy is a separate gauge.
Set `[store.read_cache] kind = "off"` for a decode-every-access control, or
`kind = "validated"` with `capacity_bytes` for a bounded policy. Inside a suite use
`[cases.scenario.store.read_cache]`. Do not pool different runtime policies or
interpreter builds. Cache-resident results do not establish beyond-cache performance.

A suite owns named scenarios, measurement modes, capture modes, repetitions,
scheduling seed and a between-run budget. Every scenario has a complete validated
configuration; there is no hidden inheritance or unvalidated override registry.
Runs execute sequentially in one Python process, each with fresh stores and handles.
Within each repetition, jobs are shuffled reproducibly. Repetitions use the same
trace to distinguish timing variation from workload variation.

Every completed run is saved immediately. The summary retains its relative file
name and reports median, minimum, maximum and median absolute deviation of
throughput. Latency summaries report distributions of per-run quantiles, not
percentiles of pooled samples. No best-run selection or confidence-interval claim
is made. Disabled counters are labeled uncollected, not zero physical work.
`source.zip` preserves both benchmark and engine Python source/typing markers and
the required license notice.

The suite creates its output directory exclusively. A budget stop returns nonzero
and retains completed runs with status `budget_exhausted`; an unexpected failure
records `failed` and propagates. The budget is checked between complete runs and
cannot interrupt a blocked syscall. Partial groups disclose completed/expected
repetitions and are not complete comparisons.

### Reference suite and decision rule

The shipped suite has eight scenarios: durable puts under leveled/tiered policies,
resident get hits, warm-table get hits, interleaved get misses, mixed workloads
under both policies, and bounded scans. It uses both measurement modes, counters
and three repetitions: 48 runs, with one 300-second between-run budget.

Put traces have 3,000 operations over 4,096 possible keys and start empty. Resident
hits use 256 loaded keys and 200,000 gets. Table hit/miss cases use 512 loaded keys
and 20,000 gets. Mixed traces use 512 loaded keys, 2,000 operations and a
60/30/5/5 put/get/scan/delete mix. Scan traces use 512 loaded keys and 1,000 scans.
All values are 64 bytes and keys 8 bytes. Table-oriented cases use 8-KiB memory/table
targets, 1-KiB blocks, L0 threshold 4, 64-KiB level budget and multiplier 4.
Resident capture uses a 1-MiB memtable. Warmup is explicit in the TOML.

Acceptance requires every correctness/control check to pass, the full prescribed
matrix to complete within budget, retained source/settings/raw artifacts, and
separate reporting of sampled and unsampled timings. No minimum throughput is
prescribed: the question is the measured capability and variability of this
implementation on the recorded host. This is not an external-engine comparison or
an optimization speedup study. A failed control invalidates its performance result.
Do not change counts, budget or settings after inspecting results to obtain a pass.

## Counter overhead

```console
uv run --locked lsm-bench suite --config examples/capture.toml --output results/capture --scratch /tmp
uv run --locked lsm-bench report results/capture/summary.json
```

This uses the same resident/table hit scenarios as the reference suite, throughput
mode only, with `off` and `counters`, nine matched repetitions and a 120-second
between-run budget: 36 runs. The primary overhead estimate is the median paired
elapsed-cost difference per operation, with median absolute deviation. Relative
cost differences and every mode/repetition remain available. Pairs require equal
trace digests and operation counts. Incomplete pairs are never synthesized.
Negative differences are retained as observed noise, not evidence of acceleration.
No fixed overhead pass threshold is imposed by this descriptive assessment.

## Profile separately

```console
uv run --locked lsm-bench profile --config examples/benchmark.toml --output results/profile --scratch /tmp
```

Only the measured trace is profiled; setup and oracle work are outside the profile.
The directory contains a marked result, `profile.prof` and `profile.txt`. Profile
filenames are reduced to basenames before export, so personal checkout paths are
not embedded in the artifacts. Profiled
throughput is diagnostic and excluded from ordinary suite summaries. Preserve
matching workloads and durability when investigating a cost. CPU profiles alone do
not explain blocking storage time. Do not run tests, builds or profilers alongside
accepted throughput measurements.

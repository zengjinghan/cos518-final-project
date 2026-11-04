# Development

Use Python 3.12 and native uv, prek and Betterleaks. The project lockfile pins the
Python dependencies. Local commands own validation; no hosted CI is required.

## Local checks

```console
uv sync --locked --python 3.12
uv run --locked ruff format --check src tests tools
uv run --locked ruff check src tests tools
uv run --locked pyright
uv run --locked pytest
prek -c prek.toml run --all-files
```

The five local hooks run formatting, lint, strict typing, behavior/architecture tests
and native Betterleaks scanning. Secret findings are fully redacted and live
credential-provider validation is disabled. Scan Git history separately with:

```console
betterleaks git --redact=100 --validation=false --no-banner .
```

The hook runner requires a Git checkout. Individual validation commands and the
distribution verifier also run from an extracted source distribution.

## Test organization

| Directory | Contract |
| --- | --- |
| `tests/unit` | Record and format boundaries, strict configuration, filter vectors and metric arithmetic |
| `tests/integration` | Public map semantics, CLI, benchmark validation, failure transitions and process ownership |
| `tests/architecture` | Package grammar, import direction and library effect boundaries |

Generated operation histories compare both compaction policies with an independent
map through flush and reopen. Independent byte vectors check encoding, framing and
Bloom hashing. Failure tests cover short I/O, incomplete appends, publication order,
interrupted synchronization, process exit, lock ownership and resource release.
Benchmark controls inject incorrect reads and dropped mutations and require rejection.
The README example is executed, and local documentation links are checked.
Benchmark controls cover clock-free throughput loops, populated read states, phase
counters, raw artifact round trips, partial budgets and paired capture comparisons.

These checks exercise the stated contract. They do not establish arbitrary hardware
power-loss behavior, unsupported platforms or adversarial authenticity. See the
[specification](specification.md) for the fault model.

## Build and verify a distribution

```console
uv run --locked python tools/check_distribution.py
```

The verifier builds a source distribution, then builds its wheel from a fresh
extraction in a private temporary directory. Checkout build-cache files cannot
enter that wheel. It checks maintained inputs and package contents, installs hash-locked
runtime dependencies and the wheel in a fresh environment, then runs isolated API,
CLI and benchmark checks outside the source tree. The installed packages must come
from site-packages and include both typing markers.

## Maintain ownership

The [architecture](architecture.md) identifies the owner of each rule. Keep storage
publication in the version owner, byte encoding in its codec, and policy selection
separate from execution. Clients may depend on the engine; the engine may not depend
on clients. A behavior change updates the specification and corresponding checks.
A persistent byte change updates the format contract and independent vectors.

Documentation describes the supported product, its use, and its guarantees.
The specification owns behavior; the format document owns bytes; typed source owns
implementation signatures. Avoid duplicating these authorities.

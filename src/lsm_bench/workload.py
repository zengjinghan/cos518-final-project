"""Deterministic client operations; no experiment owns engine implementation."""

import hashlib
import struct
from bisect import bisect_left
from collections.abc import Iterator
from dataclasses import dataclass
from itertools import accumulate
from random import Random

from .config import BenchmarkConfig, HotSet, WorkloadConfig, Zipf


@dataclass(frozen=True, slots=True)
class Write:
    """Insert an opaque value."""

    key: bytes
    value: bytes


@dataclass(frozen=True, slots=True)
class Remove:
    """Delete one key."""

    key: bytes


@dataclass(frozen=True, slots=True)
class Read:
    """Look up one key."""

    key: bytes


@dataclass(frozen=True, slots=True)
class Scan:
    """Read one inclusive interval."""

    start: bytes
    end: bytes


type Operation = Write | Remove | Read | Scan


def generate(config: WorkloadConfig) -> Iterator[Operation]:
    """Generate bounded uniform/hot-set/Zipf traces with an instance-local RNG."""
    rng = Random(config.seed)  # noqa: S311 - Reproducible workload, not a security primitive.
    cumulative = (
        tuple(
            accumulate(
                1 / (rank**config.distribution.exponent) for rank in range(1, config.keys + 1)
            )
        )
        if isinstance(config.distribution, Zipf)
        else ()
    )
    for _ in range(config.operations):
        if isinstance(config.distribution, HotSet):
            hot = int(config.keys * config.distribution.hot_fraction)
            number = (
                rng.randrange(hot)
                if rng.random() < config.distribution.hot_probability
                else rng.randrange(hot, config.keys)
            )
        elif cumulative:
            number = bisect_left(cumulative, rng.random() * cumulative[-1])
        else:
            number = rng.randrange(config.keys)
        key = (config.key_offset + number * config.key_stride).to_bytes(config.key_bytes, "big")
        choice = rng.random()
        if choice < config.put_fraction:
            yield Write(key, rng.randbytes(config.value_bytes))
        elif choice < config.put_fraction + config.get_fraction:
            yield Read(key)
        elif choice < config.put_fraction + config.get_fraction + config.scan_fraction:
            last = min(config.keys - 1, number + config.scan_length - 1)
            end = (config.key_offset + last * config.key_stride).to_bytes(config.key_bytes, "big")
            yield Scan(key, end)
        else:
            yield Remove(key)


@dataclass(frozen=True, slots=True)
class Trace:
    """Prepared inputs and a content identity independent of measurement mode."""

    load: tuple[Write, ...]
    warmup: tuple[Operation, ...]
    measured: tuple[Operation, ...]
    sha256: str


def _digest(phases: tuple[tuple[Operation, ...], ...]) -> str:
    digest = hashlib.sha256(b"LSMBENCHTRACE01")
    for operations in phases:
        digest.update(struct.pack("!I", len(operations)))
        for operation in operations:
            match operation:
                case Write(key, value):
                    tag, parts = b"P", (key, value)
                case Remove(key):
                    tag, parts = b"D", (key,)
                case Read(key):
                    tag, parts = b"G", (key,)
                case Scan(start, end):
                    tag, parts = b"S", (start, end)
            digest.update(tag)
            for part in parts:
                digest.update(struct.pack("!I", len(part)))
                digest.update(part)
    return digest.hexdigest()


def build_trace(config: BenchmarkConfig) -> Trace:
    """Materialize deterministic phases before starting any operation clock."""
    workload = config.workload
    rng = Random((workload.seed + 2) % (1 << 64))  # noqa: S311 -- Deterministic load values.
    load = (
        tuple(
            Write(
                (index * workload.key_stride).to_bytes(workload.key_bytes, "big"),
                rng.randbytes(workload.value_bytes),
            )
            for index in range(workload.keys)
        )
        if config.initial_state != "empty"
        else ()
    )
    warmup = (
        tuple(
            generate(
                WorkloadConfig.model_validate(
                    {
                        **workload.model_dump(),
                        "operations": config.warmup_operations,
                        "seed": (workload.seed + 1) % (1 << 64),
                    }
                )
            )
        )
        if config.warmup_operations
        else ()
    )
    measured = tuple(generate(workload))
    return Trace(load, warmup, measured, _digest((load, warmup, measured)))

"""Workload generators for the LSM benchmarking harness.

Each generator yields a sequence of logical operations:
    (OpCode.PUT, key_bytes, value_bytes)
    (OpCode.DELETE, key_bytes)
    (OpCode.GET, key_bytes)

The goal is to model a variety of key access patterns (uniform, skewed,
Zipfian) while keeping the operation interface simple and independent of
the storage engine implementation.

These generators are intentionally stateless and deterministic given a
random seed; this makes it easy to reproduce experiments.
"""

from __future__ import annotations

from collections.abc import Generator, Iterable
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, NamedTuple

import numpy as np

if TYPE_CHECKING:
    from .config_schema import WorkloadConfig

__all__ = (
    "OpCode",
    "WorkloadType",
    "WorkloadOp",
    "generate_workload",
    "generate_scan_ranges",
)


class OpCode(IntEnum):
    """Operation code used in workloads:
    PUT inserts, DELETE tombstones a key, and GET retrieves the latest value.
    """

    PUT = 0
    DELETE = 1
    GET = 2


class WorkloadType(str, Enum):
    """Logical type of workload to simulate.

    The workload type controls the key distribution and the mix of
    put/delete/get operations.
    """

    UNIFORM = "uniform"
    ZIPF = "zipf"
    SKEWED = "skewed"


class WorkloadOp(NamedTuple):
    op: OpCode
    key: bytes
    value: bytes | None = None


class LiveKeyTracker:
    """Efficiently tracks keys to support O(1) additions, removals, and random sampling."""

    __slots__ = (
        "_enabled",
        "_rng",
        "_dense",
        "_sparse",
        "_count",
        "_key_length",
    )

    def __init__(
        self,
        rng: np.random.Generator,
        key_range: int,
        enabled: bool = True,
    ):
        self._enabled = enabled
        self._rng = rng
        self._count = 0

        if enabled:
            # Dense: Stores the actual live keys contiguously.
            self._dense = np.zeros(key_range, dtype=np.int32)

            # Sparse: Maps key -> index_in_dense.
            # Initialize with -1 to indicate "empty/not present".
            self._sparse = np.full(key_range, -1, dtype=np.int32)

    def add(self, key: int) -> None:
        if not self._enabled:
            return

        # O(1) Check: Already exists?
        if self._sparse[key] != -1:
            return

        # O(1) Add: Append to end of dense, map in sparse
        idx = self._count
        self._dense[idx] = key
        self._sparse[key] = idx
        self._count += 1

    def remove(self, key: int) -> None:
        if not self._enabled:
            return

        # O(1) Check: Does not exist?
        idx = self._sparse[key]
        if idx == -1:
            return

        # O(1) Swap-Remove Algorithm
        last_idx = self._count - 1
        last_key_int = self._dense[last_idx]

        # 1. Overwrite the removed element with the last element
        self._dense[idx] = last_key_int
        self._sparse[last_key_int] = idx

        # 2. Mark target as removed in sparse
        self._sparse[key] = -1

        # 3. Shrink the size
        self._count -= 1

    def sample(self) -> int | None:
        if self._count == 0:
            return None

        # O(1) Sample: Integers are uniform
        rand_idx = self._rng.integers(0, self._count)
        key = self._dense[rand_idx]

        return int(key)

    def __bool__(self) -> bool:
        return self._count > 0


def _workload_uniform(
    n_ops: int,
    key_range: int = 1_000_000,
    value_range: int = 1_000_000,
    key_size_bytes: int = 8,
    value_size_bytes: int = 8,
    put_fraction: float = 0.75,
    get_fraction: float = 0.10,
    get_existing_keys_only: bool = False,
    rng: np.random.Generator | None = None,
) -> Generator[WorkloadOp, None, None]:
    """Generate a workload with uniform random key distribution.

    Keys are drawn uniformly from the range [0, key_range). This represents
    a "worst-case" scenario for caching, as there is no temporal locality
    or hot spots; every key is equally likely to be accessed at any time.

    Operation Mix:
    - PUTs, GETs, and DELETEs occur based on the provided probability fractions.
    - If `get_existing_keys_only=True`, GET operations track the set of
        currently "live" keys (keys that have been written but not deleted)
        and sample uniformly from that set.

    Args:
        n_ops: Total number of operations to generate.
        key_range: The upper bound for key integers [0, key_range).
        value_range: The upper bound for value integers [1, value_range].
        key_size_bytes: Width of the key in bytes (big-endian).
        value_size_bytes: Width of the value in bytes (big-endian).
        put_fraction: Probability of a PUT operation (0.0 to 1.0).
        get_fraction: Probability of a GET operation.
            (The remaining probability mass is allocated to DELETEs).
        get_existing_keys_only: If True, GET operations will attempt to
            sample uniformly from currently "live" keys. If False (or if
            no keys are live), falls back to the standard uniform key.
        rng: Optional NumPy random generator for reproducible sequences.

    Yields:
        WorkloadOp: A sequence of PUT, GET, and DELETE operations.
    """
    assert put_fraction + get_fraction <= 1.0

    put_limit = put_fraction
    get_limit = put_fraction + get_fraction

    if rng is None:
        rng = np.random.default_rng()

    tracker = LiveKeyTracker(
        rng,
        key_range=key_range,
        enabled=get_existing_keys_only,
    )

    # Generate all keys uniformly
    keys_np = rng.integers(0, key_range, size=n_ops)
    keys: list[int] = keys_np.tolist()

    # Generate operation probabilities
    ops_probs = rng.random(size=n_ops)

    values_np = rng.integers(1, 1 + value_range, size=n_ops)
    values: list[int] = values_np.tolist()

    for k, op_r, v in zip(keys, ops_probs, values, strict=True):
        if op_r < put_limit:
            # PUT
            key = k.to_bytes(key_size_bytes, "big", signed=False)
            val = v.to_bytes(value_size_bytes, "big", signed=False)
            tracker.add(k)
            yield WorkloadOp(OpCode.PUT, key, val)
        elif op_r < get_limit:
            # GET
            target_key = (tracker.sample() or k).to_bytes(key_size_bytes, "big", signed=False)
            yield WorkloadOp(OpCode.GET, target_key, None)
        else:
            # DELETE
            key = k.to_bytes(key_size_bytes, "big", signed=False)
            tracker.remove(k)
            yield WorkloadOp(OpCode.DELETE, key, None)


def _workload_zipf(
    n_ops: int,
    key_range: int = 1_000_000,
    value_range: int = 1_000_000,
    key_size_bytes: int = 8,
    value_size_bytes: int = 8,
    put_fraction: float = 0.75,
    get_fraction: float = 0.10,
    get_existing_keys_only: bool = False,
    theta: float = 1.2,
    rng: np.random.Generator | None = None,
) -> Generator[WorkloadOp, None, None]:
    """Generate a workload with Zipfian (power-law) key distribution.

    Keys are drawn from a Zipf distribution with parameter `theta`. The raw
    values are wrapped into [0, key_range) using modulo arithmetic. This
    creates a few extremely hot keys and a "long tail" of rarely accessed keys.

    Key Distribution Details:
        - Writes (PUTs) and Deletes always follow the Zipfian distribution,
            creating hot spots on specific keys.
        - Reads (GETs) follow the Zipfian distribution by default.
            However, if `get_existing_keys_only=True`, GETs sample uniformly
            from the set of currently live keys to ensure successful lookups.

    Args:
        n_ops: Total number of operations to generate.
        key_range: The upper bound for key integers [0, key_range).
        value_range: The upper bound for value integers [1, value_range].
        key_size_bytes: Width of the key in bytes (big-endian).
        value_size_bytes: Width of the value in bytes (big-endian).
        put_fraction: Probability of a PUT operation (0.0 to 1.0).
        get_fraction: Probability of a GET operation.
            (The remaining probability mass is allocated to DELETEs).
        get_existing_keys_only: If True, GET operations will attempt to
            sample uniformly from currently "live" keys. If False (or if
            no keys are live), falls back to the standard Zipfian key.
        rng: Optional NumPy random generator for reproducible sequences.
        theta: The skew parameter of the Zipf distribution (> 1.0).
            - Larger values (e.g., 1.5) imply steeper skew (hotter hot keys).
            - Values closer to 1.0 (e.g., 1.01) imply a flatter distribution.

    Yields:
        WorkloadOp: A sequence of PUT, GET, and DELETE operations.
    """
    assert put_fraction + get_fraction <= 1.0
    put_limit = put_fraction
    get_limit = put_fraction + get_fraction

    if rng is None:
        rng = np.random.default_rng()

    tracker = LiveKeyTracker(
        rng,
        key_range=key_range,
        enabled=get_existing_keys_only,
    )

    keys_np = rng.zipf(theta, size=n_ops)
    keys_np %= max(1, key_range)
    keys: list[int] = keys_np.tolist()

    ops_probs = rng.random(size=n_ops)

    values_np = rng.integers(1, 1 + value_range, size=n_ops)
    values: list[int] = values_np.tolist()

    for k, op_r, v in zip(keys, ops_probs, values, strict=True):
        if op_r < put_limit:
            # PUT
            key = k.to_bytes(key_size_bytes, "big", signed=False)
            val = v.to_bytes(value_size_bytes, "big", signed=False)
            tracker.add(k)
            yield WorkloadOp(OpCode.PUT, key, val)
        elif op_r < get_limit:
            # GET
            target_key = (tracker.sample() or k).to_bytes(key_size_bytes, "big", signed=False)
            yield WorkloadOp(OpCode.GET, target_key, None)
        else:
            # DELETE
            key = k.to_bytes(key_size_bytes, "big", signed=False)
            tracker.remove(k)
            yield WorkloadOp(OpCode.DELETE, key, None)


def _workload_skewed_hot(
    n_ops: int,
    key_range: int = 1_000_000,
    value_range: int = 1_000_000,
    key_size_bytes: int = 8,
    value_size_bytes: int = 8,
    put_fraction: float = 0.75,
    get_fraction: float = 0.10,
    get_existing_keys_only: bool = False,
    hot_ratio: float = 0.01,
    hot_access_prob: float = 0.80,
    rng: np.random.Generator | None = None,
) -> Generator[WorkloadOp, None, None]:
    """Generate a skewed workload with distinct hot and cold key regions.

    This workload simulates a "hotspot" access pattern common in real-world
    systems. The keyspace is divided into two regions:
    1. Hot Region: A small subset of keys (defined by `hot_ratio`).
    2. Cold Region: The remaining majority of keys.

    Traffic is probabilistically directed to the hot region based on
    `hot_access_prob`. For example, 80% of operations might target just
    1% of the keys.

    Args:
        n_ops: Total number of operations to generate.
        key_range: The upper bound for key integers [0, key_range).
        value_range: The upper bound for value integers [1, value_range].
        key_size_bytes: Width of the key in bytes (big-endian).
        value_size_bytes: Width of the value in bytes (big-endian).
        put_fraction: Probability of a PUT operation (0.0 to 1.0).
        get_fraction: Probability of a GET operation.
            (The remaining probability mass is allocated to DELETEs).
        get_existing_keys_only: If True, GET operations will attempt to
            sample uniformly from currently "live" keys (keys that have been
            PUT but not DELETEd). If no keys are live, falls back to the
            standard distribution.
        hot_ratio: Fraction of the keyspace designated as "hot".
            (e.g., 0.01 means the first 1% of keys are hot).
        hot_access_prob: Probability that an operation targets the hot region.
            (e.g., 0.80 means 80% of ops target the hot keys).
        rng: Optional NumPy random generator for reproducible sequences.

    Yields:
        WorkloadOp: A sequence of PUT, GET, and DELETE operations.
    """
    assert put_fraction + get_fraction <= 1.0
    put_limit = put_fraction
    get_limit = put_fraction + get_fraction

    if rng is None:
        rng = np.random.default_rng()

    tracker = LiveKeyTracker(
        rng,
        key_range=key_range,
        enabled=get_existing_keys_only,
    )

    # Calculate the boundary between hot and cold key IDs
    hot_key_limit = max(1, int(key_range * hot_ratio))

    # Decide for every op: is it targeting a hot key or a cold key?
    is_hot = rng.random(size=n_ops) < hot_access_prob

    # Generate random keys for both scenarios in bulk
    # Note: We generate n_ops keys for BOTH, then select. This is faster
    # than boolean indexing for assignment because it avoids Python branching.
    hot_candidates = rng.integers(0, hot_key_limit, size=n_ops)
    cold_candidates = rng.integers(hot_key_limit, key_range, size=n_ops)

    # Select the winner based on is_hot mask
    keys_np = np.where(is_hot, hot_candidates, cold_candidates)
    keys: list[int] = keys_np.tolist()

    ops_probs = rng.random(size=n_ops)

    values_np = rng.integers(1, 1 + value_range, size=n_ops)
    values: list[int] = values_np.tolist()

    for k, op_r, v in zip(keys, ops_probs, values, strict=True):
        if op_r < put_limit:
            # PUT
            key = k.to_bytes(key_size_bytes, "big", signed=False)
            val = v.to_bytes(value_size_bytes, "big", signed=False)
            tracker.add(k)
            yield WorkloadOp(OpCode.PUT, key, val)
        elif op_r < get_limit:
            # GET
            target_key = (tracker.sample() or k).to_bytes(key_size_bytes, "big", signed=False)
            yield WorkloadOp(OpCode.GET, target_key, None)
        else:
            # DELETE
            key = k.to_bytes(key_size_bytes, "big", signed=False)
            tracker.remove(k)
            yield WorkloadOp(OpCode.DELETE, key, None)


def generate_workload(workload: WorkloadConfig) -> Iterable[WorkloadOp]:
    """Top-level factory for workloads.

    Returns:
        An iterable of (op, key_bytes, value_bytes_or_none) tuples.
    """
    args = workload.model_dump(exclude={"type", "seed", "params"})

    rng = np.random.default_rng(workload.seed)

    if workload.type == WorkloadType.UNIFORM:
        return _workload_uniform(rng=rng, **args, **workload.params)
    elif workload.type == WorkloadType.ZIPF:
        return _workload_zipf(rng=rng, **args, **workload.params)
    elif workload.type == WorkloadType.SKEWED:
        return _workload_skewed_hot(rng=rng, **args, **workload.params)

    raise ValueError(f"Unsupported workload type: {workload.type}")


def generate_scan_ranges(
    n_scans: int,
    key_range: int,
    span: int,
    seed: int = 0,
) -> Iterable[tuple[bytes, bytes]]:
    """Generate a sequence of [start, end] ranges for range-scan benchmarks.

    Ranges are aligned to the logical integer keyspace [0, key_range). For
    each scan, we pick a random start key and then extend the range up to
    `span` keys (clipped to key_range - 1).

    Args:
        n_scans: Number of scan ranges to generate.
        key_range: Size of the logical keyspace. Keys will be in
            [0, key_range).
        span: Maximum number of integer keys covered by a single range. Larger
            spans produce wider scans.
        seed: Seed for the underlying RNG used to generate ranges.

    Returns:
        Iterable of (start_key_bytes, end_key_bytes) tuples, where both
        bounds are inclusive.
    """
    rng = np.random.default_rng(seed)

    # Validation
    span = max(1, span)
    key_range = max(1, key_range)

    starts_np = rng.integers(0, key_range, size=n_scans)

    # Calculate all end points
    # (start + span - 1) is broadcasted, then clipped by key_range - 1
    ends_np = np.minimum(starts_np + span - 1, key_range - 1)

    starts: list[int] = starts_np.tolist()
    ends: list[int] = ends_np.tolist()

    for s_int, e_int in zip(starts, ends, strict=True):
        start = s_int.to_bytes(8, "big", signed=False)
        end = e_int.to_bytes(8, "big", signed=False)
        yield (start, end)

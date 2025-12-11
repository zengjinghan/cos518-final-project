"""Context-manager based Timer"""

from time import perf_counter_ns

__all__ = ("Timer",)


class Timer:
    """Context manager for timing code execution using time.perf_counter_ns.

    Usage:
        ```
        with Timer("My task") as timer:
            do_something()

        print(timer.elapsed)
        ```
    """

    __slots__ = ("name", "start_time", "end_time")

    def __init__(self, name: str = "") -> None:
        self.name: str = name
        self.start_time: float = 0.0
        self.end_time: float = 0.0

    def __enter__(self):
        self.start_time = perf_counter_ns()
        return self

    def __exit__(self, *args, **kwargs):
        self.end_time = perf_counter_ns()
        return self

    def start(self):
        """Start the timer"""
        self.start_time = perf_counter_ns()

    def stop(self):
        """Stop the timer"""
        self.end_time = perf_counter_ns()

    def reset(self):
        """Reset both timestamps"""
        self.start_time = 0.0
        self.end_time = 0.0

    @property
    def elapsed_s(self) -> float:
        """Time elapsed in nanoseconds."""
        return self.elapsed_ns * 1e-9

    @property
    def elapsed_ms(self) -> float:
        """Time elapsed in nanoseconds."""
        return self.elapsed_ns * 1e-6

    @property
    def elapsed_us(self) -> float:
        """Time elapsed in nanoseconds."""
        return self.elapsed_ns * 1e-3

    @property
    def elapsed_ns(self) -> float:
        """Time elapsed in nanoseconds."""
        return self.end_time - self.start_time

    def __str__(self) -> str:
        return f"[Timer][{self.name}] Elapsed time: {self.elapsed_ns:.3f} seconds"

"""Reusable helper for toggling metrics collection on and off."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar

__all__ = ("MetricsToggle",)


# 1. Define a Protocol to tell the type checker what methods M must have
class MetricsStore(Protocol):
    def reset(self) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...


# 2. Bind M to that Protocol
M = TypeVar("M", bound=MetricsStore)


class MetricsAware(Generic[M]):
    """Mixin for classes that own a MetricsToggle.

    Expects the host class to define:
        _metrics: MetricsToggle[M]
    """

    @property
    # We use explicit typing here to help the host class know what 'M' is
    def _metrics_toggle(self) -> MetricsToggle[M]:
        # Helper to satisfy type checkers that self._metrics exists
        return self._metrics  # type: ignore

    @property
    def metrics(self) -> M:
        """Returns the active metrics container (real or no-op)."""
        return self._metrics_toggle.current

    @property
    def metrics_enabled(self) -> bool:
        return self._metrics_toggle.enabled

    def pause_metrics(self) -> None:
        self._metrics_toggle.pause()

    def resume_metrics(self) -> None:
        self._metrics_toggle.resume()

    def reset_metrics(self) -> None:
        self._metrics_toggle.reset()

    def export_metrics(self) -> dict[str, Any]:
        return self._metrics_toggle.export()

    def manage_metrics(self, enabled: bool) -> AbstractContextManager[None]:
        return self._metrics_toggle.manage(enabled)


@dataclass(slots=True)
class MetricsToggle(Generic[M]):
    """Manages a primary + no-op metrics instance with an enable flag."""

    # 3. Type the factory as a Callable that returns M
    _store_factory: Callable[[], M]
    _store: M = field(init=False)
    _noop: M = field(init=False)
    _enabled: bool = field(init=False, default=True)

    def __post_init__(self) -> None:
        self._store = self._store_factory()
        self._noop = self._store_factory()

    @property
    def current(self) -> M:
        """Return the active metrics object (real or no-op)."""
        return self._store if self._enabled else self._noop

    @property
    def enabled(self) -> bool:
        return self._enabled

    def pause(self) -> None:
        self._enabled = False

    def resume(self) -> None:
        self._enabled = True
        self._noop.reset()  # valid because M is bound to MetricsStore

    def reset(self) -> None:
        self._store.reset()
        self._noop.reset()

    def export(self) -> dict[str, Any]:
        """Return a serializable snapshot of the real metrics."""
        # No 'type: ignore' needed now; Protocol guarantees to_dict exists
        return self._store.to_dict()

    @contextmanager
    def manage(self, enabled: bool) -> Iterator[None]:
        """Temporarily force metrics on or off.

        Note: The return type must be Iterator[None] because this is a
        generator function. The @contextmanager decorator transforms
        this into a ContextManager at the call site.
        """
        original = self._enabled
        if enabled:
            self.resume()
        else:
            self.pause()
        try:
            yield
        finally:
            if original:
                self.resume()
            else:
                self.pause()

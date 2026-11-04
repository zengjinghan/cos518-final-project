"""Stable failure categories shared by the library and its adapters."""


class StoreError(Exception):
    """An expected store operation could not complete."""


class ConfigurationError(StoreError, ValueError):
    """External configuration violates the admitted schema."""


class ArgumentError(StoreError, ValueError):
    """A public argument is invalid before storage effects begin."""


class StorageError(StoreError):
    """A filesystem operation failed; its original error is chained."""


class CorruptionError(StoreError):
    """Stored bytes or committed metadata violate the format contract."""


class UnsupportedFormatError(CorruptionError):
    """A complete format identifier is not supported by this implementation."""


class DatabaseLockedError(StoreError):
    """Another owner already holds the database directory lock."""


class LifecycleError(StoreError):
    """A handle is closed, faulted, or used outside its owning execution context."""

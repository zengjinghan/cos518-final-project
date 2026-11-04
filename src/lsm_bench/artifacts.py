"""Exclusive artifact writes with contextual errors."""

from pathlib import Path

from pydantic import BaseModel

from lsm_store.errors import ConfigurationError, StorageError


def check_output(path: Path) -> None:
    """Reject invalid output targets before starting a workload."""
    if path.exists():
        raise ConfigurationError(f"result already exists: {path}")
    if not path.parent.is_dir():
        raise ConfigurationError(f"output parent does not exist: {path.parent}")


def write(path: Path, value: BaseModel) -> None:
    """Never replace an existing observation or expose installer diagnostics."""
    try:
        with path.open("x", encoding="utf-8") as target:
            target.write(value.model_dump_json(indent=2) + "\n")
    except OSError as error:
        raise StorageError(f"write benchmark artifact {path}: {error}") from error

"""One strict configuration boundary for benchmark inputs."""

import tomllib
from pathlib import Path
from typing import Annotated, cast

from pydantic import BaseModel, BeforeValidator, ConfigDict, ValidationError

from lsm_store.errors import ConfigurationError


def _array(value: object) -> object:
    # TOML arrays are lists; admit only that container conversion, not its values.
    return tuple(cast("list[object]", value)) if isinstance(value, list) else value


type Array[T] = Annotated[tuple[T, ...], BeforeValidator(_array)]


class Model(BaseModel):
    """Reject coercion, mutation and unknown fields."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid", validate_default=True)


def load[T: Model](path: Path, schema: type[T]) -> T:
    """Admit TOML through its owning schema before any output effects."""
    try:
        with path.open("rb") as source:
            return schema.model_validate(tomllib.load(source))
    except (OSError, ValueError, ValidationError) as error:
        raise ConfigurationError(f"invalid benchmark configuration {path}: {error}") from error

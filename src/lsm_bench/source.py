"""Identify and preserve installed source without requiring a Git checkout."""

import hashlib
import platform
from dataclasses import dataclass
from importlib.metadata import distribution, version
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    """Identify installed source contents even outside a Git checkout."""

    package_version: str
    source_sha256: str
    python: str
    platform: str
    pydantic: str


def source_identity() -> SourceIdentity:
    """Hash both installed packages using relative names and literal source bytes."""
    digest = hashlib.sha256()
    source_root = Path(__file__).parent.parent
    for package in ("lsm_store", "lsm_bench"):
        for path in sorted((source_root / package).rglob("*.py")):
            digest.update(str(path.relative_to(source_root)).encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    return SourceIdentity(
        version("educational-lsm-store"),
        digest.hexdigest(),
        platform.python_version(),
        platform.platform(),
        version("pydantic"),
    )


def archive_source(path: Path) -> None:
    """Preserve relative source bytes and typing markers, never user paths."""
    source_root = Path(__file__).parent.parent
    package = distribution("educational-lsm-store")
    if package.files is None:
        raise ValueError("installed distribution has no file manifest")
    notices = tuple(item for item in package.files if item.name == "LICENSE.txt")
    if len(notices) != 1:
        raise ValueError("installed distribution must contain exactly one license notice")
    notice = notices[0].read_binary()
    with ZipFile(path, "x", ZIP_DEFLATED) as archive:
        archive.writestr("LICENSE.txt", notice)
        for package in ("lsm_store", "lsm_bench"):
            for source in sorted((source_root / package).rglob("*")):
                if source.is_file() and (source.suffix == ".py" or source.name == "py.typed"):
                    archive.write(source, source.relative_to(source_root).as_posix())

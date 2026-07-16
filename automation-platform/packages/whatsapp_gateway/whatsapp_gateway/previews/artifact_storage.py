from __future__ import annotations

import hashlib
import shutil
import uuid
from pathlib import Path

from automation_core.config import get_settings
from whatsapp_gateway.previews.compiler.errors import PreviewCompileError

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def _managed_artifact_path(checksum: str, source: Path) -> Path:
    suffix = source.suffix.lower() if source.suffix else ".bin"
    return (
        get_settings().artifact_root.expanduser().resolve()
        / "dispatch-previews"
        / checksum[:2]
        / f"{checksum}{suffix}"
    )

def freeze_artifact(path: Path, checksum: str) -> Path:
    """Copy report bytes into content-addressed, platform-managed storage."""
    destination = _managed_artifact_path(checksum, path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_file() and sha256_file(destination) == checksum:
        return destination
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    shutil.copyfile(path, temporary)
    if sha256_file(temporary) != checksum:
        temporary.unlink(missing_ok=True)
        raise PreviewCompileError(f"Attachment changed while it was being frozen: {path.name}")
    temporary.replace(destination)
    return destination

def is_managed_preview_artifact(path: Path) -> bool:
    root = (
        get_settings().artifact_root.expanduser().resolve()
        / "dispatch-previews"
    )
    try:
        path.resolve(strict=False).relative_to(root)
    except ValueError:
        return False
    return True


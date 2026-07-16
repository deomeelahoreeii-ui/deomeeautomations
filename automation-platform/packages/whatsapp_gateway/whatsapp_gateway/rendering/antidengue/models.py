from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

RENDERER_KEY = "antidengue.tehsil_dormant.v1"
WING_RENDERER_KEY = "antidengue.wing_dormant.v1"
PAKISTAN_TIME = ZoneInfo("Asia/Karachi")
REQUIRED_COLUMNS = {"school emis", "school name"}


@dataclass(frozen=True)
class DormantSourceRow:
    emis: str
    school_name: str


@dataclass(frozen=True)
class ScopedDormantSchool:
    emis: str
    school_name: str
    tehsil: str
    markaz: str


@dataclass
class RenderedTehsilDormantReport:
    message: str
    context: dict[str, str]
    schools: list[ScopedDormantSchool]
    issues: list[dict[str, Any]] = field(default_factory=list)
    attachment_paths: list[Path] = field(default_factory=list)
    source_artifact_id: int | None = None
    source_artifact_sha256: str = ""


@dataclass
class RenderedWingDormantReport:
    message: str
    context: dict[str, str]
    schools: list[ScopedDormantSchool]
    issues: list[dict[str, Any]] = field(default_factory=list)
    attachment_path: Path | None = None
    attachment_paths: list[Path] = field(default_factory=list)
    source_artifact_id: int | None = None
    source_artifact_sha256: str = ""

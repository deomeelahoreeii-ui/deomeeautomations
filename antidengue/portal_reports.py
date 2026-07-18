from __future__ import annotations

import datetime as dt
import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlencode, urljoin
from zoneinfo import ZoneInfo

BUSINESS_TIMEZONE_NAME = "Asia/Karachi"
BUSINESS_TIMEZONE = ZoneInfo(BUSINESS_TIMEZONE_NAME)
DEFAULT_DISTRICT_ID = "18"


@dataclass(frozen=True, slots=True)
class PortalReportWindow:
    start: dt.datetime
    end: dt.datetime

    @classmethod
    def for_cutoff(cls, cutoff: dt.datetime | None = None) -> "PortalReportWindow":
        local_cutoff = cutoff or dt.datetime.now(BUSINESS_TIMEZONE)
        if local_cutoff.tzinfo is None:
            local_cutoff = local_cutoff.replace(tzinfo=BUSINESS_TIMEZONE)
        else:
            local_cutoff = local_cutoff.astimezone(BUSINESS_TIMEZONE)
        return cls(
            start=local_cutoff.replace(hour=0, minute=0, second=0, microsecond=0),
            end=local_cutoff.replace(second=0, microsecond=0),
        )

    @classmethod
    def from_environment(cls) -> "PortalReportWindow":
        raw = os.getenv("PORTAL_REPORT_CUTOFF", "").strip()
        if not raw:
            return cls.for_cutoff()
        normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        return cls.for_cutoff(dt.datetime.fromisoformat(normalized))

    def portal_value(self, value: dt.datetime) -> str:
        return value.astimezone(BUSINESS_TIMEZONE).strftime("%Y-%m-%dT%H:%M")

    def as_dict(self) -> dict[str, str]:
        return {
            "timezone": BUSINESS_TIMEZONE_NAME,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "datefrom": self.portal_value(self.start),
            "dateto": self.portal_value(self.end),
        }


@dataclass(frozen=True, slots=True)
class PortalReportDefinition:
    key: str
    name: str
    path: str
    required: bool
    acquisition: str
    filename_prefix: str
    static_query: tuple[tuple[str, str], ...] = ()
    uses_window: bool = False

    def export_url(
        self,
        base_url: str,
        window: PortalReportWindow,
        *,
        district_id: str = DEFAULT_DISTRICT_ID,
    ) -> str:
        query = dict(self.static_query)
        query["district_id"] = district_id
        if self.uses_window:
            query["datefrom"] = window.portal_value(window.start)
            query["dateto"] = window.portal_value(window.end)
        return f"{urljoin(base_url, self.path)}?{urlencode(query)}"

    def filename(self, window: PortalReportWindow, suffix: str = ".xls") -> str:
        cutoff = window.end.strftime("%Y%m%d_%H%M")
        return f"{self.filename_prefix}_{cutoff}{suffix}"


DORMANT_USERS = PortalReportDefinition(
    key="dormant_users",
    name="Dormant users",
    path="user_wise_larva_report",
    required=True,
    acquisition="page_export_button",
    filename_prefix="user_wise_dormancy_report",
)

HOTSPOT_DISTANCE = PortalReportDefinition(
    key="hotspot_distance",
    name="Hotspot-distance activities",
    path="reports/activities/hotspot_distance_report",
    required=False,
    acquisition="authenticated_export",
    filename_prefix="hotspot_distance_report",
    uses_window=True,
    static_query=(
        ("act_tag", ""),
        ("action", "hotspot_distance_report"),
        ("controller", "reports/activities"),
        ("format", "xls"),
        ("hotspot_distance", ""),
        ("is_filter_selected", "true"),
        ("pagination", "No"),
        ("parent_department", ""),
        ("sub_department", ""),
        ("tehsil_id", ""),
        ("uc", ""),
    ),
)

SIMPLE_ACTIVITY_LIST = PortalReportDefinition(
    key="simple_activity_list",
    name="Simple Activity List",
    path="activities/simples/line_list",
    required=False,
    acquisition="authenticated_export",
    filename_prefix="simple_activity_list",
    uses_window=True,
    static_query=(
        ("act_tag", ""),
        ("action", "line_list"),
        ("controller", "activities/simples"),
        ("format", "xls"),
        ("larva_type", ""),
        ("pagination", "No"),
        ("parent_department", ""),
        ("period", "simple_activities"),
        ("sub_department", ""),
        ("submitted_by", ""),
        ("tehsil_id", ""),
        ("uc", ""),
    ),
)

REPORT_DEFINITIONS = {
    definition.key: definition
    for definition in (DORMANT_USERS, HOTSPOT_DISTANCE, SIMPLE_ACTIVITY_LIST)
}


def selected_report_definitions() -> list[PortalReportDefinition]:
    raw = os.getenv("PORTAL_REPORTS", "dormant_users,hotspot_distance,simple_activity_list")
    keys = [value.strip() for value in raw.split(",") if value.strip()]
    unknown = sorted(set(keys).difference(REPORT_DEFINITIONS))
    if unknown:
        raise ValueError("Unknown portal report key(s): " + ", ".join(unknown))
    if DORMANT_USERS.key not in keys:
        keys.insert(0, DORMANT_USERS.key)
    return [REPORT_DEFINITIONS[key] for key in dict.fromkeys(keys)]


@dataclass(frozen=True, slots=True)
class PortalReportCapture:
    definition: PortalReportDefinition
    status: str
    requested_url: str
    path: Path | None = None
    size_bytes: int = 0
    sha256: str = ""
    content_type: str = ""
    error: str = ""
    analysis: dict[str, object] = field(default_factory=dict)

    @classmethod
    def completed(
        cls,
        definition: PortalReportDefinition,
        *,
        requested_url: str,
        path: Path,
        content_type: str = "",
        analysis: dict[str, object] | None = None,
    ) -> "PortalReportCapture":
        body = path.read_bytes()
        return cls(
            definition=definition,
            status="completed",
            requested_url=requested_url,
            path=path,
            size_bytes=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
            content_type=content_type,
            analysis=analysis or {},
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "report_key": self.definition.key,
            "report_name": self.definition.name,
            "required": self.definition.required,
            "acquisition": self.definition.acquisition,
            "status": self.status,
            "requested_url": self.requested_url,
            "path": str(self.path) if self.path else None,
            "name": self.path.name if self.path else None,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "content_type": self.content_type or None,
            "error": self.error or None,
            "analysis": self.analysis or None,
        }


@dataclass(slots=True)
class PortalReportBundle:
    window: PortalReportWindow
    captures: list[PortalReportCapture] = field(default_factory=list)

    @property
    def primary_path(self) -> Path:
        for capture in self.captures:
            if capture.definition.key == DORMANT_USERS.key and capture.path:
                return capture.path
        raise RuntimeError("The required dormant-users report was not downloaded")

    def as_dict(self) -> dict[str, object]:
        return {
            "window": self.window.as_dict(),
            "reports": [capture.as_dict() for capture in self.captures],
        }


__all__ = [
    "DORMANT_USERS",
    "HOTSPOT_DISTANCE",
    "SIMPLE_ACTIVITY_LIST",
    "PortalReportBundle",
    "PortalReportCapture",
    "PortalReportDefinition",
    "PortalReportWindow",
    "selected_report_definitions",
]

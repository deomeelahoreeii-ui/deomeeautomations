from __future__ import annotations

import hashlib
import re
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqlmodel import Session

from automation_core.models import Job
from master_data.models import Wing
from whatsapp_gateway.rendering.antidengue.issues import _issue
from whatsapp_gateway.rendering.antidengue.hotspot_artifacts import (
    ensure_attachment, review_artifact,
)

HOTSPOT_RENDERER_KEY = "antidengue.hotspot_distance.v1"
HOTSPOT_REPORT_KEY = "hotspot_distance_activity"
HOTSPOT_DEFAULT_TEMPLATE = """🚨 *Anti-Dengue Activities Requiring Review*
👥 *Group:* {{group_name}}
📌 *Scope:* {{scope_name}}
🕒 *Report window:* {{report_window}}

🏫 *School-wise activity evidence*

{{school_activity_details}}

{{attachment_note}}
{{disclaimer}}"""


@dataclass(frozen=True)
class HotspotReviewRow:
    values: dict[str, Any]
    emis: str
    school_name: str
    wing_id: str
    tehsil_id: str
    markaz_id: str
    distance_meters: float | None


@dataclass
class RenderedHotspotReport:
    message: str
    context: dict[str, str]
    rows: list[HotspotReviewRow]
    issues: list[dict[str, Any]] = field(default_factory=list)
    attachment_paths: list[Path] = field(default_factory=list)
    source_artifact_id: int | None = None
    source_artifact_sha256: str = ""


def _plain_whatsapp_text(value: Any, fallback: str = "Not available") -> str:
    text = re.sub(r"[\r\n\t]+", " ", str(value or "")).strip()
    # Do not allow source data to open or close WhatsApp formatting markers.
    text = re.sub(r"[*_~`]", "", text)
    return text or fallback


def _coordinates(value: Any) -> tuple[float, float] | None:
    numbers = re.findall(r"[-+]?\d+(?:\.\d+)?", str(value or ""))
    if len(numbers) < 2:
        return None
    latitude, longitude = float(numbers[0]), float(numbers[1])
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return None
    return latitude, longitude


def _maps_link(value: Any) -> str:
    point = _coordinates(value)
    if point is None:
        return ""
    latitude, longitude = point
    return f"https://www.google.com/maps?q={latitude:.7f},{longitude:.7f}"


def _window_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        cutoff = datetime.fromisoformat(text)
    except ValueError:
        return text.replace("T", " ")
    return f"12:00 AM – {cutoff.strftime('%I:%M %p')} ({cutoff.strftime('%d %b %Y')})"


def _school_summary_lines(rows: list[HotspotReviewRow]) -> list[str]:
    grouped: dict[str, list[HotspotReviewRow]] = {}
    for row in rows:
        grouped.setdefault(row.emis, []).append(row)

    def group_key(item: tuple[str, list[HotspotReviewRow]]) -> tuple[str, str, str]:
        _, school_rows = item
        values = school_rows[0].values
        return (
            str(values.get("Tehsil") or ""),
            str(values.get("Markaz") or ""),
            school_rows[0].school_name,
        )

    lines: list[str] = []
    for index, (emis, school_rows) in enumerate(sorted(grouped.items(), key=group_key), start=1):
        representative = school_rows[0]
        values = representative.values
        lines.extend([
            f"🏫 *{index}. {_plain_whatsapp_text(representative.school_name, 'Unknown school')}*",
            f"🆔 *EMIS:* `{_plain_whatsapp_text(emis)}`",
            (
                f"📍 *Tehsil:* {_plain_whatsapp_text(values.get('Tehsil'))}"
                f"  |  *Markaz:* {_plain_whatsapp_text(values.get('Markaz'))}"
            ),
        ])
        ordered_rows = sorted(
            school_rows,
            key=lambda row: row.distance_meters if row.distance_meters is not None else -1,
            reverse=True,
        )
        for activity_index, row in enumerate(ordered_rows, start=1):
            if len(ordered_rows) > 1:
                lines.append(f"🔸 *Activity {activity_index}*")
            distance = (
                f"{row.distance_meters:,.2f}"
                if row.distance_meters is not None
                else "Not available"
            )
            lines.append(f"📏 *Distance Difference (In meters):* {distance}")
            link = _maps_link(row.values.get("Activity(Lat,Long)"))
            if link:
                lines.append(f"🗺️ *Google Maps:* {link}")
            else:
                lines.append("🗺️ *Google Maps:* Coordinates unavailable in the portal report")
        lines.append("")
    return lines


def render_hotspot_distance_report(
    session: Session,
    *,
    source_job: Job,
    wing: Wing,
    recipient_name: str,
    scope_key: str,
    scope_value: str,
    scope_label: str,
    presentation_policy: dict[str, Any] | None = None,
    scope_values: list[str] | None = None,
) -> RenderedHotspotReport:
    source_artifact, source_path, header, source_rows = review_artifact(session, source_job)
    scoped = [row for row in source_rows if row.wing_id == str(wing.id)]
    accepted_scope_values = set(scope_values or [scope_value])
    if scope_key == "tehsil":
        scoped = [row for row in scoped if row.tehsil_id in accepted_scope_values]
    elif scope_key == "markaz":
        scoped = [row for row in scoped if row.markaz_id in accepted_scope_values]
    elif scope_key not in {"district", "wing"}:
        raise ValueError(f"Hotspot-distance delivery does not support {scope_key or 'an unbound'} scope")

    issues: list[dict[str, Any]] = []
    unmapped = [row for row in source_rows if not row.wing_id or not row.emis]
    if unmapped:
        issues.append(_issue(
            "unmapped_hotspot_submitters", "warning",
            f"{len(unmapped)} classified activity row(s) could not be mapped to authoritative school routing and were excluded.",
        ))
    if not scoped:
        issues.append(_issue(
            "nothing_to_dispatch", "warning",
            f"No activities requiring review were found for {scope_label}.",
        ))
        return RenderedHotspotReport(
            message="", context={}, rows=[], issues=issues,
            source_artifact_id=source_artifact.id,
            source_artifact_sha256=hashlib.sha256(source_path.read_bytes()).hexdigest(),
        )

    unique_schools = len({row.emis for row in scoped if row.emis})
    window = dict(((source_job.result or {}).get("summary") or {}).get("portal_acquisition") or {}).get("window") or {}
    window_label = _window_label(window.get("dateto"))
    school_activity_details = "\n".join(_school_summary_lines(scoped)).strip()
    attachment_mode = str((presentation_policy or {}).get("attachment_mode") or "excel")
    has_attachment = attachment_mode != "none"
    context = {
        "group_name": _plain_whatsapp_text(recipient_name),
        "scope_name": _plain_whatsapp_text(scope_label),
        "report_window": window_label or "Not available",
        "school_activity_details": school_activity_details,
        "attachment_note": (
            "📎 The attached Excel file contains every selected activity and its evidence."
            if has_attachment
            else ""
        ),
        "disclaimer": "⚠️ These are rule-based review candidates, not confirmed fake activities. Please verify each finding.",
        "activity_count": str(len(scoped)),
        "school_count": str(unique_schools),
    }
    message = re.sub(
        r"{{\s*([a-zA-Z0-9_]+)\s*}}",
        lambda match: context.get(match.group(1), match.group(0)),
        HOTSPOT_DEFAULT_TEMPLATE,
    ).strip()
    context["report_body"] = message
    attachment_paths: list[Path] = []
    artifact_id = source_artifact.id
    digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
    if has_attachment:
        derived, attachment, digest = ensure_attachment(
            session, source_job=source_job, wing=wing, source_digest=digest,
            header=header, rows=scoped, scope_key=scope_key, scope_label=scope_label,
        )
        artifact_id = derived.id
        attachment_paths.append(attachment)
    return RenderedHotspotReport(
        message=message,
        context=context,
        rows=scoped,
        issues=issues,
        attachment_paths=attachment_paths,
        source_artifact_id=artifact_id,
        source_artifact_sha256=digest,
    )


__all__ = [
    "HOTSPOT_DEFAULT_TEMPLATE", "HOTSPOT_RENDERER_KEY", "HOTSPOT_REPORT_KEY",
    "HotspotReviewRow", "RenderedHotspotReport", "render_hotspot_distance_report",
]

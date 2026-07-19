from __future__ import annotations

import re
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any

from sqlmodel import Session, select

from automation_core.config import get_settings  # Backward-compatible renderer test seam.
from automation_core.models import Job
from master_data.models import Markaz, School, Tehsil, Wing
from whatsapp_gateway.rendering.antidengue.digest_models import (
    CONSOLIDATED_DIGEST_DEFAULT_TEMPLATE,
    DigestSchool,
    RenderedConsolidatedDigest,
)
from whatsapp_gateway.rendering.antidengue.digest_messages import all_clear_digest_message
from whatsapp_gateway.rendering.antidengue.digest_workbook import ensure_digest_workbook
from whatsapp_gateway.rendering.antidengue.hotspot_report import (
    _plain_whatsapp_text,
    render_hotspot_distance_report,
)
from whatsapp_gateway.rendering.antidengue.issues import _issue
from whatsapp_gateway.rendering.antidengue.markaz_report import render_aeo_markaz_dormant_report
from whatsapp_gateway.rendering.antidengue.models import PAKISTAN_TIME
from whatsapp_gateway.rendering.antidengue.simple_activity_report import render_simple_activity_report
from whatsapp_gateway.rendering.antidengue.tehsil_report import render_tehsil_dormant_report
from whatsapp_gateway.rendering.antidengue.wing_report import render_wing_dormant_report


def _generated_label(source_job: Job) -> str:
    window = dict(((source_job.result or {}).get("summary") or {}).get("portal_acquisition") or {}).get("window") or {}
    raw = str(window.get("dateto") or "").strip()
    try:
        value = datetime.fromisoformat(raw)
        if value.tzinfo is not None:
            value = value.astimezone(PAKISTAN_TIME)
        return value.strftime("%d %b %Y · %I:%M %p")
    except ValueError:
        return raw.replace("T", " ") or "Time unavailable"


def _dormant_scope(
    session: Session, *, source_job: Job, wing: Wing, recipient_name: str,
    scope_key: str, scope_value: str, scope_values: list[str], deadline: str,
):
    policy = {"attachment_mode": "none", "message_style": "summary", "image_content": "summary"}
    if scope_key in {"district", "wing"}:
        return render_wing_dormant_report(
            session, source_job=source_job, wing=wing, recipient_name=recipient_name,
            presentation_policy=policy,
        )
    if scope_key == "tehsil":
        return render_tehsil_dormant_report(
            session, source_job=source_job, wing=wing, tehsil_id=uuid.UUID(scope_value),
            recipient_name=recipient_name,
            deadline=deadline,
            presentation_policy=policy,
        )
    if scope_key == "markaz":
        ids = [uuid.UUID(value) for value in (scope_values or [scope_value])]
        return render_aeo_markaz_dormant_report(
            session, source_job=source_job, wing=wing, markaz_ids=ids,
            recipient_name=recipient_name,
            deadline=deadline,
            presentation_policy=policy,
        )
    raise ValueError(f"Consolidated digest does not support {scope_key or 'an unbound'} scope")


def _school_index(
    session: Session, *, wing: Wing, emis_values: set[str],
    dormant_rows: list[Any], hotspot_rows: list[Any], timing_rows: list[Any],
) -> list[DigestSchool]:
    dormant = {row.emis for row in dormant_rows}
    hotspot = {row.emis for row in hotspot_rows if row.emis}
    timing = {row.emis for row in timing_rows if row.emis}
    dormant_info = {row.emis: row for row in dormant_rows}
    activity_info = {row.emis: row for row in [*hotspot_rows, *timing_rows] if row.emis}
    models = list(session.scalars(select(School).where(School.emis.in_(emis_values))).all()) if emis_values else []
    schools_by_emis = {item.emis: item for item in models}
    tehsil_ids = {item.tehsil_id for item in models if item.tehsil_id}
    markaz_ids = {item.markaz_id for item in models if item.markaz_id}
    tehsils = {item.id: item for item in session.scalars(select(Tehsil).where(Tehsil.id.in_(tehsil_ids))).all()} if tehsil_ids else {}
    markazes = {item.id: item for item in session.scalars(select(Markaz).where(Markaz.id.in_(markaz_ids))).all()} if markaz_ids else {}
    result: list[DigestSchool] = []
    for emis in emis_values:
        model = schools_by_emis.get(emis)
        dormant_row = dormant_info.get(emis)
        activity_row = activity_info.get(emis)
        values = activity_row.values if activity_row else {}
        name = model.name if model else (dormant_row.school_name if dormant_row else str(values.get("School Name") or "Unknown school"))
        tehsil = tehsils.get(model.tehsil_id).name if model and model.tehsil_id in tehsils else (dormant_row.tehsil if dormant_row else str(values.get("Tehsil") or "UNMAPPED TEHSIL"))
        markaz = markazes.get(model.markaz_id).name if model and model.markaz_id in markazes else (dormant_row.markaz if dormant_row else str(values.get("Markaz") or "UNMAPPED MARKAZ"))
        result.append(DigestSchool(
            emis=emis, school_name=name, wing=wing.name, tehsil=tehsil, markaz=markaz,
            dormant=emis in dormant, hotspot_distance=emis in hotspot, activity_timing=emis in timing,
        ))
    return sorted(result, key=lambda item: (
        item.tehsil.casefold(), item.markaz.casefold(), item.school_name.casefold(), item.emis
    ))


def _message(
    *, schools: list[DigestSchool], recipient_name: str, wing: Wing,
    scope_key: str, scope_label: str, generated_label: str, deadline: str,
) -> tuple[str, dict[str, str]]:
    dormant_count = sum(item.dormant for item in schools)
    hotspot_count = sum(item.hotspot_distance for item in schools)
    timing_count = sum(item.activity_timing for item in schools)
    def school_count(count: int) -> str:
        return f"{count} school{'s' if count != 1 else ''}"

    detail_lines: list[str] = []
    grouped: dict[tuple[str, str], list[DigestSchool]] = defaultdict(list)
    for school in schools:
        grouped[(school.tehsil, school.markaz)].append(school)
    shown = 0
    last_tehsil = ""
    for (tehsil, markaz), members in grouped.items():
        if scope_key in {"district", "wing"} and tehsil != last_tehsil:
            detail_lines.extend([f"*TEHSIL: {_plain_whatsapp_text(tehsil)}*", ""])
            last_tehsil = tehsil
        available = max(0, 30 - shown)
        if not available:
            break
        selected = members[:available]
        detail_lines.append(f"*{_plain_whatsapp_text(markaz)}* · {len(members)} school{'s' if len(members) != 1 else ''}")
        for school in selected:
            detail_lines.append(f"{shown + 1}. `{school.emis}` — {_plain_whatsapp_text(school.school_name)} · {school.issue_icons}")
            shown += 1
        detail_lines.append("")
    omitted = len(schools) - shown
    if omitted:
        detail_lines.extend([f"📎 {omitted} additional school{'s are' if omitted != 1 else ' is'} listed in the workbook.", ""])
    context = {
        "group_name": _plain_whatsapp_text(recipient_name),
        "scope_name": _plain_whatsapp_text(scope_label), "wing_name": _plain_whatsapp_text(wing.name),
        "generated_at": generated_label, "school_count": str(len(schools)),
        "dormant_count": str(dormant_count), "hotspot_count": str(hotspot_count),
        "timing_count": str(timing_count), "omitted_school_count": str(omitted),
        "dormant_summary": school_count(dormant_count),
        "hotspot_summary": school_count(hotspot_count),
        "timing_summary": school_count(timing_count),
        "school_details": "\n".join(detail_lines).strip(), "deadline": deadline,
    }
    message = re.sub(
        r"{{\s*([a-zA-Z0-9_]+)\s*}}",
        lambda match: context.get(match.group(1), match.group(0)),
        CONSOLIDATED_DIGEST_DEFAULT_TEMPLATE,
    ).strip()
    context["report_body"] = message
    return message, context


def render_consolidated_action_digest(
    session: Session, *, source_job: Job, wing: Wing, recipient_name: str,
    scope_key: str, scope_value: str, scope_label: str,
    presentation_policy: dict[str, Any] | None = None,
    scope_values: list[str] | None = None,
    deadline: str = "12:30 PM",
    deadline_timezone: str = "Asia/Karachi",
) -> RenderedConsolidatedDigest:
    accepted_values = list(scope_values or ([scope_value] if scope_value else []))
    dormant_report = _dormant_scope(
        session, source_job=source_job, wing=wing, recipient_name=recipient_name,
        scope_key=scope_key, scope_value=scope_value, scope_values=accepted_values,
        deadline=deadline,
    )
    activity_scope = "markaz" if scope_key == "markaz" else scope_key
    no_attachment = {"attachment_mode": "none"}
    hotspot_report = render_hotspot_distance_report(
        session, source_job=source_job, wing=wing, recipient_name=recipient_name,
        scope_key=activity_scope, scope_value=scope_value, scope_values=accepted_values,
        scope_label=scope_label, presentation_policy=no_attachment,
    )
    timing_report = render_simple_activity_report(
        session, source_job=source_job, wing=wing, recipient_name=recipient_name,
        scope_key=activity_scope, scope_value=scope_value, scope_values=accepted_values,
        scope_label=scope_label, presentation_policy=no_attachment,
    )
    dormant_rows = list(dormant_report.schools)
    hotspot_rows = list(hotspot_report.rows)
    timing_rows = list(timing_report.rows)
    emis_values = {item.emis for item in dormant_rows}
    emis_values.update(item.emis for item in hotspot_rows if item.emis)
    emis_values.update(item.emis for item in timing_rows if item.emis)
    schools = _school_index(
        session, wing=wing, emis_values=emis_values, dormant_rows=dormant_rows,
        hotspot_rows=hotspot_rows, timing_rows=timing_rows,
    )
    issues = [
        item for item in [
            *dormant_report.issues, *hotspot_report.issues,
            *timing_report.source_issues, *timing_report.issues,
        ] if item.get("code") != "nothing_to_dispatch"
    ]
    source_hashes = {
        "dormant": dormant_report.source_artifact_sha256,
        "hotspot": hotspot_report.source_artifact_sha256,
        "timing": timing_report.source_artifact_sha256,
    }
    if not schools:
        issues.append(_issue("nothing_to_dispatch", "info", f"No Anti-Dengue action items were found for {scope_label}."))
        generated_label = _generated_label(source_job)
        message, context = all_clear_digest_message(
            recipient_name=_plain_whatsapp_text(recipient_name),
            wing_name=_plain_whatsapp_text(wing.name),
            scope_label=_plain_whatsapp_text(scope_label),
            generated_label=generated_label,
            deadline=deadline,
        )
        artifact, path, digest = ensure_digest_workbook(
            session, source_job=source_job, scope_key=scope_key,
            scope_label=scope_label, scope_ids=accepted_values,
            generated_label=generated_label,
            schools=[], dormant_rows=dormant_rows, hotspot_rows=hotspot_rows,
            timing_rows=timing_rows, source_hashes=source_hashes,
            deadline_label=deadline, deadline_timezone=deadline_timezone,
        )
        return RenderedConsolidatedDigest(
            message, context, [], dormant_rows, hotspot_rows, timing_rows,
            issues=issues, attachment_paths=[path], source_artifact_id=artifact.id,
            source_artifact_sha256=digest,
        )
    generated_label = _generated_label(source_job)
    message, context = _message(
        schools=schools, recipient_name=recipient_name, wing=wing, scope_key=scope_key,
        scope_label=scope_label, generated_label=generated_label,
        deadline=deadline,
    )
    artifact, path, digest = ensure_digest_workbook(
        session, source_job=source_job, scope_key=scope_key, scope_label=scope_label,
        scope_ids=accepted_values, generated_label=generated_label,
        schools=schools, dormant_rows=dormant_rows,
        hotspot_rows=hotspot_rows, timing_rows=timing_rows, source_hashes=source_hashes,
        deadline_label=deadline, deadline_timezone=deadline_timezone,
    )
    return RenderedConsolidatedDigest(
        message, context, schools, dormant_rows, hotspot_rows, timing_rows,
        issues=issues, attachment_paths=[path], source_artifact_id=artifact.id,
        source_artifact_sha256=digest,
    )


__all__ = ["render_consolidated_action_digest"]

from __future__ import annotations

from typing import Any

from sqlmodel import Session

from automation_core.config import get_settings  # Backward-compatible renderer test seam.
from automation_core.models import Job
from master_data.models import Wing
from whatsapp_gateway.rendering.antidengue.digest_messages import all_clear_digest_message
from whatsapp_gateway.rendering.antidengue.digest_models import RenderedConsolidatedDigest
from whatsapp_gateway.rendering.antidengue.digest_presentation import (
    build_digest_message,
    parent_issue as _parent_issue,
)
from whatsapp_gateway.rendering.antidengue.digest_scope import (
    build_school_index,
    generated_label,
    render_dormant_scope,
)
from whatsapp_gateway.rendering.antidengue.digest_workbook import ensure_digest_workbook
from whatsapp_gateway.rendering.antidengue.hotspot_report import (
    _plain_whatsapp_text,
    render_hotspot_distance_report,
)
from whatsapp_gateway.rendering.antidengue.issues import _issue
from whatsapp_gateway.rendering.antidengue.simple_activity_report import (
    render_simple_activity_report,
)


def _component_reports(
    session: Session,
    *,
    source_job: Job,
    wing: Wing,
    recipient_name: str,
    scope_key: str,
    scope_value: str,
    scope_label: str,
    accepted_values: list[str],
    deadline: str,
):
    dormant_report = render_dormant_scope(
        session,
        source_job=source_job,
        wing=wing,
        recipient_name=recipient_name,
        scope_key=scope_key,
        scope_value=scope_value,
        scope_values=accepted_values,
        deadline=deadline,
    )
    activity_scope = "markaz" if scope_key == "markaz" else scope_key
    component_policy = {
        "attachment_mode": "none",
        "evidence_attachment_provided_by_parent": True,
    }
    common = {
        "source_job": source_job,
        "wing": wing,
        "recipient_name": recipient_name,
        "scope_key": activity_scope,
        "scope_value": scope_value,
        "scope_values": accepted_values,
        "scope_label": scope_label,
        "presentation_policy": component_policy,
    }
    hotspot_report = render_hotspot_distance_report(session, **common)
    timing_report = render_simple_activity_report(session, **common)
    return dormant_report, hotspot_report, timing_report


def _render_workbook(
    session: Session,
    *,
    source_job: Job,
    scope_key: str,
    scope_label: str,
    accepted_values: list[str],
    generated: str,
    schools,
    dormant_rows,
    hotspot_rows,
    timing_rows,
    source_hashes: dict[str, str],
    deadline: str,
    deadline_timezone: str,
):
    return ensure_digest_workbook(
        session,
        source_job=source_job,
        scope_key=scope_key,
        scope_label=scope_label,
        scope_ids=accepted_values,
        generated_label=generated,
        schools=schools,
        dormant_rows=dormant_rows,
        hotspot_rows=hotspot_rows,
        timing_rows=timing_rows,
        source_hashes=source_hashes,
        deadline_label=deadline,
        deadline_timezone=deadline_timezone,
    )


def render_consolidated_action_digest(
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
    deadline: str = "12:30 PM",
    deadline_timezone: str = "Asia/Karachi",
) -> RenderedConsolidatedDigest:
    del presentation_policy  # The parent always owns its final workbook and message.
    accepted_values = list(scope_values or ([scope_value] if scope_value else []))
    dormant_report, hotspot_report, timing_report = _component_reports(
        session,
        source_job=source_job,
        wing=wing,
        recipient_name=recipient_name,
        scope_key=scope_key,
        scope_value=scope_value,
        scope_label=scope_label,
        accepted_values=accepted_values,
        deadline=deadline,
    )
    dormant_rows = list(dormant_report.schools)
    hotspot_rows = list(hotspot_report.rows)
    timing_rows = list(timing_report.rows)
    emis_values = {item.emis for item in dormant_rows}
    emis_values.update(item.emis for item in hotspot_rows if item.emis)
    emis_values.update(item.emis for item in timing_rows if item.emis)
    schools = build_school_index(
        session,
        wing=wing,
        emis_values=emis_values,
        dormant_rows=dormant_rows,
        hotspot_rows=hotspot_rows,
        timing_rows=timing_rows,
    )
    issues = [
        _parent_issue(item)
        for item in [
            *dormant_report.issues,
            *hotspot_report.issues,
            *timing_report.source_issues,
            *timing_report.issues,
        ]
        if item.get("code") != "nothing_to_dispatch"
    ]
    source_hashes = {
        "dormant": dormant_report.source_artifact_sha256,
        "hotspot": hotspot_report.source_artifact_sha256,
        "timing": timing_report.source_artifact_sha256,
    }
    generated = generated_label(source_job)

    if schools:
        message, context = build_digest_message(
            schools=schools,
            recipient_name=recipient_name,
            wing=wing,
            scope_key=scope_key,
            scope_label=scope_label,
            generated_label=generated,
            deadline=deadline,
        )
    else:
        issues.append(
            _issue(
                "nothing_to_dispatch",
                "info",
                f"No Anti-Dengue action items were found for {scope_label}.",
            )
        )
        message, context = all_clear_digest_message(
            recipient_name=_plain_whatsapp_text(recipient_name),
            wing_name=_plain_whatsapp_text(wing.name),
            scope_label=_plain_whatsapp_text(scope_label),
            generated_label=generated,
            deadline=deadline,
        )

    artifact, path, digest = _render_workbook(
        session,
        source_job=source_job,
        scope_key=scope_key,
        scope_label=scope_label,
        accepted_values=accepted_values,
        generated=generated,
        schools=schools,
        dormant_rows=dormant_rows,
        hotspot_rows=hotspot_rows,
        timing_rows=timing_rows,
        source_hashes=source_hashes,
        deadline=deadline,
        deadline_timezone=deadline_timezone,
    )
    return RenderedConsolidatedDigest(
        message,
        context,
        schools,
        dormant_rows,
        hotspot_rows,
        timing_rows,
        issues=issues,
        attachment_paths=[path],
        source_artifact_id=artifact.id,
        source_artifact_sha256=digest,
    )


__all__ = ["render_consolidated_action_digest"]

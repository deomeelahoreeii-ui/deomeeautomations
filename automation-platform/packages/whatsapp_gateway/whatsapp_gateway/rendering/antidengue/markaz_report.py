from __future__ import annotations

import hashlib
import uuid
from collections import Counter
from typing import Any

from sqlmodel import Session, select

from automation_core.models import Job
from master_data.models import Markaz, School, Tehsil, Wing
from whatsapp_gateway.rendering.antidengue.excel import _ensure_excel_attachment
from whatsapp_gateway.rendering.antidengue.images import _ensure_image_attachment
from whatsapp_gateway.rendering.antidengue.issues import _issue
from whatsapp_gateway.rendering.antidengue.messages import _build_aeo_markaz_message
from whatsapp_gateway.rendering.antidengue.models import RenderedMarkazDormantReport, ScopedDormantSchool
from whatsapp_gateway.rendering.antidengue.policy import _has_attachment, normalize_presentation_policy
from whatsapp_gateway.rendering.antidengue.source_data import _canonical_report_artifact


def render_aeo_markaz_dormant_report(
    session: Session,
    *,
    source_job: Job,
    wing: Wing,
    markaz_ids: list[uuid.UUID],
    recipient_name: str,
    deadline: str = "12:30 PM",
    presentation_policy: dict[str, Any] | None = None,
) -> RenderedMarkazDormantReport:
    policy = normalize_presentation_policy(presentation_policy, report_key="markaz_dormant_summary")
    unique_ids = sorted(set(markaz_ids), key=str)
    markazes = list(session.scalars(select(Markaz).where(Markaz.id.in_(unique_ids))).all())
    if len(markazes) != len(unique_ids) or any(not item.active or item.wing_id != wing.id for item in markazes):
        raise ValueError("The AEO audience contains an inactive or cross-wing Markaz jurisdiction.")
    markazes.sort(key=lambda item: item.name.casefold())
    tehsil_ids = {item.tehsil_id for item in markazes}
    tehsils = {item.id: item for item in session.scalars(select(Tehsil).where(Tehsil.id.in_(tehsil_ids))).all()}

    artifact, artifact_path, source_rows = _canonical_report_artifact(session, source_job)
    counts = Counter(row.emis for row in source_rows)
    duplicate_emis = sorted(emis for emis, count in counts.items() if count > 1)
    source_by_emis = {row.emis: row for row in source_rows}
    schools = list(session.scalars(select(School).where(School.emis.in_(list(source_by_emis)))).all())
    schools_by_emis = {school.emis: school for school in schools}
    unknown_emis = sorted(set(source_by_emis) - set(schools_by_emis))
    markaz_by_id = {item.id: item for item in markazes}
    scoped = [
        ScopedDormantSchool(
            emis=school.emis,
            school_name=school.name or source_by_emis[school.emis].school_name,
            tehsil=tehsils[school.tehsil_id].name,
            markaz=markaz_by_id[school.markaz_id].name,
        )
        for school in schools
        if school.active and school.wing_id == wing.id and school.markaz_id in markaz_by_id
    ]
    scoped.sort(key=lambda item: (item.markaz.casefold(), item.school_name.casefold(), item.emis))
    issues: list[dict[str, Any]] = []
    if duplicate_emis:
        issues.append(_issue("duplicate_source_emis", "blocked", f"The canonical report contains {len(duplicate_emis)} duplicate school EMIS value(s).", emis=duplicate_emis[:20]))
    if unknown_emis:
        issues.append(_issue("unknown_source_schools", "warning", f"{len(unknown_emis)} dormant school EMIS value(s) are absent from PostgreSQL and were excluded.", emis=unknown_emis[:20]))
    if not scoped:
        issues.append(_issue("nothing_to_dispatch", "warning", f"No dormant schools were found for {', '.join(item.name for item in markazes)}."))
    message, context = _build_aeo_markaz_message(
        source_job=source_job, recipient_name=recipient_name, wing=wing,
        markazes=markazes, schools=scoped, deadline=deadline, policy=policy,
    )
    source_digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    attachment_paths = []
    scope_label = recipient_name
    if scoped and _has_attachment(policy, "image"):
        _, path, _ = _ensure_image_attachment(
            session, source_job=source_job, wing=wing, schools=scoped,
            source_digest=source_digest, scope_key="aeo_markaz", scope_label=scope_label,
            image_content=policy["image_content"],
        )
        attachment_paths.append(path)
    if scoped and _has_attachment(policy, "excel"):
        _, path, _ = _ensure_excel_attachment(
            session, source_job=source_job, wing=wing, schools=scoped,
            source_digest=source_digest, scope_key="aeo_markaz", scope_label=scope_label,
        )
        attachment_paths.append(path)
    return RenderedMarkazDormantReport(
        message=message, context=context, schools=scoped, issues=issues,
        attachment_paths=attachment_paths, source_artifact_id=artifact.id,
        source_artifact_sha256=source_digest,
        markaz_ids=[str(item.id) for item in markazes],
    )

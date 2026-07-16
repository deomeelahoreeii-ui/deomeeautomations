from __future__ import annotations

import hashlib
import uuid
from collections import Counter
from typing import Any

from sqlalchemy import select
from sqlmodel import Session

from automation_core.models import Job
from master_data.models import Markaz, School, Tehsil, Wing
from whatsapp_gateway.rendering.antidengue.excel import _ensure_excel_attachment
from whatsapp_gateway.rendering.antidengue.formatting import _wing_label
from whatsapp_gateway.rendering.antidengue.images import _ensure_image_attachment
from whatsapp_gateway.rendering.antidengue.issues import _issue
from whatsapp_gateway.rendering.antidengue.messages import _build_message
from whatsapp_gateway.rendering.antidengue.models import (
    RenderedTehsilDormantReport, ScopedDormantSchool,
)
from whatsapp_gateway.rendering.antidengue.policy import (
    _has_attachment, normalize_presentation_policy,
)
from whatsapp_gateway.rendering.antidengue.source_data import _canonical_report_artifact


def render_tehsil_dormant_report(
    session: Session,
    *,
    source_job: Job,
    wing: Wing,
    tehsil_id: uuid.UUID,
    recipient_name: str,
    deadline: str = "12:30 PM",
    presentation_policy: dict[str, Any] | None = None,
) -> RenderedTehsilDormantReport:
    policy = normalize_presentation_policy(
        presentation_policy,
        report_key="tehsil_dormant_summary",
    )
    tehsil = session.get(Tehsil, tehsil_id)
    if tehsil is None or not tehsil.active or tehsil.district_id != wing.district_id:
        raise ValueError("The audience target is not bound to an active tehsil in this wing's district.")

    artifact, artifact_path, source_rows = _canonical_report_artifact(session, source_job)
    counts = Counter(row.emis for row in source_rows)
    duplicate_emis = sorted(emis for emis, count in counts.items() if count > 1)
    source_by_emis = {row.emis: row for row in source_rows}
    schools = session.scalars(
        select(School).where(School.emis.in_(list(source_by_emis)))
    ).all()
    schools_by_emis = {school.emis: school for school in schools}
    unknown_emis = sorted(set(source_by_emis) - set(schools_by_emis))
    scoped_models = [
        school
        for school in schools
        if school.active and school.wing_id == wing.id and school.tehsil_id == tehsil.id
    ]
    markaz_ids = {school.markaz_id for school in scoped_models if school.markaz_id}
    markazes = {
        item.id: item
        for item in session.scalars(select(Markaz).where(Markaz.id.in_(markaz_ids))).all()
    } if markaz_ids else {}

    scoped: list[ScopedDormantSchool] = []
    missing_markaz: list[str] = []
    for school in scoped_models:
        markaz = markazes.get(school.markaz_id) if school.markaz_id else None
        if markaz is None:
            missing_markaz.append(school.emis)
        scoped.append(
            ScopedDormantSchool(
                emis=school.emis,
                school_name=school.name or source_by_emis[school.emis].school_name,
                tehsil=tehsil.name,
                markaz=markaz.name if markaz else "UNMAPPED MARKAZ",
            )
        )
    scoped.sort(key=lambda item: (item.markaz.casefold(), item.school_name.casefold(), item.emis))

    issues: list[dict[str, Any]] = []
    if duplicate_emis:
        issues.append(
            _issue(
                "duplicate_source_emis",
                "blocked",
                f"The canonical report contains {len(duplicate_emis)} duplicate school EMIS value(s).",
                emis=duplicate_emis[:20],
            )
        )
    if unknown_emis:
        issues.append(
            _issue(
                "unknown_source_schools",
                "warning",
                f"{len(unknown_emis)} dormant school EMIS value(s) are absent from PostgreSQL and were excluded.",
                emis=unknown_emis[:20],
            )
        )
    if missing_markaz:
        issues.append(
            _issue(
                "missing_markaz",
                "warning",
                f"{len(missing_markaz)} scoped school(s) have no authoritative Markaz and are listed under UNMAPPED MARKAZ.",
                emis=missing_markaz[:20],
            )
        )
    if not scoped:
        issues.append(
            _issue(
                "nothing_to_dispatch",
                "warning",
                f"No dormant { _wing_label(wing) } schools were found in {tehsil.name}.",
            )
        )

    message, context = _build_message(
        source_job=source_job,
        recipient_name=recipient_name,
        wing=wing,
        tehsil=tehsil,
        schools=scoped,
        deadline=deadline,
        policy=policy,
    )
    # A digest is calculated here without importing the preview service and
    # creating a circular dependency.
    import hashlib

    digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    attachment_paths: list[Path] = []
    if scoped and _has_attachment(policy, "image"):
        _, image_path, digest = _ensure_image_attachment(
            session,
            source_job=source_job,
            wing=wing,
            schools=scoped,
            source_digest=digest,
            scope_key="tehsil",
            scope_label=tehsil.name,
            image_content=policy["image_content"],
        )
        attachment_paths.append(image_path)
    if scoped and _has_attachment(policy, "excel"):
        _, excel_path, digest = _ensure_excel_attachment(
            session,
            source_job=source_job,
            wing=wing,
            schools=scoped,
            source_digest=hashlib.sha256(artifact_path.read_bytes()).hexdigest(),
            scope_key="tehsil",
            scope_label=tehsil.name,
        )
        attachment_paths.append(excel_path)
    return RenderedTehsilDormantReport(
        message=message,
        context=context,
        schools=scoped,
        issues=issues,
        attachment_paths=attachment_paths,
        source_artifact_id=artifact.id,
        source_artifact_sha256=digest,
    )

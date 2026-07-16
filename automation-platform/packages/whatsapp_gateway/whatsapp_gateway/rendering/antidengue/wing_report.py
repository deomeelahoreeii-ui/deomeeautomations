from __future__ import annotations

import hashlib
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
from whatsapp_gateway.rendering.antidengue.messages import _build_wing_message
from whatsapp_gateway.rendering.antidengue.models import (
    RenderedWingDormantReport, ScopedDormantSchool,
)
from whatsapp_gateway.rendering.antidengue.policy import (
    _has_attachment, normalize_presentation_policy,
)
from whatsapp_gateway.rendering.antidengue.source_data import _canonical_report_artifact


def render_wing_dormant_report(
    session: Session,
    *,
    source_job: Job,
    wing: Wing,
    recipient_name: str,
    presentation_policy: dict[str, Any] | None = None,
) -> RenderedWingDormantReport:
    policy = normalize_presentation_policy(presentation_policy, report_key="wing_summary")
    source_artifact, source_path, source_rows = _canonical_report_artifact(session, source_job)
    counts = Counter(row.emis for row in source_rows)
    duplicate_emis = sorted(emis for emis, count in counts.items() if count > 1)
    source_by_emis = {row.emis: row for row in source_rows}
    school_models = session.scalars(
        select(School).where(School.emis.in_(list(source_by_emis)))
    ).all()
    schools_by_emis = {school.emis: school for school in school_models}
    unknown_emis = sorted(set(source_by_emis) - set(schools_by_emis))
    scoped_models = [
        school for school in school_models if school.active and school.wing_id == wing.id
    ]
    tehsil_ids = {school.tehsil_id for school in scoped_models if school.tehsil_id}
    markaz_ids = {school.markaz_id for school in scoped_models if school.markaz_id}
    tehsils = {
        item.id: item
        for item in session.scalars(select(Tehsil).where(Tehsil.id.in_(tehsil_ids))).all()
    } if tehsil_ids else {}
    markazes = {
        item.id: item
        for item in session.scalars(select(Markaz).where(Markaz.id.in_(markaz_ids))).all()
    } if markaz_ids else {}

    scoped: list[ScopedDormantSchool] = []
    missing_markaz: list[str] = []
    missing_tehsil: list[str] = []
    for school in scoped_models:
        tehsil = tehsils.get(school.tehsil_id) if school.tehsil_id else None
        markaz = markazes.get(school.markaz_id) if school.markaz_id else None
        if tehsil is None:
            missing_tehsil.append(school.emis)
        if markaz is None:
            missing_markaz.append(school.emis)
        scoped.append(
            ScopedDormantSchool(
                emis=school.emis,
                school_name=school.name or source_by_emis[school.emis].school_name,
                tehsil=tehsil.name if tehsil else "UNMAPPED TEHSIL",
                markaz=markaz.name if markaz else "UNMAPPED MARKAZ",
            )
        )
    scoped.sort(
        key=lambda item: (
            item.tehsil.casefold(), item.markaz.casefold(), item.school_name.casefold(), item.emis
        )
    )

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
    if missing_tehsil:
        issues.append(
            _issue(
                "missing_tehsil",
                "warning",
                f"{len(missing_tehsil)} { _wing_label(wing) } school(s) have no authoritative Tehsil.",
                emis=missing_tehsil[:20],
            )
        )
    if missing_markaz:
        issues.append(
            _issue(
                "missing_markaz",
                "warning",
                f"{len(missing_markaz)} { _wing_label(wing) } school(s) have no authoritative Markaz and are summarized as UNMAPPED MARKAZ.",
                emis=missing_markaz[:20],
            )
        )
    if not scoped:
        issues.append(
            _issue(
                "nothing_to_dispatch",
                "warning",
                f"No dormant { _wing_label(wing) } schools were found.",
            )
        )

    message, context = _build_wing_message(
        source_job=source_job,
        recipient_name=recipient_name,
        wing=wing,
        schools=scoped,
        policy=policy,
    )
    source_digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
    attachment_path: Path | None = None
    attachment_paths: list[Path] = []
    artifact_id: int | None = source_artifact.id
    artifact_digest = source_digest
    if scoped and _has_attachment(policy, "image"):
        derived_artifact, image_path, artifact_digest = _ensure_image_attachment(
            session,
            source_job=source_job,
            wing=wing,
            schools=scoped,
            source_digest=source_digest,
            scope_key="wing",
            scope_label=wing.name,
            image_content=policy["image_content"],
        )
        artifact_id = derived_artifact.id
        attachment_paths.append(image_path)
    if scoped and _has_attachment(policy, "excel"):
        derived_artifact, attachment_path, artifact_digest = _ensure_excel_attachment(
            session,
            source_job=source_job,
            wing=wing,
            schools=scoped,
            source_digest=source_digest,
            scope_key="wing",
            scope_label=wing.name,
        )
        artifact_id = derived_artifact.id
        attachment_paths.append(attachment_path)
    return RenderedWingDormantReport(
        message=message,
        context=context,
        schools=scoped,
        issues=issues,
        attachment_path=attachment_path,
        attachment_paths=attachment_paths,
        source_artifact_id=artifact_id,
        source_artifact_sha256=artifact_digest,
    )

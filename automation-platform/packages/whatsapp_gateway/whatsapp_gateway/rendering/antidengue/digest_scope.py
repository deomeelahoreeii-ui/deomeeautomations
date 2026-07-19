from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlmodel import Session, select

from automation_core.models import Job
from master_data.models import Markaz, School, Tehsil, Wing
from whatsapp_gateway.rendering.antidengue.digest_models import DigestSchool
from whatsapp_gateway.rendering.antidengue.markaz_report import render_aeo_markaz_dormant_report
from whatsapp_gateway.rendering.antidengue.models import PAKISTAN_TIME
from whatsapp_gateway.rendering.antidengue.tehsil_report import render_tehsil_dormant_report
from whatsapp_gateway.rendering.antidengue.wing_report import render_wing_dormant_report


def generated_label(source_job: Job) -> str:
    window = dict(
        ((source_job.result or {}).get("summary") or {}).get("portal_acquisition") or {}
    ).get("window") or {}
    raw = str(window.get("dateto") or "").strip()
    try:
        value = datetime.fromisoformat(raw)
        if value.tzinfo is not None:
            value = value.astimezone(PAKISTAN_TIME)
        return value.strftime("%d %b %Y · %I:%M %p")
    except ValueError:
        return raw.replace("T", " ") or "Time unavailable"


def render_dormant_scope(
    session: Session,
    *,
    source_job: Job,
    wing: Wing,
    recipient_name: str,
    scope_key: str,
    scope_value: str,
    scope_values: list[str],
    deadline: str,
):
    policy = {
        "attachment_mode": "none",
        "message_style": "summary",
        "image_content": "summary",
    }
    if scope_key in {"district", "wing"}:
        return render_wing_dormant_report(
            session,
            source_job=source_job,
            wing=wing,
            recipient_name=recipient_name,
            presentation_policy=policy,
        )
    if scope_key == "tehsil":
        return render_tehsil_dormant_report(
            session,
            source_job=source_job,
            wing=wing,
            tehsil_id=uuid.UUID(scope_value),
            recipient_name=recipient_name,
            deadline=deadline,
            presentation_policy=policy,
        )
    if scope_key == "markaz":
        ids = [uuid.UUID(value) for value in (scope_values or [scope_value])]
        return render_aeo_markaz_dormant_report(
            session,
            source_job=source_job,
            wing=wing,
            markaz_ids=ids,
            recipient_name=recipient_name,
            deadline=deadline,
            presentation_policy=policy,
        )
    raise ValueError(
        f"Consolidated digest does not support {scope_key or 'an unbound'} scope"
    )


def build_school_index(
    session: Session,
    *,
    wing: Wing,
    emis_values: set[str],
    dormant_rows: list[Any],
    hotspot_rows: list[Any],
    timing_rows: list[Any],
) -> list[DigestSchool]:
    dormant = {row.emis for row in dormant_rows}
    hotspot = {row.emis for row in hotspot_rows if row.emis}
    timing = {row.emis for row in timing_rows if row.emis}
    dormant_info = {row.emis: row for row in dormant_rows}
    activity_info = {
        row.emis: row for row in [*hotspot_rows, *timing_rows] if row.emis
    }
    models = (
        list(session.scalars(select(School).where(School.emis.in_(emis_values))).all())
        if emis_values
        else []
    )
    schools_by_emis = {item.emis: item for item in models}
    tehsil_ids = {item.tehsil_id for item in models if item.tehsil_id}
    markaz_ids = {item.markaz_id for item in models if item.markaz_id}
    tehsils = (
        {
            item.id: item
            for item in session.scalars(
                select(Tehsil).where(Tehsil.id.in_(tehsil_ids))
            ).all()
        }
        if tehsil_ids
        else {}
    )
    markazes = (
        {
            item.id: item
            for item in session.scalars(
                select(Markaz).where(Markaz.id.in_(markaz_ids))
            ).all()
        }
        if markaz_ids
        else {}
    )

    result: list[DigestSchool] = []
    for emis in emis_values:
        model = schools_by_emis.get(emis)
        dormant_row = dormant_info.get(emis)
        activity_row = activity_info.get(emis)
        values = activity_row.values if activity_row else {}
        name = (
            model.name
            if model
            else (
                dormant_row.school_name
                if dormant_row
                else str(values.get("School Name") or "Unknown school")
            )
        )
        tehsil = (
            tehsils.get(model.tehsil_id).name
            if model and model.tehsil_id in tehsils
            else (
                dormant_row.tehsil
                if dormant_row
                else str(values.get("Tehsil") or "UNMAPPED TEHSIL")
            )
        )
        markaz = (
            markazes.get(model.markaz_id).name
            if model and model.markaz_id in markazes
            else (
                dormant_row.markaz
                if dormant_row
                else str(values.get("Markaz") or "UNMAPPED MARKAZ")
            )
        )
        result.append(
            DigestSchool(
                emis=emis,
                school_name=name,
                wing=wing.name,
                tehsil=tehsil,
                markaz=markaz,
                dormant=emis in dormant,
                hotspot_distance=emis in hotspot,
                activity_timing=emis in timing,
            )
        )
    return sorted(
        result,
        key=lambda item: (
            item.tehsil.casefold(),
            item.markaz.casefold(),
            item.school_name.casefold(),
            item.emis,
        ),
    )


__all__ = ["build_school_index", "generated_label", "render_dormant_scope"]

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from sqlmodel import Session, select

from antidengue_automation.models import (
    AntiDengueActivityRule,
    AntiDengueScheduleExecution,
    AntiDengueSimpleActivityRule,
)
from automation_core.models import Job
from master_data.models import (
    Department,
    District,
    Markaz,
    Officer,
    OfficerJurisdiction,
    School,
    SchoolOfficerOverride,
    Tehsil,
    Wing,
)
from whatsapp_gateway.persistence.configuration import (
    WhatsAppAudienceMember,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
)
from whatsapp_gateway.persistence.account import WhatsAppAccount
from whatsapp_gateway.configuration.dynamic_audiences import resolve_dynamic_audience
from antidengue_automation.dynamic_runtime_routes import dynamic_runtime_route_rows
from whatsapp_gateway.persistence.directory import (
    WhatsAppDirectoryContact,
    WhatsAppDirectoryGroup,
)

RUNTIME_SNAPSHOT_SCHEMA_VERSION = 1


def _school_level_from_name(name: str) -> str:
    return {
        "GPS": "Primary",
        "GES": "Middle",
        "GMMS": "sMosque",
    }.get(name.strip().upper().split(" ", 1)[0], "")


def _deos_wise(wing_name: str) -> str:
    return "M-EE" if wing_name.upper().replace(" ", "") in {"DEOMEE", "MEE"} else wing_name


def _active_by_id(session: Session, model: type[Any]) -> dict[uuid.UUID, Any]:
    return {
        item.id: item
        for item in session.exec(select(model).where(model.active == True)).all()  # noqa: E712
    }


def _master_school_rows(session: Session) -> tuple[list[dict[str, Any]], list[School]]:
    districts = _active_by_id(session, District)
    departments = _active_by_id(session, Department)
    wings = _active_by_id(session, Wing)
    tehsils = _active_by_id(session, Tehsil)
    markazes = _active_by_id(session, Markaz)
    schools = list(session.exec(select(School).where(School.active == True)).all())  # noqa: E712
    schools.sort(
        key=lambda school: (
            tehsils.get(school.tehsil_id).name if tehsils.get(school.tehsil_id) else "",
            markazes.get(school.markaz_id).name if school.markaz_id and markazes.get(school.markaz_id) else "",
            school.name,
            school.emis,
        )
    )

    rows: list[dict[str, Any]] = []
    for index, school in enumerate(schools, start=1):
        wing = wings.get(school.wing_id)
        rows.append(
            {
                "Sr. No.": index,
                "District": districts.get(school.district_id).name if districts.get(school.district_id) else "",
                "Department": departments.get(school.department_id).name if departments.get(school.department_id) else "",
                "Tehsil": tehsils.get(school.tehsil_id).name if tehsils.get(school.tehsil_id) else "",
                "Markaz": markazes.get(school.markaz_id).name if school.markaz_id and markazes.get(school.markaz_id) else "",
                "School Name": school.name,
                "School EMIS": school.emis,
                "School Type": school.school_type or "Male",
                "DEOs Wise": school.deos_wise or _deos_wise(wing.name if wing else ""),
                "School Level": school.school_level or _school_level_from_name(school.name),
                "_school_id": str(school.id),
                "_wing_id": str(school.wing_id),
                "_tehsil_id": str(school.tehsil_id),
                "_markaz_id": str(school.markaz_id) if school.markaz_id else "",
            }
        )
    return rows, schools


def _officer_mapping_rows(session: Session, master_rows: list[dict[str, Any]], schools: list[School]) -> list[dict[str, Any]]:
    officers = _active_by_id(session, Officer)
    jurisdictions = list(
        session.exec(select(OfficerJurisdiction).where(OfficerJurisdiction.active == True)).all()  # noqa: E712
    )
    overrides = list(
        session.exec(select(SchoolOfficerOverride).where(SchoolOfficerOverride.active == True)).all()  # noqa: E712
    )
    override_by_school_role = {(item.school_id, item.role): item.officer_id for item in overrides}

    jurisdiction_by_scope: dict[tuple[str, uuid.UUID, uuid.UUID, uuid.UUID | None], uuid.UUID] = {}
    for item in jurisdictions:
        jurisdiction_by_scope[(item.role, item.wing_id, item.tehsil_id, item.markaz_id)] = item.officer_id

    master_by_school = {row["_school_id"]: row for row in master_rows}
    rows: list[dict[str, Any]] = []
    for school in schools:
        master = master_by_school[str(school.id)]
        resolved: dict[str, Officer | None] = {}
        for role in ("ddeo", "aeo"):
            officer_id = override_by_school_role.get((school.id, role))
            if officer_id is None:
                markaz_id = school.markaz_id if role == "aeo" else None
                officer_id = jurisdiction_by_scope.get(
                    (role, school.wing_id, school.tehsil_id, markaz_id)
                )
                if officer_id is None and role == "aeo":
                    officer_id = jurisdiction_by_scope.get(
                        (role, school.wing_id, school.tehsil_id, None)
                    )
            resolved[role] = officers.get(officer_id) if officer_id else None

        rows.append(
            {
                "emis": school.emis,
                "school_name": school.name,
                "tehsil": master["Tehsil"],
                "markaz": master["Markaz"],
                "ddeo_name": resolved["ddeo"].name if resolved["ddeo"] else "",
                "ddeo_cell_number": resolved["ddeo"].mobile if resolved["ddeo"] else "",
                "aeo_name": resolved["aeo"].name if resolved["aeo"] else "",
                "aeo_cell_number": resolved["aeo"].mobile if resolved["aeo"] else "",
            }
        )
    return rows


def _route_rows(session: Session, profile_ids: Iterable[str]) -> list[dict[str, Any]]:
    parsed_ids = [uuid.UUID(str(value)) for value in profile_ids]
    profiles = [session.get(WhatsAppDispatchProfile, value) for value in parsed_ids]
    profiles = [profile for profile in profiles if profile is not None and profile.enabled]
    profiles = [
        profile
        for profile in profiles
        if (
            (report_type := session.get(WhatsAppReportType, profile.report_type_id))
            and report_type.key not in {
                "hotspot_distance_activity", "simple_activity_timing",
                "consolidated_action_digest",
            }
        )
    ]
    group_ids: set[uuid.UUID] = set()
    contact_ids: set[uuid.UUID] = set()
    members_by_profile: list[tuple[WhatsAppDispatchProfile, WhatsAppAudienceMember]] = []
    dynamic_rows: list[dict[str, Any]] = []
    for profile in profiles:
        members = session.exec(
            select(WhatsAppAudienceMember).where(
                WhatsAppAudienceMember.audience_id == profile.audience_id,
                WhatsAppAudienceMember.enabled == True,  # noqa: E712
            )
        ).all()
        for member in members:
            members_by_profile.append((profile, member))
            if member.directory_group_id:
                group_ids.add(member.directory_group_id)
            if member.directory_contact_id:
                contact_ids.add(member.directory_contact_id)
        for member in resolve_dynamic_audience(
            session, audience_id=profile.audience_id,
            account=session.get(WhatsAppAccount, profile.account_id),
        ):
            officer = session.get(Officer, member.officer_id)
            dynamic_rows.extend(dynamic_runtime_route_rows(profile, member, officer))

    groups = {value: session.get(WhatsAppDirectoryGroup, value) for value in group_ids}
    contacts = {value: session.get(WhatsAppDirectoryContact, value) for value in contact_ids}
    rows: list[dict[str, Any]] = list(dynamic_rows)
    seen: set[tuple[str, str, str]] = {
        (str(row["target"]), str(row["route_kind"]), str(row.get("markaz_ref") or row.get("tehsil_ref") or ""))
        for row in dynamic_rows
    }
    for profile, member in members_by_profile:
        if member.target_type == "group":
            directory = groups.get(member.directory_group_id)
            target = directory.jid if directory and directory.available else ""
            name = directory.name if directory else member.route_scope_label
        else:
            directory = contacts.get(member.directory_contact_id)
            target = (directory.phone_jid or directory.primary_lid_jid or "") if directory and directory.active else ""
            name = directory.display_name if directory else member.route_scope_label
        if not target:
            continue
        key = (target, member.route_scope_key, member.route_scope_value)
        if key in seen:
            continue
        seen.add(key)
        scope_key = member.route_scope_key or "district"
        rows.append(
            {
                "enabled": True,
                "name": name or member.route_scope_label or profile.name,
                "type": member.target_type,
                "target": target,
                "text": "",
                "image_path": "",
                "excel_path": "",
                "excel_filename": "",
                "attachment_text_mode": "separate",
                "delay_ms": 0,
                "message_mode": "full_report",
                "route_kind": scope_key,
                "attach_excel": True,
                "manual_only": False,
                "tehsil_ref": member.route_scope_value if scope_key == "tehsil" else "",
                "markaz_ref": member.route_scope_value if scope_key == "markaz" else "",
                "tehsil": member.route_scope_label if scope_key == "tehsil" else "",
                "markaz": member.route_scope_label if scope_key == "markaz" else "",
            }
        )
    return rows


def _previous_portal_runs(session: Session) -> list[dict[str, Any]]:
    jobs = session.exec(
        select(Job)
        .where(Job.type == "antidengue.report")
        .order_by(Job.created_at.desc())
        .limit(250)
    ).all()
    job_ids = [job.id for job in jobs]
    executions = (
        session.exec(
            select(AntiDengueScheduleExecution).where(
                AntiDengueScheduleExecution.source_job_id.in_(job_ids)
            )
        ).all()
        if job_ids
        else []
    )
    execution_by_source_job = {
        execution.source_job_id: execution
        for execution in executions
        if execution.source_job_id is not None
    }

    rows: list[dict[str, Any]] = []
    for job in jobs:
        summary = dict((job.result or {}).get("summary") or {})
        input_info = dict(summary.get("input") or {})
        sha = str(input_info.get("sha256") or "")
        started_at = str(summary.get("run_started_at") or "")
        if not sha or not started_at:
            continue
        execution = execution_by_source_job.get(job.id)
        rows.append(
            {
                "started_at": started_at,
                # The orchestration execution status distinguishes Test Run
                # (preview_ready) from a real completed Send Now/schedule run.
                "status": execution.status if execution is not None else job.status,
                "raw_file_name": str(input_info.get("name") or ""),
                "raw_file_sha256": sha,
                "job_id": str(job.id),
                "execution_id": str(execution.id) if execution is not None else "",
                "trigger_type": execution.trigger_type if execution is not None else "",
                "dispatch_policy": execution.dispatch_policy if execution is not None else "",
                "dry_run": bool((job.parameters or {}).get("dry_run")),
                "portal_acquisition": dict(summary.get("portal_acquisition") or {}),
            }
        )
    return rows


def _activity_rule_rows(session: Session) -> list[dict[str, Any]]:
    rules = session.exec(
        select(AntiDengueActivityRule).where(
            AntiDengueActivityRule.status == "published",
            AntiDengueActivityRule.enabled == True,  # noqa: E712
        )
    ).all()
    return [
        {
            "id": str(item.id),
            "rule_key": str(item.rule_key),
            "version": item.version,
            "name": item.name,
            "classification": item.classification,
            "match_mode": item.match_mode,
            "distance_enabled": item.distance_enabled,
            "distance_operator": item.distance_operator,
            "distance_threshold_meters": item.distance_threshold_meters,
            "time_enabled": item.time_enabled,
            "time_operator": item.time_operator,
            "time_start": item.time_start,
            "time_end": item.time_end,
            "timezone": item.timezone,
            "published_at": item.published_at.isoformat() if item.published_at else None,
        }
        for item in rules
    ]


def _simple_activity_rule_rows(session: Session) -> list[dict[str, Any]]:
    rules = session.exec(select(AntiDengueSimpleActivityRule).where(
        AntiDengueSimpleActivityRule.status == "published",
        AntiDengueSimpleActivityRule.enabled == True,  # noqa: E712
    )).all()
    return [{
        "id": str(item.id), "rule_key": str(item.rule_key), "version": item.version,
        "name": item.name, "operator": item.operator,
        "minimum_seconds": item.minimum_seconds, "timezone": item.timezone,
        "published_at": item.published_at.isoformat() if item.published_at else None,
    } for item in rules]


def write_runtime_snapshot(
    session: Session,
    destination: Path,
    *,
    job_id: str,
    dispatch_profile_ids: Iterable[str] = (),
    deadline_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    master_rows, schools = _master_school_rows(session)
    payload: dict[str, Any] = {
        "schema_version": RUNTIME_SNAPSHOT_SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "automation-platform-postgresql",
        "job_id": job_id,
        "operational_policy": {"deadline": dict(deadline_snapshot or {})},
        "master_schools": master_rows,
        "officer_mappings": _officer_mapping_rows(session, master_rows, schools),
        "group_routes": _route_rows(session, dispatch_profile_ids),
        "dispatch_settings": {
            "allow_individual": True,
            "allow_groups": True,
            "manual_only": False,
            "attach_excel": True,
            "send_failed_only": False,
            "require_preview": False,
        },
        "previous_portal_runs": _previous_portal_runs(session),
        "activity_rules": _activity_rule_rows(session),
        "simple_activity_rules": _simple_activity_rule_rows(session),
    }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    temporary.write_text(serialized, encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(destination)
    return {
        "path": str(destination),
        "sha256": hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
        "schema_version": RUNTIME_SNAPSHOT_SCHEMA_VERSION,
        "school_count": len(payload["master_schools"]),
        "officer_mapping_count": len(payload["officer_mappings"]),
        "route_count": len(payload["group_routes"]),
        "activity_rule_count": len(payload["activity_rules"]),
        "simple_activity_rule_count": len(payload["simple_activity_rules"]),
    }


__all__ = ["RUNTIME_SNAPSHOT_SCHEMA_VERSION", "write_runtime_snapshot"]

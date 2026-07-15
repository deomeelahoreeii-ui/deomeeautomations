from __future__ import annotations

import csv
import io
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import Response
from sqlmodel import Session, col, select

from automation_core.database import engine, get_session
from master_data.models import (
    MasterDataAudit,
    MasterDataAuditPublic,
    Officer,
    OfficerJurisdiction,
    School,
    SchoolHead,
    SchoolOfficerOverride,
)
from master_data.postgres_repository import PostgresMasterDataRepository
from master_data.schemas import OfficerWrite, SchoolWrite

router = APIRouter(prefix="/api/v1/master-data", tags=["master-data"])


def repository() -> PostgresMasterDataRepository:
    return PostgresMasterDataRepository(engine)


def audit(
    session: Session,
    *,
    entity_type: str,
    entity_id: str,
    action: str,
    summary: str,
    before: dict | None,
    after: dict | None,
) -> None:
    session.add(
        MasterDataAudit(
            entity_type=entity_type,
            entity_id=entity_id,
            action=action,
            summary=summary,
            before=before,
            after=after,
        )
    )
    session.commit()


def translate_error(exc: Exception) -> HTTPException:
    if isinstance(exc, FileNotFoundError):
        return HTTPException(status_code=503, detail=str(exc))
    if isinstance(exc, LookupError):
        return HTTPException(status_code=404, detail=str(exc))
    return HTTPException(status_code=422, detail=str(exc))


def _record_id(value: str, label: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=f"{label} not found") from exc


def _delete_contact_link_previews(
    session: Session,
    link_ids: list[uuid.UUID],
) -> tuple[set[Path], int]:
    if not link_ids:
        return set(), 0
    from whatsapp_gateway.models import (
        WhatsAppDispatchPreview,
        WhatsAppDispatchPreviewDelivery,
    )
    from whatsapp_gateway.preview_service import delete_preview_records

    preview_ids = session.scalars(
        select(WhatsAppDispatchPreviewDelivery.preview_id)
        .where(WhatsAppDispatchPreviewDelivery.contact_link_id.in_(link_ids))
        .distinct()
    ).all()
    paths: set[Path] = set()
    for preview_id in preview_ids:
        preview = session.get(WhatsAppDispatchPreview, preview_id)
        if preview is not None:
            paths.update(delete_preview_records(session, preview))
    return paths, len(preview_ids)


@router.get("/dashboard")
def read_dashboard() -> dict:
    try:
        return repository().dashboard()
    except Exception as exc:
        raise translate_error(exc) from exc


@router.get("/options")
def read_options() -> dict:
    try:
        return repository().options()
    except Exception as exc:
        raise translate_error(exc) from exc


@router.get("/schools")
def read_schools(
    search: str = "",
    tehsil_ref: str = "",
    markaz_ref: str = "",
    active: bool | None = True,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=200),
) -> dict:
    return repository().list_schools(
        search=search,
        tehsil_ref=tehsil_ref,
        markaz_ref=markaz_ref,
        active=active,
        page=page,
        page_size=page_size,
    )


@router.post("/schools", status_code=status.HTTP_201_CREATED)
def create_school(data: SchoolWrite, session: Session = Depends(get_session)) -> dict:
    try:
        before, after = repository().save_school(data)
    except Exception as exc:
        raise translate_error(exc) from exc
    audit(
        session,
        entity_type="school",
        entity_id=after["id"],
        action="created",
        summary=f"Created school {after['emis']} · {after['name']}",
        before=before,
        after=after,
    )
    return after


@router.get("/schools/{school_id}")
def read_school(school_id: str) -> dict:
    school = repository().get_school(school_id)
    if school is None:
        raise HTTPException(status_code=404, detail="School not found")
    return school


@router.put("/schools/{school_id}")
def update_school(
    school_id: str,
    data: SchoolWrite,
    session: Session = Depends(get_session),
) -> dict:
    try:
        before, after = repository().save_school(data, school_id)
    except Exception as exc:
        raise translate_error(exc) from exc
    audit(
        session,
        entity_type="school",
        entity_id=school_id,
        action="updated",
        summary=f"Updated school {after['emis']} · {after['name']}",
        before=before,
        after=after,
    )
    return after


@router.delete("/schools/{school_id}")
def archive_school(school_id: str, session: Session = Depends(get_session)) -> dict:
    try:
        before, after = repository().set_school_active(school_id, False)
    except Exception as exc:
        raise translate_error(exc) from exc
    audit(
        session,
        entity_type="school",
        entity_id=school_id,
        action="archived",
        summary=f"Archived school {after['emis']} · {after['name']}",
        before=before,
        after=after,
    )
    return after


@router.post("/schools/{school_id}/restore")
def restore_school(school_id: str, session: Session = Depends(get_session)) -> dict:
    try:
        before, after = repository().set_school_active(school_id, True)
    except Exception as exc:
        raise translate_error(exc) from exc
    audit(
        session,
        entity_type="school",
        entity_id=school_id,
        action="restored",
        summary=f"Restored school {after['emis']} · {after['name']}",
        before=before,
        after=after,
    )
    return after


@router.delete("/schools/{school_id}/hard")
def hard_delete_school(school_id: str, session: Session = Depends(get_session)) -> dict:
    record_id = _record_id(school_id, "School")
    school = session.get(School, record_id)
    before = repository().get_school(school_id)
    if school is None or before is None:
        raise HTTPException(status_code=404, detail="School not found")

    from whatsapp_gateway.models import WhatsAppContactLink
    from whatsapp_gateway.preview_service import cleanup_unreferenced_preview_files

    heads = session.scalars(select(SchoolHead).where(SchoolHead.school_id == record_id)).all()
    head_ids = [head.id for head in heads]
    links = (
        session.scalars(
            select(WhatsAppContactLink).where(WhatsAppContactLink.school_head_id.in_(head_ids))
        ).all()
        if head_ids
        else []
    )
    preview_paths, preview_count = _delete_contact_link_previews(session, [link.id for link in links])
    for link in links:
        session.delete(link)
    for override in session.scalars(
        select(SchoolOfficerOverride).where(SchoolOfficerOverride.school_id == record_id)
    ).all():
        session.delete(override)
    for head in heads:
        session.delete(head)
    session.flush()
    session.delete(school)
    session.add(
        MasterDataAudit(
            entity_type="school",
            entity_id=school_id,
            action="hard_deleted",
            summary=f"Permanently deleted school {before['emis']} · {before['name']}",
            before=before,
            after=None,
        )
    )
    session.commit()
    cleanup_unreferenced_preview_files(session, preview_paths)
    return {"id": school_id, "deleted": True, "previews_deleted": preview_count}


@router.get("/officers")
def read_officers(role: str = "aeo", search: str = "", active: bool | None = True) -> list[dict]:
    try:
        return repository().list_officers(role, search=search, active=active)
    except Exception as exc:
        raise translate_error(exc) from exc


@router.post("/officers", status_code=status.HTTP_201_CREATED)
def create_officer(data: OfficerWrite, session: Session = Depends(get_session)) -> dict:
    try:
        before, after = repository().save_officer(data)
    except Exception as exc:
        raise translate_error(exc) from exc
    audit(
        session,
        entity_type=f"{data.role}_officer",
        entity_id=after["id"],
        action="created",
        summary=f"Created {data.role.upper()} {after['name']}",
        before=before,
        after=after,
    )
    return after


@router.put("/officers/{role}/{officer_id}")
def update_officer(
    role: str,
    officer_id: str,
    data: OfficerWrite,
    session: Session = Depends(get_session),
) -> dict:
    if data.role != role:
        raise HTTPException(status_code=422, detail="Officer role cannot be changed")
    try:
        before, after = repository().save_officer(data, officer_id)
    except Exception as exc:
        raise translate_error(exc) from exc
    audit(
        session,
        entity_type=f"{role}_officer",
        entity_id=officer_id,
        action="updated",
        summary=f"Updated {role.upper()} {after['name']}",
        before=before,
        after=after,
    )
    return after


@router.delete("/officers/{role}/{officer_id}")
def archive_officer(
    role: str,
    officer_id: str,
    session: Session = Depends(get_session),
) -> dict:
    try:
        before, after = repository().set_officer_active(role, officer_id, False)
    except Exception as exc:
        raise translate_error(exc) from exc
    audit(
        session,
        entity_type=f"{role}_officer",
        entity_id=officer_id,
        action="archived",
        summary=f"Archived {role.upper()} {after['name']}",
        before=before,
        after=after,
    )
    return after


@router.post("/officers/{role}/{officer_id}/restore")
def restore_officer(
    role: str,
    officer_id: str,
    session: Session = Depends(get_session),
) -> dict:
    try:
        before, after = repository().set_officer_active(role, officer_id, True)
    except Exception as exc:
        raise translate_error(exc) from exc
    audit(
        session,
        entity_type=f"{role}_officer",
        entity_id=officer_id,
        action="restored",
        summary=f"Restored {role.upper()} {after['name']}",
        before=before,
        after=after,
    )
    return after


@router.delete("/officers/{role}/{officer_id}/hard")
def hard_delete_officer(
    role: str,
    officer_id: str,
    session: Session = Depends(get_session),
) -> dict:
    record_id = _record_id(officer_id, "Officer")
    officer = session.get(Officer, record_id)
    before = repository().get_officer(role, officer_id)
    if officer is None or officer.role != role or before is None:
        raise HTTPException(status_code=404, detail="Officer not found")

    from whatsapp_gateway.models import WhatsAppContactLink
    from whatsapp_gateway.preview_service import cleanup_unreferenced_preview_files

    links = session.scalars(
        select(WhatsAppContactLink).where(WhatsAppContactLink.officer_id == record_id)
    ).all()
    preview_paths, preview_count = _delete_contact_link_previews(session, [link.id for link in links])
    for link in links:
        session.delete(link)
    for override in session.scalars(
        select(SchoolOfficerOverride).where(SchoolOfficerOverride.officer_id == record_id)
    ).all():
        session.delete(override)
    for jurisdiction in session.scalars(
        select(OfficerJurisdiction).where(OfficerJurisdiction.officer_id == record_id)
    ).all():
        session.delete(jurisdiction)
    session.flush()
    session.delete(officer)
    session.add(
        MasterDataAudit(
            entity_type=f"{role}_officer",
            entity_id=officer_id,
            action="hard_deleted",
            summary=f"Permanently deleted {role.upper()} {before['name']}",
            before=before,
            after=None,
        )
    )
    session.commit()
    cleanup_unreferenced_preview_files(session, preview_paths)
    return {"id": officer_id, "deleted": True, "previews_deleted": preview_count}


@router.get("/heads")
def read_heads(
    search: str = "",
    missing_only: bool = False,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=200),
) -> dict:
    return repository().heads(
        search=search,
        missing_only=missing_only,
        page=page,
        page_size=page_size,
    )


@router.get("/areas")
def read_areas() -> dict:
    return repository().areas()


@router.get("/jurisdictions")
def read_jurisdictions(
    active: bool | None = True,
    wing_ref: str = "",
) -> dict:
    return repository().jurisdictions(active=active, wing_ref=wing_ref)


@router.get("/quality")
def read_quality() -> list[dict]:
    return repository().quality()


@router.get("/exports/schools.csv")
def export_schools() -> Response:
    return Response(
        repository().export_schools_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=antidengue-schools.csv"},
    )


@router.get("/exports/officers.csv")
def export_officers() -> Response:
    return Response(
        repository().export_officers_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=antidengue-officers.csv"},
    )


def csv_bool(value: object, default: bool = True) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "active", "on"}


@router.post("/imports/schools")
async def import_schools(
    file: UploadFile = File(...),
    commit: bool = Form(default=False),
    session: Session = Depends(get_session),
) -> dict:
    try:
        text = (await file.read()).decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=422, detail="Upload a UTF-8 CSV file") from exc
    reader = csv.DictReader(io.StringIO(text))
    repo = repository()
    created = updated = 0
    errors: list[dict] = []
    preview: list[dict] = []
    for number, row in enumerate(reader, start=2):
        try:
            school = SchoolWrite(
                emis=str(row.get("emis") or "").strip(),
                name=str(row.get("name") or "").strip(),
                district_ref=str(row.get("district_ref") or "").strip(),
                department_ref=str(row.get("department_ref") or "").strip(),
                wing_ref=str(row.get("wing_ref") or "").strip(),
                tehsil_ref=str(row.get("tehsil_ref") or "").strip(),
                markaz_ref=str(row.get("markaz_ref") or "").strip(),
                head_name=str(row.get("head_name") or "").strip(),
                head_contact=str(row.get("head_contact") or "").strip(),
                shift=str(row.get("shift") or "Single").strip(),
                school_type=str(row.get("school_type") or "Male").strip(),
                school_level=str(row.get("school_level") or "Primary").strip(),
                deos_wise=str(row.get("deos_wise") or "M-EE").strip(),
                notes=str(row.get("notes") or "").strip(),
                active=csv_bool(row.get("active")),
            )
            existing_id = repo.school_id_by_emis(school.emis)
            action = "update" if existing_id else "create"
            if commit:
                repo.save_school(school, existing_id)
            if existing_id:
                updated += 1
            else:
                created += 1
            if len(preview) < 10:
                preview.append({"row": number, "emis": school.emis, "name": school.name, "action": action})
        except Exception as exc:
            errors.append({"row": number, "error": str(exc)})
    if commit and (created or updated):
        audit(
            session,
            entity_type="school_import",
            entity_id=file.filename or "schools.csv",
            action="imported",
            summary=f"Imported schools: {created} created, {updated} updated, {len(errors)} errors",
            before=None,
            after={"created": created, "updated": updated, "errors": len(errors)},
        )
    return {"committed": commit, "created": created, "updated": updated, "errors": errors, "preview": preview}


@router.post("/imports/officers")
async def import_officers(
    file: UploadFile = File(...),
    commit: bool = Form(default=False),
    session: Session = Depends(get_session),
) -> dict:
    try:
        text = (await file.read()).decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=422, detail="Upload a UTF-8 CSV file") from exc
    reader = csv.DictReader(io.StringIO(text))
    repo = repository()
    created = updated = 0
    errors: list[dict] = []
    preview: list[dict] = []
    for number, row in enumerate(reader, start=2):
        try:
            role = str(row.get("role") or "").strip().lower()
            officer = OfficerWrite(
                role=role,
                name=str(row.get("name") or "").strip(),
                mobile=str(row.get("mobile") or row.get("normalized_mobile") or "").strip(),
                district_ref=str(row.get("district_ref") or "").strip(),
                department_ref=str(row.get("department_ref") or "").strip(),
                wing_ref=str(row.get("wing_ref") or "").strip(),
                tehsil_ref=str(row.get("tehsil_ref") or "").strip(),
                markaz_ref=str(row.get("markaz_ref") or "").strip(),
                active=csv_bool(row.get("active")),
            )
            existing_id = repo.officer_id_by_mobile(officer.role, officer.mobile)
            action = "update" if existing_id else "create"
            if commit:
                repo.save_officer(officer, existing_id)
            if existing_id:
                updated += 1
            else:
                created += 1
            if len(preview) < 10:
                preview.append({"row": number, "role": officer.role, "name": officer.name, "action": action})
        except Exception as exc:
            errors.append({"row": number, "error": str(exc)})
    if commit and (created or updated):
        audit(
            session,
            entity_type="officer_import",
            entity_id=file.filename or "officers.csv",
            action="imported",
            summary=f"Imported officers: {created} created, {updated} updated, {len(errors)} errors",
            before=None,
            after={"created": created, "updated": updated, "errors": len(errors)},
        )
    return {"committed": commit, "created": created, "updated": updated, "errors": errors, "preview": preview}


@router.get("/history", response_model=list[MasterDataAuditPublic])
def read_history(
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
) -> list[MasterDataAudit]:
    statement = select(MasterDataAudit).order_by(col(MasterDataAudit.created_at).desc()).limit(limit)
    return list(session.exec(statement))

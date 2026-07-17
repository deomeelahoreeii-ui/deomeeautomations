from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from sqlalchemy import func, or_, select
from sqlmodel import Session

from automation_core.database import get_session
from automation_core.job_service import append_log, create_job, get_job, mark_job_failed, set_task_id
from automation_core.models import Job, JobPublic, JobStatus, JobType
from automation_core.time import utcnow
from master_data.models import MasterContact, Officer, School, SchoolHead, Wing
from whatsapp_gateway.models import (
    WhatsAppAccount, WhatsAppApplication, WhatsAppAudience, WhatsAppAudienceMember,
    WhatsAppContactLink, WhatsAppDirectoryContact, WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact, WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchApproval, WhatsAppDispatchProfile, WhatsAppDelivery,
    WhatsAppRecipientScope, WhatsAppReportType, WhatsAppSettings,
)
from whatsapp_gateway.preview_service import (
    cleanup_unreferenced_preview_files, delete_preview_records, entity_link_details,
    is_managed_preview_artifact, preview_dict, preview_is_stale, sha256_file,
)
from whatsapp_gateway.tasks import compile_dispatch_preview_job, send_approved_preview_job
from whatsapp_gateway.previews.schemas import (
    PreviewInput, BulkPreviewInput, PreviewIdsInput, BulkPreviewApprovalInput,
    ContactLinkInput, PreviewApprovalInput,
)
from whatsapp_gateway.previews.serialization import _artifact_dict, _delivery_dict, _digits, _with_approval
from whatsapp_gateway.directory.master_contacts import ensure_master_contact, resolved_contact_dict

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp-previews"])

@router.get("/directory/contacts/{contact_id}/links")
def contact_links(
    contact_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None:
        raise HTTPException(status_code=404, detail="WhatsApp contact not found")
    links = session.scalars(
        select(WhatsAppContactLink)
        .where(
            WhatsAppContactLink.directory_contact_id == contact_id,
            WhatsAppContactLink.active.is_(True),
        )
        .order_by(WhatsAppContactLink.entity_type, WhatsAppContactLink.created_at)
    ).all()
    return {
        "contact": {
            "id": str(contact.id),
            **resolved_contact_dict(
                contact,
                session.get(MasterContact, contact.master_contact_id)
                if contact.master_contact_id
                else None,
            ),
            "phone_jid": contact.phone_jid,
            "primary_lid_jid": contact.primary_lid_jid,
        },
        "items": [entity_link_details(session, item) for item in links],
    }
@router.get("/directory/contact-link-targets")
def contact_link_targets(
    contact_id: uuid.UUID,
    entity_type: Literal["officer", "school_head"],
    search: str = "",
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None:
        raise HTTPException(status_code=404, detail="WhatsApp contact not found")
    contact_digits = _digits(contact.phone_jid)
    if entity_type == "officer":
        filters: list[Any] = [Officer.active.is_(True)]
        if search:
            pattern = f"%{search.strip()}%"
            filters.append(
                or_(Officer.name.ilike(pattern), Officer.mobile.ilike(pattern), Officer.normalized_mobile.ilike(pattern))
            )
        records = session.scalars(
            select(Officer).where(*filters).order_by(Officer.name).limit(50)
        ).all()
        items = []
        for officer in records:
            wing = session.get(Wing, officer.wing_id)
            exact = bool(contact_digits and contact_digits.endswith(_digits(officer.normalized_mobile)))
            items.append(
                {
                    "id": str(officer.id),
                    "entity_type": "officer",
                    "name": officer.name,
                    "detail": f"{officer.role.upper()} · {wing.name if wing else 'Unknown wing'}",
                    "mobile": officer.mobile,
                    "wing_name": wing.name if wing else "Unknown wing",
                    "suggested": exact,
                }
            )
    else:
        filters = [SchoolHead.active.is_(True), School.active.is_(True)]
        if search:
            pattern = f"%{search.strip()}%"
            filters.append(
                or_(
                    SchoolHead.name.ilike(pattern),
                    SchoolHead.mobile.ilike(pattern),
                    SchoolHead.normalized_mobile.ilike(pattern),
                    School.name.ilike(pattern),
                    School.emis.ilike(pattern),
                )
            )
        records = session.execute(
            select(SchoolHead, School)
            .join(School, School.id == SchoolHead.school_id)
            .where(*filters)
            .order_by(SchoolHead.name)
            .limit(50)
        ).all()
        items = []
        for head, school in records:
            wing = session.get(Wing, school.wing_id)
            exact = bool(contact_digits and contact_digits.endswith(_digits(head.normalized_mobile)))
            items.append(
                {
                    "id": str(head.id),
                    "entity_type": "school_head",
                    "name": head.name,
                    "detail": f"{school.emis} · {school.name}",
                    "mobile": head.mobile,
                    "wing_name": wing.name if wing else "Unknown wing",
                    "suggested": exact,
                }
            )
    items.sort(key=lambda item: (not item["suggested"], item["name"].lower()))
    if not search.strip():
        items = [item for item in items if item["suggested"]]
    return {"items": items[:25]}
@router.post("/directory/contacts/{contact_id}/links", status_code=status.HTTP_201_CREATED)
def save_contact_link(
    contact_id: uuid.UUID,
    data: ContactLinkInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or not contact.active:
        raise HTTPException(status_code=404, detail="Active WhatsApp contact not found")
    officer = session.get(Officer, data.entity_id) if data.entity_type == "officer" else None
    head = session.get(SchoolHead, data.entity_id) if data.entity_type == "school_head" else None
    school = session.get(School, head.school_id) if head else None
    if data.entity_type == "officer" and (officer is None or not officer.active):
        raise HTTPException(status_code=422, detail="Select an active officer")
    if data.entity_type == "school_head" and (head is None or not head.active or school is None or not school.active):
        raise HTTPException(status_code=422, detail="Select an active school head")
    wing_id = officer.wing_id if officer else school.wing_id
    entity_key = f"{data.entity_type}:{data.entity_id}"
    link = session.scalar(
        select(WhatsAppContactLink).where(
            WhatsAppContactLink.directory_contact_id == contact_id,
            WhatsAppContactLink.entity_type == data.entity_type,
            WhatsAppContactLink.entity_key == entity_key,
        )
    )
    if link is None:
        link = WhatsAppContactLink(
            directory_contact_id=contact_id,
            entity_type=data.entity_type,
            entity_key=entity_key,
            officer_id=officer.id if officer else None,
            school_head_id=head.id if head else None,
            wing_id=wing_id,
        )
    link.status = "verified"
    link.source = "manual"
    link.confidence = 1.0
    link.active = True
    link.updated_at = utcnow()
    session.add(link)
    ensure_master_contact(
        session,
        contact,
        observed_name=officer.name if officer else head.name,
        name_source="verified_entity",
        name_verified=True,
    )
    session.commit()
    session.refresh(link)
    return entity_link_details(session, link)
@router.delete("/directory/contacts/{contact_id}/links/{link_id}")
def remove_contact_link(
    contact_id: uuid.UUID,
    link_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    link = session.get(WhatsAppContactLink, link_id)
    if link is None or link.directory_contact_id != contact_id:
        raise HTTPException(status_code=404, detail="Contact link not found")
    link.active = False
    link.updated_at = utcnow()
    session.add(link)
    session.commit()
    return {"id": str(link.id), "active": False}

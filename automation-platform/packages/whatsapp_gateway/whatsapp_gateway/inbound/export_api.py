
from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func
from sqlmodel import Session, col, select

from automation_core.database import get_session
from automation_core.job_service import create_job, set_task_id
from automation_core.models import Job, JobType
from whatsapp_gateway.inbound.schemas import CreateInboundExportRequest, InboundFileFilter
from whatsapp_gateway.inbound_service import build_preview, create_export_run, serialize_run
from whatsapp_gateway.inbound_tasks import build_inbound_export_job
from whatsapp_gateway.models import WhatsAppInboundExportRun

router = APIRouter()

@router.post("/exports/preview")
def preview_inbound_export(
    filters: InboundFileFilter,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return build_preview(
            session,
            contact_id=filters.contact_id,
            date_from=filters.date_from,
            date_to=filters.date_to,
            chat_scope=filters.chat_scope,
            media_types=filters.media_types,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/exports", status_code=status.HTTP_202_ACCEPTED)
def create_inbound_export(
    data: CreateInboundExportRequest,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        preview = build_preview(
            session,
            contact_id=data.contact_id,
            date_from=data.date_from,
            date_to=data.date_to,
            chat_scope=data.chat_scope,
            media_types=data.media_types,
            item_limit=1,
        )
        if not preview["files_found"]:
            raise ValueError("No matching inbound files were found")
        contact_name = preview["contact"]["display_name"] or preview["contact"][
            "phone_jid"
        ]
        job = create_job(
            session,
            job_type=JobType.whatsapp_inbound_export.value,
            title=f"WhatsApp inbound files: {contact_name}",
            parameters={
                "contact_id": str(data.contact_id),
                "date_from": data.date_from.isoformat() if data.date_from else None,
                "date_to": data.date_to.isoformat() if data.date_to else None,
                "chat_scope": data.chat_scope,
                "media_types": data.media_types,
                "requested_by": data.requested_by,
            },
        )
        run = create_export_run(
            session,
            job_id=job.id,
            contact_id=data.contact_id,
            date_from=data.date_from,
            date_to=data.date_to,
            chat_scope=data.chat_scope,
            media_types=data.media_types,
        )
        job.parameters = {**job.parameters, "export_run_id": str(run.id)}
        session.add(job)
        session.commit()
        task = build_inbound_export_job.delay(str(job.id))
        set_task_id(session, job.id, task.id)
        return {"job_id": str(job.id), "export": serialize_run(session, run)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/exports")
def list_inbound_exports(
    contact_id: uuid.UUID | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    filters: list[Any] = []
    if contact_id:
        filters.append(WhatsAppInboundExportRun.contact_id == contact_id)
    total = session.exec(
        select(func.count()).select_from(WhatsAppInboundExportRun).where(*filters)
    ).one()
    runs = session.exec(
        select(WhatsAppInboundExportRun)
        .where(*filters)
        .order_by(col(WhatsAppInboundExportRun.created_at).desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()
    return {
        "items": [serialize_run(session, run) for run in runs],
        "total": int(total),
        "page": page,
        "page_size": page_size,
    }


@router.get("/exports/{export_id}")
def read_inbound_export(
    export_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    run = session.get(WhatsAppInboundExportRun, export_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Inbound export was not found")
    result = serialize_run(session, run)
    job = session.get(Job, run.job_id)
    result["job"] = {
        "id": str(job.id),
        "status": job.status,
        "error": job.error,
        "result": job.result,
    } if job else None
    return result

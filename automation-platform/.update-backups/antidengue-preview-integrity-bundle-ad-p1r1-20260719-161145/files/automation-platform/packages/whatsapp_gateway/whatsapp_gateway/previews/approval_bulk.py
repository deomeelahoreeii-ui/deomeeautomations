from __future__ import annotations

from typing import Any

from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlmodel import Session

from automation_core.database import get_session
from automation_core.models import JobPublic
from whatsapp_gateway.models import WhatsAppDispatchApproval, WhatsAppDispatchPreview
from whatsapp_gateway.preview_service import preview_is_stale
from whatsapp_gateway.previews.schemas import BulkPreviewApprovalInput, PreviewApprovalInput

async def approve_previews_bulk(
    data: BulkPreviewApprovalInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    from whatsapp_gateway.previews.approval import approve_preview

    preview_ids = list(dict.fromkeys(data.preview_ids))
    if not preview_ids or len(preview_ids) > 50:
        raise HTTPException(status_code=422, detail="Select between 1 and 50 previews")
    previews = session.scalars(
        select(WhatsAppDispatchPreview).where(WhatsAppDispatchPreview.id.in_(preview_ids))
    ).all()
    if len(previews) != len(preview_ids):
        raise HTTPException(status_code=404, detail="One or more selected previews no longer exist")
    if any(item.status != "ready" or item.blocked_count for item in previews):
        raise HTTPException(status_code=409, detail="Remove blocked previews from the selection")
    if any(preview_is_stale(session, item, check_files=True) for item in previews):
        raise HTTPException(status_code=409, detail="Remove stale previews and compile them again")
    if any(item.warning_count for item in previews) and not data.acknowledge_warnings:
        raise HTTPException(status_code=422, detail="Acknowledge warnings for the selected previews")
    approved_ids = set(session.scalars(
        select(WhatsAppDispatchApproval.preview_id).where(
            WhatsAppDispatchApproval.preview_id.in_(preview_ids)
        )
    ).all())
    if approved_ids:
        raise HTTPException(status_code=409, detail="Remove already approved previews from the selection")

    jobs: list[JobPublic] = []
    for preview_id in preview_ids:
        jobs.append(await approve_preview(
            preview_id,
            PreviewApprovalInput(
                acknowledge_warnings=data.acknowledge_warnings,
                approved_by=data.approved_by,
            ),
            session,
        ))
    return {"jobs": jobs, "count": len(jobs)}


__all__ = ["approve_previews_bulk"]

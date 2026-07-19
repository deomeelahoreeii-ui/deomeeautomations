from __future__ import annotations

import hashlib
import uuid
from datetime import date

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from automation_core.job_service import add_job, add_job_log
from automation_core.models import Job, JobStatus, JobType
from automation_core.time import utcnow
from whatsapp_gateway.models import (
    WhatsAppAccount, WhatsAppDailyMessageClaim, WhatsAppDispatchApproval,
    WhatsAppDispatchPreview, WhatsAppDispatchPreviewDelivery, WhatsAppTemplate,
)
from whatsapp_gateway.previews.schemas import PreviewApprovalInput


def complete_noop_approval(
    session: Session, preview: WhatsAppDispatchPreview, data: PreviewApprovalInput,
    *, skipped_count: int, excluded_deliveries: list[dict] | None = None,
) -> Job:
    now = utcnow()
    job = add_job(
        session, job_type=JobType.whatsapp_dispatch_send.value,
        title=f"No new deliveries for preview {preview.preview_key}",
        parameters={"preview_id": str(preview.id), "no_op": True},
    )
    job.status = JobStatus.succeeded.value
    job.started_at = now
    job.finished_at = now
    job.result = {
        "delivered": 0, "failed": 0, "skipped": skipped_count, "no_op": True,
        "message": "Nothing new to send; eligible acknowledgements were already sent today.",
    }
    approval = WhatsAppDispatchApproval(
        preview_id=preview.id, job_id=job.id,
        approved_by=data.approved_by.strip() or "web-operator",
        warnings_acknowledged=data.acknowledge_warnings,
        exclusions_acknowledged=data.acknowledge_exclusions,
        preview_content_sha256=preview.content_sha256,
        approved_content_sha256=hashlib.sha256(b"[]").hexdigest(),
        approved_delivery_ids=[],
        excluded_deliveries=list(excluded_deliveries or []),
        delivery_count=0,
        excluded_count=len(excluded_deliveries or []),
        partial=any(item.get("status") == "blocked" for item in (excluded_deliveries or [])),
        status="completed", completed_at=now,
    )
    session.add(approval)
    session.add(job)
    add_job_log(
        session, job.id,
        f"No new deliveries: {skipped_count} once-daily acknowledgement(s) were already claimed.",
    )
    session.commit()
    session.refresh(job)
    return job


def claim_daily_message(
    session: Session, *, preview: WhatsAppDispatchPreview,
    frozen: WhatsAppDispatchPreviewDelivery, approval: WhatsAppDispatchApproval,
    account: WhatsAppAccount,
) -> bool:
    snapshot = frozen.routing_snapshot or {}
    raw = snapshot.get("daily_claim")
    if not isinstance(raw, dict):
        return True
    required = {
        "semantic_key", "business_date", "purpose", "scope_key", "scope_value",
        "scope_label", "template_key",
    }
    if not required.issubset(raw):
        raise HTTPException(status_code=409, detail="A frozen daily acknowledgement claim is incomplete")
    template_id = raw.get("template_id")
    parsed_template_id = uuid.UUID(str(template_id)) if template_id else None
    if parsed_template_id and session.get(WhatsAppTemplate, parsed_template_id) is None:
        parsed_template_id = None
    claim = WhatsAppDailyMessageClaim(
        semantic_key=str(raw["semantic_key"]),
        business_date=date.fromisoformat(str(raw["business_date"])),
        purpose=str(raw["purpose"]), account_id=account.id,
        application_id=preview.application_id,
        report_type_id=uuid.UUID(str(snapshot.get("report_type_id"))),
        template_id=parsed_template_id, preview_id=preview.id,
        preview_delivery_id=frozen.id, approval_id=approval.id,
        target_jid=frozen.target_jid, scope_key=str(raw["scope_key"]),
        scope_value=str(raw["scope_value"]), scope_label=str(raw["scope_label"]),
        template_key=str(raw["template_key"]),
    )
    try:
        with session.begin_nested():
            session.add(claim)
            session.flush()
        return True
    except IntegrityError:
        return False


__all__ = ["claim_daily_message", "complete_noop_approval"]

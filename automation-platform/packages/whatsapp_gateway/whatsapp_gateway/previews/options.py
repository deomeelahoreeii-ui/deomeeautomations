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
from master_data.models import Officer, School, SchoolHead, Wing
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

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp-previews"])

@router.get("/previews/options")
def preview_options(session: Session = Depends(get_session)) -> dict[str, Any]:
    jobs = session.scalars(
        select(Job)
        .where(
            Job.type == JobType.antidengue_report.value,
            Job.status == JobStatus.succeeded.value,
        )
        .order_by(Job.created_at.desc())
        .limit(50)
    ).all()
    run_options = []
    for job in jobs:
        if not bool(job.parameters.get("dry_run", False)):
            continue
        summary = (job.result or {}).get("summary") or {}
        dispatch_plan = ((summary.get("whatsapp") or {}).get("dispatch_plan") or []) if isinstance(summary, dict) else []
        quality = summary.get("quality_gate") or {} if isinstance(summary, dict) else {}
        run_options.append(
            {
                "id": str(job.id),
                "title": job.title,
                "created_at": job.created_at,
                "finished_at": job.finished_at,
                "planned_delivery_count": len(dispatch_plan) if isinstance(dispatch_plan, list) else 0,
                "report_count": int((job.result or {}).get("artifact_count") or 0),
                "quality_warning_count": len(quality.get("warnings") or []) if isinstance(quality, dict) else 0,
            }
        )

    application = session.scalar(
        select(WhatsAppApplication).where(WhatsAppApplication.key == "antidengue")
    )
    profiles = []
    if application:
        profile_rows = session.scalars(
            select(WhatsAppDispatchProfile)
            .where(
                WhatsAppDispatchProfile.application_id == application.id,
                WhatsAppDispatchProfile.enabled.is_(True),
            )
            .order_by(WhatsAppDispatchProfile.name)
        ).all()
        for profile in profile_rows:
            audience = session.get(WhatsAppAudience, profile.audience_id)
            report_type = session.get(WhatsAppReportType, profile.report_type_id)
            wing = session.get(Wing, profile.wing_id) if profile.wing_id else None
            recipient_scope = (
                session.get(WhatsAppRecipientScope, profile.recipient_scope_id)
                if profile.recipient_scope_id
                else None
            )
            target_count = session.scalar(
                select(func.count())
                .select_from(WhatsAppAudienceMember)
                .where(
                    WhatsAppAudienceMember.audience_id == profile.audience_id,
                    WhatsAppAudienceMember.enabled.is_(True),
                )
            ) or 0
            profiles.append(
                {
                    "id": str(profile.id),
                    "name": profile.name,
                    "key": profile.key,
                    "application_id": str(application.id),
                    "application_key": application.key,
                    "application_name": application.name,
                    "version": profile.version,
                    "audience_name": audience.name if audience else "Missing audience",
                    "report_type_name": report_type.name if report_type else "Missing report type",
                    "wing_name": wing.name if wing else "Missing wing",
                    "recipient_channel": profile.recipient_channel,
                    "recipient_scope_name": recipient_scope.name if recipient_scope else "Legacy / unscoped",
                    "target_count": target_count,
                }
            )
    return {"runs": run_options, "profiles": profiles}

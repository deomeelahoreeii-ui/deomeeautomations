from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlmodel import Session

from automation_core.models import Job, JobStatus, JobType
from antidengue_automation.deadline_policy import (
    ResolvedDeadline,
    deadline_from_snapshot,
    resolve_deadline_policy,
)
from master_data.models import Wing
from whatsapp_gateway.models import (
    WhatsAppAccount, WhatsAppApplication, WhatsAppAudience, WhatsAppAudienceMember,
    WhatsAppDispatchProfile, WhatsAppRecipientScope, WhatsAppReportType, WhatsAppTemplate,
)
from whatsapp_gateway.previews.compiler.errors import PreviewCompileError
from whatsapp_gateway.configuration.dynamic_audiences import (
    ResolvedAudienceMember,
    resolve_dynamic_audience,
)
from whatsapp_gateway.previews.compiler.capabilities import PlannerCapability, profile_capability

@dataclass(slots=True)
class CompileContext:
    session: Session
    source_job: Job
    profile: WhatsAppDispatchProfile
    application: WhatsAppApplication
    report_type: WhatsAppReportType
    audience: WhatsAppAudience
    account: WhatsAppAccount
    template: WhatsAppTemplate | None
    wing: Wing
    recipient_scope: WhatsAppRecipientScope | None
    members: list[WhatsAppAudienceMember | ResolvedAudienceMember]
    audience_group_ids: set[uuid.UUID]
    audience_contact_ids: set[uuid.UUID]
    summary: dict[str, Any]
    whatsapp_summary: dict[str, Any]
    source_dispatch_plan: list[Any]
    source_plans: list[dict[str, Any]]
    planner_capability: PlannerCapability | None = None
    deadline: ResolvedDeadline | None = None


def load_compile_context(
    session: Session, *, source_job_id: uuid.UUID, dispatch_profile_id: uuid.UUID,
    deadline_snapshot: dict[str, Any] | None = None,
) -> CompileContext:
    source_job = session.get(Job, source_job_id)
    if source_job is None:
        raise PreviewCompileError("Source dry run was not found")
    if source_job.type != JobType.antidengue_report.value:
        raise PreviewCompileError("Only AntiDengue report runs can create this preview")
    if source_job.status != JobStatus.succeeded.value:
        raise PreviewCompileError("The source run must finish successfully before previewing dispatch")
    if not bool(source_job.parameters.get("dry_run", False)):
        raise PreviewCompileError("Dispatch previews must be built from a dry run")

    profile = session.get(WhatsAppDispatchProfile, dispatch_profile_id)
    if profile is None or not profile.enabled:
        raise PreviewCompileError("Select an enabled dispatch profile")
    application = session.get(WhatsAppApplication, profile.application_id)
    report_type = session.get(WhatsAppReportType, profile.report_type_id)
    audience = session.get(WhatsAppAudience, profile.audience_id)
    account = session.get(WhatsAppAccount, profile.account_id)
    template = session.get(WhatsAppTemplate, profile.template_id) if profile.template_id else None
    wing = session.get(Wing, profile.wing_id) if profile.wing_id else None
    recipient_scope = (
        session.get(WhatsAppRecipientScope, profile.recipient_scope_id)
        if profile.recipient_scope_id
        else None
    )
    if not all((application, report_type, audience, account)):
        raise PreviewCompileError("The selected dispatch profile has incomplete configuration")
    if application.key != "antidengue":
        raise PreviewCompileError("Select an AntiDengue dispatch profile")
    if wing is None:
        raise PreviewCompileError("AntiDengue dispatch profiles require a wing")

    members = list(session.scalars(
        select(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == audience.id,
            WhatsAppAudienceMember.enabled.is_(True),
        )
    ).all())
    members.extend(resolve_dynamic_audience(
        session, audience_id=audience.id, account=account,
        granularity=profile.delivery_granularity,
    ))
    audience_group_ids = {member.directory_group_id for member in members if member.directory_group_id}
    audience_contact_ids = {member.directory_contact_id for member in members if member.directory_contact_id}

    summary = (source_job.result or {}).get("summary") or {}
    whatsapp_summary = summary.get("whatsapp") if isinstance(summary, dict) else {}
    source_dispatch_plan = whatsapp_summary.get("dispatch_plan") if isinstance(whatsapp_summary, dict) else []
    if not isinstance(source_dispatch_plan, list):
        source_dispatch_plan = []
    source_plans = [plan for plan in source_dispatch_plan if isinstance(plan, dict)]
    deadline = (
        deadline_from_snapshot(deadline_snapshot)
        if deadline_snapshot is not None
        else resolve_deadline_policy(session)
    )
    return CompileContext(
        session=session, source_job=source_job, profile=profile, application=application,
        report_type=report_type, audience=audience, account=account, template=template,
        wing=wing, recipient_scope=recipient_scope, members=members,
        audience_group_ids=audience_group_ids, audience_contact_ids=audience_contact_ids,
        summary=summary, whatsapp_summary=whatsapp_summary or {},
        source_dispatch_plan=source_dispatch_plan, source_plans=source_plans,
        planner_capability=profile_capability(session, profile),
        deadline=deadline,
    )

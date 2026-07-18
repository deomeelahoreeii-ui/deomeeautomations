from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select

from automation_core.config import get_settings
from automation_core.time import utcnow
from master_data.models import Tehsil
from whatsapp_gateway.models import (
    WhatsAppDailyMessageClaim,
    WhatsAppDirectoryGroup,
    WhatsAppTemplate,
)
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.errors import issue
from whatsapp_gateway.previews.compiler.messages import PLACEHOLDER_RE
from whatsapp_gateway.rendering.antidengue.formatting import _wing_label

PURPOSE = "zero_result_acknowledgement"
BUSINESS_TIMEZONE = ZoneInfo("Asia/Karachi")


def _business_datetime(ctx: CompileContext) -> datetime:
    value = ctx.source_job.finished_at or ctx.source_job.created_at or utcnow()
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(BUSINESS_TIMEZONE)


def _matching_templates(ctx: CompileContext) -> list[WhatsAppTemplate]:
    scope_id = ctx.recipient_scope.id if ctx.recipient_scope else None
    statement = select(WhatsAppTemplate).where(
        WhatsAppTemplate.enabled.is_(True),
        WhatsAppTemplate.category == PURPOSE,
        WhatsAppTemplate.application_id == ctx.application.id,
        WhatsAppTemplate.report_type_id == ctx.report_type.id,
        WhatsAppTemplate.recipient_channel.in_(["group", "any"]),
    )
    if scope_id:
        statement = statement.where(
            or_(
                WhatsAppTemplate.recipient_scope_id.is_(None),
                WhatsAppTemplate.recipient_scope_id == scope_id,
            )
        )
    else:
        statement = statement.where(WhatsAppTemplate.recipient_scope_id.is_(None))
    return list(ctx.session.scalars(statement.order_by(WhatsAppTemplate.key, WhatsAppTemplate.id)))


def _select_daily_template(
    templates: list[WhatsAppTemplate], *, business_date, scope_value: str
) -> WhatsAppTemplate:
    scope_offset = int(hashlib.sha256(scope_value.encode("utf-8")).hexdigest()[:8], 16)
    return templates[(business_date.toordinal() + scope_offset) % len(templates)]


def _semantic_key(
    ctx: CompileContext,
    *,
    target_jid: str,
    scope_key: str,
    scope_value: str,
    business_date,
) -> str:
    raw = "|".join(
        (
            str(ctx.account.id),
            target_jid,
            str(ctx.report_type.id),
            scope_key,
            scope_value,
            business_date.isoformat(),
            PURPOSE,
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_zero_result_plan(
    ctx: CompileContext,
    *,
    member_id,
    directory_group: WhatsAppDirectoryGroup,
    tehsil: Tehsil,
) -> dict | None:
    quality = ctx.summary.get("quality_gate") if isinstance(ctx.summary, dict) else {}
    if isinstance(quality, dict) and quality.get("passed") is False:
        return None
    templates = _matching_templates(ctx)
    if not templates:
        return None

    business_time = _business_datetime(ctx)
    business_date = business_time.date()
    template = _select_daily_template(
        templates,
        business_date=business_date,
        scope_value=str(tehsil.id),
    )
    values = {
        "tehsil": tehsil.name,
        "wing": _wing_label(ctx.wing),
        "report_date": business_date.isoformat(),
        "checked_at": business_time.strftime("%I:%M %p").lstrip("0"),
        "group_name": directory_group.name,
        "submission_deadline": get_settings().antidengue_submission_deadline,
    }
    missing = sorted({key for key in PLACEHOLDER_RE.findall(template.body) if key not in values})
    if missing:
        raise ValueError(
            f"Acknowledgement template {template.name} has unsupported variable(s): "
            + ", ".join(missing)
        )
    message = PLACEHOLDER_RE.sub(lambda match: values[match.group(1)], template.body).strip()
    if not message:
        raise ValueError(f"Acknowledgement template {template.name} renders an empty message")

    semantic_key = _semantic_key(
        ctx,
        target_jid=directory_group.jid,
        scope_key="tehsil",
        scope_value=str(tehsil.id),
        business_date=business_date,
    )
    existing = ctx.session.scalar(
        select(WhatsAppDailyMessageClaim).where(
            WhatsAppDailyMessageClaim.semantic_key == semantic_key
        )
    )
    skipped = existing is not None
    daily_claim = {
        "semantic_key": semantic_key,
        "business_date": business_date.isoformat(),
        "purpose": PURPOSE,
        "scope_key": "tehsil",
        "scope_value": str(tehsil.id),
        "scope_label": tehsil.name,
        "template_id": str(template.id),
        "template_key": template.key,
    }
    return {
        "job_id": f"antidengue.zero_result_acknowledgement:{member_id}:{business_date}",
        "status": "skipped" if skipped else "planned",
        "cause": (
            f"{tehsil.name} was already acknowledged on {business_date.isoformat()}."
            if skipped
            else ""
        ),
        "skip_issue_code": "acknowledgement_already_sent_today" if skipped else "",
        "skip_issue_severity": "info",
        "channel": "group",
        "target": directory_group.jid,
        "recipient_name": directory_group.name,
        "route_kind": "tehsil",
        "row_count": 0,
        "native_renderer": "antidengue.zero_result_acknowledgement.v1",
        "native_context": values,
        "native_issues": [
            issue(
                "zero_result_acknowledgement_planned",
                "info",
                f"A once-daily zero-result acknowledgement is planned for {tehsil.name}.",
                template_key=template.key,
                business_date=business_date.isoformat(),
            )
        ],
        "daily_claim": daily_claim,
        "message_template_final": True,
        "message_only": True,
        "payload": {
            "type": "group",
            "target": directory_group.jid,
            "recipient_name": directory_group.name,
            "text": message,
            "attachment_paths": [],
            "dispatch_route": {
                "route_kind": "tehsil",
                "recipient_scope": "tehsil",
                "tehsil": tehsil.name,
                "tehsil_id": str(tehsil.id),
                "row_count": 0,
                "delivery_kind": PURPOSE,
            },
        },
    }


__all__ = ["BUSINESS_TIMEZONE", "PURPOSE", "build_zero_result_plan"]

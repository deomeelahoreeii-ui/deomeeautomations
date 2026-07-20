from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select

from automation_core.time import utcnow
from whatsapp_gateway.models import WhatsAppTemplate
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.messages import PLACEHOLDER_RE
from whatsapp_gateway.rendering.antidengue.formatting import _wing_label

BUSINESS_TIMEZONE = ZoneInfo("Asia/Karachi")
PURPOSE = "zero_result_acknowledgement"


@dataclass(frozen=True, slots=True)
class AcknowledgementVariant:
    id: Any | None
    key: str
    name: str
    body: str


def business_datetime(ctx: CompileContext) -> datetime:
    value = ctx.source_job.finished_at or ctx.source_job.created_at or utcnow()
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(BUSINESS_TIMEZONE)


def matching_templates(
    ctx: CompileContext, *, recipient_channel: str
) -> list[WhatsAppTemplate]:
    scope_id = ctx.recipient_scope.id if ctx.recipient_scope else None
    statement = select(WhatsAppTemplate).where(
        WhatsAppTemplate.enabled.is_(True),
        WhatsAppTemplate.category == PURPOSE,
        WhatsAppTemplate.application_id == ctx.application.id,
        WhatsAppTemplate.report_type_id == ctx.report_type.id,
        WhatsAppTemplate.recipient_channel.in_([recipient_channel, "any"]),
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
    return list(
        ctx.session.scalars(
            statement.order_by(WhatsAppTemplate.key, WhatsAppTemplate.id)
        )
    )


def fallback_variants(ctx: CompileContext) -> list[AcknowledgementVariant]:
    if ctx.report_type.key == "consolidated_action_digest":
        texts = (
            "✅ *{{scope_name}} has achieved full Anti-Dengue compliance today.*\n"
            "Zero dormant schools and no outstanding distance/timing review items were found as of {{checked_at}}. "
            "Thank you for maintaining timely and accurate portal activity.",
            "🌟 *Excellent status for {{scope_name}}.*\n"
            "Today’s consolidated review shows zero dormancy and zero pending distance or timing issues. "
            "Please continue the same standard of reporting.",
            "🏆 *{{scope_name}} is clear in today’s Anti-Dengue review.*\n"
            "Zero dormant schools and zero actionable distance/timing findings were identified. "
            "Thank you for the prompt compliance.",
        )
    else:
        texts = (
            "✅ *Zero dormancy achieved by {{scope_name}}.*\n"
            "Today’s Anti-Dengue review found no dormant {{wing}} schools as of {{checked_at}}. "
            "Thank you for keeping portal activities updated.",
            "🌟 *Good update for {{scope_name}}:* the latest report shows zero dormant {{wing}} schools today. "
            "Please continue timely reporting.",
            "🏆 *Today’s status for {{scope_name}}:* zero dormant {{wing}} schools were identified. "
            "Thank you for the prompt updates.",
        )
    return [
        AcknowledgementVariant(
            id=None,
            key=f"builtin_{ctx.report_type.key}_{index}",
            name=f"Built-in zero-result acknowledgement {index}",
            body=body,
        )
        for index, body in enumerate(texts, start=1)
    ]


def scope_type(scope_key: str) -> str:
    return {
        "markaz": "Markaz",
        "tehsil": "Tehsil",
        "wing": "Wing",
        "district": "District",
    }.get(scope_key, scope_key.replace("_", " ").title() or "Area")


def resolve_message(
    ctx: CompileContext,
    *,
    recipient_channel: str,
    recipient_name: str,
    scope_key: str,
    scope_value: str,
    scope_label: str,
) -> tuple[AcknowledgementVariant | WhatsAppTemplate, dict[str, str], str, date]:
    templates: list[AcknowledgementVariant | WhatsAppTemplate] = list(
        matching_templates(ctx, recipient_channel=recipient_channel)
    )
    if not templates:
        templates = fallback_variants(ctx)
    business_time = business_datetime(ctx)
    business_date = business_time.date()
    offset = int(hashlib.sha256(scope_value.encode("utf-8")).hexdigest()[:8], 16)
    template = templates[(business_date.toordinal() + offset) % len(templates)]
    values = {
        "scope_name": scope_label,
        "scope_type": scope_type(scope_key),
        "recipient_name": recipient_name,
        "group_name": recipient_name,
        "markaz": scope_label if scope_key == "markaz" else "",
        "tehsil": scope_label if scope_key == "tehsil" else "",
        "wing": _wing_label(ctx.wing),
        "wing_name": ctx.wing.name,
        "district": scope_label if scope_key == "district" else "",
        "report_date": business_date.isoformat(),
        "checked_at": business_time.strftime("%I:%M %p").lstrip("0"),
        "submission_deadline": ctx.deadline.label if ctx.deadline else "12:30 PM",
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
    return template, values, message, business_date


__all__ = [
    "AcknowledgementVariant",
    "BUSINESS_TIMEZONE",
    "PURPOSE",
    "business_datetime",
    "resolve_message",
]

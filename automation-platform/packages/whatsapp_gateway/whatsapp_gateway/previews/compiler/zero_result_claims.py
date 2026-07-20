from __future__ import annotations

import hashlib
from datetime import date

from sqlalchemy import or_, select

from whatsapp_gateway.models import WhatsAppDailyMessageClaim
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.zero_result_templates import PURPOSE


def semantic_key(
    ctx: CompileContext,
    *,
    target_jid: str,
    scope_key: str,
    scope_value: str,
    business_date: date,
) -> str:
    # Report/profile identity is deliberately excluded. Dormant and consolidated
    # profiles therefore share the same once-daily acknowledgement claim.
    raw = "|".join(
        (
            str(ctx.account.id),
            str(ctx.application.id),
            target_jid,
            scope_key,
            scope_value,
            business_date.isoformat(),
            PURPOSE,
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def existing_claim(
    ctx: CompileContext,
    *,
    claim_key: str,
    target_jid: str,
    scope_key: str,
    scope_value: str,
    business_date: date,
) -> WhatsAppDailyMessageClaim | None:
    # Field matching recognizes claims created by the original Tehsil-only
    # implementation, whose semantic key included report_type_id.
    return ctx.session.scalar(
        select(WhatsAppDailyMessageClaim).where(
            or_(
                WhatsAppDailyMessageClaim.semantic_key == claim_key,
                (
                    (WhatsAppDailyMessageClaim.account_id == ctx.account.id)
                    & (WhatsAppDailyMessageClaim.application_id == ctx.application.id)
                    & (WhatsAppDailyMessageClaim.target_jid == target_jid)
                    & (WhatsAppDailyMessageClaim.scope_key == scope_key)
                    & (WhatsAppDailyMessageClaim.scope_value == scope_value)
                    & (WhatsAppDailyMessageClaim.business_date == business_date)
                    & (WhatsAppDailyMessageClaim.purpose == PURPOSE)
                ),
            )
        )
    )


__all__ = ["existing_claim", "semantic_key"]

from __future__ import annotations

from whatsapp_gateway.api_imports import *

from whatsapp_gateway.api_schemas import *

def normalize_target(value: str) -> str:
    target = value.strip()
    if target.endswith("@g.us") or target.endswith("@s.whatsapp.net"):
        return target
    digits = re.sub(r"\D", "", target)
    if digits.startswith("03"):
        digits = "92" + digits[1:]
    elif digits.startswith("3") and len(digits) == 10:
        digits = "92" + digits
    if not re.fullmatch(r"923\d{9}", digits):
        raise HTTPException(status_code=422, detail="Enter a valid Pakistani WhatsApp number")
    return f"{digits}@s.whatsapp.net"

def delivery_dict(item: WhatsAppDelivery) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "recipient_type": item.recipient_type,
        "recipient_name": item.recipient_name,
        "target": item.target,
        "message": item.message,
        "status": item.status,
        "error": item.error,
        "queue_stream": item.queue_stream,
        "queue_sequence": item.queue_sequence,
        "queued_at": item.queued_at,
        "completed_at": item.completed_at,
    }

def group_dict(
    item: WhatsAppGroup,
    wing_name: str,
    directory_group: WhatsAppDirectoryGroup | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "directory_group_id": (
            str(item.directory_group_id) if item.directory_group_id else None
        ),
        "wing_id": str(item.wing_id),
        "wing_name": wing_name,
        "name": item.name,
        "jid": item.jid,
        "purpose": item.purpose,
        "enabled": item.enabled,
        "verified_at": item.verified_at,
        "notes": item.notes,
        "participant_count": directory_group.participant_count if directory_group else None,
        "last_detected_at": directory_group.last_seen_at if directory_group else None,
    }

def template_dict(
    item: WhatsAppTemplate,
    application_name: str | None = None,
    report_type_name: str | None = None,
    recipient_scope_name: str | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id) if item.application_id else None,
        "report_type_id": str(item.report_type_id) if item.report_type_id else None,
        "application_name": application_name or "Shared",
        "report_type_name": report_type_name or "All report types",
        "recipient_channel": item.recipient_channel,
        "recipient_scope_id": str(item.recipient_scope_id) if item.recipient_scope_id else None,
        "recipient_scope_name": recipient_scope_name or "All roles / scopes",
        "key": item.key,
        "name": item.name,
        "category": item.category,
        "body": item.body,
        "enabled": item.enabled,
        "updated_at": item.updated_at,
    }

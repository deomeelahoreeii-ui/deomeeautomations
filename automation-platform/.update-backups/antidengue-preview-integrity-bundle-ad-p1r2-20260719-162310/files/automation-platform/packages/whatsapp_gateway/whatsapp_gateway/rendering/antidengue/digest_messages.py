from __future__ import annotations


def all_clear_digest_message(
    *, recipient_name: str, wing_name: str, scope_label: str,
    generated_label: str, deadline: str,
) -> tuple[str, dict[str, str]]:
    context = {
        "group_name": recipient_name, "scope_name": scope_label,
        "wing_name": wing_name, "generated_at": generated_label,
        "school_count": "0", "dormant_count": "0", "hotspot_count": "0",
        "timing_count": "0", "omitted_school_count": "0",
        "dormant_summary": "0 schools", "hotspot_summary": "0 schools",
        "timing_summary": "0 schools", "school_details": "No action items found.",
        "deadline": deadline,
    }
    message = (
        "✅ *ANTI-DENGUE ACTION DIGEST — ALL CLEAR*\n\n"
        f"📅 *Generated:* {generated_label}\n"
        f"👤 *AEO:* {recipient_name}\n"
        f"🏢 *Wing:* {wing_name}\n"
        f"📍 *Scope:* {scope_label}\n\n"
        "No dormant-school, hotspot-distance or activity-timing action items "
        "were found for this jurisdiction.\n\n"
        "📎 *Attached workbook:* Overview · Dormant Schools · Hotspot Distance · Activity Timing"
    )
    context["report_body"] = message
    return message, context


__all__ = ["all_clear_digest_message"]

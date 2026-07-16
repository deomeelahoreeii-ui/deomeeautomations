from __future__ import annotations

from typing import Any


def normalize_presentation_policy(
    value: dict[str, Any] | None,
    *,
    report_key: str,
) -> dict[str, str]:
    value = value or {}
    legacy_mode = "none" if report_key == "tehsil_dormant_summary" else "excel"
    return {
        "message_style": value.get("message_style")
        if value.get("message_style") in {"summary", "detailed"}
        else ("detailed" if report_key == "tehsil_dormant_summary" else "summary"),
        "attachment_mode": value.get("attachment_mode")
        if value.get("attachment_mode") in {"none", "image", "excel", "image_excel"}
        else legacy_mode,
        "image_content": value.get("image_content")
        if value.get("image_content") in {"summary", "details"}
        else ("details" if report_key == "tehsil_dormant_summary" else "summary"),
    }


def _has_attachment(policy: dict[str, str], kind: str) -> bool:
    mode = policy["attachment_mode"]
    return mode == kind or mode == "image_excel"

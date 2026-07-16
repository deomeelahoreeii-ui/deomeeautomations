from __future__ import annotations

import re
from typing import Any

from whatsapp_gateway.models import WhatsAppTemplate
from whatsapp_gateway.previews.compiler.errors import issue

PLACEHOLDER_RE = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")
PHONE_JID_SUFFIX = "@s.whatsapp.net"

def _render_message(
    template: WhatsAppTemplate | None,
    legacy_message: str,
    context: dict[str, str],
) -> tuple[str, list[dict[str, Any]]]:
    if template is None:
        return legacy_message, []
    values = {**context, "message": legacy_message}
    missing = sorted({key for key in PLACEHOLDER_RE.findall(template.body) if key not in values})
    problems = [
        issue(
            "unknown_template_variable",
            "blocked",
            f"Template variable {{{{{key}}}}} has no preview value.",
        )
        for key in missing
    ]
    rendered = PLACEHOLDER_RE.sub(lambda match: values.get(match.group(1), match.group(0)), template.body)
    return rendered, problems

def _delivery_status(problems: list[dict[str, Any]]) -> str:
    if any(item["severity"] == "blocked" for item in problems):
        return "blocked"
    if any(item["severity"] == "warning" for item in problems):
        return "warning"
    return "ready"

def _canonical_contact_target(target: str) -> str:
    value = target.strip()
    if "@" in value:
        return value
    digits = re.sub(r"\D", "", value)
    return f"{digits}{PHONE_JID_SUFFIX}" if digits else value


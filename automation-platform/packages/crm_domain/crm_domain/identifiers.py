from __future__ import annotations

import re


CRM_COMPLAINT_PREFIX = "104"
CRM_COMPLAINT_SUFFIX_DIGITS = 7
CRM_COMPLAINT_PATTERN = re.compile(
    rf"(?<!\d){CRM_COMPLAINT_PREFIX}\s*[-_/–—:]?\s*(\d{{{CRM_COMPLAINT_SUFFIX_DIGITS}}})(?!\d)",
    re.IGNORECASE,
)


def normalize_complaint_number(value: object) -> str | None:
    """Return the canonical ``104-1234567`` identity when unambiguous."""

    text = str(value or "").strip()
    matches = {
        f"{CRM_COMPLAINT_PREFIX}-{match.group(1)}"
        for match in CRM_COMPLAINT_PATTERN.finditer(text)
    }
    return next(iter(matches)) if len(matches) == 1 else None


def complaint_numbers_in(value: object) -> list[str]:
    text = str(value or "")
    return list(
        dict.fromkeys(
            f"{CRM_COMPLAINT_PREFIX}-{match.group(1)}"
            for match in CRM_COMPLAINT_PATTERN.finditer(text)
        )
    )

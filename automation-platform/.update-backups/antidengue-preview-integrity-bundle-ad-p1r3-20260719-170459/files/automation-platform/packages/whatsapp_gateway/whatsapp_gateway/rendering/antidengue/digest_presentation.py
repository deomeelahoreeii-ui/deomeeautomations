from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from master_data.models import Wing
from whatsapp_gateway.rendering.antidengue.digest_models import (
    CONSOLIDATED_DIGEST_DEFAULT_TEMPLATE,
    DigestSchool,
)
from whatsapp_gateway.rendering.antidengue.hotspot_report import _plain_whatsapp_text


def school_count(count: int) -> str:
    return f"{count} school{'s' if count != 1 else ''}"


def _issue_count_summary(schools: list[DigestSchool]) -> str:
    dormant = sum(item.dormant for item in schools)
    hotspot = sum(item.hotspot_distance for item in schools)
    timing = sum(item.activity_timing for item in schools)
    return (
        f"{school_count(len(schools))} · 💤 {dormant} · "
        f"📏 {hotspot} · ⏱️ {timing}"
    )


def _wing_summary_lines(schools: list[DigestSchool]) -> list[str]:
    by_tehsil: dict[str, list[DigestSchool]] = defaultdict(list)
    for school in schools:
        by_tehsil[school.tehsil].append(school)

    lines = ["*TEHSIL SUMMARY*"]
    for index, (tehsil, members) in enumerate(
        sorted(by_tehsil.items(), key=lambda item: item[0].casefold()), start=1
    ):
        lines.append(
            f"{index}. {_plain_whatsapp_text(tehsil)} · "
            f"{_issue_count_summary(members)}"
        )

    lines.extend(["", "*MARKAZ SUMMARY*"])
    for tehsil, tehsil_members in sorted(
        by_tehsil.items(), key=lambda item: item[0].casefold()
    ):
        lines.append(f"*{_plain_whatsapp_text(tehsil)}*")
        by_markaz: dict[str, list[DigestSchool]] = defaultdict(list)
        for school in tehsil_members:
            by_markaz[school.markaz].append(school)
        for index, (markaz, members) in enumerate(
            sorted(by_markaz.items(), key=lambda item: item[0].casefold()), start=1
        ):
            lines.append(
                f"{index}. {_plain_whatsapp_text(markaz)} · "
                f"{_issue_count_summary(members)}"
            )
        lines.append("")
    return lines


def _tehsil_summary_lines(schools: list[DigestSchool]) -> list[str]:
    by_markaz: dict[str, list[DigestSchool]] = defaultdict(list)
    for school in schools:
        by_markaz[school.markaz].append(school)
    return [
        f"{index}. {_plain_whatsapp_text(markaz)} · "
        f"{_issue_count_summary(members)}"
        for index, (markaz, members) in enumerate(
            sorted(by_markaz.items(), key=lambda item: item[0].casefold()), start=1
        )
    ]


def _markaz_detail_lines(
    schools: list[DigestSchool], *, limit: int = 30
) -> tuple[list[str], int]:
    selected = schools[:limit]
    lines = [
        f"{index}. `{school.emis}` — {_plain_whatsapp_text(school.school_name)} "
        f"· {school.issue_icons}"
        for index, school in enumerate(selected, start=1)
    ]
    omitted = len(schools) - len(selected)
    if omitted:
        lines.extend(["", f"📎 {school_count(omitted)} more listed in the workbook."])
    return lines, omitted


def parent_issue(issue: dict[str, Any]) -> dict[str, Any]:
    """Normalize component-only presentation findings at the parent boundary."""
    if issue.get("category") != "presentation":
        return issue
    normalized = dict(issue)
    normalized["severity"] = "info"
    normalized["component_code"] = issue.get("code")
    if issue.get("code") == "message_only_evidence_truncated":
        normalized["code"] = "message_evidence_summarized"
        normalized["message"] = (
            "A component text sample omitted evidence that is preserved in the "
            "consolidated delivery workbook."
        )
    normalized["evidence_attachment_source"] = "parent"
    return normalized


def build_digest_message(
    *,
    schools: list[DigestSchool],
    recipient_name: str,
    wing: Wing,
    scope_key: str,
    scope_label: str,
    generated_label: str,
    deadline: str,
) -> tuple[str, dict[str, str]]:
    dormant_count = sum(item.dormant for item in schools)
    hotspot_count = sum(item.hotspot_distance for item in schools)
    timing_count = sum(item.activity_timing for item in schools)

    if scope_key in {"district", "wing"}:
        detail_heading = "TEHSIL AND MARKAZ SUMMARY"
        detail_lines = _wing_summary_lines(schools)
        omitted = 0
    elif scope_key == "tehsil":
        detail_heading = "MARKAZ SUMMARY"
        detail_lines = _tehsil_summary_lines(schools)
        omitted = 0
    elif scope_key == "markaz":
        detail_heading = "SCHOOL ACTION DETAILS"
        detail_lines, omitted = _markaz_detail_lines(schools)
    else:
        raise ValueError(
            f"Consolidated digest does not support {scope_key or 'an unbound'} scope"
        )

    context = {
        "group_name": _plain_whatsapp_text(recipient_name),
        "scope_name": _plain_whatsapp_text(scope_label),
        "wing_name": _plain_whatsapp_text(wing.name),
        "generated_at": generated_label,
        "school_count": str(len(schools)),
        "dormant_count": str(dormant_count),
        "hotspot_count": str(hotspot_count),
        "timing_count": str(timing_count),
        "omitted_school_count": str(omitted),
        "dormant_summary": school_count(dormant_count),
        "hotspot_summary": school_count(hotspot_count),
        "timing_summary": school_count(timing_count),
        "detail_heading": detail_heading,
        "school_details": "\n".join(detail_lines).strip(),
        "deadline": deadline,
    }
    message = re.sub(
        r"{{\s*([a-zA-Z0-9_]+)\s*}}",
        lambda match: context.get(match.group(1), match.group(0)),
        CONSOLIDATED_DIGEST_DEFAULT_TEMPLATE,
    ).strip()
    context["report_body"] = message
    return message, context


__all__ = ["build_digest_message", "parent_issue", "school_count"]

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime

from automation_core.models import Job
from master_data.models import Tehsil, Wing
from whatsapp_gateway.rendering.antidengue.formatting import (
    _report_time, _school_count, _wing_display_name, _wing_label,
)
from whatsapp_gateway.rendering.antidengue.models import (
    RENDERER_KEY, WING_RENDERER_KEY, ScopedDormantSchool,
)
from whatsapp_gateway.rendering.antidengue.policy import (
    _has_attachment, normalize_presentation_policy,
)


def _build_message(
    *,
    source_job: Job,
    recipient_name: str,
    wing: Wing,
    tehsil: Tehsil,
    schools: list[ScopedDormantSchool],
    deadline: str,
    policy: dict[str, str] | None = None,
) -> tuple[str, dict[str, str]]:
    policy = policy or normalize_presentation_policy(None, report_key="tehsil_dormant_summary")
    generated_at = _report_time(source_job)
    title = f"Anti-Dengue Dormant Users - {generated_at:%d-%b-%Y - %I:%M %p}"
    wing_label = _wing_label(wing)
    lines = [
        f"*{title}*",
        f"*Group:* {recipient_name}",
        f"*Tehsil:* {tehsil.name}",
        f"*Wing:* {wing_label}",
        f"*Total dormant:* {_school_count(len(schools))}",
        "",
    ]
    by_markaz: dict[str, list[ScopedDormantSchool]] = defaultdict(list)
    for school in schools:
        by_markaz[school.markaz].append(school)
    if policy["message_style"] == "detailed":
        lines.extend([
            "*TEHSIL SUMMARY*", f"1. {tehsil.name}: {_school_count(len(schools))}",
            "", "*DETAILS BY MARKAZ*", "", f"*{tehsil.name} - {_school_count(len(schools))}*",
        ])
    else:
        lines.extend(["*MARKAZ SUMMARY*"])
        for index, markaz_name in enumerate(sorted(by_markaz, key=str.casefold), start=1):
            lines.append(f"{index}. {markaz_name}: {_school_count(len(by_markaz[markaz_name]))}")
    for markaz_name in sorted(by_markaz, key=str.casefold):
        if policy["message_style"] != "detailed":
            break
        markaz_schools = sorted(
            by_markaz[markaz_name],
            key=lambda item: (item.school_name.casefold(), item.emis),
        )
        lines.extend(["", f"*{markaz_name} - {_school_count(len(markaz_schools))}*"])
        lines.extend(
            f"{index}. {school.emis} - {school.school_name}"
            for index, school in enumerate(markaz_schools, start=1)
        )
    try:
        parsed_deadline = datetime.strptime(deadline.strip(), "%I:%M %p").time()
        deadline_passed = generated_at.time() > parsed_deadline
    except ValueError:
        deadline_passed = False
    deadline_message = (
        f"The {deadline} submission deadline has passed. Please update activities on the portal immediately."
        if deadline_passed
        else f"Please ensure activities are submitted before {deadline} today."
    )
    lines.extend(["", deadline_message])
    if policy["attachment_mode"] != "none":
        labels = []
        if _has_attachment(policy, "image"):
            labels.append("image report")
        if _has_attachment(policy, "excel"):
            labels.append("Excel workbook")
        lines.extend(["", f"Attached: {' and '.join(labels)}."])
    message = "\n".join(lines)
    context = {
        "title": title,
        "recipient_name": recipient_name,
        "tehsil": tehsil.name,
        "wing": wing_label,
        "total_schools": str(len(schools)),
        "report_body": message,
        "deadline_message": deadline_message,
        "renderer_key": RENDERER_KEY,
    }
    return message, context


def _build_wing_message(
    *,
    source_job: Job,
    recipient_name: str,
    wing: Wing,
    schools: list[ScopedDormantSchool],
    policy: dict[str, str] | None = None,
) -> tuple[str, dict[str, str]]:
    legacy_policy = policy is None
    policy = policy or normalize_presentation_policy(None, report_key="wing_summary")
    generated_at = _report_time(source_job)
    title = f"Anti-Dengue Dormant Users - {generated_at:%d-%b-%Y - %I:%M %p}"
    wing_label = _wing_label(wing)
    wing_display_name = _wing_display_name(wing)
    tehsil_count = len({school.tehsil for school in schools})
    lines = [
        "*Anti-Dengue Dormancy Summary*",
        f"*{wing_display_name} — {generated_at:%d-%b-%Y, %I:%M %p}*",
        "",
        f"*Total dormant:* {_school_count(len(schools))} across {tehsil_count} {'tehsil' if tehsil_count == 1 else 'tehsils'}",
        "",
        "*TEHSIL BREAKDOWN*",
    ]
    by_tehsil: dict[str, list[ScopedDormantSchool]] = defaultdict(list)
    for school in schools:
        by_tehsil[school.tehsil].append(school)
    lines.extend(
        f"{index}. {tehsil_name}: {_school_count(len(tehsil_schools))}"
        for index, (tehsil_name, tehsil_schools) in enumerate(
            sorted(by_tehsil.items(), key=lambda item: item[0].casefold()), start=1
        )
    )
    authoritative_markaz = any(
        school.markaz != "UNMAPPED MARKAZ" for school in schools
    )
    if authoritative_markaz:
        lines.extend(["", "*MARKAZ BREAKDOWN*"])
        for tehsil_name, tehsil_schools in sorted(
            by_tehsil.items(), key=lambda item: item[0].casefold()
        ):
            lines.extend(["", f"*{tehsil_name} - {_school_count(len(tehsil_schools))}*"])
            markaz_counts = Counter(school.markaz for school in tehsil_schools)
            lines.extend(
                f"{index}. {markaz_name}: {_school_count(count)}"
                for index, (markaz_name, count) in enumerate(
                    sorted(markaz_counts.items(), key=lambda item: item[0].casefold()), start=1
                )
            )
    lines.extend(
        [
            "",
            "Please ensure all pending activities are updated on the portal today.",
        ]
    )
    if legacy_policy:
        lines.extend(["", "The attached workbook contains the detailed school list."])
    elif policy["attachment_mode"] != "none":
        labels = []
        if _has_attachment(policy, "image"):
            labels.append("image report")
        if _has_attachment(policy, "excel"):
            labels.append("Excel workbook")
        lines.extend(["", f"Attached: {' and '.join(labels)}."])
    message = "\n".join(lines)
    context = {
        "title": title,
        "recipient_name": recipient_name,
        "wing": wing_label,
        "wing_display_name": wing_display_name,
        "scope": "Wing",
        "total_schools": str(len(schools)),
        "report_body": message,
        "renderer_key": WING_RENDERER_KEY,
    }
    return message, context

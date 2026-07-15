from __future__ import annotations

import csv
import hashlib
import json
import re
import shlex
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.request
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

from imgui_bundle import imgui
from openpyxl import load_workbook

from complaints_manager.core.commands import (
    ArgField,
    COMMANDS,
    SYSTEMS,
    CommandSpec,
    SystemSpec,
    command_by_id,
    commands_by_system,
)
from complaints_manager.core.services import normalize_whatsapp_account_id
from complaints_manager.gui.paths import (
    apply_selected_folder,
    apply_selected_file,
    consume_file_dialog_result,
    consume_folder_dialog_result,
    existing_initial_dir,
    resolve_gui_path,
    start_file_dialog,
    start_folder_dialog,
)
from complaints_manager.gui.state import GuiState

from complaints_manager.gui.antidengue.database import _pb_bool
from complaints_manager.gui.antidengue.delivery import failed_officer_rows

def _group_scope_label(group: dict) -> str:
    if group.get("markaz"):
        return f"{group.get('tehsil') or '-'} / {group.get('markaz')}"
    if group.get("tehsil"):
        return str(group.get("tehsil"))
    return str(group.get("route_kind") or "district").replace("_", " ").title()

def _rows_for_dispatch_group(group: dict, rows: list[dict]) -> list[dict]:
    route_kind = str(group.get("route_kind") or "district").strip().lower()
    tehsil_ref = str(group.get("tehsil_ref") or "")
    markaz_ref = str(group.get("markaz_ref") or "")
    if route_kind == "tehsil" and not tehsil_ref:
        return []
    if route_kind == "markaz" and not markaz_ref:
        return []

    filtered = rows
    if tehsil_ref:
        filtered = [row for row in filtered if str(row.get("tehsil_ref") or "") == tehsil_ref]
    if markaz_ref:
        filtered = [row for row in filtered if str(row.get("markaz_ref") or "") == markaz_ref]
    return filtered

def _school_count_text(count: int) -> str:
    return f"{count} {'school' if count == 1 else 'schools'}"

def _school_line(index: int, row: dict) -> str:
    emis = str(row.get("emis") or "").strip()
    emis_part = f"{emis} - " if emis else ""
    return f"{index}. {emis_part}{row.get('school_name') or 'Unknown School'}"

def _tehsil_summary_lines(rows: list[dict]) -> list[str]:
    tehsil_counts = Counter(str(row.get("tehsil") or "Unmapped") for row in rows)
    if not tehsil_counts:
        return []
    return [
        "",
        "*TEHSIL SUMMARY*",
        *[
            f"{index}. {name}: {_school_count_text(count)}"
            for index, (name, count) in enumerate(sorted(tehsil_counts.items()), start=1)
        ],
    ]

def _markaz_summary_lines(rows: list[dict]) -> list[str]:
    if not rows:
        return []

    grouped_by_tehsil: dict[str, Counter[str]] = {}
    for row in rows:
        tehsil = str(row.get("tehsil") or "Unmapped")
        markaz = str(row.get("markaz") or "Unmapped")
        grouped_by_tehsil.setdefault(tehsil, Counter())[markaz] += 1

    lines = ["", "*MARKAZ SUMMARY*"]
    for tehsil, markaz_counts in sorted(grouped_by_tehsil.items()):
        tehsil_total = sum(markaz_counts.values())
        lines.extend(["", f"*{tehsil} - {_school_count_text(tehsil_total)}*"])
        lines.extend(
            f"{index}. {markaz}: {_school_count_text(count)}"
            for index, (markaz, count) in enumerate(sorted(markaz_counts.items()), start=1)
        )
    return lines

def _summary_only_sections(
    rows: list[dict],
    *,
    include_tehsil: bool,
    include_markaz: bool,
) -> list[str]:
    if not rows:
        return ["", "No dormant schools in this group's current scope."]

    lines: list[str] = []
    if include_tehsil:
        lines.extend(_tehsil_summary_lines(rows))
    if include_markaz:
        lines.extend(_markaz_summary_lines(rows))
    return lines

def _hierarchy_school_sections(
    rows: list[dict],
    *,
    include_heading: bool = True,
) -> list[str]:
    if not rows:
        return ["", "No dormant schools in this group's current scope."]

    grouped_by_tehsil: dict[str, list[dict]] = {}
    for row in rows:
        grouped_by_tehsil.setdefault(str(row.get("tehsil") or "Unmapped"), []).append(row)

    lines: list[str] = []
    if include_heading:
        lines.extend(["", "*DETAILS BY TEHSIL / MARKAZ*"])

    for tehsil, tehsil_rows in sorted(grouped_by_tehsil.items()):
        sorted_tehsil_rows = sorted(
            tehsil_rows,
            key=lambda item: (str(item.get("markaz") or ""), str(item.get("school_name") or "")),
        )
        lines.extend(["", f"*{tehsil} - {_school_count_text(len(sorted_tehsil_rows))}*"])

        grouped_by_markaz: dict[str, list[dict]] = {}
        for row in sorted_tehsil_rows:
            grouped_by_markaz.setdefault(str(row.get("markaz") or "Unmapped"), []).append(row)

        for markaz, markaz_rows in sorted(grouped_by_markaz.items()):
            sorted_markaz_rows = sorted(
                markaz_rows,
                key=lambda item: str(item.get("school_name") or ""),
            )
            lines.extend(
                [
                    "",
                    f"*{markaz} - {_school_count_text(len(sorted_markaz_rows))}*",
                ]
            )
            lines.extend(
                _school_line(index, row)
                for index, row in enumerate(sorted_markaz_rows, start=1)
            )

    return lines

def _markaz_school_sections(rows: list[dict]) -> list[str]:
    if not rows:
        return ["", "No dormant schools in this group's current scope."]

    grouped_by_markaz: dict[str, list[dict]] = {}
    for row in rows:
        grouped_by_markaz.setdefault(str(row.get("markaz") or "Unmapped"), []).append(row)

    lines: list[str] = []
    for markaz, markaz_rows in sorted(grouped_by_markaz.items()):
        sorted_markaz_rows = sorted(
            markaz_rows,
            key=lambda item: str(item.get("school_name") or ""),
        )
        lines.extend(["", f"*{markaz} - {_school_count_text(len(sorted_markaz_rows))}*"])
        lines.extend(
            _school_line(index, row)
            for index, row in enumerate(sorted_markaz_rows, start=1)
        )
    return lines

def _grouped_school_sections(rows: list[dict], key_fields: tuple[str, str]) -> list[str]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        key = (
            str(row.get(key_fields[0]) or "Unmapped"),
            str(row.get(key_fields[1]) or ""),
        )
        grouped.setdefault(key, []).append(row)

    lines: list[str] = []
    for (name, mobile), group_rows in sorted(grouped.items()):
        title = name if not mobile else f"{name} ({mobile})"
        lines.extend(["", f"*{title} - {_school_count_text(len(group_rows))}*"])
        lines.extend(_hierarchy_school_sections(group_rows, include_heading=False))
    return lines

def build_antidengue_group_dispatch_message(group: dict, context: dict) -> str:
    rows = _rows_for_dispatch_group(group, context.get("rows") or [])
    summary = context.get("summary") or {}
    run_time = str(context.get("run_key") or summary.get("run_started_at") or "")[:19].replace("T", " ")
    mode = str(group.get("message_mode") or "full_report")
    lines = [
        f"*Anti-Dengue Dormant Schools Report - {run_time}*",
        f"*Group:* {group.get('name') or group.get('target') or '-'}",
        f"*Scope:* {_group_scope_label(group)}",
        f"*Total dormant:* {_school_count_text(len(rows))}",
    ]

    if mode == "failed_fallback":
        failed = failed_officer_rows(
            ((summary.get("whatsapp") or {}).get("officer_delivery_audit") or [])
        )
        lines.extend(["", "Failed direct-delivery officers:"])
        if not failed:
            lines.append("No failed individual officer deliveries in the latest run.")
        for index, row in enumerate(failed, start=1):
            lines.append(
                f"{index}. {row.get('Officer Name') or '-'} ({row.get('Roles') or '-'}) "
                f"{row.get('Mobile Number') or '-'} - schools: {row.get('School Count') or 0}"
            )
    elif mode == "ddeo_wise":
        lines.extend(_grouped_school_sections(rows, ("ddeo_name", "ddeo_mobile")))
    elif mode == "aeo_wise":
        lines.extend(_grouped_school_sections(rows, ("aeo_name", "aeo_mobile")))
    elif mode == "tehsil_summary":
        lines.extend(
            _summary_only_sections(rows, include_tehsil=True, include_markaz=False)
        )
    elif mode == "markaz_summary":
        lines.extend(
            _summary_only_sections(rows, include_tehsil=False, include_markaz=True)
        )
    elif mode == "tehsil_markaz_summary":
        lines.extend(
            _summary_only_sections(rows, include_tehsil=True, include_markaz=True)
        )
    else:
        lines.extend(_tehsil_summary_lines(rows))
        lines.extend(_hierarchy_school_sections(rows))

    if _pb_bool(group.get("attach_excel"), True):
        lines.extend(["", "Excel report should be attached with this dispatch."])
    lines.extend(["", "Please ensure activities are submitted/updated on the portal today."])
    return "\n".join(lines)

def build_antidengue_manual_message(row: dict) -> str:
    officer_name = str(row.get("Officer Name") or "Officer").strip()
    roles = str(row.get("Roles") or "").strip()
    school_count = str(row.get("School Count") or "").strip()
    tehsils = ", ".join(
        line.strip()
        for line in str(row.get("Tehsils") or "").splitlines()
        if line.strip()
    )
    markazes = ", ".join(
        line.strip()
        for line in str(row.get("Markazes") or "").splitlines()
        if line.strip()
    )
    schools = [
        line.strip()
        for line in str(row.get("Schools") or "").splitlines()
        if line.strip()
    ]

    header_name = officer_name if officer_name and officer_name != "-" else "Officer"
    role_text = f" ({roles})" if roles else ""
    count_text = f"{school_count} " if school_count else ""
    context_parts = []
    if tehsils:
        context_parts.append(f"Tehsil: {tehsils}")
    if markazes:
        context_parts.append(f"Markaz: {markazes}")

    lines = [
        f"Assalam o Alaikum {header_name}{role_text},",
        "",
        f"Anti-Dengue portal is still showing {count_text}school(s) as dormant/inactive under your supervision.",
    ]

    if context_parts:
        lines.append(" | ".join(context_parts))

    if schools:
        lines.extend(["", "Schools:"])
        lines.extend(f"{index}. {school}" for index, school in enumerate(schools, start=1))

    lines.extend(
        [
            "",
            "Please ensure activities are submitted/updated on the portal today.",
            "After checking, please reply OK here so the automated WhatsApp system can refresh direct delivery for your number.",
        ]
    )
    return "\n".join(lines)

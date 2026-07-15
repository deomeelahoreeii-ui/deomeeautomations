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

from complaints_manager.gui.antidengue.history_repository import clear_archived_antidengue_runs, delete_antidengue_run_history, load_antidengue_run_details, load_antidengue_run_history, set_antidengue_run_archived
from complaints_manager.gui.antidengue.messages import build_antidengue_manual_message
from complaints_manager.gui.common.colors import status_badge_color
from complaints_manager.gui.common.context import disabled_when
from complaints_manager.gui.common.desktop import open_desktop_path
from complaints_manager.gui.common.formatting import draw_clipped_text

def _format_history_time(value: object) -> str:
    return str(value or "-").replace("T", " ")[:19]

def _history_summary_output_dir(row: dict) -> Path | None:
    summary = row.get("summary_dict") or {}
    outputs = summary.get("outputs") or {}
    output_dir = str(outputs.get("output_dir") or "").strip()
    return Path(output_dir) if output_dir else None

def _run_is_dry(row: dict) -> bool:
    summary = row.get("summary_dict") or {}
    whatsapp = summary.get("whatsapp") or {}
    return bool(summary.get("dry_run") or whatsapp.get("dry_run"))

def _run_mode_label(row: dict) -> str:
    return "Dry Run" if _run_is_dry(row) else "Live Send"

def draw_history_filter_button(state: GuiState, value: str, label: str, width: int) -> None:
    selected = state.antidengue_history_filter == value
    with disabled_when(selected):
        if imgui.button(f"{label}{' *' if selected else ''}", (width, 0)):
            state.antidengue_history_filter = value
            state.antidengue_history_selected_run_key = ""
            state.antidengue_history_pending_delete_run_key = ""

def _record_history_action_result(state: GuiState, success: bool, message: str) -> None:
    state.runner.status_message = message
    state.runner.append_log(message)
    if success:
        state.cached_metrics.pop("antidengue", None)

def draw_antidengue_run_history_section(state: GuiState) -> None:
    draw_history_filter_button(state, "active", "Active", 90)
    imgui.same_line()
    draw_history_filter_button(state, "archived", "Archived", 105)
    imgui.same_line()
    draw_history_filter_button(state, "all", "All", 70)
    imgui.same_line()
    if state.antidengue_history_pending_delete_run_key == "__clear_archived__":
        if imgui.button("Confirm Clear Archived", (185, 0)):
            _record_history_action_result(state, *clear_archived_antidengue_runs(state))
        imgui.same_line()
        if imgui.button("Cancel", (80, 0)):
            state.antidengue_history_pending_delete_run_key = ""
    else:
        if imgui.button("Clear Archived", (140, 0)):
            state.antidengue_history_pending_delete_run_key = "__clear_archived__"
    imgui.text_disabled(
        "Archive hides runs from Active. Delete/Clear remove PocketBase history records only; output files remain on disk."
    )

    history, error = load_antidengue_run_history(state)
    if error:
        imgui.text_colored(status_badge_color("warning"), error)
        return
    if not history:
        imgui.text_disabled("No PocketBase run history has been saved yet.")
        return

    if not state.antidengue_history_selected_run_key:
        state.antidengue_history_selected_run_key = str(history[0].get("run_key") or "")

    selected = next(
        (
            row
            for row in history
            if str(row.get("run_key") or "") == state.antidengue_history_selected_run_key
        ),
        history[0],
    )
    state.antidengue_history_selected_run_key = str(selected.get("run_key") or "")

    imgui.text_disabled(
        f"Showing latest {len(history)} PocketBase run(s) in {state.antidengue_history_filter} view."
    )
    imgui.same_line()
    if imgui.button("Select Latest", (120, 0)):
        state.antidengue_history_selected_run_key = str(history[0].get("run_key") or "")
        selected = history[0]

    if imgui.begin_table("antidengue_run_history", 13):
        for label in (
            "Run",
            "Status",
            "Mode",
            "Source",
            "Dormant",
            "Delivered",
            "Failed",
            "Issues",
            "Stages",
            "Plan",
            "Archived",
            "Raw File",
            "Action",
        ):
            imgui.table_setup_column(label)
        imgui.table_headers_row()

        for index, row in enumerate(history):
            run_key = str(row.get("run_key") or "")
            is_selected = run_key == state.antidengue_history_selected_run_key
            imgui.push_id(f"history_{index}")
            imgui.table_next_row()

            imgui.table_next_column()
            imgui.text_colored(
                status_badge_color("ok" if is_selected else ""),
                _format_history_time(row.get("started_at")),
            )
            imgui.table_next_column()
            status = str(row.get("status") or "-")
            imgui.text_colored(status_badge_color(status), status)
            imgui.table_next_column()
            mode_label = _run_mode_label(row)
            imgui.text_colored(
                status_badge_color("warning" if _run_is_dry(row) else "ok"),
                mode_label,
            )
            imgui.table_next_column()
            imgui.text(str(row.get("source") or "-"))
            imgui.table_next_column()
            imgui.text(str(row.get("dormant_count") or 0))
            imgui.table_next_column()
            imgui.text_colored(status_badge_color("delivered"), str(row.get("delivered_count") or 0))
            imgui.table_next_column()
            failed_count = int(row.get("failed_count") or 0)
            imgui.text_colored(
                status_badge_color("failed" if failed_count else "ok"),
                str(failed_count),
            )
            imgui.table_next_column()
            issue_count = int(row.get("issue_count") or 0)
            imgui.text_colored(
                status_badge_color("warning" if issue_count else "ok"),
                str(issue_count),
            )
            imgui.table_next_column()
            imgui.text(str(row.get("stage_count") or 0))
            imgui.table_next_column()
            imgui.text(str(row.get("dispatch_plan_count") or 0))
            imgui.table_next_column()
            archived = bool(row.get("archived"))
            imgui.text_colored(
                status_badge_color("warning" if archived else "ok"),
                "yes" if archived else "no",
            )
            imgui.table_next_column()
            draw_clipped_text(row.get("raw_file_name"), 34)
            imgui.table_next_column()
            with disabled_when(is_selected):
                if imgui.button("View", (70, 0)):
                    state.antidengue_history_selected_run_key = run_key
                    selected = row
            imgui.same_line()
            archive_label = "Restore" if archived else "Archive"
            if imgui.button(archive_label, (82, 0)):
                success, message = set_antidengue_run_archived(
                    state,
                    run_key,
                    archived=not archived,
                )
                _record_history_action_result(state, success, message)
                if success and state.antidengue_history_filter != "all":
                    state.antidengue_history_selected_run_key = ""
            imgui.pop_id()
        imgui.end_table()

    details, detail_error = load_antidengue_run_details(
        state,
        state.antidengue_history_selected_run_key,
    )
    if detail_error:
        imgui.text_colored(status_badge_color("warning"), detail_error)
        return

    summary = selected.get("summary_dict") or {}
    quality_gate = summary.get("quality_gate") or {}
    whatsapp = summary.get("whatsapp") or {}
    outputs = summary.get("outputs") or {}

    imgui.separator_text("Selected Run")
    if imgui.begin_table("antidengue_history_selected_metrics", 6):
        for label in ("Started", "Status", "Mode", "Raw rows", "Dormant", "Quality"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        imgui.table_next_row()
        imgui.table_next_column()
        imgui.text(_format_history_time(selected.get("started_at")))
        imgui.table_next_column()
        imgui.text_colored(status_badge_color(str(selected.get("status") or "")), str(selected.get("status") or "-"))
        imgui.table_next_column()
        imgui.text_colored(
            status_badge_color("warning" if _run_is_dry(selected) else "ok"),
            _run_mode_label(selected),
        )
        imgui.table_next_column()
        filter_info = summary.get("filter") or {}
        imgui.text(str(filter_info.get("raw_rows") or "-"))
        imgui.table_next_column()
        imgui.text(str(selected.get("dormant_count") or 0))
        imgui.table_next_column()
        imgui.text_colored(
            status_badge_color("passed" if quality_gate.get("passed") else "failed"),
            "passed" if quality_gate.get("passed") else "blocked",
        )
        imgui.end_table()

    selected_run_key = str(selected.get("run_key") or "")
    selected_archived = bool(selected.get("archived"))
    if imgui.button("Restore Run" if selected_archived else "Archive Run", (125, 0)):
        success, message = set_antidengue_run_archived(
            state,
            selected_run_key,
            archived=not selected_archived,
        )
        _record_history_action_result(state, success, message)
        if success and state.antidengue_history_filter != "all":
            state.antidengue_history_selected_run_key = ""
            return
    imgui.same_line()
    if state.antidengue_history_pending_delete_run_key == selected_run_key:
        if imgui.button("Confirm Delete", (135, 0)):
            _record_history_action_result(
                state,
                *delete_antidengue_run_history(state, selected_run_key),
            )
            return
        imgui.same_line()
        if imgui.button("Cancel Delete", (125, 0)):
            state.antidengue_history_pending_delete_run_key = ""
    else:
        if imgui.button("Delete Run", (110, 0)):
            state.antidengue_history_pending_delete_run_key = selected_run_key

    output_dir = _history_summary_output_dir(selected)
    if output_dir:
        with disabled_when(not output_dir.exists()):
            if imgui.button("Open Output Folder", (160, 0)):
                open_desktop_path(state, output_dir, "AntiDengue history output folder")
        imgui.same_line()
    delivery_audit_value = str(outputs.get("officer_delivery_audit") or "").strip()
    delivery_audit = Path(delivery_audit_value) if delivery_audit_value else None
    with disabled_when(delivery_audit is None or not delivery_audit.exists()):
        if imgui.button("Open Delivery Audit", (160, 0)):
            assert delivery_audit is not None
            open_desktop_path(state, delivery_audit, "AntiDengue history delivery audit")

    lifecycle_rows = details.get("lifecycle") or []
    imgui.separator_text("Run Lifecycle")
    if not lifecycle_rows:
        imgui.text_disabled("No lifecycle events saved for this run.")
    elif imgui.begin_table("antidengue_history_lifecycle", 4):
        for label in ("Stage", "Status", "Message", "Time"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        for index, row in enumerate(lifecycle_rows):
            imgui.push_id(f"history_lifecycle_{index}")
            status = str(row.get("status") or "")
            imgui.table_next_row()
            imgui.table_next_column()
            imgui.text(str(row.get("stage") or "-").replace("_", " "))
            imgui.table_next_column()
            imgui.text_colored(status_badge_color(status), status.replace("_", " ") or "-")
            imgui.table_next_column()
            draw_clipped_text(row.get("message"), 96)
            imgui.table_next_column()
            draw_clipped_text(str(row.get("created_at") or ""), 24)
            imgui.pop_id()
        imgui.end_table()
        lifecycle_total = int(selected.get("stage_count") or len(lifecycle_rows))
        if lifecycle_total > len(lifecycle_rows):
            imgui.text_disabled(
                f"Showing first {len(lifecycle_rows)} of {lifecycle_total} lifecycle events."
            )

    dispatch_plan_rows = details.get("dispatch_plan") or []
    imgui.separator_text("Dispatch Plan")
    if not dispatch_plan_rows:
        imgui.text_disabled("No dispatch plan saved for this run.")
    elif imgui.begin_table("antidengue_history_dispatch_plan", 9):
        for label in (
            "Channel",
            "Recipient",
            "Target",
            "Route",
            "Message",
            "Rows",
            "Status",
            "Copy",
            "Cause",
        ):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        for index, row in enumerate(dispatch_plan_rows):
            imgui.push_id(f"history_dispatch_plan_{index}")
            status = str(row.get("status") or "")
            payload = row.get("payload_dict") or {}
            planned_text = str(payload.get("text") or "")
            imgui.table_next_row()
            imgui.table_next_column()
            imgui.text(str(row.get("channel") or "-"))
            imgui.table_next_column()
            draw_clipped_text(row.get("recipient_name"), 30)
            imgui.table_next_column()
            draw_clipped_text(row.get("target"), 34)
            imgui.table_next_column()
            route_kind = str(row.get("route_kind") or "-").replace("_", " ")
            imgui.text(route_kind)
            imgui.table_next_column()
            message_mode = str(row.get("message_mode") or "-").replace("_", " ")
            draw_clipped_text(message_mode, 28)
            imgui.table_next_column()
            imgui.text(str(row.get("row_count") or 0))
            imgui.table_next_column()
            imgui.text_colored(status_badge_color(status), status.replace("_", " ") or "-")
            imgui.table_next_column()
            with disabled_when(not planned_text):
                if imgui.button("Copy", (70, 0)):
                    imgui.set_clipboard_text(planned_text)
                    state.runner.status_message = (
                        "Copied planned dispatch message for "
                        f"{row.get('recipient_name') or row.get('target') or 'recipient'}"
                    )
                    state.runner.append_log(state.runner.status_message)
            imgui.table_next_column()
            draw_clipped_text(row.get("cause"), 46)
            imgui.pop_id()
        imgui.end_table()
        dispatch_plan_total = int(selected.get("dispatch_plan_count") or len(dispatch_plan_rows))
        if dispatch_plan_total > len(dispatch_plan_rows):
            imgui.text_disabled(
                f"Showing first {len(dispatch_plan_rows)} of {dispatch_plan_total} dispatch plan items."
            )

    quality_rows = details.get("quality") or []
    if quality_rows:
        imgui.separator_text("Quality Issues")
        if imgui.begin_table("antidengue_history_quality", 3):
            imgui.table_setup_column("Severity")
            imgui.table_setup_column("Category")
            imgui.table_setup_column("Message")
            imgui.table_headers_row()
            for index, row in enumerate(quality_rows):
                imgui.push_id(f"history_quality_{index}")
                imgui.table_next_row()
                imgui.table_next_column()
                severity = str(row.get("severity") or "")
                imgui.text_colored(status_badge_color(severity), severity)
                imgui.table_next_column()
                imgui.text(str(row.get("category") or "-"))
                imgui.table_next_column()
                draw_clipped_text(row.get("message"), 90)
                imgui.pop_id()
            imgui.end_table()

    delivery_rows = details.get("delivery") or []
    imgui.separator_text("Delivery Events")
    if not delivery_rows:
        imgui.text_disabled("No delivery events saved for this run.")
    elif imgui.begin_table("antidengue_history_delivery", 7):
        for label in ("Recipient", "Role", "Target", "Status", "Cause", "Manual", "Job"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        for index, row in enumerate(delivery_rows):
            imgui.push_id(f"history_delivery_{index}")
            status = str(row.get("status") or "")
            payload = row.get("payload_dict") or {}
            imgui.table_next_row()
            imgui.table_next_column()
            draw_clipped_text(row.get("recipient_name"), 32)
            imgui.table_next_column()
            imgui.text(str(row.get("role") or "-"))
            imgui.table_next_column()
            draw_clipped_text(row.get("target"), 34)
            imgui.table_next_column()
            imgui.text_colored(status_badge_color(status), status or "-")
            imgui.table_next_column()
            draw_clipped_text(row.get("cause"), 46)
            imgui.table_next_column()
            can_copy = (
                status.lower() != "delivered"
                and str(row.get("role") or "").upper() in {"DDEO", "AEO"}
                and bool(payload)
            )
            with disabled_when(not can_copy):
                if imgui.button("Copy", (70, 0)):
                    imgui.set_clipboard_text(build_antidengue_manual_message(payload))
                    state.runner.status_message = (
                        "Copied manual WhatsApp message for "
                        f"{row.get('recipient_name') or row.get('target') or 'recipient'}"
                    )
                    state.runner.append_log(state.runner.status_message)
            imgui.table_next_column()
            imgui.text(str(row.get("job_id") or "-")[:12])
            imgui.pop_id()
        imgui.end_table()
        delivery_total = int(selected.get("delivery_count") or len(delivery_rows))
        if delivery_total > len(delivery_rows):
            imgui.text_disabled(
                f"Showing first {len(delivery_rows)} of {delivery_total} delivery events. Open the delivery audit for the full list."
            )

    dormant_rows = details.get("dormant") or []
    imgui.separator_text("Dormant Schools")
    if not dormant_rows:
        imgui.text_disabled("No dormant school rows saved for this run.")
    elif imgui.begin_table("antidengue_history_dormant", 4):
        for label in ("EMIS", "School", "Tehsil", "Markaz"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        for index, row in enumerate(dormant_rows):
            imgui.push_id(f"history_dormant_{index}")
            imgui.table_next_row()
            imgui.table_next_column()
            imgui.text(str(row.get("school_emis") or "-"))
            imgui.table_next_column()
            draw_clipped_text(row.get("school_name"), 48)
            imgui.table_next_column()
            imgui.text(str(row.get("tehsil") or "-"))
            imgui.table_next_column()
            draw_clipped_text(row.get("markaz"), 42)
            imgui.pop_id()
        imgui.end_table()
        dormant_total = int(selected.get("dormant_count") or len(dormant_rows))
        if dormant_total > len(dormant_rows):
            imgui.text_disabled(
                f"Showing first {len(dormant_rows)} of {dormant_total} dormant schools. Open the output folder for the full report."
            )

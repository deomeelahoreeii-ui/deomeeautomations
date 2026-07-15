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

ANTIDENGUE_VIEWS = (
    ("dispatch", "Dispatch Center"),
    ("health", "Health Stats"),
    ("history", "Run History"),
    ("failed", "Failed Officers"),
    ("groups", "Fixed / Group Delivery"),
    ("manual", "Manual Report"),
    ("master", "Master Data"),
    ("database", "Database"),
    ("files", "Files"),
    ("run", "Run / Schedule"),
)

from complaints_manager.gui.antidengue.delivery import failed_delivery_statuses, failed_officer_rows
from complaints_manager.gui.antidengue.dispatch_views import draw_antidengue_dispatch_center_section
from complaints_manager.gui.antidengue.history_view import draw_antidengue_run_history_section
from complaints_manager.gui.antidengue.runtime import draw_antidengue_worker_controls, latest_antidengue_summary
from complaints_manager.gui.antidengue.schedule import PORTAL_LOGIN_MODES, WEEKDAYS, format_weekdays, next_schedule_run, parse_schedule_times, parse_weekdays
from complaints_manager.gui.antidengue.sections import draw_antidengue_database_section, draw_antidengue_failed_officers_section, draw_antidengue_files_section, draw_antidengue_group_delivery_section, draw_antidengue_health_section, draw_antidengue_manual_report_section, draw_antidengue_master_data_section
from complaints_manager.gui.commands.execution import start_command_with_services
from complaints_manager.gui.common.colors import status_badge_color
from complaints_manager.gui.common.context import disabled_when
from complaints_manager.gui.common.formatting import _option_index

def draw_antidengue_view_button(
    state: GuiState,
    view_id: str,
    label: str,
    current_view: str,
    width: int,
) -> str:
    selected = current_view == view_id
    button_label = f"{label}{' *' if selected else ''}"
    with disabled_when(selected):
        if imgui.button(button_label, (width, 0)):
            state.antidengue_view = view_id
            return view_id
    return current_view

def draw_antidengue_run_status_screen(state: GuiState) -> None:
    summary_path, summary, error = latest_antidengue_summary(state)

    imgui.separator_text("AntiDengue Operations")

    run_dir = summary_path.parent if summary_path else None
    whatsapp = summary.get("whatsapp") or {}
    delivery = whatsapp.get("delivery") or {}
    quality_gate = summary.get("quality_gate") or {}
    filter_info = summary.get("filter") or {}
    outputs = summary.get("outputs") or {}
    audit_rows = whatsapp.get("officer_delivery_audit") or []
    fallback = whatsapp.get("fallback_delivery") or {}
    fallback_delivery = fallback.get("delivery") or {}

    current_view = state.antidengue_view if state.antidengue_view in dict(ANTIDENGUE_VIEWS) else "dispatch"
    failed_count = len(failed_officer_rows(audit_rows))
    group_failure_count = len(
        [
            item
            for item in failed_delivery_statuses(delivery)
            if str(item.get("target") or "").endswith("@g.us")
        ]
    )
    fallback_group_failure_count = len(
        [
            item
            for item in ((fallback.get("delivery") or {}).get("statuses") or {}).values()
            if str(item.get("target") or "").endswith("@g.us")
            and str(item.get("status") or "").lower() != "delivered"
        ]
    )

    draw_antidengue_worker_controls(state)
    imgui.separator()

    nav_items = (
        ("dispatch", "Dispatch Center", 170),
        ("health", "Health Stats", 135),
        ("history", "Run History", 135),
        ("failed", f"Failed Officers ({failed_count})", 180),
        ("groups", f"Fixed / Group ({group_failure_count + fallback_group_failure_count})", 175),
        ("manual", "Manual Report", 150),
        ("master", "Master Data", 135),
        ("database", "Database", 120),
        ("files", "Files", 90),
        ("run", "Run / Schedule", 150),
    )
    for index, (view_id, label, width) in enumerate(nav_items):
        if index:
            imgui.same_line()
        current_view = draw_antidengue_view_button(
            state,
            view_id,
            label,
            current_view,
            width,
        )

    imgui.separator()
    if current_view == "dispatch":
        draw_antidengue_dispatch_center_section(state)
    elif current_view == "health":
        if error:
            imgui.text_colored(status_badge_color("warning"), error)
            imgui.text_disabled("Run History, Database, Manual Report, and Run / Schedule remain available.")
        else:
            draw_antidengue_health_section(
                summary,
                whatsapp,
                delivery,
                quality_gate,
                filter_info,
                fallback,
                fallback_delivery,
            )
    elif current_view == "history":
        draw_antidengue_run_history_section(state)
    elif current_view == "failed":
        draw_antidengue_failed_officers_section(state, audit_rows, delivery)
    elif current_view == "groups":
        draw_antidengue_group_delivery_section(delivery, fallback)
    elif current_view == "manual":
        draw_antidengue_manual_report_section(state)
    elif current_view == "master":
        draw_antidengue_master_data_section(state)
    elif current_view == "database":
        draw_antidengue_database_section(state)
    elif current_view == "files":
        draw_antidengue_files_section(state, summary_path, run_dir, outputs)
    elif current_view == "run":
        draw_antidengue_run_controls(state)

def draw_antidengue_run_controls(state: GuiState) -> None:
    command = command_by_id("antidengue_run")
    values = state.command_values(command)
    values.setdefault("PORTAL_LOGIN_MODE", "manual")

    imgui.separator_text("Run")
    imgui.text_wrapped(command.summary)
    imgui.text("Folder:")
    imgui.same_line()
    imgui.text_disabled(str(command.working_dir(state.project_root)))
    imgui.text("Command:")
    imgui.same_line()
    imgui.text_disabled(" ".join(command.build(state.project_root, values)))

    login_mode = str(values.get("PORTAL_LOGIN_MODE") or "manual")
    if login_mode not in {value for value, _label in PORTAL_LOGIN_MODES}:
        login_mode = "manual"
        values["PORTAL_LOGIN_MODE"] = login_mode

    auto_click = login_mode == "auto"
    changed, auto_click = imgui.checkbox("Auto click Sign In", auto_click)
    if changed:
        values["PORTAL_LOGIN_MODE"] = "auto" if auto_click else "manual"
        state.persist_command_values(command)
        login_mode = values["PORTAL_LOGIN_MODE"]
    imgui.same_line()

    mode_index = _option_index(PORTAL_LOGIN_MODES, login_mode)
    imgui.set_next_item_width(190)
    changed, mode_index = imgui.combo(
        "Login mode",
        mode_index,
        [label for _value, label in PORTAL_LOGIN_MODES],
    )
    if changed:
        values["PORTAL_LOGIN_MODE"] = PORTAL_LOGIN_MODES[mode_index][0]
        state.persist_command_values(command)

    with disabled_when(state.runner.is_running or state.command_start_in_progress):
        if imgui.button("Run Now", (120, 0)):
            start_command_with_services(state, command, values)
    imgui.same_line()
    with disabled_when(not state.runner.is_running):
        if imgui.button("Stop", (90, 0)):
            state.runner.stop()

    imgui.separator_text("Schedule")
    form = state.antidengue_form
    mode_values = ["once", "daily", "weekly"]
    mode_labels = ["Once", "Daily", "Weekly"]
    try:
        mode_index = mode_values.index(form.get("mode", "daily"))
    except ValueError:
        mode_index = 1
    imgui.set_next_item_width(130)
    changed, mode_index = imgui.combo("Mode", mode_index, mode_labels)
    if changed:
        form["mode"] = mode_values[mode_index]
        state.persist_antidengue_form()

    if form.get("mode") == "once":
        imgui.same_line()
        imgui.set_next_item_width(140)
        changed, date_value = imgui.input_text("Date", form.get("date", ""))
        if changed:
            form["date"] = date_value
            state.persist_antidengue_form()
        imgui.same_line()
        if imgui.button("Today", (70, 0)):
            form["date"] = date.today().isoformat()
            state.persist_antidengue_form()

    if form.get("mode") == "weekly":
        selected_days = parse_weekdays(form.get("weekdays", ""))
        for index, label in enumerate(WEEKDAYS):
            if index:
                imgui.same_line()
            checked = index in selected_days
            changed, checked = imgui.checkbox(label, checked)
            if changed:
                if checked:
                    selected_days.add(index)
                else:
                    selected_days.discard(index)
                form["weekdays"] = ",".join(str(day) for day in sorted(selected_days))
                state.persist_antidengue_form()

    imgui.set_next_item_width(360)
    changed, times_value = imgui.input_text(
        "Times", form.get("times", "09:00, 13:00")
    )
    if changed:
        form["times"] = times_value
        state.persist_antidengue_form()
    imgui.same_line()
    if imgui.button("+ Now", (70, 0)):
        current = parse_schedule_times(form.get("times", ""))
        now_time = datetime.now().strftime("%H:%M")
        if now_time not in current:
            current.append(now_time)
        form["times"] = ", ".join(sorted(current))
        state.persist_antidengue_form()
    imgui.same_line()
    with disabled_when(not parse_schedule_times(form.get("times", ""))):
        if imgui.button("Add Schedule", (130, 0)):
            state.add_antidengue_schedule()

    imgui.text_disabled("Use 24-hour HH:MM times separated by commas.")

    schedules = state.antidengue_schedules()
    if imgui.begin_table("antidengue_schedules", 6):
        imgui.table_setup_column("Enabled")
        imgui.table_setup_column("Mode")
        imgui.table_setup_column("Date / Days")
        imgui.table_setup_column("Times")
        imgui.table_setup_column("Next")
        imgui.table_setup_column("Actions")
        imgui.table_headers_row()
        now = datetime.now()
        for schedule in list(schedules):
            imgui.push_id(str(schedule.get("id", "")))
            imgui.table_next_row()

            imgui.table_next_column()
            enabled = bool(schedule.get("enabled", True))
            changed, enabled = imgui.checkbox("##enabled", enabled)
            if changed:
                schedule["enabled"] = enabled
                state.update_antidengue_schedule(schedule)

            imgui.table_next_column()
            imgui.text(str(schedule.get("mode", "")))

            imgui.table_next_column()
            if schedule.get("mode") == "weekly":
                imgui.text(format_weekdays(str(schedule.get("weekdays", ""))))
            elif schedule.get("mode") == "once":
                imgui.text(str(schedule.get("date", "")))
            else:
                imgui.text("Every day")

            imgui.table_next_column()
            imgui.text(", ".join(parse_schedule_times(str(schedule.get("times", "")))))

            imgui.table_next_column()
            imgui.text(next_schedule_run(schedule, now) if enabled else "-")

            imgui.table_next_column()
            with disabled_when(state.runner.is_running or state.command_start_in_progress):
                if imgui.button("Run Now", (80, 0)):
                    start_command_with_services(state, command, values)
            imgui.same_line()
            if imgui.button("Delete", (75, 0)):
                state.remove_antidengue_schedule(str(schedule.get("id", "")))
            imgui.pop_id()
        imgui.end_table()

def draw_antidengue_dashboard(state: GuiState) -> None:
    draw_antidengue_run_status_screen(state)

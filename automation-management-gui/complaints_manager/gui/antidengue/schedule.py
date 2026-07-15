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

WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")

PORTAL_LOGIN_MODES = (
    ("manual", "Manual review"),
    ("auto", "Auto click Sign In"),
    ("remote_approve", "Remote approval"),
)

from complaints_manager.gui.commands.execution import start_command_with_services

def parse_schedule_times(value: str) -> list[str]:
    times: list[str] = []
    for raw in value.replace(";", ",").split(","):
        item = raw.strip()
        if not item:
            continue
        try:
            parsed = datetime.strptime(item, "%H:%M")
        except ValueError:
            continue
        formatted = parsed.strftime("%H:%M")
        if formatted not in times:
            times.append(formatted)
    return sorted(times)

def parse_weekdays(value: str) -> set[int]:
    result = set()
    for raw in value.split(","):
        try:
            day = int(raw.strip())
        except ValueError:
            continue
        if 0 <= day <= 6:
            result.add(day)
    return result

def format_weekdays(value: str) -> str:
    days = parse_weekdays(value)
    if not days:
        return "-"
    return ", ".join(WEEKDAYS[day] for day in sorted(days))

def schedule_matches_now(schedule: dict, now: datetime) -> tuple[bool, str]:
    if not schedule.get("enabled", True):
        return False, ""

    current_time = now.strftime("%H:%M")
    times = parse_schedule_times(str(schedule.get("times", "")))
    if current_time not in times:
        return False, ""

    mode = str(schedule.get("mode", "daily"))
    if mode == "once" and str(schedule.get("date", "")) != now.date().isoformat():
        return False, ""
    if mode == "weekly" and now.weekday() not in parse_weekdays(
        str(schedule.get("weekdays", ""))
    ):
        return False, ""

    return True, f"{now.date().isoformat()} {current_time}"

def next_schedule_run(schedule: dict, now: datetime) -> str:
    times = parse_schedule_times(str(schedule.get("times", "")))
    if not times:
        return "No valid times"

    mode = str(schedule.get("mode", "daily"))
    for day_offset in range(0, 15):
        candidate_date = now.date() + timedelta(days=day_offset)
        if mode == "once" and str(schedule.get("date", "")) != candidate_date.isoformat():
            continue
        if mode == "weekly" and candidate_date.weekday() not in parse_weekdays(
            str(schedule.get("weekdays", ""))
        ):
            continue
        for item in times:
            hour, minute = [int(part) for part in item.split(":", 1)]
            candidate = datetime.combine(
                candidate_date, datetime.min.time()
            ).replace(hour=hour, minute=minute)
            if candidate >= now:
                return candidate.strftime("%Y-%m-%d %H:%M")
    return "-"

def process_antidengue_schedules(state: GuiState) -> None:
    if state.runner.is_running:
        return

    now = datetime.now().replace(second=0, microsecond=0)
    command = command_by_id("antidengue_run")
    for schedule in state.antidengue_schedules():
        due, run_key = schedule_matches_now(schedule, now)
        if not due or str(schedule.get("last_run_key", "")) == run_key:
            continue
        schedule["last_run_key"] = run_key
        if schedule.get("mode") == "once":
            schedule["enabled"] = False
        state.update_antidengue_schedule(schedule)
        start_command_with_services(
            state,
            command,
            state.command_values(command),
            source=f"Scheduled AntiDengue run due: {run_key}",
        )
        break

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

def status_color(state: GuiState) -> tuple[float, float, float, float]:
    if state.runner.is_running:
        return (0.28, 0.64, 0.96, 1.0)
    if state.runner.last_exit_code == 0:
        return (0.26, 0.72, 0.40, 1.0)
    if state.runner.last_exit_code is not None:
        return (0.95, 0.35, 0.28, 1.0)
    return (0.82, 0.82, 0.82, 1.0)

def service_status_color(is_running: bool, exit_code: int | None) -> tuple[float, float, float, float]:
    if is_running:
        return (0.26, 0.72, 0.40, 1.0)
    if exit_code not in (None, 0):
        return (0.95, 0.35, 0.28, 1.0)
    return (0.78, 0.78, 0.78, 1.0)

def sms_campaign_status_color(status: str) -> tuple[float, float, float, float]:
    status = status.strip().lower()
    if status == "verified":
        return (0.26, 0.72, 0.40, 1.0)
    if status in {"running", "submitted"}:
        return (0.28, 0.64, 0.96, 1.0)
    if status == "needs_attention":
        return (0.95, 0.58, 0.22, 1.0)
    if status == "deleted":
        return (0.65, 0.65, 0.65, 1.0)
    return (0.82, 0.82, 0.82, 1.0)

def status_badge_color(status: str) -> tuple[float, float, float, float]:
    normalized = status.strip().lower().replace("_", " ")
    if normalized in {"ok", "passed", "delivered", "queued", "success", "manual sent", "started"}:
        return (0.26, 0.72, 0.40, 1.0)
    if normalized in {
        "warning",
        "partial",
        "pending",
        "copied",
        "manual pending",
        "preview required",
        "sent pending confirmation",
        "missing status",
    }:
        return (0.95, 0.58, 0.22, 1.0)
    if normalized in {"failed", "error", "blocked", "not queued", "skipped"}:
        return (0.95, 0.35, 0.28, 1.0)
    return (0.82, 0.82, 0.82, 1.0)

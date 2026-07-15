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

def format_elapsed(seconds: int) -> str:
    return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"

def parse_datetime_parts(value: str) -> tuple[str, int, int]:
    now = datetime.now()
    cleaned = value.strip().replace("T", " ")
    if cleaned:
        try:
            parsed = datetime.strptime(cleaned, "%Y-%m-%d %H:%M")
            return parsed.date().isoformat(), parsed.hour, parsed.minute
        except ValueError:
            pass
    return "", now.hour, now.minute

def combine_datetime_value(date_value: str, hour: int, minute: int) -> str:
    date_value = date_value.strip()
    if not date_value:
        return ""
    return f"{date_value} {hour:02d}:{minute:02d}"

def _safe_json_loads(value) -> dict:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}

def _option_index(options: tuple[tuple[str, str], ...] | list[tuple[str, str]], value: str) -> int:
    values = [item[0] for item in options]
    try:
        return values.index(value)
    except ValueError:
        return 0

def draw_clipped_text(value: object, max_chars: int = 48) -> None:
    text = str(value or "-")
    normalized = " ".join(part.strip() for part in text.splitlines() if part.strip())
    if not normalized:
        normalized = "-"
    clipped = normalized
    if len(clipped) > max_chars:
        clipped = clipped[: max_chars - 3].rstrip() + "..."
    imgui.text(clipped)
    if clipped != normalized and imgui.is_item_hovered():
        imgui.set_tooltip(normalized[:2000])

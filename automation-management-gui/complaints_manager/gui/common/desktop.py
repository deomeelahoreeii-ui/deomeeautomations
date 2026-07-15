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

def open_folder_from_gui(state: GuiState, command: CommandSpec, value: str) -> None:
    path = resolve_gui_path(state.command_root(command), value)
    path.mkdir(parents=True, exist_ok=True)
    opener = shutil.which("xdg-open")
    if not opener:
        state.runner.append_log(f"Cannot open folder; xdg-open not found: {path}")
        return
    subprocess.Popen(
        [opener, str(path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    state.runner.append_log(f"Opened folder: {path}")

def open_external_file(
    state: GuiState, path: Path, label: str, service_id: str = "whatsapp"
) -> None:
    whatsapp_service = state.services.services.get(service_id)
    if not path.is_file():
        if whatsapp_service:
            whatsapp_service.append_log(f"{label} is not available yet: {path}")
        return
    opener = shutil.which("xdg-open")
    if not opener:
        if whatsapp_service:
            whatsapp_service.append_log(
                f"Cannot open {label}; xdg-open was not found: {path}"
            )
        return
    subprocess.Popen(
        [opener, str(path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

def count_path_items(path: Path) -> int:
    if path.is_dir() and not path.is_symlink():
        return 1 + sum(1 for _item in path.rglob("*"))
    return 1

def open_desktop_path(state: GuiState, path: Path, label: str) -> None:
    if not path.exists():
        state.runner.append_log(f"{label} is not available: {path}")
        return
    opener = shutil.which("xdg-open")
    if not opener:
        state.runner.append_log(f"Cannot open {label}; xdg-open was not found: {path}")
        return
    subprocess.Popen(
        [opener, str(path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

def open_url_from_gui(state: GuiState, url: str) -> None:
    opener = shutil.which("xdg-open")
    if not opener:
        state.runner.append_log(f"Cannot open URL; xdg-open was not found: {url}")
        return
    subprocess.Popen(
        [opener, url],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    state.runner.append_log(f"Opened URL: {url}")

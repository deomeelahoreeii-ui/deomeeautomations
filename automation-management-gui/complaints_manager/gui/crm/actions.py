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

from complaints_manager.gui.common.desktop import count_path_items

def clear_duplicate_filter_results(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
) -> None:
    output_value = values.get("output_dir", "").strip()
    if not output_value:
        state.runner.append_log(f"Cannot clear {command.title} results; output folder is empty.")
        return

    command_root = state.command_root(command)
    output_dir = resolve_gui_path(command_root, output_value)
    protected_paths = {
        Path("/").resolve(strict=False),
        Path.home().resolve(strict=False),
        command_root.resolve(strict=False),
        state.project_root.resolve(strict=False),
    }
    if output_dir in protected_paths:
        state.runner.append_log(f"Refusing to clear protected folder: {output_dir}")
        return

    removed_count = 0
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        for child in output_dir.iterdir():
            removed_count += count_path_items(child)
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
    except OSError as exc:
        state.runner.append_log(f"Failed to clear results folder {output_dir}: {exc}")
        state.runner.status_message = f"Failed: Clear {command.title} results"
        return

    state.runner.append_log(
        f"Cleared results folder for {command.title}: {output_dir} ({removed_count} item(s) removed)"
    )
    state.runner.status_message = f"Finished: Clear {command.title} results"

def copy_excel_filter_values_to_notice_command(
    state: GuiState,
    filter_values: dict[str, str],
    notice_command: CommandSpec,
    status: str,
) -> dict[str, str]:
    notice_values = state.command_values(notice_command)
    notice_values["input_file"] = filter_values.get("input_file", "")
    notice_values["output_dir"] = filter_values.get(
        "output_dir", "phase1-crm/unprocessed-crm/filtered"
    )
    notice_values["status"] = status
    notice_values.setdefault("dispatch_dir", "")
    if notice_command.id == "crm_send_duplicate_notices":
        notice_values.setdefault("dispatch_csv", "")
        notice_values.setdefault("dry_run", "false")
    state.persist_command_values(notice_command)
    return notice_values

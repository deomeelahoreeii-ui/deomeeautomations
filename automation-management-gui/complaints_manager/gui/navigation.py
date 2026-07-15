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

def system_by_id(system_id: str) -> SystemSpec:
    for system in SYSTEMS:
        if system.id == system_id:
            return system
    raise KeyError(system_id)

def draw_top_bar(state: GuiState) -> None:
    imgui.text_colored((0.92, 0.92, 0.92, 1.0), "Automation Management Console")
    imgui.same_line()
    imgui.text_disabled(f"GUI: {state.project_root}")
    imgui.separator()

def draw_metric_row(state: GuiState, system_id: str) -> None:
    metrics = state.metrics(system_id) or []
    if not metrics:
        imgui.text_disabled("No metrics available.")
        return
    if imgui.begin_table(f"{system_id}_metrics", max(1, len(metrics))):
        for metric in metrics:
            imgui.table_setup_column(metric.label)
        imgui.table_headers_row()
        imgui.table_next_row()
        for metric in metrics:
            imgui.table_next_column()
            imgui.text(str(metric.value))
        imgui.end_table()

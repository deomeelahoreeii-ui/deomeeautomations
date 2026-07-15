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

from complaints_manager.gui.common.colors import status_color

def draw_logs(state: GuiState) -> None:
    imgui.separator_text("Activity")
    imgui.text_colored(status_color(state), state.runner.status_message)
    if state.runner.is_running:
        elapsed = state.runner.elapsed_seconds()
        imgui.same_line()
        imgui.text(f"{elapsed // 60:02d}:{elapsed % 60:02d}")

    if imgui.button("Clear", (90, 0)):
        state.runner.clear_log()
    imgui.same_line()
    if imgui.button("Copy", (90, 0)):
        imgui.set_clipboard_text("\n".join(state.runner.log_lines))
    imgui.same_line()
    changed, state.auto_scroll = imgui.checkbox("Auto-scroll", state.auto_scroll)
    imgui.same_line()
    imgui.text_disabled(f"{len(state.runner.log_lines)} lines")

    available = imgui.get_content_region_avail()
    imgui.begin_child("activity_log", (0, max(180, available.y - 8)), True)
    try:
        for line in state.runner.log_lines:
            imgui.text_unformatted(line)
        if state.auto_scroll and state.runner.is_running:
            imgui.set_scroll_here_y(1.0)
    finally:
        imgui.end_child()

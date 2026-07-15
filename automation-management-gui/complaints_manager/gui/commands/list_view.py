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

def draw_command_list(state: GuiState, system_id: str) -> None:
    grouped: dict[str, list[CommandSpec]] = {}
    for command in commands_by_system()[system_id]:
        grouped.setdefault(command.group, []).append(command)

    for group, commands in grouped.items():
        imgui.separator_text(group)
        for command in commands:
            selected = state.selected_command_id == command.id
            if imgui.selectable(command.title, selected)[0]:
                state.selected_command_id = command.id

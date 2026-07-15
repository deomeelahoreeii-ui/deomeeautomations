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

from complaints_manager.gui.commands.validation import command_validation_error

def start_command_with_services(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
    *,
    source: str = "",
) -> bool:
    if state.runner.is_running or state.command_start_in_progress:
        state.runner.append_log(
            f"Cannot start {command.title}; another command is already running."
        )
        return False
    validation_error = command_validation_error(state, command, values)
    if validation_error:
        state.runner.append_log(f"Cannot start {command.title}: {validation_error}")
        state.runner.status_message = f"Failed: {command.title} (validation)"
        return False

    state.persist_command_values(command)
    state.command_start_in_progress = True
    state.runner.status_message = f"Starting: {command.title}"
    state.runner.append_log(f"Preparing {command.title}...")
    values_snapshot = dict(values)
    thread = threading.Thread(
        target=_start_command_with_services_worker,
        args=(state, command, values_snapshot, source),
        daemon=True,
    )
    thread.start()
    return True

def _start_command_with_services_worker(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
    source: str,
) -> None:
    started_services = False
    try:
        for service_id in command.required_services:
            service = state.services.services.get(service_id)
            if service is None:
                continue
            ready, detail = state.services.readiness(service_id)
            if not ready and not service.is_running:
                state.runner.append_log(
                    f"Starting required service for {command.title}: {service.spec.title}"
                )
                state.services.start(service_id)
                started_services = True
            timeout_seconds = 120.0 if service.spec.kind == "whatsapp" else 15.0
            state.runner.append_log(
                f"Waiting for required service: {service.spec.title}"
            )
            ready, detail = state.services.wait_until_ready(
                service_id,
                timeout_seconds=timeout_seconds,
            )
            if not ready:
                state.runner.append_log(
                    f"Cannot start {command.title}; required service is not ready: "
                    f"{service.spec.title} ({detail})"
                )
                state.runner.status_message = f"Failed: {command.title} (service not ready)"
                return

        if started_services:
            state.services.drain_output()

        if source:
            state.runner.append_log(source)
        if command.id in {"sms_campaign_create", "sms_campaign_update", "sms_campaign_delete"}:
            state.sms_campaign_cache_at = 0.0
            state.sms_campaign_cache = []
        state.runner.start(command, values)
    except Exception as exc:
        state.runner.status_message = f"Failed: {command.title} (startup error)"
        state.runner.append_log(f"Cannot start {command.title}: {exc}")
    finally:
        state.command_start_in_progress = False

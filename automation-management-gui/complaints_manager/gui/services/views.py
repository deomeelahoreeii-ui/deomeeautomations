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

from complaints_manager.gui.common.colors import service_status_color
from complaints_manager.gui.common.context import disabled_when
from complaints_manager.gui.common.desktop import open_external_file
from complaints_manager.gui.common.formatting import format_elapsed
from complaints_manager.gui.whatsapp.accounts import draw_whatsapp_reset_modal

def draw_services_panel(state: GuiState) -> None:
    imgui.separator_text("Infrastructure Services")
    imgui.text_wrapped(
        "Manage NATS, WhatsApp workers, and PocketBase here. WhatsApp account creation and QR login are handled from WhatsApp Accounts."
    )
    if imgui.begin_table("service_dashboard", 6):
        imgui.table_setup_column("Service")
        imgui.table_setup_column("Type")
        imgui.table_setup_column("Status")
        imgui.table_setup_column("Uptime")
        imgui.table_setup_column("Controls")
        imgui.table_setup_column("Logs")
        imgui.table_headers_row()

        for service in state.services.services.values():
            imgui.push_id(service.spec.id)
            imgui.table_next_row()

            imgui.table_next_column()
            imgui.text(service.spec.title)
            imgui.text_disabled(service.spec.summary)

            imgui.table_next_column()
            if service.spec.kind == "whatsapp":
                imgui.text_colored((0.18, 0.78, 0.48, 1.0), "WhatsApp")
                imgui.text_disabled(service.spec.profile_id or "default")
            elif service.spec.id == "nats":
                imgui.text_colored((0.72, 0.62, 0.95, 1.0), "NATS")
            elif service.spec.kind == "database":
                imgui.text_colored((0.32, 0.70, 0.92, 1.0), "Database")
            else:
                imgui.text_disabled(service.spec.kind)

            imgui.table_next_column()
            imgui.text_colored(
                service_status_color(service.is_running, service.last_exit_code),
                service.status_message,
            )

            imgui.table_next_column()
            imgui.text(format_elapsed(service.elapsed_seconds()) if service.is_running else "-")

            imgui.table_next_column()
            with disabled_when(service.is_running):
                if imgui.button("Start", (72, 0)):
                    state.services.start(service.spec.id)
            imgui.same_line()
            with disabled_when(not service.is_running):
                if imgui.button("Stop", (72, 0)):
                    state.services.stop(service.spec.id)
            imgui.same_line()
            if imgui.button("Restart", (82, 0)):
                state.services.restart(service.spec.id)

            imgui.table_next_column()
            if imgui.button("Details", (88, 0)):
                state.current_service_id = service.spec.id

            imgui.pop_id()

        imgui.end_table()

    imgui.text_disabled("Tip: open Details for full logs, WhatsApp QR state, auth folder, and NATS subjects.")

def draw_service_detail(state: GuiState, service_id: str) -> None:
    service = state.services.services[service_id]
    imgui.spacing()
    imgui.text(service.spec.title)
    imgui.text_wrapped(service.spec.summary)
    imgui.spacing()

    imgui.text("Status:")
    imgui.same_line()
    imgui.text_colored(
        service_status_color(service.is_running, service.last_exit_code),
        service.status_message,
    )
    if service.is_running:
        imgui.same_line()
        imgui.text_disabled(format_elapsed(service.elapsed_seconds()))

    imgui.text("Folder:")
    imgui.same_line()
    imgui.text_wrapped(str(service.spec.cwd))
    imgui.text("Command:")
    imgui.same_line()
    imgui.text_wrapped(" ".join(service.spec.command))

    with disabled_when(service.is_running):
        if imgui.button("Start", (90, 0)):
            state.services.start(service_id)
    imgui.same_line()
    with disabled_when(not service.is_running):
        if imgui.button("Stop", (90, 0)):
            state.services.stop(service_id)
    imgui.same_line()
    if imgui.button("Restart", (100, 0)):
        state.services.restart(service_id)
    imgui.same_line()
    if imgui.button("Clear Log", (100, 0)):
        state.services.clear_log(service_id)
    imgui.same_line()
    changed, state.service_auto_scroll = imgui.checkbox(
        "Auto-scroll", state.service_auto_scroll
    )

    if service.spec.kind == "whatsapp":
        service_env = dict(service.spec.env)
        imgui.text("Profile ID:")
        imgui.same_line()
        imgui.text_disabled(service.spec.profile_id)
        imgui.text("Auth folder:")
        imgui.same_line()
        imgui.text_disabled(service_env.get("WA_AUTH_DIR", "auth_info_baileys"))
        imgui.text("Health subject:")
        imgui.same_line()
        imgui.text_disabled(
            service_env.get("NATS_HEALTH_SUBJECT", "whatsapp.worker.health")
        )
        qr_path = service.spec.cwd / service_env.get(
            "WA_QR_IMAGE_PATH", "data/whatsapp-login-qr.png"
        )
        imgui.separator_text("WhatsApp Login")
        if qr_path.is_file():
            imgui.text_colored(
                (0.28, 0.85, 0.48, 1.0),
                "QR code ready - open it and scan with WhatsApp Linked Devices.",
            )
            if imgui.button("Open QR Code", (140, 0)):
                open_external_file(state, qr_path, "WhatsApp QR code", service_id)
            imgui.same_line()
            imgui.text_disabled(str(qr_path))
        else:
            imgui.text_disabled(
                "No QR image is pending. It appears here when WhatsApp needs authentication."
            )
        with disabled_when(service.is_running):
            if imgui.button("Reset Login & Create QR", (190, 0)):
                state.whatsapp_auth_reset_service_id = service_id
                state.show_whatsapp_auth_reset_confirm = True
        imgui.same_line()
        imgui.text_disabled("Manual only; existing credentials are archived, never deleted.")

        draw_whatsapp_reset_modal(state)

    imgui.separator_text("Service Log")
    available = imgui.get_content_region_avail()
    imgui.begin_child(f"{service_id}_service_log", (0, max(260, available.y - 8)), True)
    try:
        for line in service.log_lines:
            imgui.text_unformatted(line)
        if state.service_auto_scroll and service.is_running:
            imgui.set_scroll_here_y(1.0)
    finally:
        imgui.end_child()

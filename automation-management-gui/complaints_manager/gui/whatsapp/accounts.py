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

def reset_whatsapp_authentication(state: GuiState, service_id: str) -> None:
    service = state.services.services[service_id]
    if service.is_running:
        service.append_log("Stop the WhatsApp Worker before resetting its login.")
        return
    service_env = dict(service.spec.env)
    auth_dir = service.spec.cwd / service_env.get("WA_AUTH_DIR", "auth_info_baileys")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_dir = auth_dir.with_name(f"{auth_dir.name}.manual-backup-{timestamp}")
    try:
        if auth_dir.exists():
            auth_dir.rename(backup_dir)
        auth_dir.mkdir(parents=True, exist_ok=True)
        qr_path = service.spec.cwd / service_env.get(
            "WA_QR_IMAGE_PATH", "data/whatsapp-login-qr.png"
        )
        qr_path.unlink(missing_ok=True)
    except OSError as exc:
        service.append_log(f"Could not reset WhatsApp login: {exc}")
        service.status_message = "Login reset failed"
        return
    service.append_log(f"Previous WhatsApp login archived safely: {backup_dir}")
    service.append_log("Starting a clean login; wait for the Open QR Code button.")
    state.services.start(service_id)

def whatsapp_service_id(profile_id: str) -> str:
    return "whatsapp" if profile_id == "default" else f"whatsapp_{profile_id}"

def whatsapp_service_for_account(state: GuiState, profile_id: str):
    return state.services.services.get(whatsapp_service_id(profile_id))

def whatsapp_qr_path(service) -> Path | None:
    if service is None:
        return None
    service_env = dict(service.spec.env)
    return service.spec.cwd / service_env.get("WA_QR_IMAGE_PATH", "data/whatsapp-login-qr.png")

def draw_whatsapp_accounts_dashboard(state: GuiState, command: CommandSpec) -> None:
    imgui.text(command.title)
    imgui.text_wrapped(
        "Manage isolated WhatsApp accounts. Each profile has its own auth folder, lock file, queue subjects, QR login, and worker service."
    )

    accounts = state.services.whatsapp_accounts()
    active_accounts = [account for account in accounts if bool(account.get("enabled", True))]
    running_count = sum(
        1
        for account in accounts
        if (service := whatsapp_service_for_account(state, str(account.get("id", "")))) and service.is_running
    )

    if imgui.begin_table("whatsapp_accounts_metrics", 3):
        for label in ("Accounts", "Enabled", "Running"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        imgui.table_next_row()
        for value in (len(accounts), len(active_accounts), running_count):
            imgui.table_next_column()
            imgui.text(str(value))
        imgui.end_table()

    imgui.separator_text("Accounts")
    if imgui.button("Add New Account", (170, 0)):
        state.new_whatsapp_account_id = ""
        state.new_whatsapp_account_name = ""
        state.whatsapp_account_setup_service_id = ""
        state.selected_command_id = "whatsapp_account_add"
    imgui.same_line()
    if imgui.button("Refresh Accounts", (150, 0)):
        state.services.reload_specs()
        state.runner.append_log("WhatsApp account registry refreshed.")
    imgui.text_disabled("Each account is isolated. Removing a profile preserves its credential folder on disk.")

    for account in accounts:
        draw_whatsapp_account_card(state, account)

    draw_whatsapp_reset_modal(state)

def draw_whatsapp_account_card(state: GuiState, account: dict) -> None:
    profile_id = str(account.get("id", "")).strip()
    if not profile_id:
        return
    service = whatsapp_service_for_account(state, profile_id)
    enabled = bool(account.get("enabled", True))
    auth_dir = str(account.get("auth_dir") or f"auth_info_baileys_{profile_id}")
    service_env = dict(service.spec.env) if service else {}
    qr_path = whatsapp_qr_path(service)
    title = str(account.get("name") or profile_id)

    imgui.push_id(profile_id)
    imgui.separator_text(title)
    imgui.text_disabled(f"Account ID: {profile_id}")

    if imgui.begin_table("account_card_details", 3):
        for label in ("Service", "Login", "Queue subjects"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        imgui.table_next_row()

        imgui.table_next_column()
        if service:
            imgui.text_colored(service_status_color(service.is_running, service.last_exit_code), service.status_message)
            imgui.text_disabled(f"Uptime: {format_elapsed(service.elapsed_seconds()) if service.is_running else '-'}")
        else:
            imgui.text_colored((0.95, 0.35, 0.28, 1.0), "No service registered")

        imgui.table_next_column()
        imgui.text_disabled(auth_dir)
        if qr_path and qr_path.is_file():
            imgui.text_colored((0.28, 0.85, 0.48, 1.0), "QR ready to scan")
        else:
            imgui.text_disabled("No QR pending")

        imgui.table_next_column()
        imgui.text_disabled(service_env.get("NATS_HEALTH_SUBJECT", "whatsapp.worker.health") if service else "-")
        imgui.text_disabled(service_env.get("NATS_GROUP_MEMBERS_SUBJECT", "whatsapp.worker.group-members") if service else "-")
        imgui.end_table()

    changed, new_enabled = imgui.checkbox("Enabled", enabled)
    if changed:
        ok, message = state.services.update_whatsapp_account(profile_id, enabled=new_enabled)
        state.runner.append_log(message)

    imgui.separator_text("Actions")
    with disabled_when(not service or service.is_running or not enabled):
        if imgui.button("Start Worker", (125, 0)) and service:
            state.services.start(service.spec.id)
    imgui.same_line()
    with disabled_when(not service or not service.is_running):
        if imgui.button("Stop Worker", (120, 0)) and service:
            state.services.stop(service.spec.id)
    imgui.same_line()
    with disabled_when(not service):
        if imgui.button("Restart", (90, 0)) and service:
            state.services.restart(service.spec.id)
    imgui.same_line()
    with disabled_when(not service):
        if imgui.button("Service Details", (135, 0)) and service:
            state.current_service_id = service.spec.id

    with disabled_when(not qr_path or not qr_path.is_file()):
        if imgui.button("Open QR Image", (135, 0)) and qr_path and service:
            open_external_file(state, qr_path, "WhatsApp QR code", service.spec.id)
    imgui.same_line()
    with disabled_when(not service or service.is_running):
        if imgui.button("Reset Login / New QR", (165, 0)) and service:
            state.whatsapp_auth_reset_service_id = service.spec.id
            state.show_whatsapp_auth_reset_confirm = True
    imgui.same_line()
    with disabled_when(profile_id == "default" or (service is not None and service.is_running)):
        if imgui.button("Remove Profile", (135, 0)):
            ok, message = state.services.remove_whatsapp_account(profile_id)
            state.runner.append_log(message)

    if state.edit_whatsapp_account_id == profile_id:
        imgui.set_next_item_width(320)
        changed, edit_name = imgui.input_text("Display name", state.edit_whatsapp_account_name)
        if changed:
            state.edit_whatsapp_account_name = edit_name
        if imgui.button("Save Name", (105, 0)):
            ok, message = state.services.update_whatsapp_account(profile_id, name=state.edit_whatsapp_account_name)
            state.runner.append_log(message)
            state.edit_whatsapp_account_id = ""
            state.edit_whatsapp_account_name = ""
        imgui.same_line()
        if imgui.button("Cancel Rename", (130, 0)):
            state.edit_whatsapp_account_id = ""
            state.edit_whatsapp_account_name = ""
    else:
        if imgui.button("Rename Account", (140, 0)):
            state.edit_whatsapp_account_id = profile_id
            state.edit_whatsapp_account_name = title

    imgui.pop_id()

def draw_whatsapp_account_add_screen(state: GuiState, command: CommandSpec) -> None:
    imgui.text(command.title)
    imgui.text_wrapped(
        "Create a new isolated WhatsApp profile, start its worker, then scan the generated QR code from WhatsApp on your phone. "
        "Once linked, the credentials are stored in that account's own auth folder and reused on restart."
    )

    imgui.separator_text("New account")
    imgui.set_next_item_width(260)
    changed, account_id = imgui.input_text("Account ID", state.new_whatsapp_account_id)
    if changed:
        state.new_whatsapp_account_id = account_id
    imgui.same_line()
    normalized = normalize_whatsapp_account_id(state.new_whatsapp_account_id)
    imgui.text_disabled(f"Will save as: {normalized or '-'}")

    imgui.set_next_item_width(420)
    changed, account_name = imgui.input_text("Display name", state.new_whatsapp_account_name)
    if changed:
        state.new_whatsapp_account_name = account_name
    imgui.text_disabled("Use a short stable ID like office-2, deo-main, ops-heads. Do not use spaces; they are converted to dashes.")

    existing_ids = {str(account.get("id")) for account in state.services.whatsapp_accounts()}
    cannot_create = not normalized or normalized == "default" or normalized in existing_ids
    if normalized in existing_ids:
        imgui.text_colored((0.95, 0.45, 0.25, 1.0), "That account ID already exists.")

    with disabled_when(cannot_create):
        if imgui.button("Create Account & Start QR Login", (260, 0)):
            added, message = state.services.add_whatsapp_account(state.new_whatsapp_account_id, state.new_whatsapp_account_name)
            state.runner.append_log(message)
            if added:
                service_id = whatsapp_service_id(normalized)
                state.whatsapp_account_setup_service_id = service_id
                state.services.start(service_id)
                state.new_whatsapp_account_id = ""
                state.new_whatsapp_account_name = ""

    imgui.same_line()
    if imgui.button("Back to Accounts", (150, 0)):
        state.selected_command_id = "whatsapp_accounts"

    service = state.services.services.get(state.whatsapp_account_setup_service_id)
    if not service:
        return

    imgui.separator_text("QR login setup")
    imgui.text(service.spec.title)
    imgui.text_colored(service_status_color(service.is_running, service.last_exit_code), service.status_message)
    imgui.text_disabled("Wait for the worker log to say the QR code is ready, then open and scan it.")
    qr_path = whatsapp_qr_path(service)
    if qr_path and qr_path.is_file():
        imgui.text_colored((0.28, 0.85, 0.48, 1.0), f"QR image ready: {qr_path}")
        if imgui.button("Open QR Image", (140, 0)):
            open_external_file(state, qr_path, "WhatsApp QR code", service.spec.id)
    else:
        imgui.text_disabled("QR image is not ready yet. Keep this screen open for a few seconds, or open Service Details to watch logs.")
    imgui.same_line()
    if imgui.button("Service Details", (135, 0)):
        state.current_service_id = service.spec.id
    imgui.same_line()
    with disabled_when(not service.is_running):
        if imgui.button("Stop Worker", (110, 0)):
            state.services.stop(service.spec.id)

def draw_whatsapp_reset_modal(state: GuiState) -> None:
    if state.show_whatsapp_auth_reset_confirm:
        imgui.open_popup("Confirm WhatsApp login reset")
    opened = imgui.begin_popup_modal("Confirm WhatsApp login reset", True)
    if not opened[0]:
        return
    service_id = state.whatsapp_auth_reset_service_id or "whatsapp"
    service = state.services.services.get(service_id)
    imgui.text("Reset WhatsApp Login")
    if service:
        imgui.text_disabled(service.spec.title)
    imgui.text_wrapped(
        "Use this only when WhatsApp rejects the preserved session. "
        "The current credentials will be archived and a new QR login will start."
    )
    with disabled_when(service is None or service.is_running):
        if imgui.button("Archive and Reset", (150, 0)):
            state.show_whatsapp_auth_reset_confirm = False
            reset_whatsapp_authentication(state, service_id)
            imgui.close_current_popup()
    imgui.same_line()
    if imgui.button("Cancel", (100, 0)):
        state.show_whatsapp_auth_reset_confirm = False
        state.whatsapp_auth_reset_service_id = ""
        imgui.close_current_popup()
    imgui.end_popup()

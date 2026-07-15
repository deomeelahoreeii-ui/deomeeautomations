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

from complaints_manager.gui.common.colors import status_badge_color

def antidengue_root(state: GuiState) -> Path:
    root = state.project_root.resolve(strict=False)
    for candidate in [root, *root.parents][:6]:
        if (candidate / "antidengue").exists():
            return (candidate / "antidengue").resolve(strict=False)
    return (root.parent / "antidengue").resolve(strict=False)

def antidengue_pocketbase_root(state: GuiState) -> Path:
    root = state.project_root.resolve(strict=False)
    for candidate in [root, *root.parents][:6]:
        if (candidate / "antidengue-pocketbase").exists():
            return (candidate / "antidengue-pocketbase").resolve(strict=False)
    return (root.parent / "antidengue-pocketbase").resolve(strict=False)

def whatsappbot_root(state: GuiState) -> Path:
    root = state.project_root.resolve(strict=False)
    for candidate in [root, *root.parents][:6]:
        if (candidate / "whatsappbot").exists():
            return (candidate / "whatsappbot").resolve(strict=False)
    return (root.parent / "whatsappbot").resolve(strict=False)

def whatsapp_auth_dir(state: GuiState) -> Path:
    return (whatsappbot_root(state) / "auth_info_baileys").resolve(strict=False)

def latest_antidengue_summary(state: GuiState) -> tuple[Path | None, dict, str]:
    output_root = antidengue_root(state) / "output-files"
    summaries = sorted(
        output_root.glob("*/run_summary.json"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0,
        reverse=True,
    )
    if not summaries:
        return None, {}, "No AntiDengue run summary found yet."

    summary_path = summaries[0]
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return summary_path, {}, f"Could not read latest summary: {exc}"
    return summary_path, payload if isinstance(payload, dict) else {}, ""

def read_json_file(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

def bailey_token_data_length(value) -> int:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, str):
        return len(value)
    return 0

def is_baileys_tctoken_expired(timestamp: object) -> bool:
    try:
        parsed = int(str(timestamp))
    except (TypeError, ValueError):
        return True
    bucket_duration = 604800
    bucket_count = 4
    current_bucket = int(time.time()) // bucket_duration
    cutoff_timestamp = (current_bucket - (bucket_count - 1)) * bucket_duration
    return parsed < cutoff_timestamp

def lid_for_phone(auth_dir: Path, phone: str) -> str:
    cleaned_phone = "".join(char for char in str(phone) if char.isdigit())
    if not cleaned_phone:
        return ""

    direct = read_json_file(auth_dir / f"lid-mapping-{cleaned_phone}.json")
    if isinstance(direct, str) and direct.endswith("@lid"):
        return direct

    try:
        reverse_files = list(auth_dir.glob("lid-mapping-*_reverse.json"))
    except OSError:
        return ""

    for path in reverse_files:
        mapped_phone = "".join(char for char in str(read_json_file(path) or "") if char.isdigit())
        if mapped_phone != cleaned_phone:
            continue
        lid_user = path.name.removeprefix("lid-mapping-").removesuffix("_reverse.json")
        return f"{lid_user}@lid" if lid_user else ""
    return ""

def antidengue_token_status_for_mobile(state: GuiState, mobile: object) -> dict[str, str]:
    phone = "".join(char for char in str(mobile or "") if char.isdigit())
    if not phone:
        return {"status": "unknown", "label": "no mobile", "lid": "", "detail": ""}

    cached = state.antidengue_token_cache.get(phone)
    if cached:
        return cached

    auth_dir = whatsapp_auth_dir(state)
    lid = lid_for_phone(auth_dir, phone)
    if not lid:
        result = {"status": "missing", "label": "no LID", "lid": "", "detail": "No Baileys LID mapping found."}
        state.antidengue_token_cache[phone] = result
        return result

    token_path = auth_dir / f"tctoken-{lid}.json"
    entry = read_json_file(token_path) or {}
    token_length = bailey_token_data_length((entry.get("token") or {}).get("data"))
    timestamp = entry.get("timestamp") or ""
    sender_timestamp = entry.get("senderTimestamp") or ""

    if token_length <= 0:
        result = {
            "status": "missing",
            "label": "missing",
            "lid": lid,
            "detail": f"No usable token stored. senderTimestamp={sender_timestamp or '-'}",
        }
    elif is_baileys_tctoken_expired(timestamp):
        result = {
            "status": "expired",
            "label": "expired",
            "lid": lid,
            "detail": f"Token exists but expired. timestamp={timestamp}",
        }
    else:
        result = {
            "status": "usable",
            "label": "usable",
            "lid": lid,
            "detail": f"Token is usable. timestamp={timestamp}",
        }

    state.antidengue_token_cache[phone] = result
    return result

def refresh_antidengue_token_status(state: GuiState) -> None:
    state.antidengue_token_cache = {}
    state.antidengue_token_cache_at = time.time()
    state.runner.status_message = "Refreshed AntiDengue WhatsApp token status"
    state.runner.append_log(state.runner.status_message)

def start_or_check_antidengue_worker(state: GuiState) -> None:
    nats = state.services.services.get("nats")
    whatsapp = state.services.services.get("whatsapp")

    if nats and not nats.is_running:
        state.services.start("nats")
    if whatsapp and not whatsapp.is_running:
        state.services.start("whatsapp")

    state.services.drain_output()
    refresh_antidengue_token_status(state)

    if whatsapp:
        ready, detail = state.services.readiness("whatsapp")
    else:
        ready, detail = False, ""

    if whatsapp and ready:
        state.runner.status_message = "Primary WhatsApp worker is ready for AntiDengue"
    elif whatsapp:
        state.runner.status_message = f"Primary WhatsApp worker is not ready: {detail or whatsapp.status_message}"
    else:
        state.runner.status_message = "Primary WhatsApp worker service is not configured"
    state.runner.append_log(state.runner.status_message)

def start_or_check_control_api(state: GuiState) -> None:
    service = state.services.services.get("automation_control_api")
    if service and not service.is_running:
        state.services.start("automation_control_api")
    state.services.drain_output()

    if is_control_api_reachable():
        state.runner.status_message = "Automation Control API is running on http://127.0.0.1:8787"
    elif service:
        state.runner.status_message = f"Automation Control API is {service.status_message}"
    else:
        state.runner.status_message = "Automation Control API service is not configured"
    state.runner.append_log(state.runner.status_message)

def is_control_api_reachable() -> bool:
    try:
        with urllib.request.urlopen("http://127.0.0.1:8787/health", timeout=0.35) as response:
            return 200 <= response.status < 500
    except Exception:
        return False

def draw_antidengue_worker_controls(state: GuiState) -> None:
    nats = state.services.services.get("nats")
    whatsapp = state.services.services.get("whatsapp")
    control_api = state.services.services.get("automation_control_api")
    nats_ready, _nats_detail = (
        state.services.readiness("nats") if nats else (False, "")
    )
    whatsapp_ready, whatsapp_detail = (
        state.services.readiness("whatsapp")
        if whatsapp and nats_ready
        else (False, "")
    )
    whatsapp_running = bool(whatsapp and whatsapp.is_running)
    control_api_running = bool(control_api and control_api.is_running) or is_control_api_reachable()

    imgui.text("Worker:")
    imgui.same_line()
    whatsapp_label = "Primary WhatsApp ready"
    whatsapp_status = "ok"
    if not whatsapp_ready:
        whatsapp_label = (
            "Primary WhatsApp starting"
            if whatsapp_running
            else "Primary WhatsApp stopped"
        )
        whatsapp_status = "warning" if whatsapp_running else "failed"
    imgui.text_colored(
        status_badge_color(whatsapp_status),
        whatsapp_label,
    )
    if whatsapp_running and not whatsapp_ready and whatsapp_detail:
        imgui.same_line()
        imgui.text_disabled(whatsapp_detail)
    imgui.same_line()
    imgui.text_disabled("NATS ready" if nats_ready else "NATS stopped")

    if imgui.button("Start / Check WhatsApp Worker", (230, 0)):
        start_or_check_antidengue_worker(state)
    imgui.same_line()
    if imgui.button("Refresh Tokens", (125, 0)):
        refresh_antidengue_token_status(state)
    if state.antidengue_token_cache_at:
        imgui.same_line()
        checked_at = datetime.fromtimestamp(state.antidengue_token_cache_at).strftime("%H:%M:%S")
        imgui.text_disabled(f"checked {checked_at}")

    imgui.text("Control API:")
    imgui.same_line()
    imgui.text_colored(
        status_badge_color("ok" if control_api_running else "failed"),
        "running" if control_api_running else "stopped",
    )
    imgui.same_line()
    imgui.text_disabled("http://127.0.0.1:8787")
    imgui.same_line()
    if imgui.button("Start / Check Control API", (205, 0)):
        start_or_check_control_api(state)

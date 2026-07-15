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

from complaints_manager.gui.common.colors import sms_campaign_status_color

def apply_sms_campaign_to_command(
    state: GuiState,
    command_id: str,
    campaign: dict[str, str],
) -> dict[str, str]:
    command = command_by_id(command_id)
    values = state.command_values(command)
    values["campaign_id"] = campaign.get("campaign_name", "")
    if command_id in {"sms_campaign_update", "sms_campaign_send"}:
        values["file"] = campaign.get("source_file", values.get("file", ""))
        values["sheet"] = campaign.get("sheet", values.get("sheet", ""))
        values["column"] = campaign.get("number_column", values.get("column", ""))
        values["message"] = campaign.get("message", values.get("message", ""))
        values["description"] = campaign.get("description", values.get("description", ""))
    state.persist_command_values(command)
    return values

def sms_campaign_database_path(command_root: Path) -> Path:
    env_path = command_root / ".env"
    database = "data/sms-campaigns.sqlite3"
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith("SMS_GATE_DATABASE="):
                database = stripped.split("=", 1)[1].strip().strip("\"'")
                break
    except OSError:
        pass
    path = Path(database).expanduser()
    if not path.is_absolute():
        path = command_root / path
    return path

def discovered_sms_campaigns(state: GuiState, command: CommandSpec, *, force: bool = False) -> list[dict[str, str]]:
    database = sms_campaign_database_path(state.command_root(command))
    cache_key = str(database)
    now = time.monotonic()
    if (
        not force
        and state.sms_campaign_cache_path == cache_key
        and now - state.sms_campaign_cache_at < 5
    ):
        return state.sms_campaign_cache
    if not database.is_file():
        state.sms_campaign_cache = []
        state.sms_campaign_cache_path = cache_key
        state.sms_campaign_cache_at = now
        return []
    try:
        uri = f"file:{database}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=2) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """SELECT c.campaign_name,c.source_file,c.sheet,c.number_column,c.message,c.description,c.status,
                          COALESCE(c.updated_at,c.created_at) AS updated_at,
                          COUNT(r.id) AS recipients,
                          SUM(CASE WHEN r.status='pending' THEN 1 ELSE 0 END) AS pending,
                          SUM(CASE WHEN r.status='sent' THEN 1 ELSE 0 END) AS sent,
                          SUM(CASE WHEN r.status='delivered' THEN 1 ELSE 0 END) AS delivered,
                          SUM(CASE WHEN r.status='failed' THEN 1 ELSE 0 END) AS failed,
                          SUM(CASE WHEN r.status='unknown' THEN 1 ELSE 0 END) AS unknown
                   FROM campaigns c LEFT JOIN recipients r ON r.campaign_id=c.id
                   WHERE c.campaign_name IS NOT NULL AND c.campaign_name<>'' AND c.status<>'deleted'
                   GROUP BY c.id
                   ORDER BY COALESCE(c.updated_at,c.created_at) DESC,c.id DESC"""
            ).fetchall()
    except sqlite3.Error as exc:
        try:
            output = subprocess.check_output(
                [shutil.which("uv") or "uv", "run", "sms-campaign", "campaigns", "list", "--json"],
                cwd=state.command_root(command),
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            payload = json.loads(output)
            if isinstance(payload, list):
                campaigns = [
                    {str(key): "" if value is None else str(value) for key, value in item.items()}
                    for item in payload
                    if isinstance(item, dict)
                ]
                state.sms_campaign_cache = campaigns
                state.sms_campaign_cache_path = cache_key
                state.sms_campaign_cache_at = now
                state.sms_campaign_last_error = ""
                return campaigns
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            pass
        message = f"Could not read SMS campaign registry {database}: {exc}"
        if state.sms_campaign_last_error != message:
            state.runner.append_log(message)
            state.sms_campaign_last_error = message
        state.sms_campaign_cache_path = cache_key
        state.sms_campaign_cache_at = now
        return state.sms_campaign_cache if state.sms_campaign_cache_path == cache_key else []
    campaigns = [{key: "" if row[key] is None else str(row[key]) for key in row.keys()} for row in rows]
    state.sms_campaign_cache = campaigns
    state.sms_campaign_cache_path = cache_key
    state.sms_campaign_cache_at = now
    state.sms_campaign_last_error = ""
    return campaigns

def draw_sms_campaign_selector(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
    field: ArgField,
) -> None:
    campaigns = discovered_sms_campaigns(state, command)
    current = values.get(field.key, "").strip()
    ids = [""] + [item["campaign_name"] for item in campaigns]
    labels = ["Select saved campaign..."] + [
        (
            f"{item['campaign_name']}  |  {item.get('status','-')}  |  recipients={item.get('recipients','0')} "
            f"pending={item.get('pending','0')} sent={item.get('sent','0')} "
            f"delivered={item.get('delivered','0')} failed={item.get('failed','0')}"
        )
        for item in campaigns
    ]
    try:
        index = ids.index(current)
    except ValueError:
        index = 0

    imgui.set_next_item_width(760)
    changed, index = imgui.combo(field.label, index, labels)
    if changed:
        values[field.key] = ids[index]
        if index > 0 and command.id in {"sms_campaign_send", "sms_campaign_update"}:
            selected = campaigns[index - 1]
            values["file"] = selected.get("source_file", values.get("file", ""))
            values["sheet"] = selected.get("sheet", values.get("sheet", ""))
            values["column"] = selected.get("number_column", values.get("column", ""))
            values["message"] = selected.get("message", values.get("message", ""))
            values["description"] = selected.get("description", values.get("description", ""))
        state.persist_command_values(command)

    imgui.same_line()
    if imgui.button(f"Refresh##{command.id}_{field.key}", (85, 0)):
        campaigns = discovered_sms_campaigns(state, command, force=True)
        state.runner.append_log(f"Refreshed SMS campaigns: {len(campaigns)} saved")

    imgui.set_next_item_width(420)
    changed, manual = imgui.input_text(f"Manual campaign name##{command.id}_{field.key}", current)
    if changed:
        values[field.key] = manual
        state.persist_command_values(command)

    if campaigns:
        latest = campaigns[0]
        imgui.text_disabled(
            f"Latest: {latest['campaign_name']} | {latest.get('status','-')} | recipients={latest.get('recipients','0')} | updated={latest.get('updated_at','')}"
        )
    else:
        imgui.text_disabled("No saved SMS campaigns yet. Use Create SMS Campaign first.")

def draw_sms_campaign_list_dashboard(state: GuiState, command: CommandSpec) -> None:
    imgui.text(command.title)
    imgui.text_wrapped("Manage saved SMS campaigns. Select a row action to edit, send, inspect failures, or archive a campaign while preserving audit history.")

    campaigns = discovered_sms_campaigns(state, command)
    total = len(campaigns)
    needs_attention = sum(1 for item in campaigns if item.get("status") == "needs_attention")
    running = sum(1 for item in campaigns if item.get("status") in {"running", "submitted"})
    verified = sum(1 for item in campaigns if item.get("status") == "verified")

    if imgui.begin_table("sms_campaign_overview_metrics", 4):
        for label in ("Campaigns", "Needs attention", "Active/submitted", "Verified"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        imgui.table_next_row()
        for value, color in (
            (str(total), (0.82, 0.82, 0.82, 1.0)),
            (str(needs_attention), sms_campaign_status_color("needs_attention")),
            (str(running), sms_campaign_status_color("submitted")),
            (str(verified), sms_campaign_status_color("verified")),
        ):
            imgui.table_next_column()
            imgui.text_colored(color, value)
        imgui.end_table()

    if imgui.button("Refresh Campaigns", (150, 0)):
        campaigns = discovered_sms_campaigns(state, command, force=True)
        state.runner.append_log(f"Refreshed SMS campaigns: {len(campaigns)} saved")
    imgui.same_line()
    if imgui.button("New Campaign", (130, 0)):
        create_command = command_by_id("sms_campaign_create")
        values = state.command_values(create_command)
        for key in ("campaign_id", "description", "message", "sheet", "column"):
            values[key] = ""
        state.persist_command_values(create_command)
        state.selected_command_id = create_command.id

    imgui.separator_text("Saved Campaigns")
    if not campaigns:
        imgui.text_disabled("No saved SMS campaigns yet. Click New Campaign or use Create SMS Campaign.")
        return

    if imgui.begin_table("sms_campaigns_table", 10):
        for label in ("Campaign", "Status", "Recipients", "Pending", "Sent", "Failed", "Unknown", "Updated", "Source", "Actions"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()

        for campaign in campaigns:
            name = campaign.get("campaign_name", "")
            imgui.push_id(name)
            imgui.table_next_row()

            imgui.table_next_column()
            imgui.text_wrapped(name)
            description = campaign.get("description", "")
            if description:
                imgui.text_disabled(description[:120])

            imgui.table_next_column()
            status = campaign.get("status", "-")
            imgui.text_colored(sms_campaign_status_color(status), status)

            imgui.table_next_column()
            imgui.text(campaign.get("recipients", "0"))

            imgui.table_next_column()
            imgui.text(campaign.get("pending", "0"))

            imgui.table_next_column()
            delivered = int(campaign.get("delivered") or 0)
            sent = int(campaign.get("sent") or 0)
            imgui.text(str(sent + delivered))

            imgui.table_next_column()
            failed = campaign.get("failed", "0")
            failed_color = sms_campaign_status_color("needs_attention") if failed not in {"", "0"} else (0.82, 0.82, 0.82, 1.0)
            imgui.text_colored(failed_color, failed or "0")

            imgui.table_next_column()
            imgui.text(campaign.get("unknown", "0") or "0")

            imgui.table_next_column()
            imgui.text_disabled(campaign.get("updated_at", ""))

            imgui.table_next_column()
            source = Path(campaign.get("source_file", "")).name
            sheet = campaign.get("sheet", "")
            column = campaign.get("number_column", "")
            imgui.text_wrapped(source)
            imgui.text_disabled(f"{sheet} / {column}")

            imgui.table_next_column()
            if imgui.button("Edit", (52, 0)):
                apply_sms_campaign_to_command(state, "sms_campaign_update", campaign)
                state.selected_command_id = "sms_campaign_update"
            imgui.same_line()
            if imgui.button("Send", (55, 0)):
                apply_sms_campaign_to_command(state, "sms_campaign_send", campaign)
                state.selected_command_id = "sms_campaign_send"
            imgui.same_line()
            if imgui.button("Summary", (82, 0)):
                apply_sms_campaign_to_command(state, "sms_campaign_show", campaign)
                state.selected_command_id = "sms_campaign_show"
            imgui.same_line()
            if imgui.button("Failed", (65, 0)):
                apply_sms_campaign_to_command(state, "sms_campaign_failed", campaign)
                state.selected_command_id = "sms_campaign_failed"
            imgui.same_line()
            if imgui.button("Archive", (76, 0)):
                apply_sms_campaign_to_command(state, "sms_campaign_delete", campaign)
                state.show_confirm_for = "sms_campaign_delete"
            imgui.pop_id()

        imgui.end_table()

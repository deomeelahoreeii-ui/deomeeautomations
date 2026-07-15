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

def command_validation_error(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
) -> str:
    if command.id in {"sms_campaign_create", "sms_campaign_update"}:
        if not values.get("campaign_id", "").strip():
            return "Enter a campaign name/reference." if command.id == "sms_campaign_create" else "Select an existing campaign."
        if not values.get("file", "").strip():
            return "Select an Excel file."
        if not values.get("column", "").strip():
            return "Select the column containing phone numbers."
        if not values.get("message", "").strip():
            return "Enter the SMS message."
        return ""
    if command.id in {"sms_campaign_show", "sms_campaign_failed", "sms_campaign_delete"}:
        if not values.get("campaign_id", "").strip():
            return "Select a campaign."
        return ""
    if command.id == "sms_campaign_send":
        has_campaign = bool(values.get("campaign_id", "").strip())
        if not has_campaign:
            if not values.get("file", "").strip():
                return "Select a campaign or Excel file."
            if not values.get("column", "").strip():
                return "Select the column containing phone numbers."
            if not values.get("message", "").strip():
                return "Enter the SMS message."
        try:
            minute = int(values.get("minute_limit", "0"))
            hour = int(values.get("hour_limit", "0"))
            day = int(values.get("day_limit", "0"))
            delay = float(values.get("delay_seconds", "0"))
            watch_timeout = int(values.get("watch_timeout_seconds", "1"))
            watch_poll = int(values.get("watch_poll_seconds", "30"))
        except ValueError:
            return "Rate limits and delay must be numbers."
        if min(minute, hour, day) <= 0 or delay < 0:
            return "Rate limits must be positive and delay cannot be negative."
        if watch_timeout <= 0 or watch_poll < 5:
            return "Watch timeout must be positive and watch polling interval must be at least 5 seconds."
        if values.get("retry_failed", "false").lower() in {"1", "true", "yes", "on"} and delay < 15:
            return "Failed-message retries require at least 15 seconds between recipients."
        if not minute <= hour <= day:
            return "Limits must satisfy minute <= hour <= day."
        if values.get("execute", "false").lower() in {"1", "true", "yes", "on"}:
            if values.get("consent_confirmed", "false").lower() not in {"1", "true", "yes", "on"}:
                return "Execution requires consent and suppression/DNCR confirmation."
            if not (state.command_root(command) / ".env").is_file():
                return "Create sms-campaign-manager/.env from .env.example and add gateway credentials."
        return ""
    if command.id in {"whatsapp_group_import", "whatsapp_group_membership_audit"}:
        file_value = values.get("file", "").strip()
        if not file_value:
            return "Select an Excel file."
        if command.id == "whatsapp_group_membership_audit":
            group_jid = values.get("group_jid", "").strip()
            if not group_jid.endswith("@g.us"):
                return "Select a WhatsApp group JID ending in @g.us."
            return ""
        if values.get("execute", "false").lower() in {"1", "true", "yes", "on"}:
            return (
                "Direct additions are disabled: WhatsApp invalidated linked sessions "
                "after participant-add operations. Use Audit Group Membership and an invite link."
            )
            group_jid = values.get("group_jid", "").strip()
            if not group_jid.endswith("@g.us"):
                return "Execute mode requires a WhatsApp group JID ending in @g.us."
            if values.get("consent_confirmed", "false").lower() not in {
                "1", "true", "yes", "on"
            }:
                return (
                    "Direct addition requires confirmation that recipients were informed "
                    "and consented. Use an office-distributed invite link otherwise."
                )
            try:
                delay_ms = int(values.get("delay_ms", "0"))
                wait_timeout = int(values.get("wait_timeout", "0"))
            except ValueError:
                return "Delay and completion timeout must be whole numbers."
            if delay_ms < 10_000:
                return "Execute mode requires at least 10 seconds between additions."
            if wait_timeout <= 0:
                return "Execute mode requires a positive completion timeout."
        if values.get("selection") == "range" and (
            not values.get("start", "").strip() or not values.get("end", "").strip()
        ):
            return "Range selection requires both start and end positions."
        return ""
    if command.id != "crm_filter_pdf_duplicates":
        return ""
    command_root = state.command_root(command)
    input_dir = resolve_gui_path(command_root, values.get("input_dir", ""))
    output_dir = resolve_gui_path(command_root, values.get("output_dir", ""))
    try:
        input_dir.relative_to(output_dir)
    except ValueError:
        return ""
    return (
        "Input folder is inside the filtered output folder. "
        "Select the raw PDF folder, usually phase1-crm/unprocessed-crm/pdf."
    )

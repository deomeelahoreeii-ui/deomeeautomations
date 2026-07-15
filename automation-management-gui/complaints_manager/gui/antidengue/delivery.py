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

def delivery_failure_cause(error: str, ack_statuses: str, target: str) -> str:
    text = f"{error} {ack_statuses}".lower()
    if "tctoken" in text or "privacy token" in text:
        return "Direct WhatsApp send is blocked by a missing or expired Baileys privacy token for this contact; fallback escalation should carry the report."
    if "463" in text:
        return "WhatsApp Web rejected this direct chat as a reachout/privacy-token case. Avoid repeated retries; refresh the contact token or use fallback delivery."
    if "status was pending" in text or " pending" in text:
        if target.endswith("@g.us"):
            return "Group message stayed pending; verify the sender is still a member and the group accepts messages."
        return "WhatsApp accepted the job locally, but no server acknowledgement arrived."
    if "whatsapp reported error" in text or " error" in text:
        return "WhatsApp rejected this recipient. Reauth alone usually does not fix a recipient/token/privacy rejection."
    if "missing status" in text:
        return "The worker did not send a terminal status for this job."
    return "Delivery did not complete cleanly."

def failed_officer_rows(audit_rows: list[dict]) -> list[dict]:
    return [
        row
        for row in audit_rows
        if str(row.get("Delivery Status", "")).strip().lower() != "delivered"
    ]

def failed_delivery_statuses(delivery: dict) -> list[dict]:
    return [
        item
        for item in (delivery.get("statuses") or {}).values()
        if str(item.get("status", "")).strip().lower() != "delivered"
    ]

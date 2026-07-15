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

from complaints_manager.gui.common.context import disabled_when

def discovered_whatsapp_groups(state: GuiState) -> list[tuple[str, str, str]]:
    data_dir = state.project_root.parent / "whatsappbot" / "data"
    registries = sorted(data_dir.glob("discovered-groups*.csv"))
    rows: list[dict] = []
    for registry in registries:
        try:
            with registry.open("r", encoding="utf-8-sig", newline="") as handle:
                rows.extend(csv.DictReader(handle))
        except (OSError, csv.Error) as exc:
            state.runner.append_log(f"Could not read WhatsApp group registry {registry}: {exc}")

    groups = [
        (
            str(row.get("group_id", "")).strip(),
            str(row.get("group_name", "")).strip(),
            str(row.get("discovered_at", "")).strip(),
        )
        for row in rows
        if str(row.get("group_id", "")).strip().endswith("@g.us")
    ]
    groups.sort(key=lambda item: item[2], reverse=True)
    deduplicated: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for group in groups:
        if group[0] in seen:
            continue
        seen.add(group[0])
        deduplicated.append(group)
    return deduplicated

def draw_whatsapp_group_selector(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
    field: ArgField,
) -> None:
    groups = discovered_whatsapp_groups(state)
    current = values.get(field.key, "").strip()
    ids = [""] + [group_id for group_id, _name, _when in groups]
    labels = ["Select a discovered group..."] + [
        f"{name or '(unnamed group)'}  |  {group_id}"
        for group_id, name, _when in groups
    ]
    try:
        index = ids.index(current)
    except ValueError:
        index = 0

    imgui.set_next_item_width(620)
    changed, index = imgui.combo(field.label, index, labels)
    if changed and index > 0:
        values[field.key] = ids[index]
        state.persist_command_values(command)

    imgui.same_line()
    with disabled_when(not groups):
        if imgui.button(f"Use Latest##{command.id}_{field.key}", (105, 0)):
            values[field.key] = groups[0][0]
            state.persist_command_values(command)
            state.runner.append_log(
                f"Selected latest WhatsApp group: {groups[0][1] or groups[0][0]} ({groups[0][0]})"
            )

    imgui.same_line()
    if imgui.button(f"Refresh##{command.id}_{field.key}", (85, 0)):
        state.runner.append_log(f"Refreshed WhatsApp groups: {len(groups)} discovered")

    imgui.set_next_item_width(620)
    changed, manual_value = imgui.input_text("Manual group JID", current)
    if changed:
        values[field.key] = manual_value
        state.persist_command_values(command)

    if groups:
        latest_id, latest_name, latest_when = groups[0]
        imgui.text_disabled(
            f"Latest: {latest_name or '(unnamed group)'} | {latest_id} | {latest_when}"
        )
    else:
        imgui.text_disabled(
            "No groups discovered yet. Start or restart the WhatsApp Worker, then refresh."
        )

def draw_whatsapp_account_selector(
    state: GuiState,
    command: CommandSpec,
    values: dict[str, str],
    field: ArgField,
) -> None:
    profiles = [
        (service.spec.profile_id, service.spec.title)
        for service in state.services.services.values()
        if service.spec.kind == "whatsapp"
    ]
    if not profiles:
        profiles = [("default", "Primary WhatsApp")]
    ids = [item[0] for item in profiles]
    labels = [item[1] for item in profiles]
    current = values.get(field.key, field.default)
    try:
        index = ids.index(current)
    except ValueError:
        index = 0
    imgui.set_next_item_width(360)
    changed, index = imgui.combo(field.label, index, labels)
    if changed:
        profile_id = ids[index]
        values[field.key] = profile_id
        values["health_subject"] = (
            "whatsapp.worker.health"
            if profile_id == "default"
            else f"whatsapp.worker.{profile_id}.health"
        )
        values["members_subject"] = (
            "whatsapp.worker.group-members"
            if profile_id == "default"
            else f"whatsapp.worker.{profile_id}.group-members"
        )
        state.persist_command_values(command)

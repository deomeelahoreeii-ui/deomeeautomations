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

from complaints_manager.gui.commands.execution import start_command_with_services
from complaints_manager.gui.commands.fields import draw_env_field, draw_field
from complaints_manager.gui.commands.validation import command_validation_error
from complaints_manager.gui.common.context import disabled_when
from complaints_manager.gui.crm.actions import clear_duplicate_filter_results, copy_excel_filter_values_to_notice_command
from complaints_manager.gui.sms.campaigns import draw_sms_campaign_list_dashboard
from complaints_manager.gui.whatsapp.accounts import draw_whatsapp_account_add_screen, draw_whatsapp_accounts_dashboard

def draw_command_detail(state: GuiState, command: CommandSpec) -> None:
    if command.id == "whatsapp_accounts":
        draw_whatsapp_accounts_dashboard(state, command)
        return
    if command.id == "whatsapp_account_add":
        draw_whatsapp_account_add_screen(state, command)
        return
    if command.id == "sms_campaign_list":
        draw_sms_campaign_list_dashboard(state, command)
        return

    values = state.command_values(command)
    validation_error = command_validation_error(state, command, values)
    imgui.text(command.title)
    imgui.text_wrapped(command.summary)
    if command.aliases:
        imgui.text_disabled("Aliases: " + ", ".join(command.aliases))

    if command.env_fields:
        imgui.separator_text("Limits and Environment")
        for field in command.env_fields:
            draw_env_field(
                state,
                command,
                values,
                field.key,
                field.label,
                field.kind,
            )

    if command.arg_fields:
        visible_fields = [
            field
            for field in command.arg_fields
            if not field.kind.startswith("hidden_env:")
        ]
        if visible_fields:
            imgui.separator_text("Options")
            for field in visible_fields:
                draw_field(state, command, values, field)

    imgui.separator_text("Command")
    if validation_error:
        imgui.text_colored((0.95, 0.35, 0.28, 1.0), validation_error)
    try:
        command_line = shlex.join(command.build(state.project_root, values))
        imgui.text_wrapped(command_line)
    except Exception as exc:
        imgui.text_colored((0.95, 0.35, 0.28, 1.0), str(exc))

    command_busy = state.runner.is_running or state.command_start_in_progress
    with disabled_when(command_busy or bool(validation_error)):
        if imgui.button("Run", (100, 0)):
            if command.confirm_label:
                state.show_confirm_for = command.id
            else:
                start_command_with_services(state, command, values)
    imgui.same_line()
    with disabled_when(not state.runner.is_running):
        if imgui.button("Stop", (100, 0)):
            state.runner.stop()
    if command.id in {"crm_filter_excel_duplicates", "crm_filter_pdf_duplicates"}:
        imgui.same_line()
        with disabled_when(command_busy or bool(validation_error)):
            if imgui.button("Clear Results", (130, 0)):
                clear_duplicate_filter_results(state, command, values)
    if command.id == "crm_filter_excel_duplicates":
        imgui.same_line()
        with disabled_when(command_busy or bool(validation_error)):
            if imgui.button("Prepare Submitted", (155, 0)):
                notice_command = command_by_id("crm_prepare_duplicate_notices")
                notice_values = copy_excel_filter_values_to_notice_command(
                    state, values, notice_command, "submitted"
                )
                start_command_with_services(
                    state,
                    notice_command,
                    notice_values,
                    source="Preparing submitted notice list from the current filter output.",
                )
        imgui.same_line()
        with disabled_when(command_busy or bool(validation_error)):
            if imgui.button("Send Submitted", (140, 0)):
                notice_command = command_by_id("crm_send_duplicate_notices")
                copy_excel_filter_values_to_notice_command(
                    state, values, notice_command, "submitted"
                )
                state.selected_command_id = notice_command.id
                state.show_confirm_for = notice_command.id
        imgui.same_line()
        with disabled_when(command_busy or bool(validation_error)):
            if imgui.button("Prepare Not Relevant", (180, 0)):
                notice_command = command_by_id("crm_prepare_duplicate_notices")
                notice_values = copy_excel_filter_values_to_notice_command(
                    state, values, notice_command, "not-relevant"
                )
                start_command_with_services(
                    state,
                    notice_command,
                    notice_values,
                    source="Preparing not-relevant notice list from the current filter output.",
                )
        imgui.same_line()
        with disabled_when(command_busy or bool(validation_error)):
            if imgui.button("Send Not Relevant", (165, 0)):
                notice_command = command_by_id("crm_send_duplicate_notices")
                copy_excel_filter_values_to_notice_command(
                    state, values, notice_command, "not-relevant"
                )
                state.selected_command_id = notice_command.id
                state.show_confirm_for = notice_command.id

def draw_confirm_modal(state: GuiState) -> None:
    if state.show_confirm_for is None:
        return

    command = command_by_id(state.show_confirm_for)
    imgui.open_popup("Confirm command")
    opened = imgui.begin_popup_modal("Confirm command", True)
    if not opened[0]:
        return

    imgui.text(command.title)
    imgui.text_wrapped(command.confirm_label)
    validation_error = command_validation_error(state, command, state.command_values(command))
    if validation_error:
        imgui.text_colored((0.95, 0.35, 0.28, 1.0), validation_error)
    imgui.spacing()
    with disabled_when(bool(validation_error)):
        if imgui.button("Run Command", (130, 0)):
            start_command_with_services(state, command, state.command_values(command))
            state.show_confirm_for = None
            imgui.close_current_popup()
    imgui.same_line()
    if imgui.button("Cancel", (100, 0)):
        state.show_confirm_for = None
        imgui.close_current_popup()
    imgui.end_popup()

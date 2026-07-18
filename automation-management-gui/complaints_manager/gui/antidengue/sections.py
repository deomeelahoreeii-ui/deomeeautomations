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

from complaints_manager.gui.antidengue.database import MASTER_SCHOOL_LEVELS, MASTER_SCHOOL_SHIFTS, MASTER_SCHOOL_TYPES, _draw_relation_combo, _master_school_form_defaults, _normalized_emis, _pb_bool, _relation_options, _school_level_from_name_gui, _set_single_default, antidengue_new_school_form, create_antidengue_school_from_form, load_antidengue_master_data_options, validate_antidengue_new_school_form
from complaints_manager.gui.antidengue.delivery import delivery_failure_cause, failed_delivery_statuses
from complaints_manager.gui.antidengue.dispatch_views import _record_dispatch_action_result, draw_delivery_metric
from complaints_manager.gui.antidengue.health_repository import antidengue_pocketbase_counts, antidengue_pocketbase_health_checks
from complaints_manager.gui.antidengue.messages import build_antidengue_manual_message
from complaints_manager.gui.antidengue.runtime import antidengue_pocketbase_root, antidengue_token_status_for_mobile, draw_antidengue_worker_controls
from complaints_manager.gui.commands.execution import start_command_with_services
from complaints_manager.gui.commands.fields import draw_field
from complaints_manager.gui.common.colors import service_status_color, status_badge_color
from complaints_manager.gui.common.context import disabled_when
from complaints_manager.gui.common.desktop import open_desktop_path, open_url_from_gui
from complaints_manager.gui.common.formatting import _option_index, draw_clipped_text

def draw_antidengue_health_section(
    summary: dict,
    whatsapp: dict,
    delivery: dict,
    quality_gate: dict,
    filter_info: dict,
    fallback: dict,
    fallback_delivery: dict,
) -> None:
    queued = bool(whatsapp.get("queued"))
    error_text = str(whatsapp.get("error") or "")
    skipped_text = str(whatsapp.get("skipped_reason") or "")
    status_label = (
        "delivered"
        if queued
        else "failed"
        if error_text or skipped_text
        else "queued"
    )

    if imgui.begin_table("antidengue_latest_metrics", 6):
        for label in [
            "Run",
            "Dormant",
            "Quality",
            "Delivered",
            "Failed",
            "Fallback",
        ]:
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        imgui.table_next_row()

        imgui.table_next_column()
        draw_delivery_metric(
            "Started",
            str(summary.get("run_started_at", "-")).replace("T", " ")[:19],
            status_label,
        )

        imgui.table_next_column()
        draw_delivery_metric(
            "Rows",
            filter_info.get("dormant_raw_rows")
            or quality_gate.get("final_school_count")
            or "-",
        )

        imgui.table_next_column()
        draw_delivery_metric(
            "Gate",
            "passed" if quality_gate.get("passed") else "blocked",
            "passed" if quality_gate.get("passed") else "failed",
        )

        imgui.table_next_column()
        draw_delivery_metric(
            "WhatsApp",
            delivery.get("delivered", "-"),
            "delivered" if delivery.get("failed", 0) == 0 else "partial",
        )

        imgui.table_next_column()
        draw_delivery_metric(
            "WhatsApp",
            delivery.get("failed", "-"),
            "failed" if delivery.get("failed", 0) else "ok",
        )

        imgui.table_next_column()
        if fallback:
            fallback_failed = int(fallback_delivery.get("failed") or 0)
            fallback_delivered = int(fallback_delivery.get("delivered") or 0)
            fallback_value = f"{fallback_delivered} ok / {fallback_failed} failed"
            draw_delivery_metric(
                "Escalation",
                fallback_value,
                "failed" if fallback_failed else "delivered",
            )
        else:
            draw_delivery_metric("Escalation", "-", "")

        imgui.end_table()

    if error_text or skipped_text:
        issue_text = error_text or skipped_text
        issue_color = "failed" if error_text else "warning"
        imgui.text_colored(
            status_badge_color(issue_color),
            "What went wrong:" if error_text else "Not sent:",
        )
        imgui.same_line()
        imgui.text_wrapped(issue_text)
    else:
        imgui.text_colored(status_badge_color("ok"), "Latest run completed without delivery errors.")

    warnings = quality_gate.get("warnings") or []
    if warnings:
        imgui.text_colored(status_badge_color("warning"), "Data warnings:")
        for warning in warnings[:3]:
            imgui.bullet_text(str(warning))

def draw_antidengue_failed_officers_section(
    state: GuiState,
    audit_rows: list[dict],
    delivery: dict,
) -> None:
    failed_rows = [
        row
        for row in audit_rows
        if str(row.get("Delivery Status", "")).strip().lower() != "delivered"
    ]

    if not failed_rows:
        imgui.text_colored(status_badge_color("ok"), "No failed officer direct deliveries in the latest run.")
        return

    if imgui.begin_table("antidengue_failed_officers", 8):
        imgui.table_setup_column("Officer")
        imgui.table_setup_column("Role")
        imgui.table_setup_column("Schools")
        imgui.table_setup_column("Mobile")
        imgui.table_setup_column("WhatsApp")
        imgui.table_setup_column("Token")
        imgui.table_setup_column("Manual")
        imgui.table_setup_column("Cause")
        imgui.table_headers_row()
        for index, row in enumerate(failed_rows[:30]):
            imgui.push_id(f"failed_officer_{index}")
            target = str(row.get("Target") or "")
            error_value = str(row.get("Delivery Error") or "")
            ack_value = str(row.get("WhatsApp Ack Statuses") or "")
            token_status = antidengue_token_status_for_mobile(
                state,
                row.get("Mobile Number"),
            )
            imgui.table_next_row()

            imgui.table_next_column()
            imgui.text_wrapped(str(row.get("Officer Name") or "-"))
            imgui.table_next_column()
            imgui.text(str(row.get("Roles") or "-"))
            imgui.table_next_column()
            imgui.text(str(row.get("School Count") or "-"))
            imgui.table_next_column()
            imgui.text(str(row.get("Mobile Number") or "-"))
            imgui.table_next_column()
            imgui.text_colored(status_badge_color("failed"), ack_value or "failed")
            imgui.table_next_column()
            token_color_key = {
                "usable": "ok",
                "expired": "warning",
                "missing": "failed",
            }.get(str(token_status.get("status") or ""), "")
            imgui.text_colored(
                status_badge_color(token_color_key),
                str(token_status.get("label") or "-"),
            )
            if imgui.is_item_hovered():
                detail_parts = [
                    str(token_status.get("detail") or ""),
                    f"LID: {token_status.get('lid') or '-'}",
                ]
                imgui.set_tooltip("\n".join(part for part in detail_parts if part))
            imgui.table_next_column()
            if imgui.button("Copy##manual_msg", (70, 0)):
                imgui.set_clipboard_text(build_antidengue_manual_message(row))
                state.runner.status_message = (
                    "Copied manual WhatsApp message for "
                    f"{row.get('Officer Name') or row.get('Mobile Number') or 'officer'}"
                )
                state.runner.append_log(state.runner.status_message)
            imgui.table_next_column()
            imgui.text_wrapped(delivery_failure_cause(error_value, ack_value, target))
            imgui.pop_id()
        imgui.end_table()
    if len(failed_rows) > 30:
        imgui.text_disabled(f"Showing 30 of {len(failed_rows)} failed officer rows. Open the delivery audit for all rows.")

def draw_antidengue_group_delivery_section(delivery: dict, fallback: dict) -> None:
    failed_statuses = failed_delivery_statuses(delivery)
    fixed_failures = [
        item
        for item in failed_statuses
        if str(item.get("target") or "").endswith("@g.us")
    ]
    fallback_delivery = fallback.get("delivery") or {}
    fallback_statuses = [
        item
        for item in (fallback_delivery.get("statuses") or {}).values()
        if str(item.get("target") or "").endswith("@g.us")
    ]
    rows = [*fixed_failures, *fallback_statuses]
    if not rows:
        imgui.text_colored(status_badge_color("ok"), "No fixed/group delivery failures in the latest run.")
        return

    if imgui.begin_table("antidengue_group_failures", 4):
        imgui.table_setup_column("Target")
        imgui.table_setup_column("Status")
        imgui.table_setup_column("Messages")
        imgui.table_setup_column("Cause")
        imgui.table_headers_row()
        for index, item in enumerate(rows[:12]):
            imgui.push_id(f"group_delivery_{index}")
            target = str(item.get("target") or "")
            error_value = str(item.get("error") or "")
            operation_result = item.get("operationResult") or []
            imgui.table_next_row()
            imgui.table_next_column()
            imgui.text_colored(status_badge_color("failed"), target)
            imgui.table_next_column()
            imgui.text(str(item.get("status") or "failed"))
            imgui.table_next_column()
            imgui.text(str(len(operation_result)) if isinstance(operation_result, list) else "-")
            imgui.table_next_column()
            imgui.text_wrapped(delivery_failure_cause(error_value, "PENDING", target))
            imgui.pop_id()
        imgui.end_table()

def draw_antidengue_files_section(
    state: GuiState,
    summary_path: Path | None,
    run_dir: Path | None,
    outputs: dict,
) -> None:
    if run_dir:
        if imgui.button("Open Run Folder", (150, 0)):
            open_desktop_path(state, run_dir, "AntiDengue run folder")
        imgui.same_line()
    if summary_path and imgui.button("Open Summary JSON", (165, 0)):
        open_desktop_path(state, summary_path, "AntiDengue run summary")
    imgui.same_line()
    delivery_audit_value = str(outputs.get("officer_delivery_audit") or "").strip()
    delivery_audit = Path(delivery_audit_value) if delivery_audit_value else None
    with disabled_when(delivery_audit is None or not delivery_audit.exists()):
        if imgui.button("Open Delivery Audit", (170, 0)):
            assert delivery_audit is not None
            open_desktop_path(state, delivery_audit, "Officer delivery audit")

def draw_antidengue_manual_report_section(state: GuiState) -> None:
    command = command_by_id("antidengue_manual_file")
    values = state.command_values(command)
    for field in command.arg_fields:
        values.setdefault(field.key, field.default)

    imgui.text_wrapped(
        "Use this when portal scraping is broken or unavailable. Select the report you downloaded manually; the app will copy it into AntiDengue staging and run the normal processing, audit, and WhatsApp delivery workflow."
    )
    imgui.separator()

    for field in command.arg_fields:
        draw_field(state, command, values, field)

    selected_file = resolve_gui_path(command.working_dir(state.project_root), values.get("file", ""))
    file_ok = selected_file.is_file()
    if values.get("file", "").strip():
        if file_ok:
            size_kb = selected_file.stat().st_size / 1024
            imgui.text_colored(status_badge_color("ok"), f"Ready: {selected_file.name} ({size_kb:.1f} KB)")
        else:
            imgui.text_colored(status_badge_color("failed"), f"File not found: {selected_file}")
    else:
        imgui.text_disabled("Select the downloaded portal file first.")

    imgui.separator()
    draw_antidengue_worker_controls(state)

    with disabled_when(state.runner.is_running or state.command_start_in_progress or not file_ok):
        if imgui.button("Process Manual Report & Send", (230, 0)):
            start_command_with_services(state, command, values)
    imgui.same_line()
    with disabled_when(state.runner.is_running or state.command_start_in_progress or not file_ok):
        if imgui.button("Dry Run Manual Report", (175, 0)):
            run_values = dict(values)
            run_values["dry_run"] = "true"
            start_command_with_services(state, command, run_values)

    imgui.text_disabled(
        "The selected file is copied before processing, so the original download remains untouched."
    )

def _draw_master_owner_preview(owners: dict[str, list[dict]], owner_error: str) -> None:
    imgui.separator_text("Officer Preview")
    if owner_error:
        imgui.text_colored(status_badge_color("warning"), owner_error)
        return

    if imgui.begin_table("antidengue_master_owner_preview", 4):
        for label in ("Role", "Status", "Officer", "Mobile"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        for role, rows in (("DDEO", owners.get("ddeo") or []), ("AEO", owners.get("aeo") or [])):
            status = "ok" if len(rows) == 1 else "failed"
            if not rows:
                display_rows = [{"name": "-", "normalized_mobile": "-"}]
            else:
                display_rows = rows
            for index, row in enumerate(display_rows):
                imgui.push_id(f"master_owner_{role}_{index}")
                imgui.table_next_row()
                imgui.table_next_column()
                imgui.text(role)
                imgui.table_next_column()
                if index == 0:
                    label = "ready" if len(rows) == 1 else "missing" if not rows else "duplicate"
                    imgui.text_colored(status_badge_color(status), label)
                else:
                    imgui.text_colored(status_badge_color("failed"), "duplicate")
                imgui.table_next_column()
                draw_clipped_text(row.get("name"), 38)
                imgui.table_next_column()
                imgui.text(str(row.get("normalized_mobile") or "-"))
                imgui.pop_id()
        imgui.end_table()

def _draw_master_school_choice_fields(
    state: GuiState,
    form: dict[str, object],
    options: dict[str, list[dict]],
) -> None:
    districts = options.get("districts") or []
    departments = options.get("departments") or []
    wings = options.get("wings") or []
    tehsils = options.get("tehsils") or []
    markazes = options.get("markazes") or []

    _set_single_default(form, "district_ref", districts)
    _set_single_default(form, "department_ref", departments)

    selected_district = str(form.get("district_ref") or "")
    selected_department = str(form.get("department_ref") or "")
    filtered_wings = [
        row
        for row in wings
        if (not selected_district or str(row.get("district_ref") or "") == selected_district)
        and (not selected_department or str(row.get("department_ref") or "") == selected_department)
    ]
    _set_single_default(form, "wing_ref", filtered_wings)

    selected_wing = str(form.get("wing_ref") or "")
    filtered_tehsils = [
        row
        for row in tehsils
        if not selected_district or str(row.get("district_ref") or "") == selected_district
    ]
    selected_tehsil = str(form.get("tehsil_ref") or "")
    filtered_markazes = [
        row
        for row in markazes
        if selected_tehsil
        and selected_wing
        and str(row.get("tehsil_ref") or "") == selected_tehsil
        and str(row.get("wing_ref") or "") == selected_wing
    ]

    if _draw_relation_combo(
        "District##master_school_district",
        "district_ref",
        form,
        _relation_options(districts, "Select district"),
        width=250,
    ):
        form["wing_ref"] = ""
        form["tehsil_ref"] = ""
        form["markaz_ref"] = ""
    imgui.same_line()
    if _draw_relation_combo(
        "Department##master_school_department",
        "department_ref",
        form,
        _relation_options(departments, "Select department"),
        width=320,
    ):
        form["wing_ref"] = ""
        form["markaz_ref"] = ""

    if _draw_relation_combo(
        "Wing##master_school_wing",
        "wing_ref",
        form,
        _relation_options(filtered_wings, "Select wing"),
        width=250,
    ):
        form["markaz_ref"] = ""
    imgui.same_line()
    if _draw_relation_combo(
        "Tehsil##master_school_tehsil",
        "tehsil_ref",
        form,
        _relation_options(filtered_tehsils, "Select tehsil"),
        width=250,
    ):
        form["markaz_ref"] = ""
    imgui.same_line()
    _draw_relation_combo(
        "Markaz##master_school_markaz",
        "markaz_ref",
        form,
        _relation_options(filtered_markazes, "Select markaz", include_parent=True),
        width=360,
    )

def draw_antidengue_master_data_section(state: GuiState) -> None:
    options, error = load_antidengue_master_data_options(state)
    if error:
        imgui.text_colored(status_badge_color("warning"), error)
        return

    form = antidengue_new_school_form(state)
    imgui.separator_text("Add School")

    imgui.set_next_item_width(160)
    changed, value = imgui.input_text("EMIS##master_school_emis", str(form.get("emis") or ""))
    if changed:
        form["emis"] = _normalized_emis(value)
    imgui.same_line()
    imgui.set_next_item_width(520)
    changed, value = imgui.input_text("School name##master_school_name", str(form.get("name") or ""))
    if changed:
        form["name"] = value
        if not form.get("school_level"):
            form["school_level"] = _school_level_from_name_gui(value) or "Primary"

    _draw_master_school_choice_fields(state, form, options)

    shift_index = _option_index([(item, item) for item in MASTER_SCHOOL_SHIFTS], str(form.get("shift") or "Single"))
    imgui.set_next_item_width(160)
    changed, shift_index = imgui.combo("Shift##master_school_shift", shift_index, list(MASTER_SCHOOL_SHIFTS))
    if changed:
        form["shift"] = MASTER_SCHOOL_SHIFTS[shift_index]
    imgui.same_line()

    type_index = _option_index([(item, item) for item in MASTER_SCHOOL_TYPES], str(form.get("school_type") or "Male"))
    imgui.set_next_item_width(180)
    changed, type_index = imgui.combo("Type##master_school_type", type_index, list(MASTER_SCHOOL_TYPES))
    if changed:
        form["school_type"] = MASTER_SCHOOL_TYPES[type_index]
    imgui.same_line()

    level_index = _option_index([(item, item) for item in MASTER_SCHOOL_LEVELS], str(form.get("school_level") or "Primary"))
    imgui.set_next_item_width(190)
    changed, level_index = imgui.combo("Level##master_school_level", level_index, list(MASTER_SCHOOL_LEVELS))
    if changed:
        form["school_level"] = MASTER_SCHOOL_LEVELS[level_index]
    imgui.same_line()

    imgui.set_next_item_width(130)
    changed, value = imgui.input_text("DEOs wise##master_school_deos_wise", str(form.get("deos_wise") or "M-EE"))
    if changed:
        form["deos_wise"] = value

    imgui.set_next_item_width(300)
    changed, value = imgui.input_text("Head name##master_school_head_name", str(form.get("head_name") or ""))
    if changed:
        form["head_name"] = value
    imgui.same_line()
    imgui.set_next_item_width(230)
    changed, value = imgui.input_text("Head contact##master_school_head_contact", str(form.get("head_contact") or ""))
    if changed:
        form["head_contact"] = value
    imgui.same_line()
    changed, active = imgui.checkbox("Active##master_school_active", _pb_bool(form.get("active"), True))
    if changed:
        form["active"] = active

    changed, value = imgui.input_text_multiline(
        "Notes##master_school_notes",
        str(form.get("notes") or ""),
        (760, 70),
    )
    if changed:
        form["notes"] = value

    errors, owners, owner_error = validate_antidengue_new_school_form(state, form)
    _draw_master_owner_preview(owners, owner_error)

    if errors:
        imgui.separator_text("Validation")
        for index, message in enumerate(errors[:6]):
            imgui.push_id(f"master_school_error_{index}")
            imgui.text_colored(status_badge_color("warning"), message)
            imgui.pop_id()
        if len(errors) > 6:
            imgui.text_disabled(f"{len(errors) - 6} more validation issue(s).")

    imgui.separator()
    with disabled_when(bool(errors)):
        if imgui.button("Save School", (130, 0)):
            success, message = create_antidengue_school_from_form(state, form)
            form["last_result"] = message
            form["last_success"] = success
            _record_dispatch_action_result(state, success, message)
            if success:
                form["last_saved_emis"] = _normalized_emis(form.get("emis"))
                for key in ("emis", "name", "head_name", "head_contact", "notes"):
                    form[key] = ""
    imgui.same_line()
    if imgui.button("Clear Form", (120, 0)):
        preserved = {
            key: form.get(key)
            for key in ("district_ref", "department_ref", "wing_ref", "tehsil_ref", "markaz_ref")
        }
        form.clear()
        form.update(_master_school_form_defaults())
        form.update(preserved)
    result = str(form.get("last_result") or "")
    if result:
        imgui.same_line()
        color_key = "ok" if _pb_bool(form.get("last_success")) else "failed"
        imgui.text_colored(status_badge_color(color_key), result)

def draw_antidengue_database_section(state: GuiState) -> None:
    imgui.text_wrapped(
        "AntiDengue runtime data is owned by the Automation Platform PostgreSQL "
        "database. PocketBase is retired and is no longer started by this console."
    )
    imgui.text_wrapped(
        "Use the web Master Data, WhatsApp Routing, Preview & Approval, Deliveries, "
        "and Activity pages to manage and audit the workflow."
    )
    if imgui.button("Open Automation Platform", (190, 0)):
        open_url_from_gui(state, "http://localhost:4321/")
    return

    root = antidengue_pocketbase_root(state)
    service = state.services.services.get("antidengue_pocketbase")
    migrate_command = command_by_id("antidengue_pocketbase_migrate")
    import_command = command_by_id("antidengue_pocketbase_import_officers")

    imgui.text_wrapped(
        "PocketBase stores AntiDengue schools, officers, jurisdictions, fixed WhatsApp recipients, report runs, delivery events, and data-quality issues in one local database with an admin dashboard."
    )
    imgui.separator()

    if service:
        imgui.text("Database service:")
        imgui.same_line()
        imgui.text_colored(
            service_status_color(service.is_running, service.last_exit_code),
            service.status_message,
        )
        imgui.same_line()
        imgui.text_disabled("http://127.0.0.1:8090/_/")

        with disabled_when(service.is_running):
            if imgui.button("Start Database", (140, 0)):
                state.services.start("antidengue_pocketbase")
        imgui.same_line()
        with disabled_when(not service.is_running):
            if imgui.button("Stop Database", (130, 0)):
                state.services.stop("antidengue_pocketbase")
        imgui.same_line()
        if imgui.button("Open Admin", (120, 0)):
            open_url_from_gui(state, "http://127.0.0.1:8090/_/")
    else:
        imgui.text_colored(status_badge_color("failed"), "PocketBase service is not registered.")

    imgui.same_line()
    if imgui.button("Open DB Folder", (140, 0)):
        open_desktop_path(state, root, "AntiDengue PocketBase folder")

    imgui.separator_text("Schema and Sync")
    with disabled_when(state.runner.is_running):
        if imgui.button("Apply Migrations", (155, 0)):
            start_command_with_services(state, migrate_command, {})
    imgui.same_line()
    with disabled_when(state.runner.is_running):
        if imgui.button("Sync DB Data", (155, 0)):
            start_command_with_services(state, import_command, {})
    imgui.same_line()
    imgui.text_disabled("officers_list.csv + whatsapp_recipients.csv")

    counts, error = antidengue_pocketbase_counts(state)
    if error:
        imgui.text_colored(status_badge_color("warning"), error)
        return

    if imgui.begin_table("antidengue_pocketbase_counts", 7):
        for label in ("Schools", "DDEOs", "AEOs", "Jurisdictions", "Overrides", "Recipients", "Import batches"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        imgui.table_next_row()
        values = (
            counts.get("schools", 0),
            counts.get("ddeo_officers", 0),
            counts.get("aeo_officers", 0),
            counts.get("ddeo_jurisdictions", 0)
            + counts.get("aeo_jurisdictions", 0),
            counts.get("school_ddeo_overrides", 0)
            + counts.get("school_aeo_overrides", 0),
            counts.get("whatsapp_recipients", 0),
            counts.get("officer_import_batches", 0),
        )
        for value in values:
            imgui.table_next_column()
            imgui.text(str(value))
        imgui.end_table()

        if imgui.begin_table("antidengue_pocketbase_audit_counts", 4):
            for label in ("Report runs", "Dormant rows", "Delivery events", "Data-quality issues"):
                imgui.table_setup_column(label)
            imgui.table_headers_row()
            imgui.table_next_row()
            for key in ("report_runs", "dormant_records", "delivery_events", "data_quality_issues"):
                imgui.table_next_column()
                imgui.text(str(counts.get(key, 0)))
            imgui.end_table()

        if imgui.begin_table("antidengue_pocketbase_dispatch_counts", 4):
            for label in ("Settings", "Groups", "Templates", "Dispatch events"):
                imgui.table_setup_column(label)
            imgui.table_headers_row()
            imgui.table_next_row()
            for key in ("dispatch_settings", "whatsapp_groups", "message_templates", "dispatch_events"):
                imgui.table_next_column()
                imgui.text(str(counts.get(key, 0)))
            imgui.end_table()

    imgui.separator_text("Database Health Checks")
    health_checks, health_error = antidengue_pocketbase_health_checks(state)
    if health_error:
        imgui.text_colored(status_badge_color("warning"), health_error)
        return

    has_problem = any(
        str(row.get("severity") or "").lower() in {"failed", "warning"}
        for row in health_checks
    )
    if not has_problem:
        imgui.text_colored(status_badge_color("ok"), "All database health checks passed.")

    if imgui.begin_table("antidengue_pocketbase_health_checks", 4):
        for label in ("Status", "Check", "Count", "Message"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        for index, row in enumerate(health_checks):
            imgui.push_id(f"db_health_{index}")
            severity = str(row.get("severity") or "")
            imgui.table_next_row()
            imgui.table_next_column()
            imgui.text_colored(status_badge_color(severity), severity)
            imgui.table_next_column()
            imgui.text(str(row.get("name") or "-"))
            imgui.table_next_column()
            imgui.text(str(row.get("count") or 0))
            imgui.table_next_column()
            imgui.text_wrapped(str(row.get("message") or "-"))
            imgui.pop_id()
        imgui.end_table()

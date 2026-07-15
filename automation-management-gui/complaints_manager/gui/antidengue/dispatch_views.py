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

DISPATCH_MESSAGE_MODES = (
    ("full_report", "Full report"),
    ("tehsil_summary", "Tehsil summary"),
    ("markaz_summary", "Markaz summary"),
    ("tehsil_markaz_summary", "Tehsil + Markaz summary"),
    ("ddeo_wise", "DDEO-wise"),
    ("aeo_wise", "AEO-wise"),
    ("failed_fallback", "Failed fallback"),
)

DISPATCH_ROUTE_KINDS = (
    ("district", "District"),
    ("tehsil", "Tehsil"),
    ("markaz", "Markaz"),
    ("fallback", "Fallback"),
    ("custom", "Custom"),
)

from complaints_manager.gui.antidengue.database import _pb_bool, add_antidengue_dispatch_group, load_antidengue_dispatch_groups, load_antidengue_dispatch_settings, load_antidengue_latest_dispatch_context, load_antidengue_scope_options, record_antidengue_dispatch_event, save_antidengue_dispatch_settings, update_antidengue_dispatch_group
from complaints_manager.gui.antidengue.delivery import delivery_failure_cause, failed_officer_rows
from complaints_manager.gui.antidengue.messages import _rows_for_dispatch_group, build_antidengue_group_dispatch_message, build_antidengue_manual_message
from complaints_manager.gui.antidengue.runtime import antidengue_token_status_for_mobile
from complaints_manager.gui.common.colors import status_badge_color
from complaints_manager.gui.common.context import disabled_when
from complaints_manager.gui.common.desktop import open_desktop_path
from complaints_manager.gui.common.formatting import _option_index, draw_clipped_text

def draw_delivery_metric(label: str, value: object, status: str = "") -> None:
    imgui.text_disabled(label)
    if status:
        imgui.text_colored(status_badge_color(status), str(value))
    else:
        imgui.text(str(value))

def _record_dispatch_action_result(state: GuiState, success: bool, message: str) -> None:
    state.runner.status_message = message
    state.runner.append_log(message)
    if success:
        state.cached_metrics.pop("antidengue", None)

def draw_antidengue_dispatch_settings_controls(state: GuiState) -> dict[str, object] | None:
    settings, error = load_antidengue_dispatch_settings(state)
    if error:
        imgui.text_colored(status_badge_color("warning"), error)
        return None

    imgui.separator_text("Dispatch Controls")
    changed_any = False
    for index, (key, label) in enumerate(
        (
            ("allow_individual", "Individual messages"),
            ("allow_groups", "Group messages"),
            ("manual_only", "Manual only"),
            ("attach_excel", "Attach Excel"),
            ("require_preview", "Require preview"),
        )
    ):
        if index:
            imgui.same_line()
        current = bool(settings.get(key))
        changed, current = imgui.checkbox(label, current)
        if changed:
            settings[key] = current
            changed_any = True

    if changed_any:
        _record_dispatch_action_result(
            state,
            *save_antidengue_dispatch_settings(state, settings),
        )

    if settings.get("manual_only"):
        imgui.text_colored(
            status_badge_color("warning"),
            "Manual-only is active: runs build reports and messages, but do not queue WhatsApp jobs.",
        )
    elif settings.get("require_preview"):
        imgui.text_colored(
            status_badge_color("warning"),
            "Preview is required: runs build dispatch items, then wait for manual/operator action.",
        )
    return settings

def draw_antidengue_dispatch_overview(state: GuiState, context: dict, settings: dict[str, object]) -> None:
    rows = context.get("rows") or []
    summary = context.get("summary") or {}
    whatsapp = summary.get("whatsapp") or {}
    audit_rows = whatsapp.get("officer_delivery_audit") or []
    failed_rows = failed_officer_rows(audit_rows)
    groups, group_error = load_antidengue_dispatch_groups(state)
    enabled_groups = [group for group in groups if _pb_bool(group.get("enabled"))]

    if imgui.begin_table("antidengue_dispatch_overview", 6):
        for label in ("Run", "Dormant", "Officers", "Failed/Pending", "Groups", "Mode"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        imgui.table_next_row()
        imgui.table_next_column()
        imgui.text(str(context.get("run_key") or "-").replace("T", " ")[:19])
        imgui.table_next_column()
        imgui.text(str(len(rows)))
        imgui.table_next_column()
        imgui.text(str(len(audit_rows)))
        imgui.table_next_column()
        imgui.text_colored(
            status_badge_color("failed" if failed_rows else "ok"),
            str(len(failed_rows)),
        )
        imgui.table_next_column()
        imgui.text_colored(
            status_badge_color("ok" if enabled_groups else "warning"),
            f"{len(enabled_groups)} / {len(groups)}",
        )
        imgui.table_next_column()
        mode = "Manual only" if settings.get("manual_only") else "Preview required" if settings.get("require_preview") else "Auto allowed"
        imgui.text_colored(
            status_badge_color("warning" if settings.get("manual_only") or settings.get("require_preview") else "ok"),
            mode,
        )
        imgui.end_table()

    if group_error:
        imgui.text_colored(status_badge_color("warning"), group_error)

    imgui.text_disabled(
        "Use Individuals for failed direct messages. Use Groups to enable routes, choose message shape, and copy group-ready text."
    )

def draw_antidengue_dispatch_individuals(state: GuiState, context: dict) -> None:
    summary = context.get("summary") or {}
    whatsapp = summary.get("whatsapp") or {}
    audit_rows = whatsapp.get("officer_delivery_audit") or []
    run_key = str(context.get("run_key") or summary.get("run_started_at") or "")

    show_all = _pb_bool(
        state.app_settings.get("antidengue_dispatch_show_all_individuals"),
        False,
    )
    changed, show_all = imgui.checkbox("Show delivered too", show_all)
    if changed:
        state.app_settings["antidengue_dispatch_show_all_individuals"] = show_all

    rows = audit_rows if show_all else failed_officer_rows(audit_rows)
    if not rows:
        imgui.text_colored(status_badge_color("ok"), "No individual rows need manual dispatch in the latest run.")
        return

    if imgui.begin_table("antidengue_dispatch_individuals", 9):
        for label in ("Officer", "Role", "Schools", "Mobile", "Status", "Token", "Copy", "Manual", "Cause"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        for index, row in enumerate(rows[:80]):
            imgui.push_id(f"dispatch_individual_{index}")
            status = str(row.get("Delivery Status") or "pending")
            message = build_antidengue_manual_message(row)
            imgui.table_next_row()
            imgui.table_next_column()
            draw_clipped_text(row.get("Officer Name"), 28)
            imgui.table_next_column()
            imgui.text(str(row.get("Roles") or "-"))
            imgui.table_next_column()
            imgui.text(str(row.get("School Count") or "-"))
            imgui.table_next_column()
            imgui.text(str(row.get("Mobile Number") or "-"))
            imgui.table_next_column()
            imgui.text_colored(status_badge_color(status), status)
            imgui.table_next_column()
            token_status = antidengue_token_status_for_mobile(state, row.get("Mobile Number"))
            imgui.text_colored(
                status_badge_color({"usable": "ok", "expired": "warning", "missing": "failed"}.get(str(token_status.get("status") or ""), "")),
                str(token_status.get("label") or "-"),
            )
            imgui.table_next_column()
            if imgui.button("Copy", (70, 0)):
                imgui.set_clipboard_text(message)
                record_antidengue_dispatch_event(
                    state,
                    run_key=run_key,
                    channel="individual",
                    action="copied",
                    target=str(row.get("Target") or row.get("Mobile Number") or ""),
                    recipient_name=str(row.get("Officer Name") or ""),
                    status="copied",
                    message=message,
                    payload=row,
                )
                state.runner.status_message = f"Copied manual message for {row.get('Officer Name') or row.get('Mobile Number') or 'officer'}"
                state.runner.append_log(state.runner.status_message)
            imgui.table_next_column()
            if imgui.button("Mark Sent", (95, 0)):
                record_antidengue_dispatch_event(
                    state,
                    run_key=run_key,
                    channel="individual",
                    action="marked_sent",
                    target=str(row.get("Target") or row.get("Mobile Number") or ""),
                    recipient_name=str(row.get("Officer Name") or ""),
                    status="manual_sent",
                    message=message,
                    payload=row,
                )
                state.runner.status_message = f"Marked manual dispatch sent for {row.get('Officer Name') or row.get('Mobile Number') or 'officer'}"
                state.runner.append_log(state.runner.status_message)
            imgui.table_next_column()
            draw_clipped_text(
                delivery_failure_cause(
                    str(row.get("Delivery Error") or ""),
                    str(row.get("WhatsApp Ack Statuses") or ""),
                    str(row.get("Target") or ""),
                ),
                54,
            )
            imgui.pop_id()
        imgui.end_table()
    if len(rows) > 80:
        imgui.text_disabled(f"Showing first 80 of {len(rows)} individual dispatch rows.")

def _draw_group_scope_selector(
    state: GuiState,
    form: dict[str, object],
    tehsils: list[dict],
    markazes: list[dict],
) -> None:
    route_kind = str(form.get("route_kind") or "district")
    if route_kind in {"tehsil", "markaz"}:
        tehsil_options = [("", "All tehsils"), *[(str(row["id"]), str(row["name"])) for row in tehsils]]
        tehsil_index = _option_index(tehsil_options, str(form.get("tehsil_ref") or ""))
        imgui.set_next_item_width(220)
        changed, tehsil_index = imgui.combo("Tehsil", tehsil_index, [label for _, label in tehsil_options])
        if changed:
            form["tehsil_ref"] = tehsil_options[tehsil_index][0]
            form["markaz_ref"] = ""

    if route_kind == "markaz":
        selected_tehsil = str(form.get("tehsil_ref") or "")
        scoped_markazes = [
            row for row in markazes
            if not selected_tehsil or str(row.get("tehsil_ref") or "") == selected_tehsil
        ]
        markaz_options = [
            ("", "All markazes"),
            *[
                (
                    str(row["id"]),
                    f"{row.get('tehsil') or '-'} / {row.get('name') or '-'}",
                )
                for row in scoped_markazes
            ],
        ]
        markaz_index = _option_index(markaz_options, str(form.get("markaz_ref") or ""))
        imgui.set_next_item_width(320)
        changed, markaz_index = imgui.combo("Markaz", markaz_index, [label for _, label in markaz_options])
        if changed:
            form["markaz_ref"] = markaz_options[markaz_index][0]

def _tehsil_name_by_id(tehsils: list[dict], tehsil_id: str) -> str:
    return next(
        (str(row.get("name") or "") for row in tehsils if str(row.get("id") or "") == tehsil_id),
        "",
    )

def draw_existing_group_scope_selector(
    state: GuiState,
    group: dict,
    tehsils: list[dict],
    markazes: list[dict],
) -> None:
    route_kind = str(group.get("route_kind") or "district").strip().lower()
    group_id = str(group.get("id") or "")
    if route_kind == "district":
        imgui.text("District")
        return
    if route_kind == "fallback":
        imgui.text("Fallback")
        return
    if route_kind == "custom":
        imgui.text_disabled("Custom")
        return

    tehsil_options = [("", "Select tehsil"), *[(str(row["id"]), str(row["name"])) for row in tehsils]]
    tehsil_ref = str(group.get("tehsil_ref") or "")
    tehsil_index = _option_index(tehsil_options, tehsil_ref)
    imgui.set_next_item_width(165)
    changed, tehsil_index = imgui.combo(
        "##existing_tehsil",
        tehsil_index,
        [label for _, label in tehsil_options],
    )
    if changed:
        selected_tehsil = tehsil_options[tehsil_index][0]
        group["tehsil_ref"] = selected_tehsil
        group["tehsil"] = _tehsil_name_by_id(tehsils, selected_tehsil)
        group["markaz_ref"] = ""
        group["markaz"] = ""
        _record_dispatch_action_result(
            state,
            *update_antidengue_dispatch_group(
                state,
                group_id,
                {"tehsil_ref": selected_tehsil, "markaz_ref": ""},
            ),
        )

    if route_kind != "markaz":
        return

    selected_tehsil = str(group.get("tehsil_ref") or "")
    scoped_markazes = [
        row
        for row in markazes
        if not selected_tehsil or str(row.get("tehsil_ref") or "") == selected_tehsil
    ]
    markaz_options = [
        ("", "Select markaz"),
        *[
            (
                str(row["id"]),
                f"{row.get('tehsil') or '-'} / {row.get('name') or '-'}",
            )
            for row in scoped_markazes
        ],
    ]
    markaz_ref = str(group.get("markaz_ref") or "")
    markaz_index = _option_index(markaz_options, markaz_ref)
    imgui.set_next_item_width(230)
    changed, markaz_index = imgui.combo(
        "##existing_markaz",
        markaz_index,
        [label for _, label in markaz_options],
    )
    if changed:
        selected_markaz = markaz_options[markaz_index][0]
        group["markaz_ref"] = selected_markaz
        selected_markaz_row = next(
            (row for row in markazes if str(row.get("id") or "") == selected_markaz),
            {},
        )
        group["markaz"] = str(selected_markaz_row.get("name") or "")
        if selected_markaz_row.get("tehsil_ref"):
            group["tehsil_ref"] = str(selected_markaz_row.get("tehsil_ref") or "")
            group["tehsil"] = str(selected_markaz_row.get("tehsil") or "")
        _record_dispatch_action_result(
            state,
            *update_antidengue_dispatch_group(
                state,
                group_id,
                {
                    "tehsil_ref": group.get("tehsil_ref") or "",
                    "markaz_ref": selected_markaz,
                },
            ),
        )

def draw_antidengue_group_add_form(state: GuiState) -> None:
    tehsils, markazes, scope_error = load_antidengue_scope_options(state)
    if scope_error:
        imgui.text_colored(status_badge_color("warning"), scope_error)
        return

    form = state.app_settings.setdefault(
        "antidengue_new_dispatch_group",
        {
            "name": "",
            "target": "",
            "route_kind": "district",
            "message_mode": "full_report",
            "tehsil_ref": "",
            "markaz_ref": "",
            "enabled": True,
            "attach_excel": True,
            "manual_only": False,
            "delay_ms": "1500",
        },
    )

    imgui.separator_text("Add Group Route")
    imgui.set_next_item_width(260)
    changed, value = imgui.input_text("Group name", str(form.get("name") or ""))
    if changed:
        form["name"] = value
    imgui.same_line()
    imgui.set_next_item_width(300)
    changed, value = imgui.input_text("Group JID", str(form.get("target") or ""))
    if changed:
        form["target"] = value

    route_index = _option_index(DISPATCH_ROUTE_KINDS, str(form.get("route_kind") or "district"))
    imgui.set_next_item_width(160)
    changed, route_index = imgui.combo("Route", route_index, [label for _, label in DISPATCH_ROUTE_KINDS])
    if changed:
        form["route_kind"] = DISPATCH_ROUTE_KINDS[route_index][0]
        if form["route_kind"] == "district":
            form["tehsil_ref"] = ""
            form["markaz_ref"] = ""
    imgui.same_line()
    mode_index = _option_index(DISPATCH_MESSAGE_MODES, str(form.get("message_mode") or "full_report"))
    imgui.set_next_item_width(190)
    changed, mode_index = imgui.combo("Message", mode_index, [label for _, label in DISPATCH_MESSAGE_MODES])
    if changed:
        form["message_mode"] = DISPATCH_MESSAGE_MODES[mode_index][0]

    _draw_group_scope_selector(state, form, tehsils, markazes)

    for key, label in (("enabled", "Enabled"), ("attach_excel", "Attach Excel"), ("manual_only", "Manual only")):
        current = _pb_bool(form.get(key), key != "manual_only")
        changed, current = imgui.checkbox(label, current)
        if changed:
            form[key] = current
        imgui.same_line()
    imgui.new_line()

    with disabled_when(not str(form.get("target") or "").strip()):
        if imgui.button("Add Group Route", (150, 0)):
            success, message = add_antidengue_dispatch_group(state, form)
            _record_dispatch_action_result(state, success, message)
            if success:
                form.update({"name": "", "target": "", "tehsil_ref": "", "markaz_ref": ""})

def draw_antidengue_dispatch_groups(state: GuiState, context: dict) -> None:
    groups, error = load_antidengue_dispatch_groups(state)
    if error:
        imgui.text_colored(status_badge_color("warning"), error)
        return

    summary = context.get("summary") or {}
    outputs = summary.get("outputs") or {}
    excel_path = str(outputs.get("excel_report") or "")
    run_key = str(context.get("run_key") or summary.get("run_started_at") or "")
    tehsils, markazes, scope_error = load_antidengue_scope_options(state)
    if scope_error:
        imgui.text_colored(status_badge_color("warning"), scope_error)

    draw_antidengue_group_add_form(state)
    imgui.separator_text("Group Routes")
    if not groups:
        imgui.text_disabled("No group routes configured yet.")
        return

    if excel_path:
        if imgui.button("Copy Excel Path", (130, 0)):
            imgui.set_clipboard_text(excel_path)
            state.runner.status_message = "Copied latest AntiDengue Excel report path."
            state.runner.append_log(state.runner.status_message)
        imgui.same_line()
        output_dir = Path(excel_path).parent
        with disabled_when(not output_dir.exists()):
            if imgui.button("Open Output Folder", (155, 0)):
                open_desktop_path(state, output_dir, "AntiDengue dispatch output folder")

    if imgui.begin_table("antidengue_dispatch_groups", 10):
        for label in ("Enabled", "Group", "Route", "Scope", "Message", "Excel", "Manual", "Target", "Copy", "Rows"):
            imgui.table_setup_column(label)
        imgui.table_headers_row()
        for index, group in enumerate(groups):
            imgui.push_id(f"dispatch_group_{group.get('id') or index}")
            rows = _rows_for_dispatch_group(group, context.get("rows") or [])
            message = build_antidengue_group_dispatch_message(group, context)
            imgui.table_next_row()

            imgui.table_next_column()
            enabled = _pb_bool(group.get("enabled"))
            changed, enabled = imgui.checkbox("##enabled", enabled)
            if changed:
                _record_dispatch_action_result(
                    state,
                    *update_antidengue_dispatch_group(state, str(group["id"]), {"enabled": enabled}),
                )

            imgui.table_next_column()
            draw_clipped_text(group.get("name"), 28)

            imgui.table_next_column()
            route_index = _option_index(DISPATCH_ROUTE_KINDS, str(group.get("route_kind") or "district"))
            imgui.set_next_item_width(115)
            changed, route_index = imgui.combo("##route", route_index, [label for _, label in DISPATCH_ROUTE_KINDS])
            if changed:
                next_route = DISPATCH_ROUTE_KINDS[route_index][0]
                updates: dict[str, object] = {"route_kind": next_route}
                if next_route in {"district", "fallback", "custom"}:
                    updates.update({"tehsil_ref": "", "markaz_ref": ""})
                    group["tehsil_ref"] = ""
                    group["markaz_ref"] = ""
                    group["tehsil"] = ""
                    group["markaz"] = ""
                elif next_route == "tehsil":
                    updates["markaz_ref"] = ""
                    group["markaz_ref"] = ""
                    group["markaz"] = ""
                group["route_kind"] = next_route
                _record_dispatch_action_result(
                    state,
                    *update_antidengue_dispatch_group(
                        state,
                        str(group["id"]),
                        updates,
                    ),
                )

            imgui.table_next_column()
            draw_existing_group_scope_selector(state, group, tehsils, markazes)

            imgui.table_next_column()
            mode_index = _option_index(DISPATCH_MESSAGE_MODES, str(group.get("message_mode") or "full_report"))
            imgui.set_next_item_width(190)
            changed, mode_index = imgui.combo("##mode", mode_index, [label for _, label in DISPATCH_MESSAGE_MODES])
            if changed:
                _record_dispatch_action_result(
                    state,
                    *update_antidengue_dispatch_group(
                        state,
                        str(group["id"]),
                        {"message_mode": DISPATCH_MESSAGE_MODES[mode_index][0]},
                    ),
                )

            imgui.table_next_column()
            attach_excel = _pb_bool(group.get("attach_excel"), True)
            changed, attach_excel = imgui.checkbox("##excel", attach_excel)
            if changed:
                _record_dispatch_action_result(
                    state,
                    *update_antidengue_dispatch_group(state, str(group["id"]), {"attach_excel": attach_excel}),
                )

            imgui.table_next_column()
            manual_only = _pb_bool(group.get("manual_only"))
            changed, manual_only = imgui.checkbox("##manual", manual_only)
            if changed:
                _record_dispatch_action_result(
                    state,
                    *update_antidengue_dispatch_group(state, str(group["id"]), {"manual_only": manual_only}),
                )

            imgui.table_next_column()
            draw_clipped_text(group.get("target"), 30)

            imgui.table_next_column()
            if imgui.button("Copy", (70, 0)):
                imgui.set_clipboard_text(message)
                record_antidengue_dispatch_event(
                    state,
                    run_key=run_key,
                    channel="group",
                    action="copied",
                    target=str(group.get("target") or ""),
                    recipient_name=str(group.get("name") or ""),
                    status="copied",
                    message=message,
                    payload={"group": group, "row_count": len(rows)},
                )
                state.runner.status_message = f"Copied group message for {group.get('name') or group.get('target') or 'group'}"
                state.runner.append_log(state.runner.status_message)
            imgui.same_line()
            if imgui.button("Sent", (62, 0)):
                record_antidengue_dispatch_event(
                    state,
                    run_key=run_key,
                    channel="group",
                    action="marked_sent",
                    target=str(group.get("target") or ""),
                    recipient_name=str(group.get("name") or ""),
                    status="manual_sent",
                    message=message,
                    payload={"group": group, "row_count": len(rows)},
                )
                state.runner.status_message = f"Marked group dispatch sent for {group.get('name') or group.get('target') or 'group'}"
                state.runner.append_log(state.runner.status_message)

            imgui.table_next_column()
            imgui.text(str(len(rows)))
            imgui.pop_id()
        imgui.end_table()

def draw_antidengue_dispatch_center_section(state: GuiState) -> None:
    settings = draw_antidengue_dispatch_settings_controls(state)
    if settings is None:
        return

    context, context_error = load_antidengue_latest_dispatch_context(state)
    if context_error:
        imgui.text_colored(status_badge_color("warning"), context_error)
        return

    imgui.separator()
    subviews = (
        ("overview", "Overview", 110),
        ("individuals", "Individuals", 125),
        ("groups", "Groups", 100),
    )
    current = state.antidengue_dispatch_view
    if current not in {item[0] for item in subviews}:
        current = "overview"
    for index, (view_id, label, width) in enumerate(subviews):
        if index:
            imgui.same_line()
        selected = current == view_id
        with disabled_when(selected):
            if imgui.button(f"{label}{' *' if selected else ''}", (width, 0)):
                current = view_id
                state.antidengue_dispatch_view = view_id
    imgui.separator()

    if current == "individuals":
        draw_antidengue_dispatch_individuals(state, context)
    elif current == "groups":
        draw_antidengue_dispatch_groups(state, context)
    else:
        draw_antidengue_dispatch_overview(state, context, settings)

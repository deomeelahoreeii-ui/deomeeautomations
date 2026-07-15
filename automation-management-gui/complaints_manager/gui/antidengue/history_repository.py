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

from complaints_manager.gui.antidengue.database import _now_iso, _pocketbase_report_runs_has_lifecycle
from complaints_manager.gui.antidengue.health_repository import open_pocketbase_read_connection
from complaints_manager.gui.antidengue.runtime import antidengue_pocketbase_root
from complaints_manager.gui.common.formatting import _safe_json_loads

def set_antidengue_run_archived(
    state: GuiState,
    run_key: str,
    archived: bool,
) -> tuple[bool, str]:
    db_path = antidengue_pocketbase_root(state) / "pb_data" / "data.db"
    if not db_path.is_file():
        return False, f"PocketBase database not found: {db_path}"
    try:
        with sqlite3.connect(str(db_path), timeout=5) as conn:
            if not _pocketbase_report_runs_has_lifecycle(conn):
                return False, "Run lifecycle migration is not applied. Open Database > Apply Migrations."
            conn.execute(
                """
                update report_runs
                   set archived = ?,
                       archived_at = ?
                 where started_at = ?
                """,
                (1 if archived else 0, _now_iso() if archived else "", run_key),
            )
    except sqlite3.Error as exc:
        return False, f"Could not update run archive state: {exc}"
    return True, "Archived run." if archived else "Restored run."

def delete_antidengue_run_history(state: GuiState, run_key: str) -> tuple[bool, str]:
    db_path = antidengue_pocketbase_root(state) / "pb_data" / "data.db"
    if not db_path.is_file():
        return False, f"PocketBase database not found: {db_path}"
    try:
        with sqlite3.connect(str(db_path), timeout=5) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "select name from sqlite_master where type = 'table'"
                ).fetchall()
            }
            for table in (
                "dormant_records",
                "delivery_events",
                "data_quality_issues",
                "run_stage_events",
                "dispatch_plan_items",
            ):
                if table not in tables:
                    continue
                conn.execute(f"delete from {table} where run_key = ?", (run_key,))
            conn.execute("delete from report_runs where started_at = ?", (run_key,))
    except sqlite3.Error as exc:
        return False, f"Could not delete run history: {exc}"
    if state.antidengue_history_selected_run_key == run_key:
        state.antidengue_history_selected_run_key = ""
    if state.antidengue_history_pending_delete_run_key == run_key:
        state.antidengue_history_pending_delete_run_key = ""
    return True, "Deleted run history and related records."

def clear_archived_antidengue_runs(state: GuiState) -> tuple[bool, str]:
    db_path = antidengue_pocketbase_root(state) / "pb_data" / "data.db"
    if not db_path.is_file():
        return False, f"PocketBase database not found: {db_path}"
    try:
        with sqlite3.connect(str(db_path), timeout=5) as conn:
            if not _pocketbase_report_runs_has_lifecycle(conn):
                return False, "Run lifecycle migration is not applied. Open Database > Apply Migrations."
            run_keys = [
                str(row[0])
                for row in conn.execute(
                    "select started_at from report_runs where archived = 1"
                ).fetchall()
            ]
            tables = {
                row[0]
                for row in conn.execute(
                    "select name from sqlite_master where type = 'table'"
                ).fetchall()
            }
            for run_key in run_keys:
                for table in (
                    "dormant_records",
                    "delivery_events",
                    "data_quality_issues",
                    "run_stage_events",
                    "dispatch_plan_items",
                ):
                    if table not in tables:
                        continue
                    conn.execute(f"delete from {table} where run_key = ?", (run_key,))
                conn.execute("delete from report_runs where started_at = ?", (run_key,))
    except sqlite3.Error as exc:
        return False, f"Could not clear archived runs: {exc}"
    state.antidengue_history_selected_run_key = ""
    state.antidengue_history_pending_delete_run_key = ""
    return True, f"Deleted {len(run_keys)} archived run(s)."

def _antidengue_run_key(started_at: object) -> str:
    return re.sub(r"[^0-9A-Za-z_.:-]+", "_", str(started_at or ""))[:150]

def load_antidengue_run_history(state: GuiState, limit: int = 50) -> tuple[list[dict], str]:
    db_path = antidengue_pocketbase_root(state) / "pb_data" / "data.db"
    if not db_path.is_file():
        return [], f"PocketBase database has not been initialized yet: {db_path}"

    try:
        with open_pocketbase_read_connection(db_path, state) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "select name from sqlite_master where type = 'table'"
                ).fetchall()
            }
            required = {
                "report_runs",
                "dormant_records",
                "delivery_events",
                "data_quality_issues",
            }
            missing = required.difference(tables)
            if missing:
                return [], "PocketBase is missing collections: " + ", ".join(sorted(missing))

            has_lifecycle = _pocketbase_report_runs_has_lifecycle(conn)
            has_stage_events = "run_stage_events" in tables
            has_dispatch_plan = "dispatch_plan_items" in tables
            filter_clause = ""
            filter_params: tuple = ()
            if has_lifecycle:
                if state.antidengue_history_filter == "archived":
                    filter_clause = "where coalesce(rr.archived, 0) = 1"
                elif state.antidengue_history_filter != "all":
                    filter_clause = "where coalesce(rr.archived, 0) = 0"

            rows = conn.execute(
                f"""
                select
                  rr.id,
                  rr.started_at,
                  rr.finished_at,
                  rr.source,
                  rr.status,
                  rr.raw_file_name,
                  rr.raw_file_sha256,
                  rr.summary,
                  {"coalesce(rr.archived, 0)" if has_lifecycle else "0"} as archived,
                  {"rr.archived_at" if has_lifecycle else "''"} as archived_at,
                  (select count(*) from dormant_records dr where dr.run_key = rr.started_at) as dormant_count,
                  (select count(*) from delivery_events de where de.run_key = rr.started_at) as delivery_count,
                  (select count(*) from delivery_events de where de.run_key = rr.started_at and lower(de.status) = 'delivered') as delivered_count,
                  (select count(*) from delivery_events de where de.run_key = rr.started_at and lower(de.status) != 'delivered') as failed_count,
                  (select count(*) from data_quality_issues qi where qi.run_key = rr.started_at) as issue_count,
                  {"(select count(*) from run_stage_events se where se.run_key = rr.started_at)" if has_stage_events else "0"} as stage_count,
                  {"(select count(*) from dispatch_plan_items dpi where dpi.run_key = rr.started_at)" if has_dispatch_plan else "0"} as dispatch_plan_count
                from report_runs rr
                {filter_clause}
                order by rr.started_at desc
                limit ?
                """,
                (*filter_params, limit),
            ).fetchall()
    except sqlite3.Error as exc:
        return [], (
            f"Could not read PocketBase run history: {exc}. "
            f"path={db_path}, exists={db_path.exists()}, parent_exists={db_path.parent.exists()}, "
            f"project_root={state.project_root}"
        )

    history: list[dict] = []
    for row in rows:
        item = dict(row)
        item["run_key"] = _antidengue_run_key(item.get("started_at"))
        item["summary_dict"] = _safe_json_loads(item.get("summary"))
        history.append(item)
    return history, ""

def load_antidengue_run_details(
    state: GuiState,
    run_key: str,
) -> tuple[dict[str, list[dict]], str]:
    db_path = antidengue_pocketbase_root(state) / "pb_data" / "data.db"
    if not db_path.is_file():
        return {}, f"PocketBase database has not been initialized yet: {db_path}"

    try:
        with open_pocketbase_read_connection(db_path, state) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "select name from sqlite_master where type = 'table'"
                ).fetchall()
            }
            summary_row = conn.execute(
                "select summary from report_runs where started_at = ?",
                (run_key,),
            ).fetchone()
            summary_dict = _safe_json_loads(summary_row["summary"]) if summary_row else {}
            dormant_columns = {
                str(row[1])
                for row in conn.execute("pragma table_info(dormant_records)").fetchall()
            }
            if "school_ref" in dormant_columns:
                dormant_sql = """
                    select
                      s.emis as school_emis,
                      s.name as school_name,
                      t.name as tehsil,
                      m.name as markaz,
                      dr.row_data
                    from dormant_records dr
                    left join schools s
                      on s.id = dr.school_ref
                    left join tehsils t
                      on t.id = s.tehsil_ref
                    left join markazes m
                      on m.id = s.markaz_ref
                    where dr.run_key = ?
                    order by t.name, m.name, s.name
                    limit 80
                """
            else:
                dormant_sql = """
                    select school_emis, school_name, tehsil, markaz, row_data
                    from dormant_records
                    where run_key = ?
                    order by tehsil, markaz, school_name
                    limit 80
                """
            dormant_rows = [
                dict(row)
                for row in conn.execute(dormant_sql, (run_key,)).fetchall()
            ]
            delivery_rows = [
                dict(row)
                for row in conn.execute(
                    """
                    select job_id, target, recipient_name, role, status, cause, payload
                    from delivery_events
                    where run_key = ?
                    order by
                      case when lower(status) = 'delivered' then 1 else 0 end,
                      role,
                      recipient_name
                    limit 80
                    """,
                    (run_key,),
                ).fetchall()
            ]
            quality_rows = [
                dict(row)
                for row in conn.execute(
                    """
                    select severity, category, message, context
                    from data_quality_issues
                    where run_key = ?
                    order by severity desc, category, message
                    limit 100
                    """,
                    (run_key,),
                ).fetchall()
            ]
            if "run_stage_events" in tables:
                lifecycle_rows = [
                    dict(row)
                    for row in conn.execute(
                        """
                        select stage, status, message, payload, created_at
                        from run_stage_events
                        where run_key = ?
                        order by created_at, id
                        limit 120
                        """,
                        (run_key,),
                    ).fetchall()
                ]
            else:
                lifecycle_rows = []
            if not lifecycle_rows:
                lifecycle_rows = [
                    row
                    for row in summary_dict.get("lifecycle") or []
                    if isinstance(row, dict)
                ][:120]

            if "dispatch_plan_items" in tables:
                dispatch_plan_rows = [
                    dict(row)
                    for row in conn.execute(
                        """
                        select
                          job_id,
                          channel,
                          recipient_name,
                          target,
                          route_kind,
                          message_mode,
                          row_count,
                          excel_path,
                          status,
                          cause,
                          payload,
                          created_at,
                          updated_at
                        from dispatch_plan_items
                        where run_key = ?
                        order by
                          case channel
                            when 'group' then 0
                            when 'fallback' then 1
                            else 2
                          end,
                          recipient_name,
                          target
                        limit 160
                        """,
                        (run_key,),
                    ).fetchall()
                ]
            else:
                dispatch_plan_rows = []
            if not dispatch_plan_rows:
                dispatch_plan_rows = [
                    row
                    for row in (
                        (summary_dict.get("whatsapp") or {}).get("dispatch_plan") or []
                    )
                    if isinstance(row, dict)
                ][:160]
    except sqlite3.Error as exc:
        return {}, (
            f"Could not read selected run details: {exc}. "
            f"path={db_path}, exists={db_path.exists()}, parent_exists={db_path.parent.exists()}, "
            f"project_root={state.project_root}"
        )

    for row in delivery_rows:
        row["payload_dict"] = _safe_json_loads(row.get("payload"))
    for row in dormant_rows:
        row["row_data_dict"] = _safe_json_loads(row.get("row_data"))
    for row in lifecycle_rows:
        row["payload_dict"] = _safe_json_loads(row.get("payload"))
    for row in dispatch_plan_rows:
        row["payload_dict"] = _safe_json_loads(row.get("payload"))
    return {
        "dormant": dormant_rows,
        "delivery": delivery_rows,
        "quality": quality_rows,
        "lifecycle": lifecycle_rows,
        "dispatch_plan": dispatch_plan_rows,
    }, ""

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

from complaints_manager.gui.antidengue.runtime import antidengue_pocketbase_root

def antidengue_pocketbase_counts(state: GuiState) -> tuple[dict[str, int], str]:
    db_path = antidengue_pocketbase_root(state) / "pb_data" / "data.db"
    if not db_path.is_file():
        return {}, f"PocketBase database has not been initialized yet: {db_path}"
    tables = (
        "districts",
        "departments",
        "wings",
        "tehsils",
        "markazes",
        "schools",
        "ddeo_officers",
        "aeo_officers",
        "ddeo_jurisdictions",
        "aeo_jurisdictions",
        "school_ddeo_overrides",
        "school_aeo_overrides",
        "whatsapp_recipients",
        "officer_import_batches",
        "report_runs",
        "dormant_records",
        "delivery_events",
        "data_quality_issues",
        "dispatch_settings",
        "whatsapp_groups",
        "message_templates",
        "dispatch_events",
    )
    counts: dict[str, int] = {}
    try:
        with open_pocketbase_read_connection(db_path, state) as conn:
            existing = {
                row[0]
                for row in conn.execute(
                    "select name from sqlite_master where type = 'table'"
                ).fetchall()
            }
            for table in tables:
                if table in existing:
                    counts[table] = int(
                        conn.execute(f"select count(*) from {table}").fetchone()[0]
                    )
                else:
                    counts[table] = 0
    except sqlite3.Error as exc:
        return {}, (
            f"Could not read PocketBase database: {exc}. "
            f"path={db_path}, exists={db_path.exists()}, parent_exists={db_path.parent.exists()}, "
            f"project_root={state.project_root}"
        )
    return counts, ""

def antidengue_pocketbase_health_checks(state: GuiState) -> tuple[list[dict], str]:
    db_path = antidengue_pocketbase_root(state) / "pb_data" / "data.db"
    if not db_path.is_file():
        return [], f"PocketBase database has not been initialized yet: {db_path}"

    checks: list[dict] = []

    def add_check(severity: str, name: str, count: int, message: str) -> None:
        checks.append(
            {
                "severity": severity,
                "name": name,
                "count": count,
                "message": message,
            }
        )

    def severity_for_count(count: int, *, warning: bool = False) -> str:
        if count <= 0:
            return "ok"
        return "warning" if warning else "failed"

    try:
        with open_pocketbase_read_connection(db_path, state) as conn:
            existing = {
                row[0]
                for row in conn.execute(
                    "select name from sqlite_master where type = 'table'"
                ).fetchall()
            }
            required_tables = {
                "schools",
                "districts",
                "departments",
                "wings",
                "tehsils",
                "markazes",
                "ddeo_officers",
                "aeo_officers",
                "ddeo_jurisdictions",
                "aeo_jurisdictions",
                "school_ddeo_overrides",
                "school_aeo_overrides",
                "whatsapp_recipients",
                "dispatch_settings",
                "whatsapp_groups",
                "message_templates",
                "dispatch_events",
            }
            missing_tables = sorted(required_tables.difference(existing))
            if missing_tables:
                add_check(
                    "failed",
                    "Required collections",
                    len(missing_tables),
                    "Missing: " + ", ".join(missing_tables),
                )
                return checks, ""

            required_migration = "202607020007_required_refs.js"
            migration_count = int(
                conn.execute(
                    "select count(*) from _migrations where file = ?",
                    (required_migration,),
                ).fetchone()[0]
            )
            add_check(
                "ok" if migration_count else "warning",
                "Required-ref migration",
                0 if migration_count else 1,
                "Applied" if migration_count else "Open Database > Apply Migrations.",
            )

            query_checks = [
                (
                    "School hierarchy refs",
                    """
                    select count(*) from schools
                    where active = 1
                      and (
                        district_ref = '' or department_ref = '' or wing_ref = ''
                        or tehsil_ref = '' or markaz_ref = ''
                      )
                    """,
                    "Active schools must have district/department/wing/tehsil/markaz refs.",
                    False,
                ),
                (
                    "School report fields",
                    """
                    select count(*) from schools
                    where active = 1
                      and (
                        school_type = '' or deos_wise = '' or school_level = ''
                      )
                    """,
                    "Active schools should have report-facing type/wing/level fields.",
                    True,
                ),
                (
                    "School head contacts",
                    """
                    select count(*) from schools
                    where active = 1
                      and (head_name = '' or head_contact = '')
                    """,
                    "Active schools should have head name and contact for follow-up.",
                    True,
                ),
                (
                    "DDEO jurisdiction refs",
                    """
                    select count(*) from ddeo_jurisdictions
                    where active = 1
                      and (
                        ddeo_ref = '' or district_ref = '' or department_ref = ''
                        or wing_ref = '' or tehsil_ref = ''
                      )
                    """,
                    "Active DDEO jurisdictions must be fully linked.",
                    False,
                ),
                (
                    "AEO jurisdiction refs",
                    """
                    select count(*) from aeo_jurisdictions
                    where active = 1
                      and (
                        aeo_ref = '' or district_ref = '' or department_ref = ''
                        or wing_ref = '' or tehsil_ref = '' or markaz_ref = ''
                      )
                    """,
                    "Active AEO jurisdictions must be fully linked.",
                    False,
                ),
                (
                    "Schools without DDEO",
                    """
                    select count(*) from schools s
                    where s.active = 1
                      and not exists (
                        select 1
                        from school_ddeo_overrides o
                        join ddeo_officers d on d.id = o.ddeo_ref and d.active = 1
                        where o.school_ref = s.id and o.active = 1
                      )
                      and not exists (
                        select 1
                        from ddeo_jurisdictions j
                        join ddeo_officers d on d.id = j.ddeo_ref and d.active = 1
                        where j.wing_ref = s.wing_ref
                          and j.tehsil_ref = s.tehsil_ref
                          and j.active = 1
                      )
                    """,
                    "Every active school should resolve to a DDEO.",
                    False,
                ),
                (
                    "Schools without AEO",
                    """
                    select count(*) from schools s
                    where s.active = 1
                      and not exists (
                        select 1
                        from school_aeo_overrides o
                        join aeo_officers a on a.id = o.aeo_ref and a.active = 1
                        where o.school_ref = s.id and o.active = 1
                      )
                      and not exists (
                        select 1
                        from aeo_jurisdictions j
                        join aeo_officers a on a.id = j.aeo_ref and a.active = 1
                        where j.wing_ref = s.wing_ref
                          and j.markaz_ref = s.markaz_ref
                          and (j.tehsil_ref = '' or j.tehsil_ref = s.tehsil_ref)
                          and j.active = 1
                      )
                    """,
                    "Every active school should resolve to an AEO.",
                    False,
                ),
                (
                    "Invalid officer mobiles",
                    """
                    select
                      (select count(*) from ddeo_officers
                       where active = 1
                         and (length(normalized_mobile) != 12 or substr(normalized_mobile, 1, 3) != '923'))
                      +
                      (select count(*) from aeo_officers
                       where active = 1
                         and (length(normalized_mobile) != 12 or substr(normalized_mobile, 1, 3) != '923'))
                    """,
                    "Officer mobiles must be normalized Pakistani WhatsApp numbers.",
                    False,
                ),
                (
                    "Invalid recipients",
                    """
                    select count(*) from whatsapp_recipients
                    where enabled = 1
                      and (
                        target = ''
                        or (type = 'group' and target not like '%@g.us')
                      )
                    """,
                    "Enabled fixed/group recipients must have valid targets.",
                    False,
                ),
                (
                    "Dispatch settings",
                    """
                    select count(*) from dispatch_settings
                    where active = 1
                    """,
                    "At least one active Dispatch Center settings row should exist.",
                    False,
                ),
                (
                    "Enabled dispatch groups",
                    """
                    select count(*) from whatsapp_groups
                    where enabled = 1
                    """,
                    "At least one group route should be enabled for group/fallback dispatch.",
                    True,
                ),
            ]

            for name, sql, message, warning in query_checks:
                count = int(conn.execute(sql).fetchone()[0])
                if name in {"Dispatch settings", "Enabled dispatch groups"}:
                    severity = "ok" if count else ("warning" if warning else "failed")
                else:
                    severity = severity_for_count(count, warning=warning)
                add_check(severity, name, count, message)

            duplicate_ddeo_rows = conn.execute(
                """
                select
                  w.name as wing,
                  t.name as tehsil,
                  count(distinct j.ddeo_ref) as owners,
                  group_concat(distinct d.name || ' ' || d.normalized_mobile) as officers
                from ddeo_jurisdictions j
                join wings w on w.id = j.wing_ref
                join tehsils t on t.id = j.tehsil_ref
                join ddeo_officers d on d.id = j.ddeo_ref
                where j.active = 1
                group by j.wing_ref, j.tehsil_ref
                having owners > 1
                order by t.name
                """
            ).fetchall()
            duplicate_ddeo_message = "A wing/tehsil should not have multiple active DDEO owners."
            if duplicate_ddeo_rows:
                samples = [
                    f"{row['tehsil']} ({row['owners']} owners: {row['officers']})"
                    for row in duplicate_ddeo_rows[:2]
                ]
                duplicate_ddeo_message += " " + " | ".join(samples)
            add_check(
                severity_for_count(len(duplicate_ddeo_rows)),
                "Duplicate DDEO jurisdictions",
                len(duplicate_ddeo_rows),
                duplicate_ddeo_message,
            )

            duplicate_aeo_rows = conn.execute(
                """
                select
                  w.name as wing,
                  t.name as tehsil,
                  m.name as markaz,
                  count(distinct j.aeo_ref) as owners,
                  group_concat(distinct a.name || ' ' || a.normalized_mobile) as officers
                from aeo_jurisdictions j
                join wings w on w.id = j.wing_ref
                join tehsils t on t.id = j.tehsil_ref
                join markazes m on m.id = j.markaz_ref
                join aeo_officers a on a.id = j.aeo_ref
                where j.active = 1
                group by j.wing_ref, j.tehsil_ref, j.markaz_ref
                having owners > 1
                order by t.name, m.name
                """
            ).fetchall()
            duplicate_aeo_message = "A wing/tehsil/markaz should not have multiple active AEO owners."
            if duplicate_aeo_rows:
                samples = [
                    f"{row['tehsil']} / {row['markaz']} ({row['owners']} owners: {row['officers']})"
                    for row in duplicate_aeo_rows[:2]
                ]
                duplicate_aeo_message += " " + " | ".join(samples)
            add_check(
                severity_for_count(len(duplicate_aeo_rows)),
                "Duplicate AEO jurisdictions",
                len(duplicate_aeo_rows),
                duplicate_aeo_message,
            )

            enabled_recipients = int(
                conn.execute(
                    "select count(*) from whatsapp_recipients where enabled = 1"
                ).fetchone()[0]
            )
            add_check(
                "ok" if enabled_recipients else "failed",
                "Enabled fixed recipients",
                enabled_recipients,
                "At least one enabled fixed/group recipient should exist.",
            )
    except sqlite3.Error as exc:
        return [], (
            f"Could not run PocketBase health checks: {exc}. "
            f"path={db_path}, exists={db_path.exists()}, parent_exists={db_path.parent.exists()}"
        )

    return checks, ""

@contextmanager
def open_pocketbase_read_connection(db_path: Path, state: GuiState):
    live_error = ""
    conn = None
    try:
        conn = sqlite3.connect(str(db_path), timeout=2)
        conn.row_factory = sqlite3.Row
        conn.execute("pragma query_only = 1")
    except sqlite3.Error as exc:
        live_error = str(exc)
    else:
        try:
            yield conn
        finally:
            conn.close()
        return

    snapshot_path = Path(tempfile.gettempdir()) / f"antidengue-pocketbase-{uuid.uuid4().hex}.db"
    snapshot_conn = None
    try:
        shutil.copy2(db_path, snapshot_path)
        snapshot_conn = sqlite3.connect(str(snapshot_path), timeout=2)
        snapshot_conn.row_factory = sqlite3.Row
        snapshot_conn.execute("pragma query_only = 1")
    except (OSError, sqlite3.Error) as exc:
        raise sqlite3.OperationalError(
            "unable to open live database or snapshot. "
            f"live_error={live_error}; snapshot_error={exc}; "
            f"path={db_path}; snapshot={snapshot_path}; "
            f"exists={db_path.exists()}; parent_exists={db_path.parent.exists()}; "
            f"project_root={state.project_root}"
        ) from exc
    try:
        yield snapshot_conn
    finally:
        snapshot_conn.close()
        try:
            snapshot_path.unlink(missing_ok=True)
        except OSError:
            pass

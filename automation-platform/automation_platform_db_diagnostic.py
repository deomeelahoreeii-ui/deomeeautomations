#!/usr/bin/env python3
"""Create a read-only diagnostic bundle for the Deomee Automation Platform DB.

Recommended usage from the automation-platform directory:

    uv run python /path/to/automation_platform_db_diagnostic.py \
      --preview-key AD-20260719-085555-0E335C

The script creates a ZIP containing:
- database schema, constraints, indexes and row counts
- sanitized runtime/environment metadata
- samples from every table
- complete configuration tables
- a deep, cross-linked report for selected/latest WhatsApp previews
- count-consistency and issue summaries

It never writes to the database and never exports environment credentials.
Personal identifiers are redacted by default. Use --include-sensitive only if the
exact names, phone numbers, JIDs and message bodies are genuinely needed.
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import decimal
import hashlib
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import traceback
import uuid
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import quote

SCRIPT_VERSION = "2026.07.19.2"

SECRET_ENV_RE = re.compile(
    r"(?:PASSWORD|PASSWD|TOKEN|SECRET|ACCESS_KEY|API_KEY|PRIVATE_KEY|COOKIE|AUTHORIZATION|CREDENTIAL)",
    re.IGNORECASE,
)
SECRET_COLUMN_RE = re.compile(
    r"(?:password|passwd|secret|access[_-]?key|api[_-]?key|(?:^|_)token(?:$|_)|auth[_-]?token|bearer|credential|cookie)",
    re.IGNORECASE,
)
PERSONAL_COLUMN_RE = re.compile(
    r"(?:^|_)(?:mobile|phone|telephone|jid|target|email|cnic|address)(?:$|_)",
    re.IGNORECASE,
)
MESSAGE_COLUMN_RE = re.compile(
    r"(?:^|_)(?:message|body|text|description|notes|error|details|result|parameters|snapshot|issues)(?:$|_)",
    re.IGNORECASE,
)
DATE_COLUMN_PREFERENCE = (
    "created_at",
    "updated_at",
    "finished_at",
    "started_at",
    "frozen_at",
    "queued_at",
    "approved_at",
    "completed_at",
    "last_seen_at",
    "id",
)

FULL_EXPORT_TABLES = {
    "alembic_version",
    "whatsapp_accounts",
    "whatsapp_applications",
    "whatsapp_report_types",
    "whatsapp_recipient_scopes",
    "whatsapp_audiences",
    "whatsapp_audience_members",
    "whatsapp_audience_sources",
    "whatsapp_templates",
    "whatsapp_dispatch_profiles",
    "whatsapp_settings",
    "whatsapp_directory_groups",
    "whatsapp_directory_contacts",
    "whatsapp_identity_aliases",
    "whatsapp_group_members",
    "whatsapp_groups",
    "whatsapp_contact_links",
    "districts",
    "departments",
    "wings",
    "tehsils",
    "markazes",
    "officers",
    "officer_jurisdictions",
    "school_officer_overrides",
    "antidengue_deadline_policies",
    "antidengue_schedules",
    "antidengue_activity_rules",
    "antidengue_simple_activity_rules",
    "automation_worker_runtimes",
}

OPERATIONAL_TABLE_LIMITS = {
    "jobs": 1000,
    "job_logs": 3000,
    "task_outbox": 1000,
    "antidengue_schedule_executions": 1000,
    "antidengue_schedule_events": 3000,
    "whatsapp_dispatch_previews": 500,
    "whatsapp_dispatch_preview_artifacts": 3000,
    "whatsapp_dispatch_preview_deliveries": 10000,
    "whatsapp_dispatch_approvals": 1000,
    "whatsapp_deliveries": 3000,
    "whatsapp_activity": 3000,
}


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def find_project_root(start: Path) -> Path:
    candidates = [start.resolve(), *start.resolve().parents]
    for candidate in candidates:
        if (candidate / "pyproject.toml").is_file() and (candidate / "packages").is_dir():
            return candidate
    child = start.resolve() / "automation-platform"
    if (child / "pyproject.toml").is_file():
        return child
    return start.resolve()


def parse_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and value[0] in {'"', "'"} and value[-1:] == value[0]:
            value = value[1:-1]
        values[key] = value
    return values


def resolve_database_url(args: argparse.Namespace, root: Path) -> tuple[str, str]:
    if args.database_url:
        return args.database_url, "--database-url"
    if os.environ.get("DATABASE_URL"):
        return os.environ["DATABASE_URL"], "environment"
    env_values = parse_dotenv(root / ".env")
    if env_values.get("DATABASE_URL"):
        return env_values["DATABASE_URL"], str(root / ".env")
    default = f"sqlite:///{(root / 'data' / 'automation-platform.db').resolve()}"
    return default, "project default"


def safe_db_url(url: str) -> str:
    try:
        from sqlalchemy.engine import make_url

        parsed = make_url(url)
        return parsed.render_as_string(hide_password=True)
    except Exception:
        return re.sub(r"(?<=://)([^:/@]+):([^@]+)@", r"\1:***@", url)


def json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, (uuid.UUID, Path, decimal.Decimal)):
        return str(value)
    if isinstance(value, bytes):
        return {"__bytes_base64__": base64.b64encode(value[:4096]).decode("ascii"), "length": len(value)}
    if isinstance(value, Mapping):
        return {str(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [json_value(v) for v in value]
    return str(value)


def stable_redaction(value: Any) -> str:
    raw = str(value)
    digest = hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:16]
    suffix = re.sub(r"\D", "", raw)[-4:]
    return f"<redacted sha256:{digest}{' last4:' + suffix if suffix else ''}>"


def redact_nested(value: Any, *, include_sensitive: bool, key_hint: str = "") -> Any:
    if include_sensitive:
        return json_value(value)
    if value is None:
        return None
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            key_str = str(key)
            if SECRET_COLUMN_RE.search(key_str):
                out[key_str] = "<secret omitted>"
            elif PERSONAL_COLUMN_RE.search(key_str):
                out[key_str] = stable_redaction(item) if item not in (None, "") else item
            elif MESSAGE_COLUMN_RE.search(key_str):
                out[key_str] = redact_nested(item, include_sensitive=False, key_hint=key_str)
            else:
                out[key_str] = redact_nested(item, include_sensitive=False, key_hint=key_str)
        return out
    if isinstance(value, (list, tuple, set, frozenset)):
        return [redact_nested(v, include_sensitive=False, key_hint=key_hint) for v in value]
    if PERSONAL_COLUMN_RE.search(key_hint):
        return stable_redaction(value)
    return json_value(value)


def sanitize_row(row: Mapping[str, Any], *, include_sensitive: bool) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for column, value in row.items():
        name = str(column)
        if SECRET_COLUMN_RE.search(name):
            result[name] = "<secret omitted>"
            continue
        if not include_sensitive and PERSONAL_COLUMN_RE.search(name):
            result[name] = stable_redaction(value) if value not in (None, "") else value
            continue
        if not include_sensitive and name.lower() in {"message", "body"}:
            text = "" if value is None else str(value)
            result[name] = {
                "redacted": True,
                "length": len(text),
                "sha256": hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest(),
            }
            continue
        result[name] = redact_nested(value, include_sensitive=include_sensitive, key_hint=name)
    return result


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_value(data), indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v for k, v in row.items()})


def run_command(args: Sequence[str], cwd: Path, timeout: int = 10) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            list(args),
            cwd=cwd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
        return {
            "command": list(args),
            "returncode": proc.returncode,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
        }
    except Exception as exc:
        return {"command": list(args), "error": f"{type(exc).__name__}: {exc}"}


def get_table_names(inspector: Any, dialect: str) -> list[tuple[str | None, str]]:
    schemas: list[str | None]
    if dialect == "postgresql":
        schemas = [s for s in inspector.get_schema_names() if s not in {"information_schema"} and not s.startswith("pg_")]
        if "public" in schemas:
            schemas = ["public", *[s for s in schemas if s != "public"]]
    else:
        schemas = [None]
    result: list[tuple[str | None, str]] = []
    for schema in schemas:
        for table in inspector.get_table_names(schema=schema):
            result.append((schema, table))
    return result


def qname(preparer: Any, schema: str | None, table: str) -> str:
    quoted_table = preparer.quote_identifier(table)
    if schema:
        return f"{preparer.quote_identifier(schema)}.{quoted_table}"
    return quoted_table


def schema_snapshot(inspector: Any, table_pairs: Sequence[tuple[str | None, str]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for schema, table in table_pairs:
        key = f"{schema}.{table}" if schema else table
        try:
            output[key] = {
                "schema": schema,
                "table": table,
                "columns": [
                    {
                        "name": col.get("name"),
                        "type": str(col.get("type")),
                        "nullable": col.get("nullable"),
                        "default": str(col.get("default")) if col.get("default") is not None else None,
                        "computed": json_value(col.get("computed")),
                        "identity": json_value(col.get("identity")),
                    }
                    for col in inspector.get_columns(table, schema=schema)
                ],
                "primary_key": inspector.get_pk_constraint(table, schema=schema),
                "foreign_keys": inspector.get_foreign_keys(table, schema=schema),
                "indexes": inspector.get_indexes(table, schema=schema),
                "unique_constraints": inspector.get_unique_constraints(table, schema=schema),
                "check_constraints": inspector.get_check_constraints(table, schema=schema),
            }
        except Exception as exc:
            output[key] = {"schema": schema, "table": table, "error": f"{type(exc).__name__}: {exc}"}
    return output


def fetch_mappings(conn: Any, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    from sqlalchemy import text

    return [dict(row) for row in conn.execute(text(sql), params or {}).mappings().all()]


def scalar(conn: Any, sql: str, params: Mapping[str, Any] | None = None) -> Any:
    from sqlalchemy import text

    return conn.execute(text(sql), params or {}).scalar()


def choose_order_column(column_names: set[str]) -> str | None:
    return next((name for name in DATE_COLUMN_PREFERENCE if name in column_names), None)


def dump_table(
    conn: Any,
    preparer: Any,
    schema: str | None,
    table: str,
    columns: Sequence[Mapping[str, Any]],
    row_count: int,
    default_limit: int,
    include_sensitive: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    names = {str(c["name"]) for c in columns}
    if table in FULL_EXPORT_TABLES:
        limit = max(row_count, 1)
        mode = "complete"
    else:
        limit = OPERATIONAL_TABLE_LIMITS.get(table, default_limit)
        mode = "sample"
    limit = min(max(limit, 1), 50000)
    order_col = choose_order_column(names)
    query = f"SELECT * FROM {qname(preparer, schema, table)}"
    if order_col:
        query += f" ORDER BY {preparer.quote_identifier(order_col)} DESC"
    query += " LIMIT :limit"
    rows = fetch_mappings(conn, query, {"limit": limit})
    sanitized = [sanitize_row(row, include_sensitive=include_sensitive) for row in rows]
    meta = {
        "row_count": row_count,
        "exported_rows": len(sanitized),
        "mode": mode,
        "limit": limit,
        "order_by": order_col,
        "truncated": row_count > len(sanitized),
    }
    return sanitized, meta


def parse_jsonish(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                return value
    return value


def issue_counter(rows: Iterable[Mapping[str, Any]], field: str = "issues") -> Counter[tuple[str, str, str]]:
    counts: Counter[tuple[str, str, str]] = Counter()
    for row in rows:
        issues = parse_jsonish(row.get(field))
        if isinstance(issues, list):
            for issue in issues:
                if not isinstance(issue, Mapping):
                    continue
                severity = str(issue.get("severity") or "unknown")
                code = str(issue.get("code") or "unknown")
                message = str(issue.get("message") or "")
                counts[(severity, code, message)] += 1
    return counts


def summarize_issues(*groups: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    total: Counter[tuple[str, str, str]] = Counter()
    for group in groups:
        total.update(issue_counter(group))
    return [
        {"severity": severity, "code": code, "message": message, "occurrences": count}
        for (severity, code, message), count in sorted(total.items(), key=lambda x: (-x[1], x[0]))
    ]


def table_exists(table_names: set[str], table: str) -> bool:
    return table in table_names


def select_by_ids(conn: Any, preparer: Any, table: str, column: str, ids: Sequence[Any], schema: str | None = None) -> list[dict[str, Any]]:
    if not ids:
        return []
    placeholders = ", ".join(f":v{i}" for i in range(len(ids)))
    params = {f"v{i}": value for i, value in enumerate(ids)}
    return fetch_mappings(
        conn,
        f"SELECT * FROM {qname(preparer, schema, table)} WHERE {preparer.quote_identifier(column)} IN ({placeholders})",
        params,
    )


def deep_preview_report(
    conn: Any,
    preparer: Any,
    table_names: set[str],
    preview_keys: Sequence[str],
    latest_previews: int,
    include_sensitive: bool,
    verify_files: bool,
) -> dict[str, Any]:
    if "whatsapp_dispatch_previews" not in table_names:
        return {"available": False, "reason": "whatsapp_dispatch_previews table not found"}

    if preview_keys:
        placeholders = ", ".join(f":k{i}" for i in range(len(preview_keys)))
        params = {f"k{i}": value for i, value in enumerate(preview_keys)}
        preview_rows = fetch_mappings(
            conn,
            f"SELECT * FROM whatsapp_dispatch_previews WHERE preview_key IN ({placeholders}) ORDER BY created_at DESC",
            params,
        )
    else:
        preview_rows = fetch_mappings(
            conn,
            "SELECT * FROM whatsapp_dispatch_previews ORDER BY created_at DESC LIMIT :limit",
            {"limit": latest_previews},
        )

    reports: list[dict[str, Any]] = []
    for preview in preview_rows:
        preview_id = preview.get("id")
        deliveries = (
            fetch_mappings(
                conn,
                "SELECT * FROM whatsapp_dispatch_preview_deliveries WHERE preview_id = :preview_id ORDER BY sequence, created_at",
                {"preview_id": preview_id},
            )
            if table_exists(table_names, "whatsapp_dispatch_preview_deliveries")
            else []
        )
        artifacts = (
            fetch_mappings(
                conn,
                "SELECT * FROM whatsapp_dispatch_preview_artifacts WHERE preview_id = :preview_id ORDER BY created_at, name",
                {"preview_id": preview_id},
            )
            if table_exists(table_names, "whatsapp_dispatch_preview_artifacts")
            else []
        )
        approvals = (
            fetch_mappings(
                conn,
                "SELECT * FROM whatsapp_dispatch_approvals WHERE preview_id = :preview_id ORDER BY approved_at",
                {"preview_id": preview_id},
            )
            if table_exists(table_names, "whatsapp_dispatch_approvals")
            else []
        )
        approval_ids = [r.get("id") for r in approvals if r.get("id") is not None]
        preview_delivery_ids = [r.get("id") for r in deliveries if r.get("id") is not None]
        live_deliveries: list[dict[str, Any]] = []
        if table_exists(table_names, "whatsapp_deliveries"):
            live_deliveries.extend(select_by_ids(conn, preparer, "whatsapp_deliveries", "approval_id", approval_ids))
            linked = select_by_ids(conn, preparer, "whatsapp_deliveries", "preview_delivery_id", preview_delivery_ids)
            known_ids = {str(r.get("id")) for r in live_deliveries}
            live_deliveries.extend(r for r in linked if str(r.get("id")) not in known_ids)

        source_job_id = preview.get("source_job_id")
        job_ids: list[Any] = [source_job_id] if source_job_id else []
        job_ids.extend(r.get("job_id") for r in approvals if r.get("job_id"))
        executions: list[dict[str, Any]] = []
        if table_exists(table_names, "antidengue_schedule_executions"):
            executions = fetch_mappings(
                conn,
                """
                SELECT * FROM antidengue_schedule_executions
                WHERE preview_id = :preview_id
                   OR source_job_id = :source_job_id
                   OR preview_job_id = :source_job_id
                   OR send_job_id = :source_job_id
                ORDER BY created_at
                """,
                {"preview_id": preview_id, "source_job_id": source_job_id},
            )
            for execution in executions:
                for key in ("source_job_id", "preview_job_id", "send_job_id"):
                    if execution.get(key):
                        job_ids.append(execution[key])
        job_ids = list(dict.fromkeys(x for x in job_ids if x is not None))
        jobs = select_by_ids(conn, preparer, "jobs", "id", job_ids) if table_exists(table_names, "jobs") else []
        job_logs = select_by_ids(conn, preparer, "job_logs", "job_id", job_ids) if table_exists(table_names, "job_logs") else []
        task_outbox = select_by_ids(conn, preparer, "task_outbox", "job_id", job_ids) if table_exists(table_names, "task_outbox") else []
        execution_ids = [r.get("id") for r in executions if r.get("id")]
        execution_events = (
            select_by_ids(conn, preparer, "antidengue_schedule_events", "execution_id", execution_ids)
            if table_exists(table_names, "antidengue_schedule_events")
            else []
        )

        profile_id = preview.get("dispatch_profile_id")
        profiles = select_by_ids(conn, preparer, "whatsapp_dispatch_profiles", "id", [profile_id]) if table_exists(table_names, "whatsapp_dispatch_profiles") else []
        snapshot = parse_jsonish(preview.get("configuration_snapshot"))
        snapshot_profile_ids: list[str] = []
        if isinstance(snapshot, Mapping):
            profile_items = snapshot.get("profiles")
            if isinstance(profile_items, list):
                for item in profile_items:
                    if isinstance(item, Mapping) and item.get("id"):
                        snapshot_profile_ids.append(str(item["id"]))
        if snapshot_profile_ids and table_exists(table_names, "whatsapp_dispatch_profiles"):
            extra_profiles = select_by_ids(conn, preparer, "whatsapp_dispatch_profiles", "id", snapshot_profile_ids)
            known = {str(row.get("id")) for row in profiles}
            profiles.extend(row for row in extra_profiles if str(row.get("id")) not in known)

        application_ids = [r.get("application_id") for r in profiles if r.get("application_id")]
        report_type_ids = [r.get("report_type_id") for r in profiles if r.get("report_type_id")]
        audience_ids = [r.get("audience_id") for r in profiles if r.get("audience_id")]
        account_ids = [r.get("account_id") for r in profiles if r.get("account_id")]
        template_ids = [r.get("template_id") for r in profiles if r.get("template_id")]
        scope_ids = [r.get("recipient_scope_id") for r in profiles if r.get("recipient_scope_id")]
        wing_ids = [r.get("wing_id") for r in profiles if r.get("wing_id")]
        wing_ids.extend(r.get("wing_id") for r in deliveries if r.get("wing_id"))

        related: dict[str, list[dict[str, Any]]] = {}
        relation_specs = [
            ("applications", "whatsapp_applications", "id", application_ids),
            ("report_types", "whatsapp_report_types", "id", report_type_ids),
            ("audiences", "whatsapp_audiences", "id", audience_ids),
            ("accounts", "whatsapp_accounts", "id", account_ids),
            ("templates", "whatsapp_templates", "id", template_ids),
            ("recipient_scopes", "whatsapp_recipient_scopes", "id", scope_ids),
            ("wings", "wings", "id", wing_ids),
        ]
        for label, table, column, ids in relation_specs:
            related[label] = select_by_ids(conn, preparer, table, column, list(dict.fromkeys(ids))) if table_exists(table_names, table) else []

        audience_members = (
            select_by_ids(conn, preparer, "whatsapp_audience_members", "audience_id", audience_ids)
            if table_exists(table_names, "whatsapp_audience_members")
            else []
        )
        related["audience_members"] = audience_members
        directory_group_ids = [r.get("directory_group_id") for r in audience_members + deliveries if r.get("directory_group_id")]
        directory_contact_ids = [r.get("directory_contact_id") for r in audience_members + deliveries if r.get("directory_contact_id")]
        contact_link_ids = [r.get("contact_link_id") for r in deliveries if r.get("contact_link_id")]
        related["directory_groups"] = select_by_ids(conn, preparer, "whatsapp_directory_groups", "id", directory_group_ids) if table_exists(table_names, "whatsapp_directory_groups") else []
        related["directory_contacts"] = select_by_ids(conn, preparer, "whatsapp_directory_contacts", "id", directory_contact_ids) if table_exists(table_names, "whatsapp_directory_contacts") else []
        related["contact_links"] = select_by_ids(conn, preparer, "whatsapp_contact_links", "id", contact_link_ids) if table_exists(table_names, "whatsapp_contact_links") else []

        stored_statuses = Counter(str(r.get("status") or "unknown") for r in deliveries)
        artifact_statuses = Counter(str(r.get("status") or "unknown") for r in artifacts)
        issue_summary = summarize_issues([preview], deliveries, artifacts)
        severity_counts = Counter()
        for item in issue_summary:
            severity_counts[item["severity"]] += item["occurrences"]

        duplicates = {
            "idempotency_keys": [
                {"value": key, "count": count}
                for key, count in Counter(str(r.get("idempotency_key")) for r in deliveries if r.get("idempotency_key")).items()
                if count > 1
            ],
            "targets": [
                {"value": stable_redaction(key) if not include_sensitive else key, "count": count}
                for key, count in Counter(str(r.get("target_jid")) for r in deliveries if r.get("target_jid")).items()
                if count > 1
            ],
        }

        file_checks: list[dict[str, Any]] = []
        for artifact in artifacts:
            path_value = artifact.get("path_snapshot")
            if not path_value:
                continue
            path = Path(str(path_value)).expanduser()
            check: dict[str, Any] = {
                "artifact_id": str(artifact.get("id")),
                "name": artifact.get("name"),
                "path": str(path),
                "exists": path.is_file(),
            }
            if path.is_file():
                stat = path.stat()
                check["actual_size_bytes"] = stat.st_size
                check["size_matches"] = stat.st_size == int(artifact.get("size_bytes") or 0)
                if verify_files:
                    hasher = hashlib.sha256()
                    with path.open("rb") as handle:
                        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                            hasher.update(chunk)
                    check["actual_sha256"] = hasher.hexdigest()
                    check["checksum_matches"] = hasher.hexdigest() == str(artifact.get("checksum_sha256") or "")
            file_checks.append(check)

        stored_counts = {
            "delivery_count": preview.get("delivery_count"),
            "ready_count": preview.get("ready_count"),
            "warning_count": preview.get("warning_count"),
            "blocked_count": preview.get("blocked_count"),
            "skipped_count": preview.get("skipped_count"),
            "artifact_count": preview.get("artifact_count"),
        }
        actual_counts = {
            "delivery_count": len(deliveries),
            "ready_delivery_count": stored_statuses.get("ready", 0),
            "warning_delivery_count": stored_statuses.get("warning", 0),
            "blocked_delivery_count": stored_statuses.get("blocked", 0),
            "skipped_delivery_count": stored_statuses.get("skipped", 0),
            "artifact_count": len(artifacts),
            "warning_issue_count": severity_counts.get("warning", 0),
            "blocked_issue_count": severity_counts.get("blocked", 0),
        }
        count_checks = {
            "delivery_count_matches": stored_counts["delivery_count"] == actual_counts["delivery_count"],
            "ready_count_matches_ready_deliveries": stored_counts["ready_count"] == actual_counts["ready_delivery_count"],
            "warning_count_matches_warning_issues": stored_counts["warning_count"] == actual_counts["warning_issue_count"],
            "warning_count_matches_warning_deliveries": stored_counts["warning_count"] == actual_counts["warning_delivery_count"],
            "blocked_count_matches_blocked_issues": stored_counts["blocked_count"] == actual_counts["blocked_issue_count"],
            "blocked_count_matches_blocked_deliveries": stored_counts["blocked_count"] == actual_counts["blocked_delivery_count"],
            "artifact_count_matches": stored_counts["artifact_count"] == actual_counts["artifact_count"],
        }

        reports.append(
            {
                "preview": sanitize_row(preview, include_sensitive=include_sensitive),
                "stored_counts": stored_counts,
                "actual_counts": actual_counts,
                "count_checks": count_checks,
                "delivery_status_counts": dict(stored_statuses),
                "artifact_status_counts": dict(artifact_statuses),
                "issue_summary": issue_summary,
                "duplicates": duplicates,
                "deliveries": [sanitize_row(r, include_sensitive=include_sensitive) for r in deliveries],
                "artifacts": [sanitize_row(r, include_sensitive=include_sensitive) for r in artifacts],
                "artifact_file_checks": file_checks,
                "approvals": [sanitize_row(r, include_sensitive=include_sensitive) for r in approvals],
                "live_deliveries": [sanitize_row(r, include_sensitive=include_sensitive) for r in live_deliveries],
                "profiles": [sanitize_row(r, include_sensitive=include_sensitive) for r in profiles],
                "related_configuration": {
                    key: [sanitize_row(r, include_sensitive=include_sensitive) for r in value]
                    for key, value in related.items()
                },
                "jobs": [sanitize_row(r, include_sensitive=include_sensitive) for r in jobs],
                "job_logs": [sanitize_row(r, include_sensitive=include_sensitive) for r in job_logs],
                "task_outbox": [sanitize_row(r, include_sensitive=include_sensitive) for r in task_outbox],
                "antidengue_executions": [sanitize_row(r, include_sensitive=include_sensitive) for r in executions],
                "antidengue_execution_events": [sanitize_row(r, include_sensitive=include_sensitive) for r in execution_events],
            }
        )

    found_keys = {str(r.get("preview", {}).get("preview_key")) for r in reports}
    missing_keys = [key for key in preview_keys if key not in found_keys]
    return {
        "available": True,
        "requested_preview_keys": list(preview_keys),
        "missing_preview_keys": missing_keys,
        "preview_count": len(reports),
        "previews": reports,
    }


def make_summary(
    metadata: Mapping[str, Any],
    counts: Mapping[str, Any],
    deep: Mapping[str, Any],
    errors: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "DEOMEE AUTOMATION PLATFORM - DATABASE DIAGNOSTIC",
        "=" * 58,
        f"Generated: {metadata.get('generated_at')}",
        f"Script version: {metadata.get('script_version')}",
        f"Database: {metadata.get('database', {}).get('url')}",
        f"Dialect: {metadata.get('database', {}).get('dialect')}",
        f"Tables discovered: {len(counts)}",
        f"Collection errors: {len(errors)}",
        "",
    ]
    if deep.get("available"):
        lines.append(f"Deep preview reports: {deep.get('preview_count', 0)}")
        missing = deep.get("missing_preview_keys") or []
        if missing:
            lines.append("Missing requested preview keys: " + ", ".join(map(str, missing)))
        for item in deep.get("previews", []):
            preview = item.get("preview", {})
            lines.extend(
                [
                    "",
                    f"Preview {preview.get('preview_key')}",
                    f"  Stored status: {preview.get('status')}",
                    f"  Stored counts: {item.get('stored_counts')}",
                    f"  Actual counts: {item.get('actual_counts')}",
                    f"  Count checks: {item.get('count_checks')}",
                    f"  Issue groups: {len(item.get('issue_summary', []))}",
                ]
            )
            for issue in item.get("issue_summary", [])[:20]:
                lines.append(
                    f"    - {issue.get('severity')} / {issue.get('code')} x{issue.get('occurrences')}: {issue.get('message')}"
                )
    else:
        lines.append(f"Deep preview report unavailable: {deep.get('reason')}")
    lines.extend(
        [
            "",
            "Files in this ZIP:",
            "  SUMMARY.txt                         Human-readable overview",
            "  metadata.json                       Runtime and sanitized DB metadata",
            "  schema.json                         Columns, PKs, FKs, indexes and constraints",
            "  table_counts.json                   Exact row counts",
            "  table_export_manifest.json          Per-table export limits/truncation",
            "  tables/*.json and tables/*.csv      Exported table data",
            "  whatsapp_preview_deep_report.json   Cross-linked preview diagnosis",
            "  collection_errors.json              Any non-fatal collection errors",
            "",
            "Security note:",
            "  Environment passwords/tokens/secrets are never exported.",
            "  Personal identifiers are redacted unless --include-sensitive was used.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-root", type=Path, help="automation-platform root; auto-detected from current directory")
    parser.add_argument("--database-url", help="override DATABASE_URL (not written to the report with password)")
    parser.add_argument("--preview-key", action="append", default=[], help="preview key to diagnose; repeat for multiple keys")
    parser.add_argument("--latest-previews", type=int, default=20, help="when no key is supplied, deeply inspect this many latest previews")
    parser.add_argument("--max-rows", type=int, default=500, help="default sample limit for each non-configuration table")
    parser.add_argument("--include-sensitive", action="store_true", help="include names, phone numbers, JIDs and full message bodies")
    parser.add_argument("--verify-files", action="store_true", help="recompute SHA-256 for local preview artifact files")
    parser.add_argument("--output", type=Path, help="output ZIP path; default is generated in current directory")
    args = parser.parse_args()

    root = find_project_root(args.project_root or Path.cwd())
    database_url, database_source = resolve_database_url(args, root)
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output or Path.cwd() / f"automation-platform-db-diagnostic-{timestamp}.zip").resolve()

    try:
        from sqlalchemy import create_engine, inspect, text
    except ImportError:
        eprint("ERROR: SQLAlchemy is not available in this Python environment.")
        eprint("Run the script from automation-platform with:")
        eprint(f"  uv run python {Path(__file__).resolve()} --preview-key <PREVIEW_KEY>")
        return 2

    errors: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="automation-db-diagnostic-") as temp_dir:
        bundle = Path(temp_dir) / "diagnostic"
        bundle.mkdir(parents=True)
        tables_dir = bundle / "tables"
        tables_dir.mkdir()

        engine = create_engine(database_url, pool_pre_ping=True)
        metadata: dict[str, Any] = {
            "script_version": SCRIPT_VERSION,
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "host": {
                "hostname": socket.gethostname(),
                "platform": platform.platform(),
                "python": sys.version,
                "executable": sys.executable,
            },
            "project": {
                "root": str(root),
                "git_revision": run_command(["git", "rev-parse", "HEAD"], root),
                "git_branch": run_command(["git", "branch", "--show-current"], root),
                "git_status": run_command(["git", "status", "--short"], root),
            },
            "database": {
                "url": safe_db_url(database_url),
                "url_source": database_source,
            },
            "options": {
                "preview_keys": args.preview_key,
                "latest_previews": args.latest_previews,
                "max_rows": args.max_rows,
                "include_sensitive": args.include_sensitive,
                "verify_files": args.verify_files,
            },
            "environment": {},
        }

        env_values = {**parse_dotenv(root / ".env"), **dict(os.environ)}
        relevant_prefixes = ("DATABASE_", "WHATSAPP_", "ANTIDENGUE_", "OBJECT_STORAGE_", "PLATFORM_STORAGE_", "CELERY_", "NTFY_")
        for key in sorted(k for k in env_values if k.startswith(relevant_prefixes)):
            value = env_values[key]
            if SECRET_ENV_RE.search(key):
                metadata["environment"][key] = "<secret omitted>"
            elif key == "DATABASE_URL":
                metadata["environment"][key] = safe_db_url(value)
            else:
                metadata["environment"][key] = value

        schema: dict[str, Any] = {}
        counts: dict[str, int | str] = {}
        export_manifest: dict[str, Any] = {}
        deep: dict[str, Any] = {}

        try:
            with engine.connect() as conn:
                dialect = conn.dialect.name
                metadata["database"]["dialect"] = dialect
                metadata["database"]["driver"] = conn.dialect.driver
                metadata["database"]["server_version"] = str(getattr(conn.dialect, "server_version_info", None))
                if dialect == "postgresql":
                    try:
                        conn.execute(text("SET TRANSACTION READ ONLY"))
                    except Exception as exc:
                        errors.append({"stage": "set_read_only", "error": f"{type(exc).__name__}: {exc}"})
                    try:
                        metadata["database"]["current_database"] = scalar(conn, "SELECT current_database()")
                        metadata["database"]["current_schema"] = scalar(conn, "SELECT current_schema()")
                        metadata["database"]["postgres_version"] = scalar(conn, "SELECT version()")
                    except Exception as exc:
                        errors.append({"stage": "database_identity", "error": f"{type(exc).__name__}: {exc}"})
                elif dialect == "sqlite":
                    try:
                        metadata["database"]["sqlite_version"] = scalar(conn, "SELECT sqlite_version()")
                    except Exception as exc:
                        errors.append({"stage": "database_identity", "error": f"{type(exc).__name__}: {exc}"})

                inspector = inspect(conn)
                preparer = conn.dialect.identifier_preparer
                pairs = get_table_names(inspector, dialect)
                table_name_set = {table for _, table in pairs}
                schema = schema_snapshot(inspector, pairs)

                for schema_name, table in pairs:
                    full_name = f"{schema_name}.{table}" if schema_name else table
                    try:
                        count = int(scalar(conn, f"SELECT COUNT(*) FROM {qname(preparer, schema_name, table)}") or 0)
                        counts[full_name] = count
                    except Exception as exc:
                        counts[full_name] = f"ERROR: {type(exc).__name__}: {exc}"
                        errors.append({"stage": "count", "table": full_name, "error": f"{type(exc).__name__}: {exc}"})
                        continue
                    try:
                        columns = inspector.get_columns(table, schema=schema_name)
                        rows, table_meta = dump_table(
                            conn,
                            preparer,
                            schema_name,
                            table,
                            columns,
                            count,
                            max(1, args.max_rows),
                            args.include_sensitive,
                        )
                        safe_filename = re.sub(r"[^A-Za-z0-9_.-]+", "_", full_name)
                        write_json(tables_dir / f"{safe_filename}.json", rows)
                        write_csv(tables_dir / f"{safe_filename}.csv", rows)
                        export_manifest[full_name] = table_meta
                    except Exception as exc:
                        errors.append(
                            {
                                "stage": "table_export",
                                "table": full_name,
                                "error": f"{type(exc).__name__}: {exc}",
                                "traceback": traceback.format_exc(),
                            }
                        )

                try:
                    deep = deep_preview_report(
                        conn,
                        preparer,
                        table_name_set,
                        args.preview_key,
                        max(1, args.latest_previews),
                        args.include_sensitive,
                        args.verify_files,
                    )
                except Exception as exc:
                    deep = {"available": False, "reason": f"{type(exc).__name__}: {exc}"}
                    errors.append({"stage": "deep_preview_report", "error": str(exc), "traceback": traceback.format_exc()})
        except Exception as exc:
            errors.append({"stage": "database_connection", "error": f"{type(exc).__name__}: {exc}", "traceback": traceback.format_exc()})
            write_json(bundle / "metadata.json", metadata)
            write_json(bundle / "collection_errors.json", errors)
            (bundle / "SUMMARY.txt").write_text(
                "Database connection failed. See collection_errors.json.\n"
                f"Sanitized database URL: {safe_db_url(database_url)}\n",
                encoding="utf-8",
            )
        finally:
            engine.dispose()

        write_json(bundle / "metadata.json", metadata)
        write_json(bundle / "schema.json", schema)
        write_json(bundle / "table_counts.json", counts)
        write_json(bundle / "table_export_manifest.json", export_manifest)
        write_json(bundle / "whatsapp_preview_deep_report.json", deep)
        write_json(bundle / "collection_errors.json", errors)
        (bundle / "SUMMARY.txt").write_text(make_summary(metadata, counts, deep, errors), encoding="utf-8")

        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists():
            output.unlink()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
            for file in sorted(bundle.rglob("*")):
                if file.is_file():
                    archive.write(file, file.relative_to(bundle))

    print(f"Created diagnostic bundle: {output}")
    print(f"Size: {output.stat().st_size / (1024 * 1024):.2f} MiB")
    if errors:
        print(f"Completed with {len(errors)} non-fatal collection error(s); see collection_errors.json.")
    print("Share the generated ZIP, not your .env file or a raw database dump.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

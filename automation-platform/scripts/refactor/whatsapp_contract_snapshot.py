#!/usr/bin/env python3
"""Capture stable WhatsApp package contracts for behavior-preserving refactors."""

from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, UniqueConstraint
from sqlmodel import SQLModel


PUBLIC_SYMBOLS: dict[str, list[str]] = {
    "whatsapp_gateway.api": ["ensure_defaults", "router", "worker_health"],
    "whatsapp_gateway.models": [
        "WhatsAppAccount",
        "WhatsAppActivity",
        "WhatsAppApplication",
        "WhatsAppAudience",
        "WhatsAppAudienceMember",
        "WhatsAppContactLink",
        "WhatsAppDelivery",
        "WhatsAppDirectoryContact",
        "WhatsAppDirectoryGroup",
        "WhatsAppDispatchApproval",
        "WhatsAppDispatchPreview",
        "WhatsAppDispatchPreviewArtifact",
        "WhatsAppDispatchPreviewDelivery",
        "WhatsAppDispatchProfile",
        "WhatsAppGroup",
        "WhatsAppGroupMember",
        "WhatsAppIdentityAlias",
        "WhatsAppInboundAttachment",
        "WhatsAppInboundExportItem",
        "WhatsAppInboundExportRun",
        "WhatsAppInboundHistoryRequest",
        "WhatsAppInboundMessage",
        "WhatsAppRecipientScope",
        "WhatsAppReportType",
        "WhatsAppSettings",
        "WhatsAppTemplate",
    ],
    "whatsapp_gateway.preview_api": ["router"],
    "whatsapp_gateway.preview_service": [
        "_canonical_contact_target",
        "_plan_matches_audience_route",
        "_plan_report_type",
        "_plan_route_label",
        "_retarget_group_plan",
        "_same_route_value",
        "cleanup_unreferenced_preview_files",
        "compile_antidengue_preview",
        "delete_preview_records",
        "entity_link_details",
        "is_managed_preview_artifact",
        "preview_dict",
        "preview_is_stale",
        "sha256_file",
    ],
    "whatsapp_gateway.inbound_api": [
        "RequestInboundHistory",
        "request_inbound_history",
        "router",
    ],
    "whatsapp_gateway.inbound_service": [
        "build_preview",
        "contact_message_filter",
        "create_export_run",
        "find_matching_attachments",
        "normalize_media_types",
        "package_export",
        "serialize_run",
    ],
    "whatsapp_gateway.inbound_tasks": [
        "_snapshot_account",
        "_snapshot_attachment",
        "_snapshot_message",
        "build_inbound_export_job",
    ],
    "whatsapp_gateway.tasks": [
        "compile_dispatch_preview_job",
        "send_approved_preview_job",
    ],
    "whatsapp_gateway.identity_repair": ["repair_inbound_message_identities"],
}


def _callable_name(value: Any) -> str:
    module = getattr(value, "__module__", "")
    name = getattr(value, "__qualname__", getattr(value, "__name__", ""))
    return f"{module}.{name}".strip(".") or type(value).__name__


def _default_value(default: Any) -> Any:
    if default is None:
        return None
    arg = getattr(default, "arg", None)
    if callable(arg):
        return {"kind": type(default).__name__, "callable": _callable_name(arg)}
    return {"kind": type(default).__name__, "value": repr(arg)}


def _constraint_value(constraint: Any) -> dict[str, Any] | None:
    if isinstance(constraint, CheckConstraint):
        return {
            "kind": "check",
            "name": constraint.name,
            "sqltext": str(constraint.sqltext),
        }
    if isinstance(constraint, UniqueConstraint):
        return {
            "kind": "unique",
            "name": constraint.name,
            "columns": sorted(column.name for column in constraint.columns),
        }
    if isinstance(constraint, ForeignKeyConstraint):
        return {
            "kind": "foreign_key",
            "name": constraint.name,
            "columns": [element.parent.name for element in constraint.elements],
            "targets": [element.target_fullname for element in constraint.elements],
        }
    return None


def _table_snapshot(table: Any) -> dict[str, Any]:
    columns = []
    for column in table.columns:
        columns.append(
            {
                "name": column.name,
                "type": str(column.type),
                "nullable": column.nullable,
                "primary_key": column.primary_key,
                "unique": bool(column.unique),
                "index": bool(column.index),
                "default": _default_value(column.default),
                "server_default": (
                    str(column.server_default.arg) if column.server_default else None
                ),
                "foreign_keys": sorted(foreign_key.target_fullname for foreign_key in column.foreign_keys),
            }
        )

    constraints = [
        value
        for value in (_constraint_value(item) for item in table.constraints)
        if value is not None
    ]
    constraints.sort(key=lambda item: json.dumps(item, sort_keys=True))

    indexes = []
    for index in table.indexes:
        indexes.append(
            {
                "name": index.name,
                "unique": bool(index.unique),
                "columns": [getattr(expression, "name", str(expression)) for expression in index.expressions],
            }
        )
    indexes.sort(key=lambda item: json.dumps(item, sort_keys=True))

    return {
        "name": table.name,
        "columns": columns,
        "constraints": constraints,
        "indexes": indexes,
    }


def capture_contract() -> dict[str, Any]:
    from automation_api.main import app
    from automation_core.celery_app import celery_app
    from automation_core.config import Settings

    importlib.import_module("whatsapp_gateway.tasks")
    importlib.import_module("whatsapp_gateway.inbound_tasks")

    openapi = app.openapi()
    whatsapp_paths = {
        path: value
        for path, value in openapi.get("paths", {}).items()
        if path.startswith("/api/v1/whatsapp")
    }

    whatsapp_tables = {
        name: _table_snapshot(table)
        for name, table in sorted(SQLModel.metadata.tables.items())
        if name.startswith("whatsapp_")
    }

    public_symbols: dict[str, list[str]] = {}
    for module_name, names in PUBLIC_SYMBOLS.items():
        module = importlib.import_module(module_name)
        missing = [name for name in names if not hasattr(module, name)]
        if missing:
            raise RuntimeError(f"Missing public symbols in {module_name}: {missing}")
        public_symbols[module_name] = names

    nats_subjects = {
        name: field.default
        for name, field in sorted(Settings.model_fields.items())
        if name.startswith("whatsapp_") and name.endswith("_subject")
    }

    task_routes = {
        str(name): dict(route)
        for name, route in sorted(dict(celery_app.conf.task_routes or {}).items())
    }
    task_names = sorted(
        name
        for name in celery_app.tasks
        if name.startswith("whatsapp_gateway.")
    )

    return {
        "openapi_paths": whatsapp_paths,
        "openapi_schemas": openapi.get("components", {}).get("schemas", {}),
        "whatsapp_tables": whatsapp_tables,
        "public_symbols": public_symbols,
        "celery": {"task_names": task_names, "task_routes": task_routes},
        "nats_subjects": nats_subjects,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(capture_contract(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(args.output)


if __name__ == "__main__":
    main()

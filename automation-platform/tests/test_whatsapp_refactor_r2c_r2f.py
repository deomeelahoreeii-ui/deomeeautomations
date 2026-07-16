from __future__ import annotations

import importlib
import json
from pathlib import Path

import whatsapp_gateway.identity_repair as legacy_identity
import whatsapp_gateway.inbound_api as legacy_api
import whatsapp_gateway.inbound_service as legacy_service
import whatsapp_gateway.inbound_tasks as legacy_tasks
from automation_core.celery_app import celery_app
from whatsapp_gateway.inbound import (
    common,
    contact_matching,
    export_api,
    export_items,
    export_packaging,
    export_preview,
    export_runs,
    export_task,
    history,
    identity_index,
    identity_repair_service,
    identity_resolution,
    identity_types,
    status,
    task_snapshots,
    worker_client,
)


EXACT_EXPORTS = {
    # R2C/R2D API endpoints
    (legacy_api, "request_inbound_history"): history.request_inbound_history,
    (legacy_api, "list_inbound_history_requests"): history.list_inbound_history_requests,
    (legacy_api, "get_inbound_history_request"): history.get_inbound_history_request,
    (legacy_api, "preview_inbound_export"): export_api.preview_inbound_export,
    (legacy_api, "create_inbound_export"): export_api.create_inbound_export,
    (legacy_api, "list_inbound_exports"): export_api.list_inbound_exports,
    (legacy_api, "read_inbound_export"): export_api.read_inbound_export,
    (legacy_api, "inbound_status"): status.inbound_status,
    # R2D services
    (legacy_service, "normalize_utc_naive"): common.normalize_utc_naive,
    (legacy_service, "normalize_media_types"): common.normalize_media_types,
    (legacy_service, "require_contact"): common.require_contact,
    (legacy_service, "contact_identity_jids"): contact_matching.contact_identity_jids,
    (legacy_service, "contact_message_filter"): contact_matching.contact_message_filter,
    (legacy_service, "attachment_matches_category"): contact_matching.attachment_matches_category,
    (legacy_service, "find_matching_attachments"): contact_matching.find_matching_attachments,
    (legacy_service, "contact_coverage"): contact_matching.contact_coverage,
    (legacy_service, "build_preview"): export_preview.build_preview,
    (legacy_service, "create_export_run"): export_runs.create_export_run,
    (legacy_service, "serialize_run"): export_runs.serialize_run,
    (legacy_service, "_export_name"): export_packaging._export_name,
    (legacy_service, "package_export"): export_packaging.package_export,
    # R2E task helpers and entrypoint
    (legacy_tasks, "_AccountSnapshot"): task_snapshots._AccountSnapshot,
    (legacy_tasks, "_AttachmentSnapshot"): task_snapshots._AttachmentSnapshot,
    (legacy_tasks, "_MessageSnapshot"): task_snapshots._MessageSnapshot,
    (legacy_tasks, "_snapshot_account"): task_snapshots._snapshot_account,
    (legacy_tasks, "_snapshot_attachment"): task_snapshots._snapshot_attachment,
    (legacy_tasks, "_snapshot_message"): task_snapshots._snapshot_message,
    (legacy_tasks, "_open_media_client"): worker_client._open_media_client,
    (legacy_tasks, "_request_media_download"): worker_client._request_media_download,
    (legacy_tasks, "_load_export_item"): export_items._load_export_item,
    (legacy_tasks, "build_inbound_export_job"): export_task.build_inbound_export_job,
    # R2F identity repair
    (legacy_identity, "IdentityRepairRow"): identity_types.IdentityRepairRow,
    (legacy_identity, "_clean_jids"): identity_index._clean_jids,
    (legacy_identity, "build_contact_identity_index"): identity_index.build_contact_identity_index,
    (legacy_identity, "message_identity_jids"): identity_resolution.message_identity_jids,
    (legacy_identity, "resolve_message_contact_id"): identity_resolution.resolve_message_contact_id,
    (legacy_identity, "repair_inbound_message_identities"): identity_repair_service.repair_inbound_message_identities,
}


def test_r2c_r2f_compatibility_exports_are_exact_objects() -> None:
    for (legacy_module, name), implementation in EXACT_EXPORTS.items():
        assert getattr(legacy_module, name) is implementation


def test_every_original_symbol_in_selected_files_has_a_movement_record() -> None:
    project_root = Path(__file__).resolve().parents[1]
    package_root = project_root / "packages/whatsapp_gateway/whatsapp_gateway"
    inventory = json.loads((package_root / "REFACTOR_INVENTORY.json").read_text())
    moved = json.loads((package_root / "MOVED_SYMBOLS.json").read_text())

    for filename, legacy_module in {
        "inbound_api.py": "whatsapp_gateway.inbound_api",
        "inbound_service.py": "whatsapp_gateway.inbound_service",
        "inbound_tasks.py": "whatsapp_gateway.inbound_tasks",
        "identity_repair.py": "whatsapp_gateway.identity_repair",
    }.items():
        expected = {
            f"{legacy_module}.{item['name']}"
            for item in inventory[filename]["symbols"]
        }
        missing = sorted(expected - set(moved))
        assert not missing, f"Unmapped original symbols from {filename}: {missing}"


def test_every_movement_record_resolves_to_the_same_object() -> None:
    project_root = Path(__file__).resolve().parents[1]
    package_root = project_root / "packages/whatsapp_gateway/whatsapp_gateway"
    moved = json.loads((package_root / "MOVED_SYMBOLS.json").read_text())
    prefixes = (
        "whatsapp_gateway.inbound_api.",
        "whatsapp_gateway.inbound_service.",
        "whatsapp_gateway.inbound_tasks.",
        "whatsapp_gateway.identity_repair.",
    )
    for legacy_path, implementation_path in moved.items():
        if not legacy_path.startswith(prefixes):
            continue
        legacy_module_name, legacy_name = legacy_path.rsplit(".", 1)
        implementation_module_name, implementation_name = implementation_path.rsplit(".", 1)
        legacy_module = importlib.import_module(legacy_module_name)
        implementation_module = importlib.import_module(implementation_module_name)
        assert getattr(legacy_module, legacy_name) is getattr(
            implementation_module, implementation_name
        )


def test_inbound_routes_are_registered_exactly_once() -> None:
    expected = {
        ("POST", "/api/v1/whatsapp/inbound/events"),
        ("POST", "/api/v1/whatsapp/inbound/attachments/{attachment_id}/content"),
        ("POST", "/api/v1/whatsapp/inbound/history/request"),
        ("GET", "/api/v1/whatsapp/inbound/history/requests"),
        ("GET", "/api/v1/whatsapp/inbound/history/requests/{request_id}"),
        ("POST", "/api/v1/whatsapp/inbound/exports/preview"),
        ("POST", "/api/v1/whatsapp/inbound/exports"),
        ("GET", "/api/v1/whatsapp/inbound/exports"),
        ("GET", "/api/v1/whatsapp/inbound/exports/{export_id}"),
        ("GET", "/api/v1/whatsapp/inbound/status"),
    }
    actual: list[tuple[str, str]] = []
    for route in legacy_api.router.routes:
        path = getattr(route, "path", None)
        for method in getattr(route, "methods", set()):
            if method in {"GET", "POST"} and path:
                actual.append((method, path))
    for item in expected:
        assert actual.count(item) == 1, item


def test_legacy_nats_monkeypatch_path_remains_available() -> None:
    import nats

    assert legacy_api.nats is nats
    assert history.nats is nats


def test_export_celery_task_name_and_object_remain_stable() -> None:
    task_name = "whatsapp_gateway.build_inbound_export"
    assert legacy_tasks.build_inbound_export_job is export_task.build_inbound_export_job
    assert export_task.build_inbound_export_job.name == task_name
    assert celery_app.tasks[task_name].name == task_name


def test_r2c_r2f_modules_are_responsibility_sized() -> None:
    project_root = Path(__file__).resolve().parents[1]
    package_root = project_root / "packages/whatsapp_gateway/whatsapp_gateway"
    limits = {
        "inbound_api.py": 120,
        "inbound_service.py": 60,
        "inbound_tasks.py": 50,
        "identity_repair.py": 40,
        "inbound/history.py": 210,
        "inbound/export_api.py": 180,
        "inbound/status.py": 80,
        "inbound/common.py": 80,
        "inbound/contact_matching.py": 190,
        "inbound/export_preview.py": 110,
        "inbound/export_runs.py": 140,
        "inbound/export_packaging.py": 240,
        "inbound/task_snapshots.py": 80,
        "inbound/worker_client.py": 100,
        "inbound/export_items.py": 70,
        "inbound/export_task.py": 330,
        "inbound/identity_types.py": 50,
        "inbound/identity_index.py": 90,
        "inbound/identity_resolution.py": 70,
        "inbound/identity_repair_service.py": 180,
    }
    for relative, maximum in limits.items():
        line_count = len((package_root / relative).read_text().splitlines())
        assert line_count <= maximum, f"{relative} has {line_count} lines"

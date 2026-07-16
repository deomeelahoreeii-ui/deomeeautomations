from __future__ import annotations

import importlib
import json
from pathlib import Path

import whatsapp_gateway.inbound_api as legacy_api
from whatsapp_gateway.inbound import history_tracking, ingestion, upserts


MOVED_FUNCTIONS = {
    "_insert_idempotently": upserts.insert_idempotently,
    "_upsert_inbound_message": upserts.upsert_inbound_message,
    "_upsert_inbound_attachment": upserts.upsert_inbound_attachment,
    "_history_contact_counts": history_tracking.history_contact_counts,
    "_utc_naive": history_tracking.utc_naive,
    "_serialize_history_request": history_tracking.serialize_history_request,
    "_reconcile_history_requests": history_tracking.reconcile_history_requests,
    "_record_history_progress": history_tracking.record_history_progress,
    "ingest_event": ingestion.ingest_event,
}

MOVED_CONSTANTS = {
    "HISTORY_ACTIVE_STATUSES": history_tracking.HISTORY_ACTIVE_STATUSES,
    "HISTORY_QUIET_SECONDS": history_tracking.HISTORY_QUIET_SECONDS,
    "HISTORY_NO_RESULT_SECONDS": history_tracking.HISTORY_NO_RESULT_SECONDS,
    "HISTORY_HARD_TIMEOUT_SECONDS": history_tracking.HISTORY_HARD_TIMEOUT_SECONDS,
}


def test_inbound_api_r2b_compatibility_exports_are_exact_objects() -> None:
    for name, implementation in MOVED_FUNCTIONS.items():
        assert getattr(legacy_api, name) is implementation
    for name, implementation in MOVED_CONSTANTS.items():
        assert getattr(legacy_api, name) is implementation


def test_r2b_movement_map_covers_every_selected_original_function() -> None:
    project_root = Path(__file__).resolve().parents[1]
    package_root = project_root / "packages/whatsapp_gateway/whatsapp_gateway"
    inventory = json.loads((package_root / "REFACTOR_INVENTORY.json").read_text())
    moved = json.loads((package_root / "MOVED_SYMBOLS.json").read_text())

    expected = set(MOVED_FUNCTIONS)
    original = {
        item["name"]
        for item in inventory["inbound_api.py"]["symbols"]
        if item["name"] in expected
    }
    assert original == expected

    expected_legacy_paths = {
        f"whatsapp_gateway.inbound_api.{name}"
        for name in (*MOVED_FUNCTIONS, *MOVED_CONSTANTS)
    }
    assert expected_legacy_paths <= set(moved)

    for legacy_path in expected_legacy_paths:
        implementation_path = moved[legacy_path]
        legacy_module_name, legacy_name = legacy_path.rsplit(".", 1)
        implementation_module_name, implementation_name = implementation_path.rsplit(
            ".", 1
        )
        legacy_module = importlib.import_module(legacy_module_name)
        implementation_module = importlib.import_module(implementation_module_name)
        assert getattr(legacy_module, legacy_name) is getattr(
            implementation_module,
            implementation_name,
        )


def test_inbound_event_route_is_registered_once() -> None:
    matches = [
        route
        for route in legacy_api.router.routes
        if getattr(route, "path", None) == "/api/v1/whatsapp/inbound/events"
        and "POST" in getattr(route, "methods", set())
    ]
    assert len(matches) == 1
    assert matches[0].endpoint is ingestion.ingest_event


def test_r2b_modules_are_responsibility_sized() -> None:
    project_root = Path(__file__).resolve().parents[1]
    package_root = project_root / "packages/whatsapp_gateway/whatsapp_gateway"
    limits = {
        "inbound_api.py": 400,
        "inbound/ingestion.py": 140,
        "inbound/upserts.py": 170,
        "inbound/history_tracking.py": 210,
    }
    for relative, maximum in limits.items():
        line_count = len((package_root / relative).read_text().splitlines())
        assert line_count <= maximum, f"{relative} has {line_count} lines"

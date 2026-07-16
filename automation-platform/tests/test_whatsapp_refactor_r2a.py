from __future__ import annotations

import importlib
import json
from pathlib import Path

import whatsapp_gateway.inbound_api as legacy_api
import whatsapp_gateway.inbound_media as legacy_media
from whatsapp_gateway.inbound import accounts, authentication, media_types, media_upload, schemas


API_MOVES = {
    "AttachmentEvent": schemas.AttachmentEvent,
    "InboundMessageEvent": schemas.InboundMessageEvent,
    "InboundFileFilter": schemas.InboundFileFilter,
    "CreateInboundExportRequest": schemas.CreateInboundExportRequest,
    "RequestInboundHistory": schemas.RequestInboundHistory,
    "_verify_worker_token": authentication.verify_worker_token,
    "_resolve_account": accounts.resolve_account,
    "_resolve_contact_id": accounts.resolve_contact_id,
    "upload_attachment_content": media_upload.upload_attachment_content,
}

MEDIA_MOVES = {
    "normalize_mime": media_types.normalize_mime,
    "safe_filename": media_types.safe_filename,
    "safe_slug": media_types.safe_slug,
    "classify_attachment_metadata": media_types.classify_attachment_metadata,
    "_looks_like_csv": media_types._looks_like_csv,
    "detect_file_type": media_types.detect_file_type,
}


def test_inbound_api_compatibility_exports_are_exact_objects() -> None:
    for name, implementation in API_MOVES.items():
        assert getattr(legacy_api, name) is implementation


def test_inbound_media_compatibility_exports_are_exact_objects() -> None:
    for name, implementation in MEDIA_MOVES.items():
        assert getattr(legacy_media, name) is implementation
    assert legacy_media.SUPPORTED_CATEGORIES is media_types.SUPPORTED_CATEGORIES
    assert legacy_media._MIME_CATEGORY is media_types._MIME_CATEGORY
    assert legacy_media._EXTENSION_CATEGORY is media_types._EXTENSION_CATEGORY


def test_r2a_movement_map_covers_every_selected_original_symbol() -> None:
    project_root = Path(__file__).resolve().parents[1]
    package_root = project_root / "packages/whatsapp_gateway/whatsapp_gateway"
    inventory = json.loads((package_root / "REFACTOR_INVENTORY.json").read_text())
    moved = json.loads((package_root / "MOVED_SYMBOLS.json").read_text())

    expected_api = {
        "AttachmentEvent",
        "InboundMessageEvent",
        "InboundFileFilter",
        "CreateInboundExportRequest",
        "RequestInboundHistory",
        "_verify_worker_token",
        "_resolve_account",
        "_resolve_contact_id",
        "upload_attachment_content",
    }
    original_api = {
        item["name"]
        for item in inventory["inbound_api.py"]["symbols"]
        if item["name"] in expected_api
    }
    assert original_api == expected_api

    original_media = {
        item["name"] for item in inventory["inbound_media.py"]["symbols"]
    }
    assert original_media == set(MEDIA_MOVES)

    expected_legacy_paths = {
        *(f"whatsapp_gateway.inbound_api.{name}" for name in expected_api),
        *(f"whatsapp_gateway.inbound_media.{name}" for name in original_media),
    }
    assert expected_legacy_paths <= set(moved)

    for legacy_path in expected_legacy_paths:
        implementation_path = moved[legacy_path]
        legacy_module_name, legacy_name = legacy_path.rsplit(".", 1)
        implementation_module_name, implementation_name = implementation_path.rsplit(".", 1)
        legacy_module = importlib.import_module(legacy_module_name)
        implementation_module = importlib.import_module(implementation_module_name)
        assert getattr(legacy_module, legacy_name) is getattr(
            implementation_module, implementation_name
        )


def test_media_upload_route_is_registered_once() -> None:
    matches = [
        route
        for route in legacy_api.router.routes
        if getattr(route, "path", None)
        == "/api/v1/whatsapp/inbound/attachments/{attachment_id}/content"
        and "POST" in getattr(route, "methods", set())
    ]
    assert len(matches) == 1
    assert matches[0].endpoint is media_upload.upload_attachment_content

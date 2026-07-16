from __future__ import annotations

import importlib
import inspect
import json
from pathlib import Path

import whatsapp_gateway.preview_api as legacy_api
import whatsapp_gateway.preview_service as legacy_service
from whatsapp_gateway.previews import (
    approval, artifacts, contact_links, creation, deletion, deliveries, options, queries, serialization,
)
from whatsapp_gateway.previews.compiler import (
    attachments, context, deliveries as compiler_deliveries, errors, finalize, messages, orchestrator, plans, routes,
)

EXPECTED_ROUTES = {
    ("GET", "/api/v1/whatsapp/previews/options"),
    ("POST", "/api/v1/whatsapp/previews"),
    ("POST", "/api/v1/whatsapp/previews/bulk"),
    ("GET", "/api/v1/whatsapp/previews"),
    ("GET", "/api/v1/whatsapp/previews/selection"),
    ("GET", "/api/v1/whatsapp/previews/{preview_id}"),
    ("POST", "/api/v1/whatsapp/previews/{preview_id}/approve"),
    ("POST", "/api/v1/whatsapp/preview-bulk/approve"),
    ("DELETE", "/api/v1/whatsapp/preview-bulk"),
    ("DELETE", "/api/v1/whatsapp/previews/{preview_id}"),
    ("GET", "/api/v1/whatsapp/previews/{preview_id}/deliveries"),
    ("GET", "/api/v1/whatsapp/previews/{preview_id}/artifacts"),
    ("GET", "/api/v1/whatsapp/previews/{preview_id}/issues"),
    ("GET", "/api/v1/whatsapp/previews/{preview_id}/artifacts/{artifact_id}/download"),
    ("GET", "/api/v1/whatsapp/directory/contacts/{contact_id}/links"),
    ("GET", "/api/v1/whatsapp/directory/contact-link-targets"),
    ("POST", "/api/v1/whatsapp/directory/contacts/{contact_id}/links"),
    ("DELETE", "/api/v1/whatsapp/directory/contacts/{contact_id}/links/{link_id}"),
}

def test_preview_facade_router_contains_every_route_once() -> None:
    actual = []
    for route in legacy_api.router.routes:
        for method in getattr(route, "methods", set()):
            if method in {"GET", "POST", "DELETE"}:
                actual.append((method, route.path))
    assert set(actual) == EXPECTED_ROUTES
    assert len(actual) == len(EXPECTED_ROUTES)

def test_preview_api_facade_reexports_moved_callables() -> None:
    assert legacy_api.preview_options is options.preview_options
    assert legacy_api.create_preview is creation.create_preview
    assert legacy_api.create_previews_bulk is creation.create_previews_bulk
    assert legacy_api.previews is queries.previews
    assert legacy_api.approve_preview is approval.approve_preview
    assert legacy_api.hard_delete_preview is deletion.hard_delete_preview
    assert legacy_api.preview_deliveries is deliveries.preview_deliveries
    assert legacy_api.preview_artifacts is artifacts.preview_artifacts
    assert legacy_api.save_contact_link is contact_links.save_contact_link
    assert legacy_api._delivery_dict is serialization._delivery_dict


def _resolve_symbol(dotted: str):
    module_name, symbol_name = dotted.rsplit(".", 1)
    return getattr(importlib.import_module(module_name), symbol_name)


def test_every_mapped_legacy_symbol_is_reexported_by_identity() -> None:
    package = Path(legacy_api.__file__).parent
    mapping = json.loads((package / "MOVED_SYMBOLS_R4_R5.json").read_text(encoding="utf-8"))
    facades = {
        "preview_api.py": legacy_api,
        "preview_service.py": legacy_service,
    }
    missing = {}
    mismatched = {}
    for source_name, facade in facades.items():
        for symbol_name, destination in mapping[source_name].items():
            if not hasattr(facade, symbol_name):
                missing.setdefault(source_name, []).append(symbol_name)
                continue
            expected = _resolve_symbol(destination)
            actual = getattr(facade, symbol_name)
            if actual is not expected:
                mismatched.setdefault(source_name, []).append(
                    {"symbol": symbol_name, "destination": destination}
                )
    assert missing == {}
    assert mismatched == {}

def test_preview_service_facade_reexports_helpers_and_compiler() -> None:
    assert legacy_service.PreviewCompileError is errors.PreviewCompileError
    assert legacy_service._plan_matches_audience_route is routes._plan_matches_audience_route
    assert legacy_service._render_message is messages._render_message
    assert legacy_service._classify_attachment_wings is attachments._classify_attachment_wings
    assert legacy_service.compile_antidengue_preview is orchestrator.compile_antidengue_preview

def test_compiler_is_explicit_short_orchestrator() -> None:
    source = inspect.getsource(orchestrator.compile_antidengue_preview)
    for stage in (
        "load_compile_context", "build_dispatch_plan", "configuration_snapshot",
        "create_preview_record", "ArtifactSnapshotStore", "persist_deliveries", "finalize_preview",
    ):
        assert stage in source
    assert len(source.splitlines()) <= 20
    assert callable(context.load_compile_context)
    assert callable(plans.build_dispatch_plan)
    assert callable(compiler_deliveries.persist_deliveries)
    assert callable(finalize.finalize_preview)

def test_refactored_modules_respect_size_budget() -> None:
    package = Path(legacy_api.__file__).parent / "previews"
    oversized = {}
    for path in package.rglob("*.py"):
        lines = len(path.read_text(encoding="utf-8").splitlines())
        if lines > 250:
            oversized[str(path.relative_to(package))] = lines
    assert oversized == {}

def test_movement_map_covers_every_legacy_top_level_symbol() -> None:
    package = Path(legacy_api.__file__).parent
    mapping = json.loads((package / "MOVED_SYMBOLS_R4_R5.json").read_text(encoding="utf-8"))
    expected_api = {
        "PreviewInput", "BulkPreviewInput", "PreviewIdsInput", "BulkPreviewApprovalInput",
        "ContactLinkInput", "PreviewApprovalInput", "_digits", "_delivery_dict", "_artifact_dict",
        "_with_approval", "preview_options", "create_preview", "create_previews_bulk", "previews",
        "preview_selection", "preview_detail", "approve_preview", "approve_previews_bulk",
        "hard_delete_previews_bulk", "hard_delete_preview", "preview_deliveries", "preview_artifacts",
        "preview_issues", "download_preview_artifact", "contact_links", "contact_link_targets",
        "save_contact_link", "remove_contact_link",
    }
    expected_service = {
        "PreviewCompileError", "issue", "sha256_file", "_managed_artifact_path", "freeze_artifact",
        "is_managed_preview_artifact", "_preview_key", "_artifact_role", "_attachment_paths",
        "_render_message", "_delivery_status", "_canonical_contact_target", "_plan_report_type",
        "_plan_recipient_channel", "_plan_recipient_scope", "_plan_target", "_plan_route_label",
        "_same_route_value", "_plan_matches_audience_route", "_retarget_group_plan",
        "_xlsx_emis_values", "_classify_attachment_wings", "_classify_scoped_emis_wings",
        "compile_antidengue_preview", "preview_is_stale", "preview_dict", "delete_preview_records",
        "cleanup_unreferenced_preview_files", "entity_link_details",
    }
    assert set(mapping["preview_api.py"]) == expected_api
    assert set(mapping["preview_service.py"]) == expected_service

def test_r3_shared_service_dependencies_are_explicitly_bound() -> None:
    import whatsapp_gateway.directory.services as directory_services
    import whatsapp_gateway.gateway.services as gateway_services
    from whatsapp_gateway.configuration.defaults import (
        DEFAULT_APPLICATIONS,
        DEFAULT_RECIPIENT_SCOPES,
        DEFAULT_REPORT_TYPES,
    )

    assert gateway_services.DEFAULT_APPLICATIONS is DEFAULT_APPLICATIONS
    assert gateway_services.DEFAULT_REPORT_TYPES is DEFAULT_REPORT_TYPES
    assert gateway_services.DEFAULT_RECIPIENT_SCOPES is DEFAULT_RECIPIENT_SCOPES
    assert directory_services.gateway_datetime is gateway_services.gateway_datetime

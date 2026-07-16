from __future__ import annotations

import inspect

import whatsapp_gateway.api as legacy_api


def test_legacy_api_facade_exports_core_symbols() -> None:
    required = {
        "ensure_defaults", "worker_health", "gateway_request", "overview", "connection",
        "sync_directory", "directory_groups", "directory_contacts", "configuration_options",
        "report_types", "recipient_scopes", "audiences", "dispatch_profiles",
        "setup_reporting_route", "send_test_message", "deliveries", "recipients",
        "groups", "templates", "read_settings", "update_settings", "activity_log",
    }
    missing = sorted(name for name in required if not hasattr(legacy_api, name))
    assert not missing, missing


def test_child_routers_and_facade_router_are_populated() -> None:
    child_routers = legacy_api._CHILD_ROUTERS
    assert child_routers
    assert all(item.routes for item in child_routers)
    assert len(legacy_api.router.routes) == sum(len(item.routes) for item in child_routers)


def test_all_whatsapp_routes_are_registered_once() -> None:
    actual: list[tuple[str, str]] = []
    for route in legacy_api.router.routes:
        path = getattr(route, "path", None)
        for method in getattr(route, "methods", set()):
            if method in {"GET", "POST", "PUT", "DELETE"} and path:
                actual.append((method, path))
    assert len(actual) == len(set(actual)), "Duplicate WhatsApp routes found"
    expected_paths = {
        "/api/v1/whatsapp/overview",
        "/api/v1/whatsapp/connection",
        "/api/v1/whatsapp/directory/sync",
        "/api/v1/whatsapp/configuration/options",
        "/api/v1/whatsapp/report-types",
        "/api/v1/whatsapp/recipient-scopes",
        "/api/v1/whatsapp/audiences",
        "/api/v1/whatsapp/dispatch-profiles",
        "/api/v1/whatsapp/reporting-routes/setup",
        "/api/v1/whatsapp/test-message",
        "/api/v1/whatsapp/deliveries",
        "/api/v1/whatsapp/groups",
        "/api/v1/whatsapp/templates",
        "/api/v1/whatsapp/settings",
        "/api/v1/whatsapp/activity",
    }
    found = {path for _, path in actual}
    assert expected_paths <= found


def test_facade_is_small_and_implementations_are_moved() -> None:
    source = inspect.getsource(legacy_api)
    assert len(source.splitlines()) <= 60
    assert legacy_api.overview.__module__ == "whatsapp_gateway.gateway.connection"
    assert legacy_api.sync_directory.__module__ == "whatsapp_gateway.directory.router"
    assert legacy_api.configuration_options.__module__ == "whatsapp_gateway.configuration.catalog"
    assert legacy_api.audiences.__module__ == "whatsapp_gateway.configuration.audiences"
    assert legacy_api.dispatch_profiles.__module__ == "whatsapp_gateway.configuration.profiles"
    assert legacy_api.send_test_message.__module__ == "whatsapp_gateway.dispatch.delivery"
    assert legacy_api.groups.__module__ == "whatsapp_gateway.dispatch.groups"
    assert legacy_api.templates.__module__ == "whatsapp_gateway.dispatch.templates"
    assert legacy_api.read_settings.__module__ == "whatsapp_gateway.dispatch.settings"

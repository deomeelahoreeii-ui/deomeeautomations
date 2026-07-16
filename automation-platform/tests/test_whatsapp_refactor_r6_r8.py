from __future__ import annotations

import ast
import hashlib
import importlib
import json
from pathlib import Path

import whatsapp_gateway.antidengue_renderer as legacy_renderer
import whatsapp_gateway.tasks as legacy_tasks

RENDERER_SYMBOLS = {
    "RENDERER_KEY", "WING_RENDERER_KEY", "PAKISTAN_TIME", "REQUIRED_COLUMNS",
    "DormantSourceRow", "ScopedDormantSchool", "RenderedTehsilDormantReport",
    "RenderedWingDormantReport", "_issue", "_normalize_emis", "_report_rows",
    "_canonical_report_artifact", "_school_count", "normalize_presentation_policy",
    "_has_attachment", "_wing_label", "_wing_display_name", "_report_time",
    "_build_message", "_build_wing_message", "_ensure_excel_attachment", "_font",
    "_wrap_text", "_ensure_image_attachment", "render_wing_dormant_report",
    "render_tehsil_dormant_report",
}
TASK_SYMBOLS = {
    "compile_dispatch_preview_job", "_publish_approved_deliveries",
    "send_approved_preview_job",
}


def package_root() -> Path:
    return Path(legacy_renderer.__file__).resolve().parent


def resolve(dotted: str):
    module_name, symbol_name = dotted.rsplit(".", 1)
    return getattr(importlib.import_module(module_name), symbol_name)


def test_movement_map_covers_every_legacy_renderer_and_task_symbol() -> None:
    mapping = json.loads(
        (package_root() / "MOVED_SYMBOLS_R6_R8.json").read_text(encoding="utf-8")
    )
    assert set(mapping["antidengue_renderer.py"]) == RENDERER_SYMBOLS
    assert set(mapping["tasks.py"]) == TASK_SYMBOLS


def test_every_moved_symbol_is_reexported_by_identity() -> None:
    mapping = json.loads(
        (package_root() / "MOVED_SYMBOLS_R6_R8.json").read_text(encoding="utf-8")
    )
    facades = {
        "antidengue_renderer.py": legacy_renderer,
        "tasks.py": legacy_tasks,
    }
    missing: dict[str, list[str]] = {}
    mismatched: dict[str, list[str]] = {}
    for source_name, symbols in mapping.items():
        facade = facades[source_name]
        for symbol_name, destination in symbols.items():
            if not hasattr(facade, symbol_name):
                missing.setdefault(source_name, []).append(symbol_name)
                continue
            if getattr(facade, symbol_name) is not resolve(destination):
                mismatched.setdefault(source_name, []).append(symbol_name)
    assert missing == {}
    assert mismatched == {}


def test_every_moved_definition_matches_original_structural_hash() -> None:
    contracts = json.loads(
        (package_root() / "SYMBOL_CONTRACTS_R6_R8.json").read_text(encoding="utf-8")
    )
    parsed: dict[str, dict[str, ast.AST]] = {}
    for symbol_name, contract in contracts.items():
        relative = contract["destination_file"]
        if relative not in parsed:
            tree = ast.parse((package_root() / relative).read_text(encoding="utf-8"))
            parsed[relative] = {
                node.name: node
                for node in tree.body
                if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            }
        node = parsed[relative][symbol_name]
        digest = hashlib.sha256(
            ast.dump(node, include_attributes=False).encode("utf-8")
        ).hexdigest()
        assert digest == contract["ast_sha256"], symbol_name


def test_renderer_facade_is_small_and_feature_modules_are_bounded() -> None:
    assert len(Path(legacy_renderer.__file__).read_text().splitlines()) <= 80
    renderer_root = package_root() / "rendering" / "antidengue"
    oversized = {
        path.name: len(path.read_text(encoding="utf-8").splitlines())
        for path in renderer_root.glob("*.py")
        if len(path.read_text(encoding="utf-8").splitlines()) > 250
    }
    assert oversized == {}


def test_task_facade_is_small_and_task_names_are_stable() -> None:
    assert len(Path(legacy_tasks.__file__).read_text().splitlines()) <= 80
    assert legacy_tasks.compile_dispatch_preview_job.name == (
        "whatsapp_gateway.compile_dispatch_preview"
    )
    assert legacy_tasks.send_approved_preview_job.name == (
        "whatsapp_gateway.send_approved_preview"
    )


def test_implementation_does_not_import_its_compatibility_facade() -> None:
    renderer_root = package_root() / "rendering" / "antidengue"
    for path in renderer_root.glob("*.py"):
        assert "whatsapp_gateway.antidengue_renderer" not in path.read_text(encoding="utf-8")
    for relative in ("dispatch/approved_delivery.py", "dispatch/task_entrypoints.py"):
        assert "whatsapp_gateway.tasks" not in (
            package_root() / relative
        ).read_text(encoding="utf-8")


def test_complete_package_respects_final_file_size_guardrail() -> None:
    oversized = {
        str(path.relative_to(package_root())): len(
            path.read_text(encoding="utf-8").splitlines()
        )
        for path in package_root().rglob("*.py")
        if len(path.read_text(encoding="utf-8").splitlines()) > 450
    }
    assert oversized == {}

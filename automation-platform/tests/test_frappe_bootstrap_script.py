from __future__ import annotations

import ast
from pathlib import Path
from typing import Any


def _load_normalizer():
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "frappe_helpdesk_bootstrap.py"
    tree = ast.parse(script.read_text(encoding="utf-8"))
    selected = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = {target.id for target in node.targets if isinstance(target, ast.Name)}
            if names & {"TRUE_CHECK_DEFAULTS", "FALSE_CHECK_DEFAULTS"}:
                selected.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == "normalize_check_default":
            selected.append(node)
    module = ast.Module(
        body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *selected],
        type_ignores=[],
    )
    ast.fix_missing_locations(module)
    namespace: dict[str, Any] = {"Any": Any}
    exec(compile(module, str(script), "exec"), namespace)
    return namespace["normalize_check_default"]


def test_frappe_check_default_normalization_handles_legacy_values() -> None:
    normalize = _load_normalizer()
    assert normalize(None) == "0"
    assert normalize(False) == "0"
    assert normalize(True) == "1"
    assert normalize(0) == "0"
    assert normalize(1) == "1"
    assert normalize(" false ") == "0"
    assert normalize("TRUE") == "1"
    assert normalize("off") == "0"
    assert normalize("yes") == "1"
    assert normalize("unexpected legacy value") == "0"


def test_bootstrap_repairs_helpdesk_metadata_before_custom_field_creation() -> None:
    project_root = Path(__file__).resolve().parents[1]
    script = (project_root / "scripts" / "frappe_helpdesk_bootstrap.py").read_text(encoding="utf-8")
    repair_call = '"check_default_repairs": repair_invalid_check_defaults("HD Ticket")'
    assert repair_call in script
    assert script.index(repair_call) < script.index("for field in FIELDS:")
    assert "UPDATE `tabDocField` SET `default`" in script
    assert "UPDATE `tabCustom Field` SET `default`" in script
    assert "UPDATE `tabProperty Setter` SET value" in script

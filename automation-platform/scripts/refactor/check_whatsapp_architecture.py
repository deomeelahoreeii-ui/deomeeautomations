#!/usr/bin/env python3
"""Enforce maintainability boundaries for the WhatsApp Gateway package."""

from __future__ import annotations

import argparse
import ast
from pathlib import Path

FACADE_LIMIT = 80
FEATURE_LIMIT = 250
PACKAGE_LIMIT = 450

FACADE_FILES = {
    "api.py",
    "models.py",
    "preview_api.py",
    "preview_service.py",
    "inbound_api.py",
    "inbound_service.py",
    "inbound_tasks.py",
    "identity_repair.py",
    "antidengue_renderer.py",
    "tasks.py",
}

STRICT_FEATURE_PREFIXES = (
    "rendering/antidengue/",
    "dispatch/approved_delivery.py",
    "dispatch/task_entrypoints.py",
)


def line_count(path: Path) -> int:
    return len(path.read_text(encoding="utf-8").splitlines())


def imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def check(package: Path) -> None:
    violations: list[str] = []
    for path in sorted(package.rglob("*.py")):
        relative = path.relative_to(package).as_posix()
        count = line_count(path)
        if relative in FACADE_FILES and count > FACADE_LIMIT:
            violations.append(f"facade {relative}: {count} > {FACADE_LIMIT}")
        if relative.startswith(STRICT_FEATURE_PREFIXES) and count > FEATURE_LIMIT:
            violations.append(f"feature {relative}: {count} > {FEATURE_LIMIT}")
        if count > PACKAGE_LIMIT:
            violations.append(f"package module {relative}: {count} > {PACKAGE_LIMIT}")

    renderer_root = package / "rendering" / "antidengue"
    for path in renderer_root.rglob("*.py"):
        imports = imported_modules(path)
        if "whatsapp_gateway.antidengue_renderer" in imports:
            violations.append(
                f"renderer implementation imports compatibility facade: {path.relative_to(package)}"
            )

    for relative in ("dispatch/approved_delivery.py", "dispatch/task_entrypoints.py"):
        path = package / relative
        if "whatsapp_gateway.tasks" in imported_modules(path):
            violations.append(f"dispatch task implementation imports facade: {relative}")

    if violations:
        raise SystemExit("WhatsApp architecture violations:\n- " + "\n- ".join(violations))

    print(
        "WhatsApp architecture checks passed: "
        f"facades <= {FACADE_LIMIT}, refactored features <= {FEATURE_LIMIT}, "
        f"all modules <= {PACKAGE_LIMIT}."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", type=Path, required=True)
    args = parser.parse_args()
    check(args.package.resolve())


if __name__ == "__main__":
    main()

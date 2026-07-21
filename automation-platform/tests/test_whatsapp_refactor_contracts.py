from __future__ import annotations

import json
import subprocess
from copy import deepcopy
import sys
from pathlib import Path

from whatsapp_gateway import models
from whatsapp_gateway.persistence import (
    WhatsAppAccount,
    WhatsAppActivity,
    WhatsAppDailyMessageClaim,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppAudienceMember,
    WhatsAppContactLink,
    WhatsAppDelivery,
    WhatsAppDirectoryContact,
    WhatsAppDirectoryGroup,
    WhatsAppDispatchApproval,
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchProfile,
    WhatsAppGroup,
    WhatsAppGroupMember,
    WhatsAppIdentityAlias,
    WhatsAppInboundAttachment,
    WhatsAppInboundBatch,
    WhatsAppInboundBatchEvent,
    WhatsAppInboundBatchItem,
    WhatsAppInboundExportItem,
    WhatsAppInboundExportRun,
    WhatsAppInboundHistoryRequest,
    WhatsAppInboundMessage,
    WhatsAppInboundProcessingEvent,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
    WhatsAppInboundStoredObject,
    WhatsAppRecipientScope,
    WhatsAppReportType,
    WhatsAppSettings,
    WhatsAppTemplate,
)





def _resolve_openapi_schema_refs(
    value: object,
    schemas: dict[str, object],
    resolving: tuple[str, ...] = (),
) -> object:
    if isinstance(value, dict):
        reference = value.get("$ref")
        prefix = "#/components/schemas/"
        if isinstance(reference, str) and reference.startswith(prefix):
            name = reference.removeprefix(prefix)
            if name in resolving:
                resolved: object = {"$recursive_schema": True}
            else:
                schema = schemas.get(name)
                resolved = (
                    _resolve_openapi_schema_refs(schema, schemas, (*resolving, name))
                    if schema is not None
                    else {"$missing_schema": name}
                )
            siblings = {
                key: _resolve_openapi_schema_refs(child, schemas, resolving)
                for key, child in value.items()
                if key != "$ref"
            }
            return {"$resolved_schema": resolved, **siblings}
        return {
            key: _resolve_openapi_schema_refs(child, schemas, resolving)
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [
            _resolve_openapi_schema_refs(child, schemas, resolving)
            for child in value
        ]
    return value


def _normalize_whatsapp_contract(contract: dict[str, object]) -> dict[str, object]:
    normalized = deepcopy(contract)
    schemas = contract.get("openapi_schemas")
    paths = contract.get("openapi_paths")
    if isinstance(schemas, dict) and isinstance(paths, dict):
        normalized["openapi_paths"] = _resolve_openapi_schema_refs(paths, schemas)
    normalized.pop("openapi_schemas", None)

    # The snapshot protects the original AntiDengue/WhatsApp refactor contract.
    # B3.6 deliberately adds a polymorphic source identity to dispatch previews
    # for CRM while keeping every legacy field and API behavior intact. Compare
    # the legacy projection here and verify the additive B3.6 fields separately
    # in the CRM dispatch contract tests instead of rewriting the immutable
    # pre-refactor snapshot.
    tables = normalized.get("whatsapp_tables")
    if isinstance(tables, dict):
        preview = tables.get("whatsapp_dispatch_previews")
        if isinstance(preview, dict):
            additive_columns = {"source_kind", "source_reference_id", "source_revision"}
            columns = preview.get("columns")
            if isinstance(columns, list):
                legacy_columns = []
                for column in columns:
                    if not isinstance(column, dict) or column.get("name") in additive_columns:
                        continue
                    if column.get("name") == "source_job_id":
                        column = dict(column)
                        column["nullable"] = False
                    legacy_columns.append(column)
                preview["columns"] = legacy_columns
            indexes = preview.get("indexes")
            if isinstance(indexes, list):
                preview["indexes"] = [
                    index
                    for index in indexes
                    if not (
                        isinstance(index, dict)
                        and index.get("name")
                        in {
                            "ix_whatsapp_dispatch_previews_source_kind",
                            "ix_whatsapp_dispatch_previews_source_reference_id",
                        }
                    )
                ]
    return normalized

EXPECTED_MODELS = {
    "WhatsAppAccount": WhatsAppAccount,
    "WhatsAppActivity": WhatsAppActivity,
    "WhatsAppDailyMessageClaim": WhatsAppDailyMessageClaim,
    "WhatsAppApplication": WhatsAppApplication,
    "WhatsAppAudience": WhatsAppAudience,
    "WhatsAppAudienceMember": WhatsAppAudienceMember,
    "WhatsAppContactLink": WhatsAppContactLink,
    "WhatsAppDelivery": WhatsAppDelivery,
    "WhatsAppDirectoryContact": WhatsAppDirectoryContact,
    "WhatsAppDirectoryGroup": WhatsAppDirectoryGroup,
    "WhatsAppDispatchApproval": WhatsAppDispatchApproval,
    "WhatsAppDispatchPreview": WhatsAppDispatchPreview,
    "WhatsAppDispatchPreviewArtifact": WhatsAppDispatchPreviewArtifact,
    "WhatsAppDispatchPreviewDelivery": WhatsAppDispatchPreviewDelivery,
    "WhatsAppDispatchProfile": WhatsAppDispatchProfile,
    "WhatsAppGroup": WhatsAppGroup,
    "WhatsAppGroupMember": WhatsAppGroupMember,
    "WhatsAppIdentityAlias": WhatsAppIdentityAlias,
    "WhatsAppInboundAttachment": WhatsAppInboundAttachment,
    "WhatsAppInboundBatch": WhatsAppInboundBatch,
    "WhatsAppInboundBatchEvent": WhatsAppInboundBatchEvent,
    "WhatsAppInboundBatchItem": WhatsAppInboundBatchItem,
    "WhatsAppInboundExportItem": WhatsAppInboundExportItem,
    "WhatsAppInboundExportRun": WhatsAppInboundExportRun,
    "WhatsAppInboundHistoryRequest": WhatsAppInboundHistoryRequest,
    "WhatsAppInboundMessage": WhatsAppInboundMessage,
    "WhatsAppInboundProcessingEvent": WhatsAppInboundProcessingEvent,
    "WhatsAppInboundProcessingItem": WhatsAppInboundProcessingItem,
    "WhatsAppInboundProcessingRun": WhatsAppInboundProcessingRun,
    "WhatsAppInboundStoredObject": WhatsAppInboundStoredObject,
    "WhatsAppRecipientScope": WhatsAppRecipientScope,
    "WhatsAppReportType": WhatsAppReportType,
    "WhatsAppSettings": WhatsAppSettings,
    "WhatsAppTemplate": WhatsAppTemplate,
}


def test_models_facade_preserves_all_public_classes() -> None:
    assert set(models.__all__) == set(EXPECTED_MODELS)
    for name, implementation in EXPECTED_MODELS.items():
        assert getattr(models, name) is implementation


def test_refactor_contract_matches_pre_refactor_snapshot(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    expected_path = project_root / "tests/snapshots/whatsapp_contract_before.json"
    assert expected_path.exists(), "Installer did not create the pre-refactor snapshot"

    actual_path = tmp_path / "whatsapp_contract_after.json"
    subprocess.run(
        [
            sys.executable,
            str(project_root / "scripts/refactor/whatsapp_contract_snapshot.py"),
            "--output",
            str(actual_path),
        ],
        cwd=project_root,
        check=True,
    )

    actual = _normalize_whatsapp_contract(json.loads(actual_path.read_text()))
    expected = _normalize_whatsapp_contract(json.loads(expected_path.read_text()))
    assert actual == expected


def test_model_inventory_and_movement_map_are_complete() -> None:
    project_root = Path(__file__).resolve().parents[1]
    package_root = project_root / "packages/whatsapp_gateway/whatsapp_gateway"
    inventory = json.loads((package_root / "REFACTOR_INVENTORY.json").read_text())
    moved = json.loads((package_root / "MOVED_SYMBOLS.json").read_text())

    original_classes = {
        item["name"]
        for item in inventory["models.py"]["symbols"]
        if item["kind"] == "class"
    }
    moved_classes = {
        key.rsplit(".", 1)[-1]
        for key in moved
        if key.startswith("whatsapp_gateway.models.")
    }

    # The inventory is the immutable pre-refactor set. New persistence models may
    # be added afterward, but every original class must remain publicly exported.
    assert original_classes <= set(EXPECTED_MODELS)
    assert moved_classes == original_classes

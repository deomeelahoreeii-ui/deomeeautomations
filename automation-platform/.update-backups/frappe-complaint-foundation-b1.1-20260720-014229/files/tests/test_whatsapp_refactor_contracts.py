from __future__ import annotations

import json
import subprocess
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

    assert json.loads(actual_path.read_text()) == json.loads(expected_path.read_text())


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

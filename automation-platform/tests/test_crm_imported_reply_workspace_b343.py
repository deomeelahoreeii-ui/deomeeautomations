from __future__ import annotations

import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from crm_domain.bulk_operations import CrmBulkOperationService
from crm_domain.models import ComplaintCase, ComplaintReply, ComplaintReplyRevision
from crm_domain.reply_workspace import ComplaintReplyWorkspaceService, ReplyRemoteError
from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskError


ROOT = Path(__file__).resolve().parents[1]
EDITOR_PAGE = ROOT / "apps/web/src/pages/crm/replies/[id].astro"
BATCH_PAGE = ROOT / "apps/web/src/pages/crm/replies/bulk/batches/[id].astro"
MIGRATION = ROOT / "alembic/versions/f7d1e3a5c842_crm_imported_reply_workspace.py"


class FakeClient:
    def __init__(self) -> None:
        self.ticket: dict[str, Any] = {
            "name": "0900",
            "status": "New",
            "modified": "2026-07-21T08:00:00+00:00",
            "custom_reply_approval_status": "Not Prepared",
            "custom_final_approved_reply": "",
            "custom_inquiry_findings": "",
            "custom_school_version": "",
            "custom_applicable_policy": "",
            "custom_disposal_outcome": "",
            "custom_ai_eligible": 0,
        }
        self.fail_update = False

    def get_resource(self, doctype: str, name: str) -> dict[str, Any]:
        assert doctype == "HD Ticket"
        assert name == "0900"
        return deepcopy(self.ticket)

    def update_resource(self, doctype: str, name: str, values: dict[str, Any]) -> dict[str, Any]:
        if self.fail_update:
            raise FrappeHelpdeskError("Helpdesk unavailable", status_code=503)
        self.ticket.update(deepcopy(values))
        self.ticket["modified"] = "2026-07-21T08:30:00+00:00"
        return deepcopy(self.ticket)

    def open_assignments(self, ticket_name: str) -> list[dict[str, Any]]:
        return []


def engine():
    db = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db)
    return db


def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        database_url="sqlite://",
        artifact_root=tmp_path / "artifacts",
        deomee_root=tmp_path,
        frappe_helpdesk_enabled=True,
        frappe_helpdesk_url="http://helpdesk.test",
        frappe_helpdesk_public_url="http://helpdesk.test",
        frappe_helpdesk_api_key="key",
        frappe_helpdesk_api_secret="secret",
        paperless_url="http://paperless.test",
        frappe_helpdesk_approved_reply_statuses="Approved,Issued",
    )


def seed(session: Session) -> ComplaintCase:
    case = ComplaintCase(
        complaint_number="104-9900001",
        state="published",
        remarks="Complaint requiring a reviewed official response.",
        category="Administrative",
        sub_category="Others",
        frappe_ticket_id="0900",
        frappe_ticket_url="http://helpdesk.test/helpdesk/tickets/0900",
        frappe_sync_status="synchronized",
        frappe_reply_approval_status="Not Prepared",
    )
    session.add(case)
    session.commit()
    session.refresh(case)
    return case


def import_reply(session: Session, cfg: Settings, case: ComplaintCase) -> dict[str, Any]:
    service = CrmBulkOperationService(session, cfg)
    validation = service.validate_import_batch(
        content=(
            "Complaint Number,Reply\n"
            f'{case.complaint_number},"Imported reply ready for human review."\n'
        ).encode(),
        filename="completed-replies.csv",
    )
    return service.commit_import_batch(uuid.UUID(validation["id"]))


def test_imported_reply_is_immediately_visible_and_traceable_in_editor(tmp_path: Path) -> None:
    db = engine()
    client = FakeClient()
    with Session(db) as session:
        case = seed(session)
        committed = import_reply(session, settings(tmp_path), case)
        editor = ComplaintReplyWorkspaceService(session, settings(tmp_path), client).editor(case.id)

        assert editor["reply"]["final_reply"] == "Imported reply ready for human review."
        assert editor["reply"]["reply_status"] == "Draft"
        assert editor["reply"]["display_status"] == "Imported Draft"
        assert editor["reply"]["source"] == "local_workspace"
        assert editor["reply"]["sync_status"] == "not_synced"
        assert editor["reply"]["source_batch"]["id"] == committed["id"]
        assert editor["reply"]["source_filename"] == "completed-replies.csv"
        assert editor["approved_archive"]["reply_text"] == ""
        assert session.exec(select(ComplaintReplyRevision)).first() is None


def test_editor_save_synchronizes_imported_draft_without_losing_batch_lineage(tmp_path: Path) -> None:
    db = engine()
    client = FakeClient()
    with Session(db) as session:
        case = seed(session)
        committed = import_reply(session, settings(tmp_path), case)
        result = ComplaintReplyWorkspaceService(session, settings(tmp_path), client).save_reply(
            case.id,
            inquiry_findings="Available record was reviewed.",
            final_reply="Imported reply ready for human review.",
            reply_status="Draft",
        )
        assert result["write_verified"] is True
        workspace = session.exec(
            select(ComplaintReply).where(ComplaintReply.complaint_case_id == case.id)
        ).one()
        assert workspace.workspace_status == "Draft"
        assert workspace.sync_status == "synchronized"
        assert workspace.source_batch_id == uuid.UUID(committed["id"])
        assert workspace.last_synced_at is not None
        assert client.ticket["custom_final_approved_reply"] == workspace.reply_text


def test_remote_failure_preserves_latest_local_edit(tmp_path: Path) -> None:
    db = engine()
    client = FakeClient()
    with Session(db) as session:
        case = seed(session)
        import_reply(session, settings(tmp_path), case)
        client.fail_update = True
        with pytest.raises(ReplyRemoteError, match="Helpdesk unavailable"):
            ComplaintReplyWorkspaceService(session, settings(tmp_path), client).save_reply(
                case.id,
                final_reply="Reviewer edited reply that must not be lost.",
                reply_status="Draft",
            )
        workspace = session.exec(
            select(ComplaintReply).where(ComplaintReply.complaint_case_id == case.id)
        ).one()
        assert workspace.reply_text == "Reviewer edited reply that must not be lost."
        assert workspace.sync_status == "failed"
        assert "Helpdesk unavailable" in (workspace.sync_error or "")
        editor = ComplaintReplyWorkspaceService(session, settings(tmp_path), client).editor(case.id)
        assert editor["reply"]["final_reply"] == workspace.reply_text
        assert editor["reply"]["sync_status"] == "failed"


def test_different_remote_text_is_reported_as_conflict_without_overwriting_local(tmp_path: Path) -> None:
    db = engine()
    client = FakeClient()
    with Session(db) as session:
        case = seed(session)
        import_reply(session, settings(tmp_path), case)
        client.ticket.update(
            custom_final_approved_reply="Different Helpdesk draft.",
            custom_reply_approval_status="Draft",
        )
        editor = ComplaintReplyWorkspaceService(session, settings(tmp_path), client).editor(case.id)
        assert editor["reply"]["conflict"] is True
        assert editor["reply"]["final_reply"] == "Imported reply ready for human review."
        assert editor["reply"]["remote_final_reply"] == "Different Helpdesk draft."
        persisted = session.exec(select(ComplaintReply)).one()
        assert persisted.reply_text == "Imported reply ready for human review."


def test_ui_and_migration_expose_import_to_editor_contract() -> None:
    editor = EDITOR_PAGE.read_text(encoding="utf-8")
    batch = BATCH_PAGE.read_text(encoding="utf-8")
    migration = MIGRATION.read_text(encoding="utf-8")
    for token in (
        "reply-source-panel",
        "reply-source-title",
        "reply-conflict-dialog",
        "local-conflict-reply",
        "remote-conflict-reply",
        "Local draft preserved",
    ):
        assert token in editor
    assert "Review reply" in batch
    assert "reply_preview" in batch
    assert '?return=${encodeURIComponent(location.pathname)}' in batch
    assert 'revision: str = "f7d1e3a5c842"' in migration
    assert 'down_revision: Union[str, None] = "f5c9e1a3b620"' in migration
    for column in (
        "workspace_status",
        "sync_status",
        "source_batch_id",
        "source_item_id",
        "inquiry_findings",
        "applicable_policy",
    ):
        assert f'"{column}"' in migration

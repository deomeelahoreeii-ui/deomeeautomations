from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintCategory,
    ComplaintReply,
    ComplaintReplyRevision,
    ComplaintSubcategory,
)
from crm_domain.reply_workspace import (
    ComplaintReplyWorkspaceService,
    ReplyRemoteError,
    ReplyValidationError,
)
from crm_domain.reply_workspace_api import router
from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskError


class ReplyFakeClient:
    def __init__(self) -> None:
        self.resources: dict[str, dict[str, dict[str, Any]]] = {
            "HD Ticket Type": {
                "Administrative": {"name": "Administrative", "disabled": 0, "is_system": 0}
            },
            "HD Ticket": {},
        }
        self.assignments: dict[str, list[dict[str, Any]]] = {}
        self.mismatch_field = ""
        self.fail_update: Exception | None = None
        self.update_calls = 0

    def get_resource(self, doctype: str, name: str) -> dict[str, Any]:
        try:
            row = deepcopy(self.resources[doctype][name])
        except KeyError as exc:
            raise FrappeHelpdeskError("not found", status_code=404) from exc
        if doctype == "HD Ticket" and self.mismatch_field:
            row[self.mismatch_field] = "server changed this value"
        return row

    def create_resource(self, doctype: str, values: dict[str, Any]) -> dict[str, Any]:
        name = str(values.get("name") or "")
        row = {"name": name, **deepcopy(values)}
        self.resources.setdefault(doctype, {})[name] = row
        return deepcopy(row)

    def update_resource(self, doctype: str, name: str, values: dict[str, Any]) -> dict[str, Any]:
        if self.fail_update:
            raise self.fail_update
        self.update_calls += 1
        row = self.resources.setdefault(doctype, {}).setdefault(name, {"name": name})
        row.update(deepcopy(values))
        row["modified"] = f"2026-07-20T12:{self.update_calls:02d}:00+00:00"
        return deepcopy(row)

    def open_assignments(self, ticket_name: str) -> list[dict[str, Any]]:
        return deepcopy(self.assignments.get(ticket_name, []))


def settings() -> Settings:
    return Settings(
        _env_file=None,
        frappe_helpdesk_enabled=True,
        frappe_helpdesk_url="http://helpdesk.test",
        frappe_helpdesk_public_url="http://helpdesk.test",
        frappe_helpdesk_api_key="key",
        frappe_helpdesk_api_secret="secret",
        frappe_helpdesk_crm_public_url="http://crm.test",
        paperless_url="http://paperless.test",
        frappe_helpdesk_approved_reply_statuses="Approved,Issued",
    )


def engine():
    value = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(value)
    return value


def seed(session: Session, client: ReplyFakeClient) -> tuple[ComplaintCase, ComplaintCategory, ComplaintSubcategory]:
    category = ComplaintCategory(
        name="Administrative",
        normalized_name="administrative",
        frappe_ticket_type_name="Administrative",
        frappe_sync_status="synchronized",
    )
    session.add(category)
    session.flush()
    subcategory = ComplaintSubcategory(
        category_id=category.id,
        name="Others",
        normalized_name="others",
    )
    session.add(subcategory)
    session.flush()
    case = ComplaintCase(
        complaint_number="104-7000200",
        state="published",
        remarks="The complainant requested administrative action.",
        category="Administrative",
        sub_category="Others",
        category_id=category.id,
        sub_category_id=subcategory.id,
        canonical_paperless_document_id=785,
        frappe_ticket_id="0015",
        frappe_ticket_url="http://helpdesk.test/helpdesk/tickets/0015",
        frappe_sync_status="synchronized",
        classification_sync_status="synchronized",
    )
    session.add(case)
    session.commit()
    client.resources["HD Ticket"]["0015"] = {
        "name": "0015",
        "subject": "Complaint 104-7000200 — Administrative",
        "ticket_type": "Administrative",
        "custom_sub_category": "Others",
        "status": "New",
        "agent_group": None,
        "custom_inquiry_findings": "",
        "custom_school_version": "",
        "custom_applicable_policy": "",
        "custom_final_approved_reply": "",
        "custom_reply_approval_status": "Not Prepared",
        "custom_disposal_outcome": "",
        "custom_ai_eligible": 0,
        "modified": "2026-07-20T12:00:00+00:00",
    }
    return case, category, subcategory


def test_classification_write_is_read_back_and_audited() -> None:
    db = engine()
    client = ReplyFakeClient()
    with Session(db) as session:
        case, category, subcategory = seed(session, client)
        result = ComplaintReplyWorkspaceService(session, settings(), client).save_classification(
            case.id,
            category_id=category.id,
            subcategory_id=subcategory.id,
            synchronize=True,
        )
        assert result["synchronized"] is True
        assert client.resources["HD Ticket"]["0015"]["custom_sub_category"] == "Others"
        persisted = session.get(ComplaintCase, case.id)
        assert persisted is not None
        assert persisted.classification_sync_status == "synchronized"
        assert persisted.classification_last_synced_at is not None
        sync_audit = session.exec(
            select(ComplaintAuditEvent).where(
                ComplaintAuditEvent.event_type == "classification_synchronized"
            )
        ).one()
        assert isinstance(sync_audit.after_json["last_synced_at"], str)
        assert {
            event.event_type
            for event in session.exec(select(ComplaintAuditEvent)).all()
        } >= {"classification_saved", "classification_synchronized"}


def test_draft_is_persisted_as_snapshot_but_not_approved_archive() -> None:
    db = engine()
    client = ReplyFakeClient()
    with Session(db) as session:
        case, _, _ = seed(session, client)
        result = ComplaintReplyWorkspaceService(session, settings(), client).save_reply(
            case.id,
            inquiry_findings="School was visited.",
            final_reply="This is still a working draft.",
            reply_status="Draft",
            ai_eligible=False,
        )
        assert result["write_verified"] is True
        persisted = session.get(ComplaintCase, case.id)
        assert persisted is not None
        assert persisted.frappe_reply_approval_status == "Draft"
        assert persisted.frappe_reply_text_snapshot == "This is still a working draft."
        workspace = session.exec(select(ComplaintReply)).one()
        assert workspace.reply_text == "This is still a working draft."
        assert workspace.workspace_status == "Draft"
        assert workspace.sync_status == "synchronized"
        assert session.exec(select(ComplaintReplyRevision)).first() is None
        reply_audit = session.exec(
            select(ComplaintAuditEvent).where(
                ComplaintAuditEvent.event_type == "reply_saved"
            )
        ).one()
        assert isinstance(reply_audit.after_json["last_pulled_at"], str)


def test_approved_and_revised_replies_create_immutable_versions() -> None:
    db = engine()
    client = ReplyFakeClient()
    with Session(db) as session:
        case, _, _ = seed(session, client)
        service = ComplaintReplyWorkspaceService(session, settings(), client)
        first = service.save_reply(
            case.id,
            inquiry_findings="The school was sensitized.",
            applicable_policy="Applicable instructions were reviewed.",
            final_reply="The matter has been examined and addressed.",
            reply_status="Approved",
            ai_eligible=True,
        )
        assert first["archive"]["imported"] is True
        second = service.save_reply(
            case.id,
            inquiry_findings="The school was revisited.",
            applicable_policy="Applicable instructions were reviewed.",
            final_reply="The revised matter has been examined and the school was sensitized.",
            reply_status="Issued",
            ai_eligible=True,
        )
        assert second["archive"]["changed"] is True
        current = session.exec(select(ComplaintReply)).one()
        revisions = session.exec(
            select(ComplaintReplyRevision).order_by(ComplaintReplyRevision.captured_at)
        ).all()
        assert current.version == 2
        assert current.reply_text.startswith("The revised")
        assert len(revisions) == 2
        assert sum(row.is_current for row in revisions) == 1
        assert {row.approval_status for row in revisions} == {"Approved", "Issued"}


def test_ai_eligibility_and_empty_approval_are_rejected_before_remote_write() -> None:
    db = engine()
    client = ReplyFakeClient()
    with Session(db) as session:
        case, _, _ = seed(session, client)
        service = ComplaintReplyWorkspaceService(session, settings(), client)
        with pytest.raises(ReplyValidationError, match="AI eligibility"):
            service.save_reply(
                case.id,
                final_reply="Draft",
                reply_status="Draft",
                ai_eligible=True,
            )
        with pytest.raises(ReplyValidationError, match="final reply"):
            service.save_reply(case.id, final_reply="", reply_status="Approved")
        assert client.update_calls == 0


def test_remote_readback_mismatch_is_a_failure_and_does_not_create_archive() -> None:
    db = engine()
    client = ReplyFakeClient()
    client.mismatch_field = "custom_final_approved_reply"
    with Session(db) as session:
        case, _, _ = seed(session, client)
        with pytest.raises(ReplyRemoteError, match="read-back mismatch"):
            ComplaintReplyWorkspaceService(session, settings(), client).save_reply(
                case.id,
                final_reply="Approved text",
                reply_status="Approved",
                ai_eligible=True,
            )
        workspace = session.exec(select(ComplaintReply)).one()
        assert workspace.reply_text == "Approved text"
        assert workspace.sync_status == "failed"
        assert workspace.sync_error
        assert session.exec(select(ComplaintReplyRevision)).first() is None
        failed = session.exec(
            select(ComplaintAuditEvent).where(
                ComplaintAuditEvent.event_type == "reply_save_failed"
            )
        ).one()
        assert failed.state == "failed"


def test_queue_statistics_and_editor_use_live_helpdesk_values() -> None:
    db = engine()
    client = ReplyFakeClient()
    with Session(db) as session:
        case, _, _ = seed(session, client)
        client.resources["HD Ticket"]["0015"].update(
            custom_final_approved_reply="Live draft",
            custom_reply_approval_status="Draft",
            custom_inquiry_findings="Live findings",
        )
        service = ComplaintReplyWorkspaceService(session, settings(), client)
        editor = service.editor(case.id)
        assert editor["live_helpdesk"] is True
        assert editor["reply"]["final_reply"] == "Live draft"
        assert editor["reply"]["inquiry_findings"] == "Live findings"
        queue = service.list_cases(page=1, page_size=10)
        assert queue["total"] == 1
        assert queue["items"][0]["complaint_number"] == "104-7000200"
        assert service.statistics()["awaiting_reply"] == 1


def test_reply_api_distinguishes_validation_and_remote_failures() -> None:
    db = engine()
    client = ReplyFakeClient()
    with Session(db) as session:
        case, _, _ = seed(session, client)
        case_id = case.id

    app = FastAPI()
    app.include_router(router)

    def local_session():
        with Session(db) as session:
            yield session

    app.dependency_overrides[get_session] = local_session
    app.dependency_overrides[get_settings] = settings
    # Patch the service constructor's client builder through a dependency-neutral
    # fake by replacing the module-level build_client used by the service.
    import crm_domain.reply_workspace as workspace_module

    original = workspace_module.build_client
    workspace_module.build_client = lambda _settings: client
    try:
        test_client = TestClient(app)
        validation = test_client.put(
            f"/api/v1/crm/reply-workspace/cases/{case_id}/reply",
            json={
                "final_reply": "Draft",
                "reply_status": "Draft",
                "ai_eligible": True,
            },
        )
        assert validation.status_code == 409
        client.fail_update = FrappeHelpdeskError("unavailable", status_code=503)
        remote = test_client.put(
            f"/api/v1/crm/reply-workspace/cases/{case_id}/reply",
            json={
                "final_reply": "Approved reply",
                "reply_status": "Approved",
                "ai_eligible": True,
            },
        )
        assert remote.status_code == 502
    finally:
        workspace_module.build_client = original
        app.dependency_overrides.clear()


def test_rejected_reply_is_verified_but_never_enters_approved_archive() -> None:
    db = engine()
    client = ReplyFakeClient()
    with Session(db) as session:
        case, _, _ = seed(session, client)
        result = ComplaintReplyWorkspaceService(session, settings(), client).save_reply(
            case.id,
            inquiry_findings="Evidence was insufficient.",
            final_reply="This draft was rejected during review.",
            reply_status="Rejected",
            ai_eligible=False,
        )
        assert result["write_verified"] is True
        persisted = session.get(ComplaintCase, case.id)
        assert persisted is not None
        assert persisted.frappe_reply_approval_status == "Rejected"
        assert persisted.frappe_reply_text_snapshot == "This draft was rejected during review."
        workspace = session.exec(select(ComplaintReply)).one()
        assert workspace.workspace_status == "Rejected"
        assert workspace.sync_status == "synchronized"
        assert session.exec(select(ComplaintReplyRevision)).first() is None


def test_bulk_classification_reports_each_case_without_aborting_successes() -> None:
    db = engine()
    client = ReplyFakeClient()
    with Session(db) as session:
        case, category, subcategory = seed(session, client)
        missing_id = uuid.uuid4()
        result = ComplaintReplyWorkspaceService(session, settings(), client).bulk_classify(
            [case.id, missing_id],
            category_id=category.id,
            subcategory_id=subcategory.id,
            synchronize=True,
        )
        assert result["requested"] == 2
        assert result["counts"]["synchronized"] == 1
        assert result["counts"]["failed"] == 1
        assert result["results"][0]["case_id"] == str(case.id)
        assert result["results"][1]["case_id"] == str(missing_id)

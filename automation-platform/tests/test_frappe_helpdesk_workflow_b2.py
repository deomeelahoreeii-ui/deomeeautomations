from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from crm_domain.models import ComplaintCase, ComplaintReply, ComplaintReplyRevision
from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskError
from crm_integrations.frappe_helpdesk.routing import ComplaintHelpdeskRoutingService
from crm_integrations.frappe_helpdesk.workflow import ComplaintHelpdeskWorkflowService
from master_data.models import Officer, Tehsil


class WorkflowFakeClient:
    def __init__(self) -> None:
        self.resources: dict[str, dict[str, dict[str, Any]]] = {
            "HD Agent": {
                "ahmadkakarr@gmail.com": {
                    "name": "ahmadkakarr@gmail.com",
                    "user": "ahmadkakarr@gmail.com",
                    "is_active": 1,
                },
                "ddeo@example.com": {
                    "name": "ddeo@example.com",
                    "user": "ddeo@example.com",
                    "is_active": 1,
                },
            },
            "HD Team": {},
            "HD Ticket": {},
        }
        self.assignments: dict[str, list[dict[str, Any]]] = {}
        self.updates = 0

    def get_resource(self, doctype: str, name: str) -> dict[str, Any]:
        try:
            return deepcopy(self.resources[doctype][name])
        except KeyError as exc:
            raise FrappeHelpdeskError("not found", status_code=404) from exc

    def create_resource(self, doctype: str, values: dict[str, Any]) -> dict[str, Any]:
        name = str(values.get("name") or values.get("team_name") or "")
        row = {"name": name, **deepcopy(values)}
        self.resources.setdefault(doctype, {})[name] = row
        return deepcopy(row)

    def update_resource(self, doctype: str, name: str, values: dict[str, Any]) -> dict[str, Any]:
        self.updates += 1
        row = self.resources.setdefault(doctype, {}).setdefault(name, {"name": name})
        row.update(deepcopy(values))
        row["modified"] = "2026-07-20T03:00:00+00:00"
        return deepcopy(row)

    def open_assignments(self, ticket_name: str) -> list[dict[str, Any]]:
        return deepcopy(self.assignments.get(ticket_name, []))

    def assign_to_ticket(self, ticket_name: str, users: list[str], *, description: str = "") -> None:
        existing = {row["allocated_to"] for row in self.assignments.get(ticket_name, [])}
        for user in users:
            if user not in existing:
                self.assignments.setdefault(ticket_name, []).append(
                    {"name": uuid.uuid4().hex, "allocated_to": user, "status": "Open", "description": description}
                )


def settings() -> Settings:
    return Settings(
        _env_file=None,
        frappe_helpdesk_enabled=True,
        frappe_helpdesk_url="http://helpdesk.test",
        frappe_helpdesk_public_url="http://helpdesk.test",
        frappe_helpdesk_api_key="key",
        frappe_helpdesk_api_secret="secret",
        frappe_helpdesk_fallback_agent_email="ahmadkakarr@gmail.com",
        frappe_helpdesk_triage_team="Complaint Triage",
        frappe_helpdesk_team_prefix="Complaints",
    )


def engine():
    value = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(value)
    return value


def add_case_and_officer(session: Session) -> tuple[ComplaintCase, Officer]:
    district = uuid.uuid4()
    department = uuid.uuid4()
    wing = uuid.uuid4()
    tehsil = Tehsil(name="Shalimar", district_id=district)
    session.add(tehsil)
    session.flush()
    officer = Officer(
        role="ddeo",
        name="DDEO Shalimar",
        mobile="923001234567",
        normalized_mobile="923001234567",
        helpdesk_user_email="ddeo@example.com",
        helpdesk_enabled=True,
        district_id=district,
        department_id=department,
        wing_id=wing,
        tehsil_id=tehsil.id,
    )
    case = ComplaintCase(
        complaint_number="104-7000001",
        state="published",
        tehsil="shalimar",
        remarks="Complaint text",
        category="Administrative",
        canonical_paperless_document_id=1,
        frappe_ticket_id="0002",
        frappe_ticket_url="http://helpdesk.test/helpdesk/tickets/0002",
        frappe_sync_status="synchronized",
    )
    session.add(officer)
    session.add(case)
    session.commit()
    return case, officer


def add_ticket(client: WorkflowFakeClient, **changes: Any) -> None:
    row = {
        "name": "0002",
        "status": "New",
        "agent_group": None,
        "modified": "2026-07-20T02:30:00+00:00",
        "custom_reply_approval_status": "Not Prepared",
        "custom_final_approved_reply": "",
        "custom_ai_eligible": 0,
    }
    row.update(changes)
    client.resources["HD Ticket"]["0002"] = row


def test_team_bootstrap_is_idempotent_and_uses_fallback_agent() -> None:
    db = engine()
    client = WorkflowFakeClient()
    with Session(db) as session:
        add_case_and_officer(session)
        service = ComplaintHelpdeskRoutingService(session, settings(), client)
        first = service.bootstrap_teams()
        second = service.bootstrap_teams()
        assert first["teams"]["created"] == 2
        assert second["teams"]["unchanged"] == 2
        assert client.resources["HD Team"]["Complaint Triage"]["users"] == [
            {"user": "ahmadkakarr@gmail.com"}
        ]


def test_signed_routing_preview_applies_exact_team_and_officer_assignment() -> None:
    db = engine()
    client = WorkflowFakeClient()
    add_ticket(client)
    with Session(db) as session:
        case, officer = add_case_and_officer(session)
        service = ComplaintHelpdeskRoutingService(session, settings(), client)
        preview = service.preview(limit=1)
        assert preview["items"][0]["target_team"] == "Complaints - Shalimar"
        assert preview["items"][0]["officer_id"] == str(officer.id)
        result = service.apply(preview["batch_token"])
        assert result["counts"] == {"routed": 1}
        persisted = session.get(ComplaintCase, case.id)
        assert persisted is not None
        assert persisted.frappe_agent_group == "Complaints - Shalimar"
        assert persisted.frappe_routing_status == "routed"
        assert persisted.frappe_assigned_officer_id == officer.id
        assert persisted.frappe_assigned_users_json == ["ddeo@example.com"]
        assert client.resources["HD Ticket"]["0002"]["status"] == "Assigned"


def test_routing_preserves_unrelated_manual_assignment_without_force() -> None:
    db = engine()
    client = WorkflowFakeClient()
    add_ticket(client, agent_group="Product Experts")
    client.assignments["0002"] = [{"allocated_to": "manual@example.com", "status": "Open"}]
    with Session(db) as session:
        case, _ = add_case_and_officer(session)
        service = ComplaintHelpdeskRoutingService(session, settings(), client)
        preview = service.preview(limit=1)
        assert preview["items"][0]["disposition"] == "manual_assignment_preserved"
        result = service.apply(preview["batch_token"])
        assert result["counts"] == {"manual_preserved": 1}
        assert session.get(ComplaintCase, case.id).frappe_routing_status == "manual_preserved"
        assert client.updates == 0


def test_draft_reply_is_never_imported() -> None:
    db = engine()
    client = WorkflowFakeClient()
    add_ticket(
        client,
        status="Reply Under Preparation",
        custom_reply_approval_status="Draft",
        custom_final_approved_reply="This draft must stay in Helpdesk.",
    )
    with Session(db) as session:
        case, _ = add_case_and_officer(session)
        result = ComplaintHelpdeskWorkflowService(session, settings(), client).pull_case(case.id)
        assert result["status"] == "pulled"
        assert result["reply"] == {"imported": False, "reason": "reply_not_approved"}
        assert session.exec(select(ComplaintReply)).first() is None
        assert session.exec(select(ComplaintReplyRevision)).first() is None


def test_approved_reply_updates_current_record_and_preserves_revisions() -> None:
    db = engine()
    client = WorkflowFakeClient()
    add_ticket(
        client,
        status="Approved",
        custom_reply_approval_status="Approved",
        custom_final_approved_reply="The complaint has been addressed.",
        custom_ai_eligible=1,
    )
    with Session(db) as session:
        case, _ = add_case_and_officer(session)
        service = ComplaintHelpdeskWorkflowService(session, settings(), client)
        first = service.pull_case(case.id)
        second = service.pull_case(case.id)
        assert first["reply"]["imported"] is True
        assert second["reply"]["changed"] is False
        assert len(session.exec(select(ComplaintReplyRevision)).all()) == 1
        reply = session.exec(select(ComplaintReply)).one()
        assert reply.reply_text == "The complaint has been addressed."
        assert reply.version == 1
        assert session.get(ComplaintCase, case.id).frappe_ai_eligible is True

        client.resources["HD Ticket"]["0002"].update(
            custom_final_approved_reply="The revised approved reply has been issued.",
            custom_reply_approval_status="Issued",
            modified="2026-07-20T04:00:00+00:00",
        )
        third = service.pull_case(case.id)
        assert third["reply"]["changed"] is True
        revisions = session.exec(
            select(ComplaintReplyRevision).order_by(ComplaintReplyRevision.captured_at)
        ).all()
        assert len(revisions) == 2
        assert [row.is_current for row in revisions].count(True) == 1
        reply = session.exec(select(ComplaintReply)).one()
        assert reply.version == 2
        assert reply.reply_text == "The revised approved reply has been issued."



def test_signed_batch_codec_rejects_noncanonical_base64url_component() -> None:
    import base64

    import pytest

    from crm_integrations.frappe_helpdesk.batch_tokens import SignedBatchCodec

    codec = SignedBatchCodec(secret="secret", purpose="canonical-test")
    canonical = codec._encode(b"x")
    decoded = base64.urlsafe_b64decode(canonical + "=" * (-len(canonical) % 4))
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    alternate = next(
        canonical[:-1] + candidate
        for candidate in alphabet
        if candidate != canonical[-1]
        and base64.urlsafe_b64decode(
            canonical[:-1] + candidate + "=" * (-len(canonical) % 4)
        )
        == decoded
    )

    with pytest.raises(ValueError, match="canonical"):
        codec._decode(alternate)

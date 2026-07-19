from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from crm_domain.models import ComplaintCase, FrappeSyncEvent
from crm_integrations.frappe_helpdesk.bootstrap import ensure_helpdesk_foundation
from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskError
from crm_integrations.frappe_helpdesk.mapping import ticket_payloads
from crm_integrations.frappe_helpdesk.service import ComplaintHelpdeskSyncService


class FakeHelpdeskClient:
    def __init__(self) -> None:
        self.resources: dict[str, dict[str, dict[str, Any]]] = {
            "HD Ticket Template": {"Default": {"name": "Default", "fields": []}},
            "HD Ticket": {},
        }
        self.created_tickets = 0
        self.fail_create: Exception | None = None

    def health(self) -> dict[str, Any]:
        return {"status": "ok", "base_url": "http://helpdesk.test", "authenticated_user": "integration@test"}

    def get_resource(self, doctype: str, name: str) -> dict[str, Any]:
        try:
            return deepcopy(self.resources[doctype][name])
        except KeyError as exc:
            raise FrappeHelpdeskError("not found", status_code=404) from exc

    def list_resources(self, doctype: str, **_: Any) -> list[dict[str, Any]]:
        return [deepcopy(item) for item in self.resources.get(doctype, {}).values()]

    def find_ticket_by_deomee_case(self, case_id: str) -> dict[str, Any] | None:
        for ticket in self.resources.get("HD Ticket", {}).values():
            if ticket.get("custom_deomee_case_id") == case_id:
                return deepcopy(ticket)
        return None

    def create_resource(self, doctype: str, values: dict[str, Any]) -> dict[str, Any]:
        if doctype == "HD Ticket" and self.fail_create:
            raise self.fail_create
        name = str(values.get("name") or values.get("label_agent") or values.get("fieldname") or "")
        if doctype == "Custom Field":
            name = f"{values['dt']}-{values['fieldname']}"
        elif doctype == "HD Ticket":
            self.created_tickets += 1
            name = f"HD-TICKET-{self.created_tickets:05d}"
        if not name:
            raise AssertionError(f"No fake resource name for {doctype}: {values}")
        row = {"name": name, **deepcopy(values), "modified": "2026-07-20T12:00:00+00:00"}
        self.resources.setdefault(doctype, {})[name] = row
        return deepcopy(row)

    def update_resource(self, doctype: str, name: str, values: dict[str, Any]) -> dict[str, Any]:
        current = self.resources.setdefault(doctype, {}).setdefault(name, {"name": name})
        current.update(deepcopy(values))
        current["modified"] = "2026-07-20T12:01:00+00:00"
        return deepcopy(current)


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
    )


def engine():
    value = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(value)
    return value


def complaint(**changes: Any) -> ComplaintCase:
    values = {
        "complaint_number": "104-6609317",
        "state": "published",
        "remarks": "The school demanded an unauthorized fee.",
        "category": "Private School Fee",
        "sub_category": "Advance Fee",
        "tehsil": "Shalimar",
        "canonical_paperless_document_id": 123,
    }
    values.update(changes)
    return ComplaintCase(**values)


def test_ticket_mapping_escapes_html_and_keeps_fingerprint_stable() -> None:
    case = complaint(remarks='<script>alert("x")</script>\nSecond line')
    create_one, update_one, fingerprint_one = ticket_payloads(case, settings(), sync_version=1)
    create_two, update_two, fingerprint_two = ticket_payloads(case, settings(), sync_version=2)

    assert "<script>" not in create_one["description"]
    assert "&lt;script&gt;" in create_one["description"]
    assert create_one["custom_paperless_case_url"].endswith("/documents/123/details")
    assert create_one["custom_deomee_case_url"].endswith(f"/crm/cases/{case.id}")
    assert fingerprint_one == fingerprint_two
    assert update_one["custom_deomee_sync_version"] == 1
    assert update_two["custom_deomee_sync_version"] == 2


def test_foundation_bootstrap_is_idempotent() -> None:
    client = FakeHelpdeskClient()
    first = ensure_helpdesk_foundation(client, ["Administrative", "Teacher behaviour"])
    second = ensure_helpdesk_foundation(client, ["Administrative", "Teacher behaviour"])

    assert first["custom_fields"]["created"] > 0
    assert first["template"] == "updated"
    assert second["custom_fields"]["created"] == 0
    assert second["custom_fields"]["unchanged"] > 0
    assert second["template"] == "unchanged"
    assert "Administrative" in client.resources["HD Ticket Type"]


def test_sync_is_idempotent_and_records_audit_events() -> None:
    db = engine()
    client = FakeHelpdeskClient()
    with Session(db) as session:
        case = complaint()
        session.add(case)
        session.commit()
        case_id = case.id

        service = ComplaintHelpdeskSyncService(session, settings(), client)
        first = service.sync_case(case_id)
        second = service.sync_case(case_id)

        assert first["status"] == "created"
        assert second["status"] == "unchanged"
        assert client.created_tickets == 1
        persisted = session.get(ComplaintCase, case_id)
        assert persisted is not None
        assert persisted.frappe_sync_status == "synchronized"
        assert persisted.frappe_sync_version == 1
        events = session.exec(
            select(FrappeSyncEvent).where(FrappeSyncEvent.complaint_case_id == case_id)
        ).all()
        assert [event.state for event in events] == ["succeeded", "succeeded"]
        assert events[-1].details_json["action"] == "unchanged"


def test_sync_failure_is_saved_without_losing_the_case() -> None:
    db = engine()
    client = FakeHelpdeskClient()
    client.fail_create = FrappeHelpdeskError("permission denied", status_code=403)
    with Session(db) as session:
        case = complaint()
        session.add(case)
        session.commit()
        result = ComplaintHelpdeskSyncService(session, settings(), client).sync_case(case.id)
        persisted = session.get(ComplaintCase, case.id)
        assert result["status"] == "failed"
        assert result["http_status"] == 403
        assert persisted is not None
        assert persisted.frappe_sync_status == "failed"
        assert "permission denied" in (persisted.frappe_last_error or "")


def test_unpublished_or_incomplete_case_is_blocked_without_remote_write() -> None:
    db = engine()
    client = FakeHelpdeskClient()
    with Session(db) as session:
        case = complaint(state="fresh", canonical_paperless_document_id=None)
        session.add(case)
        session.commit()
        result = ComplaintHelpdeskSyncService(session, settings(), client).sync_case(case.id)
        assert result["status"] == "blocked"
        assert client.created_tickets == 0
        assert any("state" in blocker.lower() for blocker in result["blockers"])
        assert any("paperless" in blocker.lower() for blocker in result["blockers"])

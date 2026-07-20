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
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintCategory,
    ComplaintSubcategory,
)
from crm_domain.taxonomy import ComplaintTaxonomyService, TaxonomyError
from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskError


class TaxonomyFakeClient:
    def __init__(self) -> None:
        self.resources: dict[str, dict[str, dict[str, Any]]] = {
            "HD Ticket Type": {},
            "HD Ticket": {},
        }
        self.fail_ticket_type = False

    def get_resource(self, doctype: str, name: str) -> dict[str, Any]:
        try:
            return deepcopy(self.resources[doctype][name])
        except KeyError as exc:
            raise FrappeHelpdeskError("not found", status_code=404) from exc

    def create_resource(self, doctype: str, values: dict[str, Any]) -> dict[str, Any]:
        if self.fail_ticket_type and doctype == "HD Ticket Type":
            raise FrappeHelpdeskError("temporary failure", status_code=503)
        name = str(values.get("name") or "")
        row = {"name": name, **deepcopy(values)}
        self.resources.setdefault(doctype, {})[name] = row
        return deepcopy(row)

    def update_resource(self, doctype: str, name: str, values: dict[str, Any]) -> dict[str, Any]:
        if self.fail_ticket_type and doctype == "HD Ticket Type":
            raise FrappeHelpdeskError("temporary failure", status_code=503)
        row = self.resources.setdefault(doctype, {}).setdefault(name, {"name": name})
        row.update(deepcopy(values))
        return deepcopy(row)


def settings() -> Settings:
    return Settings(
        _env_file=None,
        frappe_helpdesk_enabled=True,
        frappe_helpdesk_url="http://helpdesk.test",
        frappe_helpdesk_public_url="http://helpdesk.test",
        frappe_helpdesk_api_key="key",
        frappe_helpdesk_api_secret="secret",
    )


def engine():
    value = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(value)
    return value


def test_category_and_subcategory_crud_sync_and_audit() -> None:
    db = engine()
    client = TaxonomyFakeClient()
    with Session(db) as session:
        service = ComplaintTaxonomyService(session, settings(), client)
        category_mutation = service.create_category(
            name=" Fee Complaints ",
            description="Private-school fee grievances",
            default_ai_eligible=True,
            reply_guidance="Check documentary evidence.",
            policy_notes="Apply current fee policy.",
        )
        category_id = uuid.UUID(category_mutation.entity["id"])
        assert category_mutation.remote_sync == {
            "status": "synchronized",
            "action": "created",
            "category_id": str(category_id),
            "ticket_type": "Fee Complaints",
        }
        assert client.resources["HD Ticket Type"]["Fee Complaints"]["disabled"] == 0

        subcategory_mutation = service.create_subcategory(
            category_id=category_id,
            name=" Summer vacation fee ",
            reply_guidance="Confirm the billing period.",
        )
        tree = service.tree()
        assert tree["statistics"]["categories"] == 1
        assert tree["categories"][0]["name"] == "Fee Complaints"
        assert tree["categories"][0]["default_ai_eligible"] is True
        assert tree["categories"][0]["subcategories"][0]["name"] == "Summer vacation fee"
        assert subcategory_mutation.entity["reply_guidance"] == "Confirm the billing period."
        events = session.exec(select(ComplaintAuditEvent)).all()
        assert {event.event_type for event in events} == {
            "category_created",
            "subcategory_created",
        }


def test_taxonomy_rejects_case_insensitive_duplicates() -> None:
    db = engine()
    with Session(db) as session:
        service = ComplaintTaxonomyService(session, settings(), TaxonomyFakeClient())
        first = service.create_category(name="Administrative")
        with pytest.raises(TaxonomyError, match="already exists"):
            service.create_category(name=" administrative ")
        category_id = uuid.UUID(first.entity["id"])
        service.create_subcategory(category_id=category_id, name="Others")
        with pytest.raises(TaxonomyError, match="already exists"):
            service.create_subcategory(category_id=category_id, name="OTHERS")


def test_category_rename_updates_cases_and_disables_old_frappe_type() -> None:
    db = engine()
    client = TaxonomyFakeClient()
    with Session(db) as session:
        service = ComplaintTaxonomyService(session, settings(), client)
        created = service.create_category(name="Teacher behaviour")
        category_id = uuid.UUID(created.entity["id"])
        case = ComplaintCase(
            complaint_number="104-7000100",
            state="published",
            category="Teacher behaviour",
            category_id=category_id,
            frappe_ticket_id="0002",
            classification_sync_status="synchronized",
        )
        session.add(case)
        session.commit()

        mutation = service.update_category(
            category_id,
            name="Teacher Conduct",
            description="Updated label",
        )
        persisted = session.get(ComplaintCase, case.id)
        assert persisted is not None
        assert persisted.category == "Teacher Conduct"
        assert persisted.classification_sync_status == "pending"
        assert mutation.affected_case_ids == (case.id,)
        assert client.resources["HD Ticket Type"]["Teacher behaviour"]["disabled"] == 1
        assert client.resources["HD Ticket Type"]["Teacher Conduct"]["disabled"] == 0


def test_category_merge_moves_cases_and_consolidates_duplicate_subcategories() -> None:
    db = engine()
    client = TaxonomyFakeClient()
    with Session(db) as session:
        service = ComplaintTaxonomyService(session, settings(), client)
        source = service.create_category(name="Fee").entity
        target = service.create_category(name="Fee Complaints").entity
        source_id, target_id = uuid.UUID(source["id"]), uuid.UUID(target["id"])
        source_sub = service.create_subcategory(category_id=source_id, name="Advance Fee").entity
        target_sub = service.create_subcategory(category_id=target_id, name="advance fee").entity
        case = ComplaintCase(
            complaint_number="104-7000101",
            state="published",
            category="Fee",
            sub_category="Advance Fee",
            category_id=source_id,
            sub_category_id=uuid.UUID(source_sub["id"]),
            frappe_ticket_id="0003",
        )
        session.add(case)
        session.commit()

        mutation = service.merge_category(source_id, target_id)
        persisted = session.get(ComplaintCase, case.id)
        source_row = session.get(ComplaintCategory, source_id)
        source_sub_row = session.get(ComplaintSubcategory, uuid.UUID(source_sub["id"]))
        assert persisted is not None
        assert persisted.category_id == target_id
        assert persisted.category == "Fee Complaints"
        assert persisted.sub_category_id == uuid.UUID(target_sub["id"])
        assert source_row is not None and source_row.active is False
        assert source_row.merged_into_id == target_id
        assert source_sub_row is not None and source_sub_row.merged_into_id == uuid.UUID(target_sub["id"])
        assert mutation.affected_case_ids == (case.id,)


def test_failed_frappe_sync_is_visible_without_losing_postgres_category() -> None:
    db = engine()
    client = TaxonomyFakeClient()
    client.fail_ticket_type = True
    with Session(db) as session:
        mutation = ComplaintTaxonomyService(session, settings(), client).create_category(
            name="Administrative"
        )
        category = session.get(ComplaintCategory, uuid.UUID(mutation.entity["id"]))
        assert category is not None
        assert category.name == "Administrative"
        assert category.frappe_sync_status == "failed"
        assert "temporary failure" in (category.frappe_last_error or "")
        assert mutation.remote_sync and mutation.remote_sync["status"] == "failed"


def test_taxonomy_api_creates_updates_and_rejects_duplicates() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from automation_core.config import get_settings
    from automation_core.database import get_session
    from crm_domain.taxonomy_api import router
    import crm_domain.taxonomy as taxonomy_module

    db = engine()
    client = TaxonomyFakeClient()
    app = FastAPI()
    app.include_router(router)

    def local_session():
        with Session(db) as session:
            yield session

    app.dependency_overrides[get_session] = local_session
    app.dependency_overrides[get_settings] = settings
    original = taxonomy_module.build_client
    taxonomy_module.build_client = lambda _settings: client
    try:
        http = TestClient(app)
        created = http.post(
            "/api/v1/crm/taxonomy/categories",
            json={
                "name": "Fee Complaints",
                "reply_guidance": "Check the fee demand.",
                "default_ai_eligible": True,
            },
        )
        assert created.status_code == 200
        category_id = created.json()["entity"]["id"]
        sub = http.post(
            "/api/v1/crm/taxonomy/subcategories",
            json={
                "category_id": category_id,
                "name": "Summer vacation fee",
            },
        )
        assert sub.status_code == 200
        tree = http.get("/api/v1/crm/taxonomy?include_inactive=true")
        assert tree.status_code == 200
        assert tree.json()["categories"][0]["subcategories"][0]["name"] == "Summer vacation fee"
        duplicate = http.post(
            "/api/v1/crm/taxonomy/categories",
            json={"name": " fee complaints "},
        )
        assert duplicate.status_code == 409
    finally:
        taxonomy_module.build_client = original
        app.dependency_overrides.clear()

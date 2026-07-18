from __future__ import annotations

import uuid

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import antidengue_automation.models  # noqa: F401
import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.database import get_session
from master_data.models import Department, District, Wing
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppActivity,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppAudienceMember,
    WhatsAppDirectoryGroup,
    WhatsAppGroup,
)


def test_audience_target_can_be_disabled_listed_and_reenabled(monkeypatch) -> None:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        account = WhatsAppAccount(name="Primary", worker_key=f"worker-{uuid.uuid4().hex}")
        application = WhatsAppApplication(key="antidengue", name="AntiDengue")
        district = District(name=f"Lahore-{uuid.uuid4().hex}", code="LHR")
        department = Department(name=f"Education-{uuid.uuid4().hex}", code="EDU")
        session.add(account)
        session.add(application)
        session.add(district)
        session.add(department)
        session.flush()
        wing = Wing(
            district_id=district.id,
            department_id=department.id,
            name="MEE",
            code="MEE",
        )
        session.add(wing)
        session.flush()
        audience = WhatsAppAudience(
            application_id=application.id,
            key="mee_groups",
            name="MEE groups",
        )
        group = WhatsAppDirectoryGroup(
            account_id=account.id,
            jid="120363000000000000@g.us",
            name="MEE Lahore",
        )
        second_group = WhatsAppDirectoryGroup(
            account_id=account.id,
            jid="120363000000000001@g.us",
            name="MEE Lahore Two",
        )
        session.add(audience)
        session.add(group)
        session.add(second_group)
        session.flush()
        member = WhatsAppAudienceMember(
            audience_id=audience.id,
            target_type="group",
            target_key=f"group:{group.id}",
            directory_group_id=group.id,
            route_scope_key="wing",
            route_scope_value=str(uuid.uuid4()),
            route_scope_label="DEO MEE",
        )
        session.add(member)
        session.commit()
        account_id, application_id = account.id, application.id
        audience_id, member_id = audience.id, member.id
        second_group_id, wing_id = second_group.id, wing.id

    def session_override():
        with Session(engine) as session:
            yield session

    def fake_defaults(session: Session):
        return session.get(WhatsAppAccount, account_id), session.get(WhatsAppApplication, application_id)

    monkeypatch.setattr("whatsapp_gateway.configuration.audiences.ensure_defaults", fake_defaults)
    app.dependency_overrides[get_session] = session_override
    try:
        with TestClient(app) as client:
            disabled = client.patch(
                f"/api/v1/whatsapp/audiences/{audience_id}/members/{member_id}/enabled",
                json={"enabled": False},
            )
            assert disabled.status_code == 200, disabled.text
            assert disabled.json() == {"id": str(member_id), "enabled": False, "changed": True}

            listed = client.get(f"/api/v1/whatsapp/audiences/{audience_id}/members")
            assert listed.status_code == 200, listed.text
            assert listed.json()["total"] == 1
            assert listed.json()["items"][0]["enabled"] is False

            enabled = client.patch(
                f"/api/v1/whatsapp/audiences/{audience_id}/members/{member_id}/enabled",
                json={"enabled": True},
            )
            assert enabled.status_code == 200, enabled.text
            assert enabled.json()["enabled"] is True

            added = client.post(
                f"/api/v1/whatsapp/audiences/{audience_id}/members",
                json={
                    "target_type": "group",
                    "target_id": str(second_group_id),
                    "wing_id": str(wing_id),
                    "route_scope_key": "wing",
                    "route_scope_value": str(wing_id),
                    "route_scope_label": "MEE",
                },
            )
            assert added.status_code == 201, added.text
            assert added.json()["target_id"] == str(second_group_id)
    finally:
        app.dependency_overrides.clear()

    with Session(engine) as session:
        member = session.get(WhatsAppAudienceMember, member_id)
        assert member is not None and member.enabled is True
        configured = session.exec(
            select(WhatsAppGroup).where(WhatsAppGroup.directory_group_id == second_group_id)
        ).one()
        assert configured.enabled is True
        assert configured.wing_id == wing_id
        event_types = list(session.scalars(select(WhatsAppActivity.event_type)))
        assert "audience_member_disabled" in event_types
        assert "audience_member_enabled" in event_types

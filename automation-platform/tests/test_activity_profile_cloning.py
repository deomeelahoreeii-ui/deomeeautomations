from __future__ import annotations

import uuid

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from antidengue_automation.activity_profile_cloning import (
    clone_activity_profile,
    is_activity_clone_source,
)
from antidengue_automation.routing_profile_api import router as clone_router
from automation_core.database import get_session
from master_data.models import Department, District, Wing
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppActivity,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppDispatchProfile,
    WhatsAppRecipientScope,
    WhatsAppReportType,
)
from whatsapp_gateway.persistence.audience_sources import WhatsAppAudienceSource


def _engine():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _dynamic_markaz_source(session: Session) -> WhatsAppDispatchProfile:
    district = District(name="Lahore")
    department = Department(name="School Education")
    session.add_all([district, department]); session.flush()
    wing = Wing(
        district_id=district.id, department_id=department.id, name="DEO MEE", code="DEO MEE"
    )
    application = WhatsAppApplication(key="antidengue", name="AntiDengue")
    account = WhatsAppAccount(name="Primary", worker_key="clone-test")
    session.add_all([wing, application, account]); session.flush()
    dormant = WhatsAppReportType(
        application_id=application.id, key="markaz_dormant_summary", name="Markaz Dormant Summary"
    )
    hotspot = WhatsAppReportType(
        application_id=application.id, key="hotspot_distance_activity", name="Hotspot distance review"
    )
    simple = WhatsAppReportType(
        application_id=application.id, key="simple_activity_timing", name="Simple Activity timing review"
    )
    scope = WhatsAppRecipientScope(
        application_id=application.id, channel="individual", key="aeo", name="AEO"
    )
    audience = WhatsAppAudience(
        application_id=application.id, key="dynamic-aeo", name="Dynamic AEO Markazes"
    )
    session.add_all([dormant, hotspot, simple, scope, audience]); session.flush()
    session.add(WhatsAppAudienceSource(
        audience_id=audience.id, recipient_role="aeo", wing_id=wing.id,
        route_scope_key="markaz", aggregate_by_recipient=True,
    ))
    source = WhatsAppDispatchProfile(
        application_id=application.id, key="markaz-dormant", name="AEO dormant",
        report_type_id=dormant.id, audience_id=audience.id, account_id=account.id,
        recipient_scope_id=scope.id, recipient_channel="individual", wing_id=wing.id,
        delivery_mode="individuals", require_approval=True,
        presentation_policy={"message_style": "detailed", "attachment_mode": "image_excel"},
    )
    session.add(source); session.commit(); session.refresh(source)
    return source


def test_name_only_clone_preserves_dynamic_aeo_routing_for_both_activity_reports() -> None:
    with Session(_engine()) as session:
        source = _dynamic_markaz_source(session)
        assert is_activity_clone_source(session, source)

        hotspot = clone_activity_profile(
            session, source=source, activity="hotspot", name="AEO hotspot review"
        )
        simple = clone_activity_profile(
            session, source=source, activity="simple", name="AEO timing review"
        )
        session.commit()

        for clone in (hotspot, simple):
            assert clone.audience_id == source.audience_id
            assert clone.recipient_scope_id == source.recipient_scope_id
            assert clone.recipient_channel == "individual"
            assert clone.delivery_mode == "individuals"
            assert clone.delivery_granularity == "scope"
            assert clone.wing_id == source.wing_id
            assert clone.account_id == source.account_id
            assert clone.require_approval is True
            assert clone.presentation_policy["linked_source_profile_id"] == str(source.id)
            assert clone.presentation_policy["attachment_mode"] == "excel"
        assert hotspot.template_id != simple.template_id


def test_clone_rejects_second_activity_profile_for_same_source() -> None:
    with Session(_engine()) as session:
        source = _dynamic_markaz_source(session)
        clone_activity_profile(session, source=source, activity="hotspot", name="First clone")
        session.flush()

        try:
            clone_activity_profile(session, source=source, activity="hotspot", name="Second clone")
        except ValueError as exc:
            assert "already has" in str(exc)
        else:
            raise AssertionError("A second linked clone should be rejected")


def test_clone_api_needs_only_name_and_records_audit_event() -> None:
    engine = _engine()
    with Session(engine) as seed_session:
        source = _dynamic_markaz_source(seed_session)
        source_id = source.id

    app = FastAPI()
    app.include_router(clone_router)

    def override_session():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = override_session
    response = TestClient(app).post(
        f"/api/v1/antidengue/routing-profiles/{source_id}/clone/hotspot",
        json={"name": "Named by operator"},
    )

    assert response.status_code == 201
    assert response.json()["name"] == "Named by operator"
    assert response.json()["recipient_channel"] == "individual"
    with Session(engine) as session:
        event = session.scalar(select(WhatsAppActivity).where(
            WhatsAppActivity.event_type == "dispatch_profile_cloned"
        ))
        assert event is not None
        assert event.details["source_profile_id"] == str(source_id)

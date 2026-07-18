from __future__ import annotations

from datetime import UTC, datetime

from sqlmodel import Session, SQLModel, create_engine, select

from master_data.models import (
    Department, District, Markaz, Officer, OfficerJurisdiction, School, Tehsil, Wing,
)
from antidengue_automation.dynamic_runtime_routes import dynamic_runtime_route_rows
from whatsapp_gateway.configuration.dynamic_audiences import (
    dynamic_audience_fingerprint, resolve_dynamic_audience,
)
from whatsapp_gateway.models import WhatsAppApplication, WhatsAppAudience
from whatsapp_gateway.persistence.audience_sources import WhatsAppAudienceSource
from whatsapp_gateway.rendering.antidengue.messages import _build_aeo_markaz_message
from whatsapp_gateway.rendering.antidengue.models import ScopedDormantSchool


def _session(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'dynamic-audience.db'}")
    SQLModel.metadata.create_all(engine)
    return Session(engine)


def _seed(session: Session):
    district = District(name="Lahore")
    department = Department(name="School Education")
    session.add_all([district, department]); session.flush()
    wing = Wing(district_id=district.id, department_id=department.id, name="DEO MEE", code="DEO MEE")
    tehsil = Tehsil(district_id=district.id, name="Raiwind")
    session.add_all([wing, tehsil]); session.flush()
    markaz_a = Markaz(wing_id=wing.id, tehsil_id=tehsil.id, name="Markaz A")
    markaz_b = Markaz(wing_id=wing.id, tehsil_id=tehsil.id, name="Markaz B")
    session.add_all([markaz_a, markaz_b]); session.flush()
    officer = Officer(
        role="aeo", name="AEO One", mobile="03001234567", normalized_mobile="923001234567",
        district_id=district.id, department_id=department.id, wing_id=wing.id,
        tehsil_id=tehsil.id, markaz_id=markaz_a.id,
    )
    session.add(officer); session.flush()
    for markaz in (markaz_a, markaz_b):
        session.add(OfficerJurisdiction(
            role="aeo", officer_id=officer.id, district_id=district.id,
            department_id=department.id, wing_id=wing.id, tehsil_id=tehsil.id,
            markaz_id=markaz.id,
        ))
    school_a = School(
        emis="1001", name="School A", district_id=district.id, department_id=department.id,
        wing_id=wing.id, tehsil_id=tehsil.id, markaz_id=markaz_a.id,
    )
    school_b = School(
        emis="1002", name="School B", district_id=district.id, department_id=department.id,
        wing_id=wing.id, tehsil_id=tehsil.id, markaz_id=markaz_b.id,
    )
    application = WhatsAppApplication(key="antidengue-test", name="AntiDengue test")
    session.add_all([school_a, school_b, application]); session.flush()
    audience = WhatsAppAudience(application_id=application.id, key="dynamic-aeo", name="Dynamic AEO")
    session.add(audience); session.flush()
    source = WhatsAppAudienceSource(
        audience_id=audience.id, recipient_role="aeo", wing_id=wing.id,
        route_scope_key="markaz", aggregate_by_recipient=True,
    )
    session.add(source); session.commit()
    return wing, markaz_a, markaz_b, officer, school_a, audience


def test_dynamic_jurisdiction_audience_aggregates_markazes_per_aeo(tmp_path) -> None:
    with _session(tmp_path) as session:
        _, markaz_a, markaz_b, officer, _, audience = _seed(session)

        members = resolve_dynamic_audience(session, audience_id=audience.id)

        assert len(members) == 1
        member = members[0]
        assert member.officer_id == officer.id
        assert member.target_jid == "923001234567@s.whatsapp.net"
        assert member.scope_ids == sorted([markaz_a.id, markaz_b.id], key=str)
        assert member.scope_labels == ["Markaz A", "Markaz B"]
        assert member.school_emis == ["1001", "1002"]


def test_dynamic_audience_fingerprint_changes_with_routing_inputs(tmp_path) -> None:
    with _session(tmp_path) as session:
        _, _, _, officer, school, audience = _seed(session)
        before = dynamic_audience_fingerprint(session, audience.id)
        school.active = False
        officer.normalized_mobile = "923001111111"
        session.add_all([school, officer]); session.commit()

        after = dynamic_audience_fingerprint(session, audience.id)

        assert after != before
        member = resolve_dynamic_audience(session, audience_id=audience.id)[0]
        assert member.target_jid == "923001111111@s.whatsapp.net"
        assert member.school_emis == ["1002"]


def test_non_aggregated_source_keeps_stable_member_per_jurisdiction(tmp_path) -> None:
    with _session(tmp_path) as session:
        _, markaz_a, markaz_b, officer, _, audience = _seed(session)
        source = session.scalar(
            select(WhatsAppAudienceSource).where(
                WhatsAppAudienceSource.audience_id == audience.id
            )
        )
        source.aggregate_by_recipient = False
        session.add(source)
        session.commit()

        members = resolve_dynamic_audience(session, audience_id=audience.id)

        assert len(members) == 2
        assert len({member.id for member in members}) == 2
        assert {tuple(member.scope_ids) for member in members} == {(markaz_a.id,), (markaz_b.id,)}
        assert {member.officer_id for member in members} == {officer.id}


def test_profile_scope_granularity_overrides_shared_recipient_aggregation(tmp_path) -> None:
    with _session(tmp_path) as session:
        _, markaz_a, markaz_b, officer, _, audience = _seed(session)

        members = resolve_dynamic_audience(
            session, audience_id=audience.id, granularity="scope",
        )

        assert len(members) == 2
        assert {tuple(member.scope_ids) for member in members} == {
            (markaz_a.id,), (markaz_b.id,),
        }
        assert {member.officer_id for member in members} == {officer.id}
        assert len({member.id for member in members}) == 2


def test_scope_routes_follow_live_markaz_reassignment(tmp_path) -> None:
    with _session(tmp_path) as session:
        wing, _, markaz_b, officer, _, audience = _seed(session)
        assignment = session.scalar(select(OfficerJurisdiction).where(
            OfficerJurisdiction.officer_id == officer.id,
            OfficerJurisdiction.markaz_id == markaz_b.id,
        ))
        replacement = Officer(
            role="aeo", name="AEO Two", mobile="03007654321",
            normalized_mobile="923007654321", district_id=officer.district_id,
            department_id=officer.department_id, wing_id=wing.id,
            tehsil_id=officer.tehsil_id, markaz_id=markaz_b.id,
        )
        assignment.active = False
        session.add_all([assignment, replacement]); session.flush()
        session.add(OfficerJurisdiction(
            role="aeo", officer_id=replacement.id, district_id=officer.district_id,
            department_id=officer.department_id, wing_id=wing.id,
            tehsil_id=officer.tehsil_id, markaz_id=markaz_b.id,
        ))
        session.commit()

        members = resolve_dynamic_audience(
            session, audience_id=audience.id, granularity="scope",
        )
        by_scope = {member.scope_ids[0]: member for member in members}

        assert len(members) == 2
        assert by_scope[markaz_b.id].officer_id == replacement.id
        assert by_scope[markaz_b.id].target_jid == "923007654321@s.whatsapp.net"


def test_aggregated_aeo_expands_to_exact_markaz_routes_for_dry_run(tmp_path) -> None:
    with _session(tmp_path) as session:
        _, markaz_a, markaz_b, officer, _, audience = _seed(session)
        member = resolve_dynamic_audience(session, audience_id=audience.id)[0]
        profile = type("Profile", (), {"name": "AEO Markaz profile"})()

        rows = dynamic_runtime_route_rows(profile, member, officer)

        assert {row["markaz_ref"] for row in rows} == {str(markaz_a.id), str(markaz_b.id)}
        assert all("," not in row["markaz_ref"] for row in rows)
        assert {row["target"] for row in rows} == {"923001234567@s.whatsapp.net"}


def test_aeo_message_separates_multiple_markaz_sections() -> None:
    source_job = type("Job", (), {
        "finished_at": datetime(2026, 7, 18, 6, 30, tzinfo=UTC),
        "started_at": None,
        "created_at": datetime(2026, 7, 18, 6, 0, tzinfo=UTC),
    })()
    wing = type("Wing", (), {"code": "DEO MEE", "name": "DEO MEE"})()
    markazes = [type("Markaz", (), {"name": name})() for name in ("Markaz A", "Markaz B")]
    schools = [
        ScopedDormantSchool("1001", "School A", "Raiwind", "Markaz A"),
        ScopedDormantSchool("1002", "School B", "Raiwind", "Markaz B"),
    ]

    message, context = _build_aeo_markaz_message(
        source_job=source_job, recipient_name="AEO One", wing=wing,
        markazes=markazes, schools=schools, deadline="12:30 PM",
        policy={"message_style": "detailed", "attachment_mode": "none", "image_content": "details"},
    )

    assert "*AEO:* AEO One" in message
    assert "1. Markaz A: 1 school" in message
    assert "2. Markaz B: 1 school" in message
    assert "1001 - School A" in message
    assert context["markaz_count"] == "2"

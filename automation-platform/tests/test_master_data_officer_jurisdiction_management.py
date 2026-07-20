from __future__ import annotations

import uuid

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from master_data.models import (
    Department,
    District,
    Markaz,
    Officer,
    OfficerJurisdiction,
    School,
    Tehsil,
    Wing,
)
from master_data.postgres_repository import PostgresMasterDataRepository
from master_data.schemas import JurisdictionAssignmentWrite, OfficerWrite
from whatsapp_gateway.configuration.dynamic_audiences import resolve_dynamic_audience
from whatsapp_gateway.models import WhatsAppApplication, WhatsAppAudience
from whatsapp_gateway.persistence.audience_sources import WhatsAppAudienceSource


def _database(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'officer-jurisdictions.db'}")
    SQLModel.metadata.create_all(engine)
    return engine


def _seed(engine):
    with Session(engine) as session:
        district = District(name="Lahore")
        department = Department(name="School Education")
        session.add_all([district, department])
        session.flush()
        wing = Wing(
            district_id=district.id,
            department_id=department.id,
            name="DEO MEE",
            code="DEO MEE",
        )
        tehsil_a = Tehsil(district_id=district.id, name="Cantt")
        tehsil_b = Tehsil(district_id=district.id, name="Model Town")
        session.add_all([wing, tehsil_a, tehsil_b])
        session.flush()
        markaz_a = Markaz(wing_id=wing.id, tehsil_id=tehsil_a.id, name="Barki - Male")
        markaz_b = Markaz(wing_id=wing.id, tehsil_id=tehsil_a.id, name="Hair - Male")
        markaz_c = Markaz(wing_id=wing.id, tehsil_id=tehsil_b.id, name="Kahna - Male")
        session.add_all([markaz_a, markaz_b, markaz_c])
        session.commit()
        return {
            "district": district.id,
            "department": department.id,
            "wing": wing.id,
            "tehsil_a": tehsil_a.id,
            "tehsil_b": tehsil_b.id,
            "markaz_a": markaz_a.id,
            "markaz_b": markaz_b.id,
            "markaz_c": markaz_c.id,
        }


def _aeo(scope, *, name, mobile, primary, refs=None, transfer=False):
    tehsil = scope["tehsil_b"] if primary == scope["markaz_c"] else scope["tehsil_a"]
    return OfficerWrite(
        role="aeo",
        name=name,
        mobile=mobile,
        district_ref=str(scope["district"]),
        department_ref=str(scope["department"]),
        wing_ref=str(scope["wing"]),
        tehsil_ref=str(tehsil),
        markaz_ref=str(primary),
        jurisdiction_refs=[str(item) for item in (refs or [primary])],
        replace_conflicts=transfer,
    )


def _ddeo(scope, *, name, mobile, primary, refs=None, transfer=False):
    return OfficerWrite(
        role="ddeo",
        name=name,
        mobile=mobile,
        district_ref=str(scope["district"]),
        department_ref=str(scope["department"]),
        wing_ref=str(scope["wing"]),
        tehsil_ref=str(primary),
        jurisdiction_refs=[str(item) for item in (refs or [primary])],
        replace_conflicts=transfer,
    )


def _active_assignments(engine, officer_id):
    with Session(engine) as session:
        return list(
            session.exec(
                select(OfficerJurisdiction).where(
                    OfficerJurisdiction.officer_id == uuid.UUID(officer_id),
                    OfficerJurisdiction.active.is_(True),
                )
            ).all()
        )


def test_officer_form_creates_authoritative_aeo_jurisdiction(tmp_path) -> None:
    engine = _database(tmp_path)
    scope = _seed(engine)
    repository = PostgresMasterDataRepository(engine)

    _, officer = repository.save_officer(
        _aeo(
            scope,
            name="New Barki AEO",
            mobile="03001234567",
            primary=scope["markaz_a"],
        )
    )

    assignments = _active_assignments(engine, officer["id"])
    assert len(assignments) == 1
    assert assignments[0].markaz_id == scope["markaz_a"]
    assert officer["primary_jurisdiction"]["scope_name"] == "Barki - Male"
    assert officer["assignment_summary"] == "Barki - Male"


def test_existing_scope_requires_explicit_transfer_and_rolls_back_cleanly(tmp_path) -> None:
    engine = _database(tmp_path)
    scope = _seed(engine)
    repository = PostgresMasterDataRepository(engine)
    _, current = repository.save_officer(
        _aeo(
            scope,
            name="Additional Charge AEO",
            mobile="03001234567",
            primary=scope["markaz_a"],
        )
    )

    with pytest.raises(ValueError, match="currently assigned to Additional Charge AEO"):
        repository.save_officer(
            _aeo(
                scope,
                name="New Regular AEO",
                mobile="03007654321",
                primary=scope["markaz_a"],
            )
        )

    with Session(engine) as session:
        assert session.scalar(select(Officer).where(Officer.name == "New Regular AEO")) is None
        assignment = session.scalar(
            select(OfficerJurisdiction).where(
                OfficerJurisdiction.officer_id == uuid.UUID(current["id"]),
                OfficerJurisdiction.markaz_id == scope["markaz_a"],
            )
        )
        assert assignment is not None and assignment.active is True


def test_transfer_archives_old_charge_and_reconciles_old_primary(tmp_path) -> None:
    engine = _database(tmp_path)
    scope = _seed(engine)
    repository = PostgresMasterDataRepository(engine)
    _, old = repository.save_officer(
        _aeo(
            scope,
            name="Additional Charge AEO",
            mobile="03001234567",
            primary=scope["markaz_a"],
            refs=[scope["markaz_a"], scope["markaz_b"]],
        )
    )

    _, new = repository.save_officer(
        _aeo(
            scope,
            name="New Regular AEO",
            mobile="03007654321",
            primary=scope["markaz_a"],
            transfer=True,
        )
    )

    old_after = repository.get_officer("aeo", old["id"])
    assert old_after is not None
    assert old_after["primary_jurisdiction"]["scope_ref"] == str(scope["markaz_b"])
    assert old_after["additional_charge_count"] == 0
    assert new["primary_jurisdiction"]["scope_ref"] == str(scope["markaz_a"])
    with Session(engine) as session:
        archived = session.scalar(
            select(OfficerJurisdiction).where(
                OfficerJurisdiction.officer_id == uuid.UUID(old["id"]),
                OfficerJurisdiction.markaz_id == scope["markaz_a"],
            )
        )
        assert archived is not None and archived.active is False
        assert "Transferred from Additional Charge AEO to New Regular AEO" in archived.notes


def test_editing_officer_replaces_complete_additional_charge_set(tmp_path) -> None:
    engine = _database(tmp_path)
    scope = _seed(engine)
    repository = PostgresMasterDataRepository(engine)
    _, officer = repository.save_officer(
        _aeo(
            scope,
            name="Multi Markaz AEO",
            mobile="03001234567",
            primary=scope["markaz_a"],
            refs=[scope["markaz_a"], scope["markaz_b"]],
        )
    )

    _, updated = repository.save_officer(
        _aeo(
            scope,
            name="Multi Markaz AEO",
            mobile="03001234567",
            primary=scope["markaz_a"],
            refs=[scope["markaz_a"], scope["markaz_c"]],
        ),
        officer["id"],
    )

    assert {item["scope_ref"] for item in updated["jurisdictions"]} == {
        str(scope["markaz_a"]),
        str(scope["markaz_c"]),
    }
    with Session(engine) as session:
        removed = session.scalar(
            select(OfficerJurisdiction).where(
                OfficerJurisdiction.officer_id == uuid.UUID(officer["id"]),
                OfficerJurisdiction.markaz_id == scope["markaz_b"],
            )
        )
        assert removed is not None and removed.active is False


def test_ddeo_primary_and_additional_tehsils_are_authoritative(tmp_path) -> None:
    engine = _database(tmp_path)
    scope = _seed(engine)
    repository = PostgresMasterDataRepository(engine)

    _, officer = repository.save_officer(
        _ddeo(
            scope,
            name="DDEO One",
            mobile="03001234567",
            primary=scope["tehsil_a"],
            refs=[scope["tehsil_a"], scope["tehsil_b"]],
        )
    )

    assert officer["primary_jurisdiction"]["scope_ref"] == str(scope["tehsil_a"])
    assert {item["scope_ref"] for item in officer["additional_jurisdictions"]} == {
        str(scope["tehsil_b"])
    }


def test_standalone_assignment_becomes_primary_for_unassigned_officer(tmp_path) -> None:
    engine = _database(tmp_path)
    scope = _seed(engine)
    repository = PostgresMasterDataRepository(engine)
    with Session(engine) as session:
        officer = Officer(
            role="aeo",
            name="Imported Orphan",
            mobile="03001234567",
            normalized_mobile="923001234567",
            district_id=scope["district"],
            department_id=scope["department"],
            wing_id=scope["wing"],
            tehsil_id=scope["tehsil_a"],
            markaz_id=scope["markaz_a"],
        )
        session.add(officer)
        session.commit()
        officer_id = str(officer.id)

    _, after = repository.assign_jurisdiction(
        JurisdictionAssignmentWrite(
            role="aeo",
            officer_ref=officer_id,
            scope_ref=str(scope["markaz_b"]),
        )
    )

    assert after["primary_jurisdiction"]["scope_ref"] == str(scope["markaz_b"])
    assert after["markaz_ref"] == str(scope["markaz_b"])


def test_antidengue_dynamic_audience_immediately_follows_transfer(tmp_path) -> None:
    engine = _database(tmp_path)
    scope = _seed(engine)
    repository = PostgresMasterDataRepository(engine)
    _, old = repository.save_officer(
        _aeo(
            scope,
            name="Old AEO",
            mobile="03001234567",
            primary=scope["markaz_a"],
        )
    )
    with Session(engine) as session:
        session.add(
            School(
                emis="35200001",
                name="Barki School",
                district_id=scope["district"],
                department_id=scope["department"],
                wing_id=scope["wing"],
                tehsil_id=scope["tehsil_a"],
                markaz_id=scope["markaz_a"],
            )
        )
        application = WhatsAppApplication(key="assignment-test", name="Assignment test")
        session.add(application)
        session.flush()
        audience = WhatsAppAudience(
            application_id=application.id,
            key="aeo-routes",
            name="AEO routes",
        )
        session.add(audience)
        session.flush()
        session.add(
            WhatsAppAudienceSource(
                audience_id=audience.id,
                recipient_role="aeo",
                wing_id=scope["wing"],
                route_scope_key="markaz",
                aggregate_by_recipient=False,
            )
        )
        session.commit()
        audience_id = audience.id

    _, new = repository.save_officer(
        _aeo(
            scope,
            name="New AEO",
            mobile="03007654321",
            primary=scope["markaz_a"],
            transfer=True,
        )
    )

    with Session(engine) as session:
        members = resolve_dynamic_audience(session, audience_id=audience_id)
    assert len(members) == 1
    assert str(members[0].officer_id) == new["id"]
    assert members[0].target_jid == "923007654321@s.whatsapp.net"
    assert str(members[0].officer_id) != old["id"]


def test_dashboard_tehsil_coverage_uses_authoritative_additional_charges(tmp_path) -> None:
    engine = _database(tmp_path)
    scope = _seed(engine)
    repository = PostgresMasterDataRepository(engine)
    repository.save_officer(
        _aeo(
            scope,
            name="Cross Tehsil AEO",
            mobile="03001234567",
            primary=scope["markaz_a"],
            refs=[scope["markaz_a"], scope["markaz_c"]],
        )
    )
    repository.save_officer(
        _ddeo(
            scope,
            name="Cross Tehsil DDEO",
            mobile="03007654321",
            primary=scope["tehsil_a"],
            refs=[scope["tehsil_a"], scope["tehsil_b"]],
        )
    )

    by_name = {row["name"]: row for row in repository.dashboard()["tehsils"]}
    assert by_name["Cantt"]["aeos"] == 1
    assert by_name["Model Town"]["aeos"] == 1
    assert by_name["Cantt"]["ddeos"] == 1
    assert by_name["Model Town"]["ddeos"] == 1

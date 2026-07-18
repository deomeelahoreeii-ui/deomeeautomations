from __future__ import annotations

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import master_data.models  # noqa: F401
from master_data.hierarchy import build_hierarchy
from master_data.markaz_catalog import markaz_catalog
from master_data.models import (
    Department, District, Markaz, Officer, OfficerJurisdiction, School, Tehsil, Wing,
)


def _engine():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _seed(session: Session):
    district = District(name="Lahore", code="LHR")
    department = Department(name="School Education", code="SED")
    session.add_all([district, department]); session.flush()
    mee = Wing(
        district_id=district.id, department_id=department.id,
        name="DEO MEE", code="DEO MEE",
    )
    wee = Wing(
        district_id=district.id, department_id=department.id,
        name="DEO WEE", code="DEO WEE",
    )
    city = Tehsil(district_id=district.id, name="City")
    cantt = Tehsil(district_id=district.id, name="Cantt")
    session.add_all([mee, wee, city, cantt]); session.flush()
    markaz = Markaz(wing_id=mee.id, tehsil_id=city.id, name="Lahore Khas")
    session.add(markaz); session.flush()
    ddeo = Officer(
        role="ddeo", name="DDEO City", mobile="03000000001",
        normalized_mobile="923000000001", district_id=district.id,
        department_id=department.id, wing_id=mee.id, tehsil_id=city.id,
    )
    aeo = Officer(
        role="aeo", name="AEO Lahore Khas", mobile="03000000002",
        normalized_mobile="923000000002", district_id=district.id,
        department_id=department.id, wing_id=mee.id, tehsil_id=city.id,
        markaz_id=markaz.id,
    )
    session.add_all([ddeo, aeo]); session.flush()
    session.add_all([
        OfficerJurisdiction(
            role="ddeo", officer_id=ddeo.id, district_id=district.id,
            department_id=department.id, wing_id=mee.id, tehsil_id=city.id,
        ),
        OfficerJurisdiction(
            role="aeo", officer_id=aeo.id, district_id=district.id,
            department_id=department.id, wing_id=mee.id, tehsil_id=city.id,
            markaz_id=markaz.id,
        ),
        School(
            emis="35220001", name="School One", district_id=district.id,
            department_id=department.id, wing_id=mee.id, tehsil_id=city.id,
            markaz_id=markaz.id,
        ),
    ])
    session.commit()
    return district, mee, wee, city, cantt


def test_hierarchy_projects_offices_coverage_and_drilldowns() -> None:
    with Session(_engine()) as session:
        district, mee, wee, city, _ = _seed(session)
        result = build_hierarchy(session)

    assert result["summary"] == {
        "ceo_offices": 1, "deo_offices": 2, "ddeos": 1, "aeos": 1,
        "markazes": 1, "schools": 1, "vacant_ddeo_posts": 3,
        "unassigned_markazes": 0,
    }
    root = result["roots"][0]
    assert root["role"] == "CEO" and root["subtitle"] == "Lahore District"
    offices = {item["entity_id"]: item for item in root["children"]}
    mee_node, wee_node = offices[str(mee.id)], offices[str(wee.id)]
    actual = next(item for item in mee_node["children"] if item["status"] == "active")
    assert actual["label"] == "DDEO City"
    assert actual["href"] == f"/master-data/hierarchy/ddeos?entity_id={actual['entity_id']}"
    assert actual["coverage_href"] == f"/master-data/schools?tehsil_ref={city.id}"
    assert actual["children"][0]["label"] == "AEO Lahore Khas"
    assert actual["children"][0]["metrics"] == {"markazes": 1, "schools": 1}
    assert len([item for item in mee_node["children"] if item["status"] == "vacant"]) == 1
    assert len(wee_node["children"]) == 2
    assert result["filters"]["districts"] == [{"id": str(district.id), "name": "Lahore"}]


def test_hierarchy_reports_ambiguous_ddeo_coverage_once() -> None:
    with Session(_engine()) as session:
        district, mee, _, city, _ = _seed(session)
        original = session.exec(select(Officer).where(Officer.role == "ddeo")).one()
        second = Officer(
            role="ddeo", name="Second DDEO", mobile="03000000003",
            normalized_mobile="923000000003", district_id=district.id,
            department_id=original.department_id, wing_id=mee.id, tehsil_id=city.id,
        )
        session.add(second); session.flush()
        session.add(OfficerJurisdiction(
            role="ddeo", officer_id=second.id, district_id=district.id,
            department_id=second.department_id, wing_id=mee.id, tehsil_id=city.id,
        ))
        session.commit()
        result = build_hierarchy(session)

    ambiguous = [item for item in result["issues"] if item["code"] == "ambiguous_ddeo_coverage"]
    assert len(ambiguous) == 1
    assert "2 active DDEOs" in ambiguous[0]["message"]


def test_markaz_catalog_has_wing_stats_coverage_and_pagination() -> None:
    with Session(_engine()) as session:
        district, _, wee, _, cantt = _seed(session)
        department_id = session.exec(select(Department.id)).one()
        unassigned = Markaz(wing_id=wee.id, tehsil_id=cantt.id, name="Baghbanpura")
        session.add(unassigned); session.flush()
        session.add(School(
            emis="35220002", name="School Two", district_id=district.id,
            department_id=department_id, wing_id=wee.id, tehsil_id=cantt.id,
            markaz_id=unassigned.id,
        ))
        session.commit()

        all_rows = markaz_catalog(session, page=1, page_size=1)
        gaps = markaz_catalog(session, coverage="unassigned", page=1, page_size=20)

    assert all_rows["stats"]["total"] == 2
    assert all_rows["stats"]["assigned"] == 1
    assert all_rows["stats"]["unassigned"] == 1
    assert all_rows["total"] == 2 and all_rows["pages"] == 2
    assert len(all_rows["items"]) == 1
    assert gaps["total"] == 1
    assert gaps["items"][0]["name"] == "Baghbanpura"
    assert gaps["items"][0]["href"] == f"/master-data/schools?markaz_ref={unassigned.id}"

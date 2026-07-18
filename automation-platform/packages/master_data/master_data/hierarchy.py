from __future__ import annotations

import uuid
from collections import Counter, defaultdict
from typing import Any

from sqlmodel import Session, select

from master_data.models import (
    District, Markaz, Officer, OfficerJurisdiction, School, Tehsil, Wing,
)


def _rows(session: Session, model: type[Any], include_inactive: bool) -> list[Any]:
    statement = select(model)
    if not include_inactive:
        statement = statement.where(model.active.is_(True))
    return list(session.scalars(statement).all())


def _school_href(*, tehsil_id: uuid.UUID | None = None, markaz_id: uuid.UUID | None = None) -> str:
    if markaz_id:
        return f"/master-data/schools?markaz_ref={markaz_id}"
    if tehsil_id:
        return f"/master-data/schools?tehsil_ref={tehsil_id}"
    return "/master-data/schools"


def build_hierarchy(
    session: Session, *, include_inactive: bool = False,
    district_id: uuid.UUID | None = None, wing_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    districts = _rows(session, District, include_inactive)
    wings = _rows(session, Wing, include_inactive)
    tehsils = _rows(session, Tehsil, include_inactive)
    markazes = _rows(session, Markaz, include_inactive)
    officers = _rows(session, Officer, include_inactive)
    jurisdictions = _rows(session, OfficerJurisdiction, include_inactive)
    schools = _rows(session, School, include_inactive)
    if district_id:
        districts = [item for item in districts if item.id == district_id]
    district_ids = {item.id for item in districts}
    wings = [item for item in wings if item.district_id in district_ids]
    if wing_id:
        wings = [item for item in wings if item.id == wing_id]
    wing_ids = {item.id for item in wings}
    tehsils = [item for item in tehsils if item.district_id in district_ids]
    markazes = [item for item in markazes if item.wing_id in wing_ids]
    officers = [item for item in officers if item.wing_id in wing_ids]
    officer_ids = {item.id for item in officers}
    jurisdictions = [
        item for item in jurisdictions
        if item.wing_id in wing_ids and item.officer_id in officer_ids
    ]
    schools = [item for item in schools if item.wing_id in wing_ids]

    tehsil_by_id = {item.id: item for item in tehsils}
    markaz_by_id = {item.id: item for item in markazes}
    officer_by_id = {item.id: item for item in officers}
    jurisdictions_by_officer: dict[uuid.UUID, list[OfficerJurisdiction]] = defaultdict(list)
    for item in jurisdictions:
        jurisdictions_by_officer[item.officer_id].append(item)
    schools_by_wing = Counter(item.wing_id for item in schools)
    schools_by_wing_tehsil = Counter((item.wing_id, item.tehsil_id) for item in schools)
    schools_by_markaz = Counter(item.markaz_id for item in schools if item.markaz_id)
    markazes_by_wing_tehsil: dict[tuple[uuid.UUID, uuid.UUID], list[Markaz]] = defaultdict(list)
    for item in markazes:
        markazes_by_wing_tehsil[(item.wing_id, item.tehsil_id)].append(item)

    aeo_markaz_owner: dict[uuid.UUID, set[uuid.UUID]] = defaultdict(set)
    for item in jurisdictions:
        if item.role == "aeo" and item.markaz_id:
            aeo_markaz_owner[item.markaz_id].add(item.officer_id)
    issues: list[dict[str, Any]] = []
    roots: list[dict[str, Any]] = []

    for district in sorted(districts, key=lambda item: item.name):
        district_wings = sorted(
            [item for item in wings if item.district_id == district.id],
            key=lambda item: item.name,
        )
        district_tehsils = sorted(
            [item for item in tehsils if item.district_id == district.id],
            key=lambda item: item.name,
        )
        wing_nodes: list[dict[str, Any]] = []
        for wing in district_wings:
            wing_officers = [item for item in officers if item.wing_id == wing.id]
            ddeos = sorted(
                [item for item in wing_officers if item.role == "ddeo"],
                key=lambda item: item.name,
            )
            aeos = [item for item in wing_officers if item.role == "aeo"]
            aeos_by_tehsil: dict[uuid.UUID, list[Officer]] = defaultdict(list)
            for aeo in aeos:
                scopes = jurisdictions_by_officer.get(aeo.id) or []
                scope_ids = {item.tehsil_id for item in scopes} or {aeo.tehsil_id}
                for scope_id in scope_ids:
                    aeos_by_tehsil[scope_id].append(aeo)

            ddeo_nodes: list[dict[str, Any]] = []
            covered_tehsils: set[uuid.UUID] = set()
            claimed_aeos: set[uuid.UUID] = set()
            ddeo_candidates: dict[uuid.UUID, list[Officer]] = defaultdict(list)
            for ddeo in ddeos:
                scopes = jurisdictions_by_officer.get(ddeo.id) or []
                scope_ids = {item.tehsil_id for item in scopes} or {ddeo.tehsil_id}
                for scope_id in scope_ids:
                    ddeo_candidates[scope_id].append(ddeo)
            for scope_id, candidates in ddeo_candidates.items():
                if len(candidates) > 1:
                    issues.append({
                        "code": "ambiguous_ddeo_coverage", "severity": "warning",
                        "message": f"{wing.name} / {tehsil_by_id[scope_id].name} has {len(candidates)} active DDEOs.",
                        "wing_id": str(wing.id), "tehsil_id": str(scope_id),
                    })

            for ddeo in ddeos:
                scopes = jurisdictions_by_officer.get(ddeo.id) or []
                scope_ids = sorted(
                    {item.tehsil_id for item in scopes} or {ddeo.tehsil_id},
                    key=lambda value: tehsil_by_id.get(value).name if value in tehsil_by_id else str(value),
                )
                covered_tehsils.update(scope_ids)
                child_aeos: list[dict[str, Any]] = []
                for scope_id in scope_ids:
                    for aeo in sorted(aeos_by_tehsil.get(scope_id, []), key=lambda item: item.name):
                        if aeo.id in claimed_aeos:
                            continue
                        claimed_aeos.add(aeo.id)
                        child_aeos.append(_aeo_node(
                            aeo, jurisdictions_by_officer.get(aeo.id, []),
                            markaz_by_id, schools_by_markaz,
                        ))
                school_count = sum(schools_by_wing_tehsil[(wing.id, value)] for value in scope_ids)
                markaz_count = sum(len(markazes_by_wing_tehsil[(wing.id, value)]) for value in scope_ids)
                ddeo_nodes.append({
                    "id": f"ddeo:{ddeo.id}", "entity_id": str(ddeo.id),
                    "type": "ddeo", "role": "DDEO", "label": ddeo.name,
                    "subtitle": ", ".join(tehsil_by_id[value].name for value in scope_ids if value in tehsil_by_id),
                    "contact": ddeo.mobile, "status": "active" if ddeo.active else "inactive",
                    "metrics": {
                        "aeos": len(child_aeos), "markazes": markaz_count,
                        "schools": school_count, "tehsils": len(scope_ids),
                    },
                    "href": f"/master-data/hierarchy/ddeos?entity_id={ddeo.id}",
                    "coverage_href": _school_href(tehsil_id=scope_ids[0] if len(scope_ids) == 1 else None),
                    "scopes": [
                        {"id": str(value), "name": tehsil_by_id[value].name, "href": _school_href(tehsil_id=value)}
                        for value in scope_ids if value in tehsil_by_id
                    ],
                    "children": child_aeos,
                })

            for tehsil in district_tehsils:
                if tehsil.id in covered_tehsils:
                    continue
                child_aeos = [
                    _aeo_node(aeo, jurisdictions_by_officer.get(aeo.id, []), markaz_by_id, schools_by_markaz)
                    for aeo in sorted(aeos_by_tehsil.get(tehsil.id, []), key=lambda item: item.name)
                    if aeo.id not in claimed_aeos
                ]
                claimed_aeos.update(aeo.id for aeo in aeos_by_tehsil.get(tehsil.id, []))
                issues.append({
                    "code": "vacant_ddeo_post", "severity": "warning",
                    "message": f"{wing.name} / {tehsil.name} has no active DDEO assignment.",
                    "wing_id": str(wing.id), "tehsil_id": str(tehsil.id),
                })
                ddeo_nodes.append({
                    "id": f"ddeo-vacancy:{wing.id}:{tehsil.id}", "type": "ddeo",
                    "role": "DDEO", "label": "Vacant DDEO post", "subtitle": tehsil.name,
                    "contact": "", "status": "vacant",
                    "metrics": {
                        "aeos": len(child_aeos),
                        "markazes": len(markazes_by_wing_tehsil[(wing.id, tehsil.id)]),
                        "schools": schools_by_wing_tehsil[(wing.id, tehsil.id)], "tehsils": 1,
                    },
                    "href": f"/master-data/hierarchy/vacant-ddeo-posts?wing_id={wing.id}&tehsil_id={tehsil.id}",
                    "coverage_href": _school_href(tehsil_id=tehsil.id),
                    "scopes": [{"id": str(tehsil.id), "name": tehsil.name, "href": _school_href(tehsil_id=tehsil.id)}],
                    "children": child_aeos,
                })

            unassigned_markazes = [
                item for item in markazes if item.wing_id == wing.id and not aeo_markaz_owner[item.id]
            ]
            if unassigned_markazes:
                issues.append({
                    "code": "unassigned_aeo_coverage", "severity": "warning",
                    "message": f"{wing.name} has {len(unassigned_markazes)} Markaz(es) without an active AEO assignment.",
                    "wing_id": str(wing.id),
                    "markaz_ids": [str(item.id) for item in unassigned_markazes],
                })
            wing_nodes.append({
                "id": f"deo:{wing.id}", "entity_id": str(wing.id), "type": "deo",
                "role": "DEO", "label": wing.code or wing.name,
                "subtitle": "Organizational office", "status": "active" if wing.active else "inactive",
                "metrics": {
                    "ddeos": len(ddeos), "expected_ddeos": len(district_tehsils),
                    "aeos": len(aeos), "schools": schools_by_wing[wing.id],
                    "markazes": sum(len(markazes_by_wing_tehsil[(wing.id, item.id)]) for item in district_tehsils),
                    "unassigned_markazes": len(unassigned_markazes),
                },
                "href": f"/master-data/hierarchy/deo-offices?wing_id={wing.id}",
                "coverage_href": f"/master-data/jurisdictions?wing_id={wing.id}",
                "children": ddeo_nodes,
            })

        district_officers = [item for item in officers if item.district_id == district.id]
        district_schools = [item for item in schools if item.district_id == district.id]
        roots.append({
            "id": f"ceo:{district.id}", "entity_id": str(district.id), "type": "ceo",
            "role": "CEO", "label": "CEO Office", "subtitle": f"{district.name} District",
            "status": "active" if district.active else "inactive",
            "metrics": {
                "deo_offices": len(district_wings),
                "ddeos": sum(item.role == "ddeo" for item in district_officers),
                "aeos": sum(item.role == "aeo" for item in district_officers),
                "markazes": sum(
                    len(markazes_by_wing_tehsil[(wing.id, tehsil.id)])
                    for wing in district_wings for tehsil in district_tehsils
                ),
                "schools": len(district_schools),
            },
            "href": f"/master-data/hierarchy/ceo-offices?entity_id={district.id}",
            "coverage_href": "/master-data/areas", "children": wing_nodes,
        })

    summary = {
        "ceo_offices": len(roots), "deo_offices": len(wings),
        "ddeos": sum(item.role == "ddeo" for item in officers),
        "aeos": sum(item.role == "aeo" for item in officers),
        "markazes": len(markazes), "schools": len(schools),
        "vacant_ddeo_posts": sum(item["code"] == "vacant_ddeo_post" for item in issues),
        "unassigned_markazes": sum(not aeo_markaz_owner[item.id] for item in markazes),
    }
    return {
        "summary": summary, "roots": roots, "issues": issues,
        "filters": {
            "districts": [{"id": str(item.id), "name": item.name} for item in districts],
            "wings": [{"id": str(item.id), "name": item.name, "code": item.code} for item in wings],
        },
    }


def _aeo_node(
    officer: Officer, scopes: list[OfficerJurisdiction],
    markaz_by_id: dict[uuid.UUID, Markaz], schools_by_markaz: Counter,
) -> dict[str, Any]:
    markaz_ids = sorted(
        {item.markaz_id for item in scopes if item.markaz_id},
        key=lambda value: markaz_by_id[value].name if value in markaz_by_id else str(value),
    )
    markaz_rows = [
        {
            "id": str(value), "name": markaz_by_id[value].name,
            "schools": schools_by_markaz[value], "href": _school_href(markaz_id=value),
        }
        for value in markaz_ids if value in markaz_by_id
    ]
    return {
        "id": f"aeo:{officer.id}", "entity_id": str(officer.id),
        "type": "aeo", "role": "AEO", "label": officer.name,
        "subtitle": ", ".join(item["name"] for item in markaz_rows) or "No active Markaz",
        "contact": officer.mobile, "status": "active" if officer.active else "inactive",
        "metrics": {
            "markazes": len(markaz_rows),
            "schools": sum(item["schools"] for item in markaz_rows),
        },
        "href": f"/master-data/hierarchy/aeos?entity_id={officer.id}",
        "coverage_href": markaz_rows[0]["href"] if len(markaz_rows) == 1 else "/master-data/jurisdictions",
        "markazes": markaz_rows, "children": [],
    }


__all__ = ["build_hierarchy"]

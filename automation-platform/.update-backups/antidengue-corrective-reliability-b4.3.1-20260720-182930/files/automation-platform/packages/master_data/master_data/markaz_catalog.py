from __future__ import annotations

import math
import uuid
from collections import Counter, defaultdict
from typing import Any, Literal

from sqlmodel import Session, select

from master_data.models import Markaz, Officer, OfficerJurisdiction, School, Tehsil, Wing


def markaz_catalog(
    session: Session, *, search: str = "", wing_id: uuid.UUID | None = None,
    tehsil_id: uuid.UUID | None = None,
    coverage: Literal["all", "assigned", "unassigned"] = "all",
    include_inactive: bool = False, page: int = 1, page_size: int = 20,
) -> dict[str, Any]:
    markaz_query = select(Markaz)
    school_query = select(School)
    officer_query = select(Officer)
    jurisdiction_query = select(OfficerJurisdiction).where(OfficerJurisdiction.role == "aeo")
    if not include_inactive:
        markaz_query = markaz_query.where(Markaz.active.is_(True))
        school_query = school_query.where(School.active.is_(True))
        officer_query = officer_query.where(Officer.active.is_(True))
        jurisdiction_query = jurisdiction_query.where(OfficerJurisdiction.active.is_(True))
    markazes = list(session.scalars(markaz_query).all())
    wings = {
        item.id: item for item in session.scalars(select(Wing)).all()
        if include_inactive or item.active
    }
    tehsils = {
        item.id: item for item in session.scalars(select(Tehsil)).all()
        if include_inactive or item.active
    }
    schools = list(session.scalars(school_query).all())
    officers = {item.id: item for item in session.scalars(officer_query).all()}
    jurisdictions = [
        item for item in session.scalars(jurisdiction_query).all()
        if item.officer_id in officers and item.markaz_id
    ]
    schools_by_markaz = Counter(item.markaz_id for item in schools if item.markaz_id)
    aeos_by_markaz: dict[uuid.UUID, dict[uuid.UUID, Officer]] = defaultdict(dict)
    for item in jurisdictions:
        aeos_by_markaz[item.markaz_id][item.officer_id] = officers[item.officer_id]

    rows: list[dict[str, Any]] = []
    for item in markazes:
        wing = wings.get(item.wing_id)
        tehsil = tehsils.get(item.tehsil_id)
        if wing is None or tehsil is None:
            continue
        aeos = sorted(aeos_by_markaz[item.id].values(), key=lambda officer: officer.name)
        rows.append({
            "id": str(item.id), "name": item.name, "active": item.active,
            "wing_id": str(wing.id), "wing_name": wing.name, "wing_code": wing.code,
            "tehsil_id": str(tehsil.id), "tehsil_name": tehsil.name,
            "schools": schools_by_markaz[item.id], "assigned": bool(aeos),
            "aeo_count": len(aeos),
            "aeos": [
                {"id": str(officer.id), "name": officer.name, "mobile": officer.mobile}
                for officer in aeos
            ],
            "href": f"/master-data/schools?markaz_ref={item.id}",
        })
    rows.sort(key=lambda item: (item["wing_name"], item["tehsil_name"], item["name"]))
    by_wing = [
        {
            "id": str(wing.id), "name": wing.name, "code": wing.code,
            "total": sum(row["wing_id"] == str(wing.id) for row in rows),
            "assigned": sum(row["wing_id"] == str(wing.id) and row["assigned"] for row in rows),
            "unassigned": sum(row["wing_id"] == str(wing.id) and not row["assigned"] for row in rows),
            "schools": sum(row["schools"] for row in rows if row["wing_id"] == str(wing.id)),
        }
        for wing in sorted(wings.values(), key=lambda item: item.name)
    ]
    stats = {
        "total": len(rows), "assigned": sum(row["assigned"] for row in rows),
        "unassigned": sum(not row["assigned"] for row in rows),
        "schools": sum(row["schools"] for row in rows),
        "empty": sum(row["schools"] == 0 for row in rows),
        "by_wing": by_wing,
    }

    normalized = search.strip().lower()
    filtered = [
        row for row in rows
        if (not normalized or normalized in f"{row['name']} {row['tehsil_name']} {row['wing_name']} {' '.join(aeo['name'] for aeo in row['aeos'])}".lower())
        and (not wing_id or row["wing_id"] == str(wing_id))
        and (not tehsil_id or row["tehsil_id"] == str(tehsil_id))
        and (coverage == "all" or row["assigned"] == (coverage == "assigned"))
    ]
    total = len(filtered)
    start = (page - 1) * page_size
    return {
        "stats": stats, "items": filtered[start:start + page_size],
        "total": total, "page": page, "page_size": page_size,
        "pages": max(1, math.ceil(total / page_size)),
        "filters": {
            "wings": [
                {"id": item["id"], "name": item["name"], "code": item["code"]}
                for item in by_wing
            ],
            "tehsils": [
                {"id": str(item.id), "name": item.name}
                for item in sorted(tehsils.values(), key=lambda value: value.name)
            ],
        },
    }


__all__ = ["markaz_catalog"]

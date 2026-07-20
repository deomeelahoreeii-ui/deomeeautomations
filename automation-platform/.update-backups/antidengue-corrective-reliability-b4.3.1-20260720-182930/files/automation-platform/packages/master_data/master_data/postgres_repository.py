from __future__ import annotations

import csv
import io
import re
import uuid
from typing import Any

from sqlalchemy import Engine, and_, exists, func, or_, select
from sqlmodel import Session

from automation_core.time import utcnow
from master_data.models import (
    Department,
    District,
    Markaz,
    Officer,
    OfficerJurisdiction,
    School,
    SchoolHead,
    SchoolOfficerOverride,
    Tehsil,
    Wing,
)
from master_data.repository import normalize_phone
from master_data.schemas import OfficerWrite, SchoolWrite


def _id(value: uuid.UUID | None) -> str:
    return str(value) if value is not None else ""


def _uuid(value: str, label: str = "record") -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Select a valid {label}") from exc


def _school_dict(school: School, head: SchoolHead | None = None) -> dict[str, Any]:
    return {
        "id": str(school.id),
        "legacy_id": school.legacy_id,
        "active": school.active,
        "emis": school.emis,
        "name": school.name,
        "district_ref": _id(school.district_id),
        "department_ref": _id(school.department_id),
        "wing_ref": _id(school.wing_id),
        "tehsil_ref": _id(school.tehsil_id),
        "markaz_ref": _id(school.markaz_id),
        "head_name": head.name if head and head.active else "",
        "head_contact": head.mobile if head and head.active else "",
        "shift": school.shift,
        "school_type": school.school_type,
        "school_level": school.school_level,
        "deos_wise": school.deos_wise,
        "source": school.source,
        "source_row_hash": school.source_row_hash,
        "notes": school.notes,
    }


def _officer_dict(officer: Officer) -> dict[str, Any]:
    return {
        "id": str(officer.id),
        "legacy_id": officer.legacy_id,
        "active": officer.active,
        "role": officer.role,
        "name": officer.name,
        "mobile": officer.mobile,
        "normalized_mobile": officer.normalized_mobile,
        "helpdesk_user_email": officer.helpdesk_user_email,
        "helpdesk_enabled": officer.helpdesk_enabled,
        "district_ref": _id(officer.district_id),
        "department_ref": _id(officer.department_id),
        "wing_ref": _id(officer.wing_id),
        "tehsil_ref": _id(officer.tehsil_id),
        "markaz_ref": _id(officer.markaz_id),
    }


class PostgresMasterDataRepository:
    def __init__(self, engine: Engine):
        self.engine = engine

    @staticmethod
    def _active_record(
        session: Session, model: type[Any], value: str, label: str
    ) -> Any:
        record = session.get(model, _uuid(value, label))
        if record is None or not record.active:
            raise ValueError(f"Select a valid {label}")
        return record

    @staticmethod
    def _head(session: Session, school_id: uuid.UUID) -> SchoolHead | None:
        return session.scalar(select(SchoolHead).where(SchoolHead.school_id == school_id))

    def dashboard(self) -> dict[str, Any]:
        with Session(self.engine) as session:
            count = lambda model, *filters: session.scalar(
                select(func.count()).select_from(model).where(*filters)
            ) or 0
            active_head = exists().where(
                SchoolHead.school_id == School.id,
                SchoolHead.active.is_(True),
                func.trim(SchoolHead.name) != "",
            )
            active_aeo_override = exists().where(
                SchoolOfficerOverride.school_id == School.id,
                SchoolOfficerOverride.role == "aeo",
                SchoolOfficerOverride.active.is_(True),
            )
            active_ddeo_override = exists().where(
                SchoolOfficerOverride.school_id == School.id,
                SchoolOfficerOverride.role == "ddeo",
                SchoolOfficerOverride.active.is_(True),
            )
            aeo_jurisdiction = exists().where(
                OfficerJurisdiction.role == "aeo",
                OfficerJurisdiction.wing_id == School.wing_id,
                OfficerJurisdiction.markaz_id == School.markaz_id,
                OfficerJurisdiction.active.is_(True),
            )
            ddeo_jurisdiction = exists().where(
                OfficerJurisdiction.role == "ddeo",
                OfficerJurisdiction.wing_id == School.wing_id,
                OfficerJurisdiction.tehsil_id == School.tehsil_id,
                OfficerJurisdiction.active.is_(True),
            )
            counts = {
                "schools": count(School, School.active.is_(True)),
                "inactive_schools": count(School, School.active.is_(False)),
                "aeos": count(Officer, Officer.active.is_(True), Officer.role == "aeo"),
                "ddeos": count(Officer, Officer.active.is_(True), Officer.role == "ddeo"),
                "school_heads": count(School, School.active.is_(True), active_head),
                "missing_heads": count(School, School.active.is_(True), ~active_head),
                "wings": count(Wing, Wing.active.is_(True)),
                "tehsils": count(Tehsil, Tehsil.active.is_(True)),
                "markazes": count(Markaz, Markaz.active.is_(True)),
                "unmapped_aeo": count(
                    School,
                    School.active.is_(True),
                    School.markaz_id.is_not(None),
                    ~active_aeo_override,
                    ~aeo_jurisdiction,
                ),
                "pending_markaz": count(
                    School, School.active.is_(True), School.markaz_id.is_(None)
                ),
                "unmapped_ddeo": count(
                    School,
                    School.active.is_(True),
                    ~active_ddeo_override,
                    ~ddeo_jurisdiction,
                ),
            }

            tehsils: list[dict[str, Any]] = []
            for tehsil in session.scalars(
                select(Tehsil).where(Tehsil.active.is_(True)).order_by(Tehsil.name)
            ):
                tehsils.append(
                    {
                        "id": str(tehsil.id),
                        "name": tehsil.name,
                        "schools": count(
                            School,
                            School.active.is_(True),
                            School.tehsil_id == tehsil.id,
                        ),
                        "school_heads": count(
                            School,
                            School.active.is_(True),
                            School.tehsil_id == tehsil.id,
                            active_head,
                        ),
                        "aeos": count(
                            Officer,
                            Officer.active.is_(True),
                            Officer.role == "aeo",
                            Officer.tehsil_id == tehsil.id,
                        ),
                        "ddeos": count(
                            Officer,
                            Officer.active.is_(True),
                            Officer.role == "ddeo",
                            Officer.tehsil_id == tehsil.id,
                        ),
                        "missing_markaz": count(
                            School,
                            School.active.is_(True),
                            School.tehsil_id == tehsil.id,
                            School.markaz_id.is_(None),
                        ),
                    }
                )

            wings: list[dict[str, Any]] = []
            for wing in session.scalars(
                select(Wing).where(Wing.active.is_(True)).order_by(Wing.name)
            ):
                wings.append(
                    {
                        "id": str(wing.id),
                        "name": wing.name,
                        "schools": count(
                            School, School.active.is_(True), School.wing_id == wing.id
                        ),
                        "aeos": count(
                            Officer,
                            Officer.active.is_(True),
                            Officer.role == "aeo",
                            Officer.wing_id == wing.id,
                        ),
                        "ddeos": count(
                            Officer,
                            Officer.active.is_(True),
                            Officer.role == "ddeo",
                            Officer.wing_id == wing.id,
                        ),
                    }
                )
        return {"counts": counts, "tehsils": tehsils, "wings": wings}

    def options(self) -> dict[str, list[dict[str, Any]]]:
        with Session(self.engine) as session:
            districts = [
                {
                    "id": str(item.id),
                    "legacy_id": item.legacy_id,
                    "name": item.name,
                    "code": item.code,
                    "active": item.active,
                }
                for item in session.scalars(
                    select(District).where(District.active.is_(True)).order_by(District.name)
                )
            ]
            departments = [
                {
                    "id": str(item.id),
                    "legacy_id": item.legacy_id,
                    "name": item.name,
                    "code": item.code,
                    "active": item.active,
                }
                for item in session.scalars(
                    select(Department)
                    .where(Department.active.is_(True))
                    .order_by(Department.name)
                )
            ]
            wings = [
                {
                    "id": str(item.id),
                    "legacy_id": item.legacy_id,
                    "name": item.name,
                    "code": item.code,
                    "district_ref": str(item.district_id),
                    "department_ref": str(item.department_id),
                    "active": item.active,
                }
                for item in session.scalars(
                    select(Wing).where(Wing.active.is_(True)).order_by(Wing.name)
                )
            ]
            tehsils = [
                {
                    "id": str(item.id),
                    "legacy_id": item.legacy_id,
                    "name": item.name,
                    "district_ref": str(item.district_id),
                    "active": item.active,
                }
                for item in session.scalars(
                    select(Tehsil).where(Tehsil.active.is_(True)).order_by(Tehsil.name)
                )
            ]
            markazes = [
                {
                    "id": str(item.id),
                    "legacy_id": item.legacy_id,
                    "name": item.name,
                    "wing_ref": str(item.wing_id),
                    "tehsil_ref": str(item.tehsil_id),
                    "active": item.active,
                }
                for item in session.scalars(
                    select(Markaz).where(Markaz.active.is_(True)).order_by(Markaz.name)
                )
            ]
        return {
            "districts": districts,
            "departments": departments,
            "wings": wings,
            "tehsils": tehsils,
            "markazes": markazes,
        }

    def list_schools(
        self,
        *,
        search: str = "",
        tehsil_ref: str = "",
        markaz_ref: str = "",
        active: bool | None = True,
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        filters: list[Any] = []
        if search:
            needle = f"%{search}%"
            filters.append(
                or_(
                    School.emis.ilike(needle),
                    School.name.ilike(needle),
                    SchoolHead.name.ilike(needle),
                )
            )
        if tehsil_ref:
            filters.append(School.tehsil_id == _uuid(tehsil_ref, "tehsil"))
        if markaz_ref:
            filters.append(School.markaz_id == _uuid(markaz_ref, "markaz"))
        if active is not None:
            filters.append(School.active.is_(active))

        base = select(School, SchoolHead).outerjoin(
            SchoolHead, SchoolHead.school_id == School.id
        ).where(*filters)
        with Session(self.engine) as session:
            total = session.scalar(
                select(func.count()).select_from(base.subquery())
            ) or 0
            records = session.execute(
                base.order_by(School.name).limit(page_size).offset((page - 1) * page_size)
            ).all()
            items = []
            for school, head in records:
                item = _school_dict(school, head)
                item.update(
                    {
                        "district_name": session.get(District, school.district_id).name,
                        "department_name": session.get(Department, school.department_id).name,
                        "wing_name": session.get(Wing, school.wing_id).name,
                        "tehsil_name": session.get(Tehsil, school.tehsil_id).name,
                        "markaz_name": (
                            session.get(Markaz, school.markaz_id).name
                            if school.markaz_id
                            else ""
                        ),
                    }
                )
                items.append(item)
        return {"items": items, "total": total, "page": page, "page_size": page_size}

    def get_school(self, school_id: str) -> dict[str, Any] | None:
        try:
            record_id = uuid.UUID(school_id)
        except ValueError:
            return None
        with Session(self.engine) as session:
            school = session.get(School, record_id)
            return _school_dict(school, self._head(session, record_id)) if school else None

    def school_id_by_emis(self, emis: str) -> str | None:
        with Session(self.engine) as session:
            record_id = session.scalar(select(School.id).where(School.emis == emis))
            return str(record_id) if record_id else None

    def _validate_school_scope(self, session: Session, data: SchoolWrite) -> tuple[Any, ...]:
        district = self._active_record(session, District, data.district_ref, "district")
        department = self._active_record(
            session, Department, data.department_ref, "department"
        )
        wing = self._active_record(session, Wing, data.wing_ref, "wing")
        tehsil = self._active_record(session, Tehsil, data.tehsil_ref, "tehsil")
        if wing.district_id != district.id or wing.department_id != department.id:
            raise ValueError("Wing does not belong to the selected district and department")
        if tehsil.district_id != district.id:
            raise ValueError("Tehsil does not belong to the selected district")
        markaz = None
        if data.markaz_ref:
            markaz = self._active_record(session, Markaz, data.markaz_ref, "markaz")
            if markaz.wing_id != wing.id or markaz.tehsil_id != tehsil.id:
                raise ValueError("Markaz does not belong to the selected wing and tehsil")
        return district, department, wing, tehsil, markaz

    def save_school(
        self, data: SchoolWrite, school_id: str | None = None
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        emis = re.sub(r"\s+", "", data.emis)
        with Session(self.engine) as session:
            district, department, wing, tehsil, markaz = self._validate_school_scope(
                session, data
            )
            record_id = _uuid(school_id, "school") if school_id else uuid.uuid4()
            school = session.get(School, record_id)
            if school_id and school is None:
                raise LookupError("School not found")
            duplicate = session.scalar(
                select(School).where(School.emis == emis, School.id != record_id)
            )
            if duplicate:
                raise ValueError(f"EMIS {emis} already exists")
            before = (
                _school_dict(school, self._head(session, record_id)) if school else None
            )
            if school is None:
                school = School(
                    id=record_id,
                    emis=emis,
                    name=data.name,
                    district_id=district.id,
                    department_id=department.id,
                    wing_id=wing.id,
                    tehsil_id=tehsil.id,
                    markaz_id=markaz.id if markaz else None,
                    source="web_platform",
                    source_row_hash="",
                )
            school.emis = emis
            school.name = data.name
            school.district_id = district.id
            school.department_id = department.id
            school.wing_id = wing.id
            school.tehsil_id = tehsil.id
            school.markaz_id = markaz.id if markaz else None
            school.shift = data.shift
            school.school_type = data.school_type
            school.school_level = data.school_level
            school.deos_wise = data.deos_wise
            school.notes = data.notes
            school.active = data.active
            school.updated_at = utcnow()
            session.add(school)
            session.flush()

            head = self._head(session, record_id)
            if data.head_name.strip() or data.head_contact.strip():
                if head is None:
                    head = SchoolHead(school_id=record_id, name=data.head_name)
                head.name = data.head_name
                head.mobile = normalize_phone(data.head_contact)
                head.normalized_mobile = normalize_phone(data.head_contact)
                head.active = data.active
                head.updated_at = utcnow()
                session.add(head)
            elif head is not None:
                session.delete(head)
            session.flush()
            after_head = head if head is not None and head in session else None
            after = _school_dict(school, after_head)
            session.commit()
        return before, after

    def set_school_active(self, school_id: str, active: bool) -> tuple[dict, dict]:
        with Session(self.engine) as session:
            record_id = _uuid(school_id, "school")
            school = session.get(School, record_id)
            if school is None:
                raise LookupError("School not found")
            head = self._head(session, record_id)
            before = _school_dict(school, head)
            school.active = active
            school.updated_at = utcnow()
            if head:
                head.active = active
                head.updated_at = utcnow()
                session.add(head)
            session.add(school)
            session.commit()
            after = _school_dict(school, head)
        return before, after

    @staticmethod
    def _validate_role(role: str) -> None:
        if role not in {"aeo", "ddeo"}:
            raise ValueError("Role must be aeo or ddeo")

    def list_officers(
        self, role: str, *, search: str = "", active: bool | None = True
    ) -> list[dict[str, Any]]:
        self._validate_role(role)
        filters: list[Any] = [Officer.role == role]
        if search:
            needle = f"%{search}%"
            filters.append(or_(Officer.name.ilike(needle), Officer.mobile.ilike(needle)))
        if active is not None:
            filters.append(Officer.active.is_(active))
        with Session(self.engine) as session:
            officers = session.scalars(
                select(Officer).where(*filters).order_by(Officer.name)
            ).all()
            result = []
            for officer in officers:
                item = _officer_dict(officer)
                item.update(
                    {
                        "tehsil_name": session.get(Tehsil, officer.tehsil_id).name,
                        "markaz_name": (
                            session.get(Markaz, officer.markaz_id).name
                            if officer.markaz_id
                            else ""
                        ),
                        "wing_name": session.get(Wing, officer.wing_id).name,
                    }
                )
                result.append(item)
            return result

    def get_officer(self, role: str, officer_id: str) -> dict[str, Any] | None:
        self._validate_role(role)
        try:
            record_id = uuid.UUID(officer_id)
        except ValueError:
            return None
        with Session(self.engine) as session:
            officer = session.get(Officer, record_id)
            if officer is None or officer.role != role:
                return None
            return _officer_dict(officer)

    def officer_id_by_mobile(self, role: str, mobile: str) -> str | None:
        self._validate_role(role)
        normalized = normalize_phone(mobile)
        with Session(self.engine) as session:
            record_id = session.scalar(
                select(Officer.id).where(
                    Officer.role == role, Officer.normalized_mobile == normalized
                )
            )
            return str(record_id) if record_id else None

    def _validate_officer_scope(
        self, session: Session, data: OfficerWrite
    ) -> tuple[Any, ...]:
        self._validate_role(data.role)
        district = self._active_record(session, District, data.district_ref, "district")
        department = self._active_record(
            session, Department, data.department_ref, "department"
        )
        wing = self._active_record(session, Wing, data.wing_ref, "wing")
        tehsil = self._active_record(session, Tehsil, data.tehsil_ref, "tehsil")
        if wing.district_id != district.id or wing.department_id != department.id:
            raise ValueError("Wing does not belong to the selected district and department")
        if tehsil.district_id != district.id:
            raise ValueError("Tehsil does not belong to the selected district")
        markaz = None
        if data.role == "aeo":
            markaz = self._active_record(session, Markaz, data.markaz_ref, "markaz")
            if markaz.wing_id != wing.id or markaz.tehsil_id != tehsil.id:
                raise ValueError("Markaz does not belong to the selected wing and tehsil")
        return district, department, wing, tehsil, markaz

    def save_officer(
        self, data: OfficerWrite, officer_id: str | None = None
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        normalized = normalize_phone(data.mobile)
        if not re.fullmatch(r"923\d{9}", normalized):
            raise ValueError("Enter a valid Pakistani mobile number")
        with Session(self.engine) as session:
            district, department, wing, tehsil, markaz = self._validate_officer_scope(
                session, data
            )
            record_id = _uuid(officer_id, "officer") if officer_id else uuid.uuid4()
            officer = session.get(Officer, record_id)
            if officer_id and (officer is None or officer.role != data.role):
                raise LookupError("Officer not found")
            duplicate = session.scalar(
                select(Officer).where(
                    Officer.role == data.role,
                    Officer.normalized_mobile == normalized,
                    Officer.id != record_id,
                )
            )
            if duplicate:
                raise ValueError(f"{data.role.upper()} mobile already exists")
            before = _officer_dict(officer) if officer else None
            if officer is None:
                officer = Officer(
                    id=record_id,
                    role=data.role,
                    name=data.name,
                    mobile=normalized,
                    normalized_mobile=normalized,
                    helpdesk_user_email=data.helpdesk_user_email.strip().lower(),
                    helpdesk_enabled=data.helpdesk_enabled,
                    district_id=district.id,
                    department_id=department.id,
                    wing_id=wing.id,
                    tehsil_id=tehsil.id,
                    markaz_id=markaz.id if markaz else None,
                )
            officer.name = data.name
            officer.mobile = normalized
            officer.normalized_mobile = normalized
            officer.helpdesk_user_email = data.helpdesk_user_email.strip().lower()
            officer.helpdesk_enabled = data.helpdesk_enabled
            officer.district_id = district.id
            officer.department_id = department.id
            officer.wing_id = wing.id
            officer.tehsil_id = tehsil.id
            officer.markaz_id = markaz.id if markaz else None
            officer.active = data.active
            officer.updated_at = utcnow()
            session.add(officer)
            session.commit()
            after = _officer_dict(officer)
        return before, after

    def set_officer_active(
        self, role: str, officer_id: str, active: bool
    ) -> tuple[dict, dict]:
        self._validate_role(role)
        with Session(self.engine) as session:
            officer = session.get(Officer, _uuid(officer_id, "officer"))
            if officer is None or officer.role != role:
                raise LookupError("Officer not found")
            before = _officer_dict(officer)
            officer.active = active
            officer.updated_at = utcnow()
            session.add(officer)
            session.commit()
            after = _officer_dict(officer)
        return before, after

    def heads(
        self,
        *,
        search: str = "",
        missing_only: bool = False,
        page: int = 1,
        page_size: int = 10,
    ) -> dict[str, Any]:
        filters: list[Any] = [School.active.is_(True)]
        if missing_only:
            filters.append(
                or_(
                    SchoolHead.id.is_(None),
                    SchoolHead.active.is_(False),
                    func.trim(SchoolHead.name) == "",
                    func.trim(SchoolHead.mobile) == "",
                )
            )
        if search:
            needle = f"%{search}%"
            filters.append(
                or_(
                    School.emis.ilike(needle),
                    School.name.ilike(needle),
                    SchoolHead.name.ilike(needle),
                    SchoolHead.mobile.ilike(needle),
                )
            )
        base = (
            select(School, SchoolHead)
            .outerjoin(SchoolHead, SchoolHead.school_id == School.id)
            .where(*filters)
        )
        with Session(self.engine) as session:
            total = session.scalar(
                select(func.count()).select_from(base.subquery())
            ) or 0
            records = session.execute(
                base.order_by(School.name).limit(page_size).offset((page - 1) * page_size)
            ).all()
            items = [
                {
                    "id": str(school.id),
                    "emis": school.emis,
                    "school_name": school.name,
                    "head_name": head.name if head and head.active else "",
                    "head_contact": head.mobile if head and head.active else "",
                    "tehsil_name": session.get(Tehsil, school.tehsil_id).name,
                    "markaz_name": (
                        session.get(Markaz, school.markaz_id).name
                        if school.markaz_id
                        else ""
                    ),
                }
                for school, head in records
            ]
        return {"items": items, "total": total, "page": page, "page_size": page_size}

    def areas(self) -> dict[str, list[dict[str, Any]]]:
        return self.options()

    def jurisdictions(
        self, *, active: bool | None = True, wing_ref: str = ""
    ) -> dict[str, list[dict[str, Any]]]:
        filters: list[Any] = []
        if active is not None:
            filters.extend(
                [OfficerJurisdiction.active.is_(active), Officer.active.is_(active)]
            )
        if wing_ref:
            filters.append(OfficerJurisdiction.wing_id == _uuid(wing_ref, "wing"))
        with Session(self.engine) as session:
            result: dict[str, list[dict[str, Any]]] = {"aeo": [], "ddeo": []}
            records = session.execute(
                select(OfficerJurisdiction, Officer)
                .join(Officer, Officer.id == OfficerJurisdiction.officer_id)
                .where(*filters)
                .order_by(OfficerJurisdiction.role, Officer.name)
            ).all()
            for jurisdiction, officer in records:
                item = {
                    "id": str(jurisdiction.id),
                    "active": jurisdiction.active,
                    "officer_name": officer.name,
                    "mobile": officer.mobile,
                    "tehsil_ref": str(jurisdiction.tehsil_id),
                    "tehsil_name": session.get(Tehsil, jurisdiction.tehsil_id).name,
                    "markaz_ref": (
                        str(jurisdiction.markaz_id) if jurisdiction.markaz_id else ""
                    ),
                    "markaz_name": (
                        session.get(Markaz, jurisdiction.markaz_id).name
                        if jurisdiction.markaz_id
                        else ""
                    ),
                    "wing_name": session.get(Wing, jurisdiction.wing_id).name,
                }
                result[jurisdiction.role].append(item)
        return result

    def quality(self) -> list[dict[str, Any]]:
        with Session(self.engine) as session:
            active_head = exists().where(
                SchoolHead.school_id == School.id,
                SchoolHead.active.is_(True),
                func.trim(SchoolHead.name) != "",
                func.trim(SchoolHead.mobile) != "",
            )
            missing_heads = session.scalar(
                select(func.count())
                .select_from(School)
                .where(School.active.is_(True), ~active_head)
            ) or 0
            secondary = session.scalar(select(Wing.id).where(Wing.code == "DEO SE"))
            pending_secondary = (
                session.scalar(
                    select(func.count())
                    .select_from(School)
                    .where(
                        School.active.is_(True),
                        School.wing_id == secondary,
                        School.markaz_id.is_(None),
                    )
                )
                if secondary
                else 0
            ) or 0
            missing_operational = session.scalar(
                select(func.count())
                .select_from(School)
                .where(
                    School.active.is_(True),
                    School.wing_id != secondary if secondary else True,
                    School.markaz_id.is_(None),
                )
            ) or 0
            mobiles = session.scalars(
                select(SchoolHead.mobile).where(
                    SchoolHead.active.is_(True), SchoolHead.mobile != ""
                )
            ).all()
            invalid_mobile = sum(
                not bool(re.fullmatch(r"923\d{9}", normalize_phone(value)))
                for value in mobiles
            )
        return [
            {
                "key": "missing_heads",
                "title": "Schools missing head details",
                "count": missing_heads,
                "severity": "warning",
                "href": "/master-data/heads?missing=true",
            },
            {
                "key": "pending_secondary_markaz",
                "title": "Secondary markaz assignments pending",
                "count": pending_secondary,
                "severity": "info",
                "href": "/master-data/schools",
            },
            {
                "key": "missing_operational_markaz",
                "title": "Non-secondary schools without markaz",
                "count": missing_operational,
                "severity": "error",
                "href": "/master-data/schools",
            },
            {
                "key": "invalid_head_mobile",
                "title": "Invalid head mobile numbers",
                "count": invalid_mobile,
                "severity": "warning",
                "href": "/master-data/heads",
            },
        ]

    def export_schools_csv(self) -> str:
        return self._csv(self.list_schools(active=None, page_size=100_000)["items"])

    def export_officers_csv(self) -> str:
        return self._csv(
            [
                *self.list_officers("aeo", active=None),
                *self.list_officers("ddeo", active=None),
            ]
        )

    @staticmethod
    def _csv(rows: list[dict[str, Any]]) -> str:
        output = io.StringIO()
        if not rows:
            return ""
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue()

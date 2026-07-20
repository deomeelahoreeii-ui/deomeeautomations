from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    JSON,
    Text,
    UniqueConstraint,
)
from sqlmodel import Field, SQLModel

from automation_core.time import utcnow


class MasterDataAudit(SQLModel, table=True):
    __tablename__ = "master_data_audits"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    entity_type: str = Field(index=True)
    entity_id: str = Field(index=True)
    action: str = Field(index=True)
    actor: str = Field(default="web", index=True)
    summary: str = Field(sa_column=Column(Text))
    before: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    after: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utcnow, index=True)


class MasterDataAuditPublic(SQLModel):
    id: uuid.UUID
    entity_type: str
    entity_id: str
    action: str
    actor: str
    summary: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    created_at: datetime


class MasterDataRecord(SQLModel):
    """Fields shared by PostgreSQL-owned master-data records."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    legacy_id: str | None = Field(default=None, unique=True, index=True)
    active: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class MasterContact(MasterDataRecord, table=True):
    """Channel-independent person/contact identity shared by every module."""

    __tablename__ = "master_contacts"
    __table_args__ = (
        CheckConstraint(
            "name_source IN ('unknown', 'whatsapp_push', 'whatsapp_profile', "
            "'verified_entity', 'manual', 'import')",
            name="ck_master_contacts_name_source",
        ),
    )

    name: str = Field(default="", index=True)
    normalized_mobile: str | None = Field(default=None, unique=True, index=True)
    name_source: str = Field(default="unknown", index=True)
    name_verified: bool = Field(default=False, index=True)
    last_observed_at: datetime | None = Field(default=None, index=True)
    notes: str = Field(default="", sa_column=Column(Text))


class District(MasterDataRecord, table=True):
    __tablename__ = "districts"

    name: str = Field(unique=True, index=True)
    code: str = Field(default="", index=True)


class Department(MasterDataRecord, table=True):
    __tablename__ = "departments"

    name: str = Field(unique=True, index=True)
    code: str = Field(default="", index=True)


class Wing(MasterDataRecord, table=True):
    __tablename__ = "wings"
    __table_args__ = (
        UniqueConstraint(
            "district_id", "department_id", "name", name="uq_wings_scope_name"
        ),
        UniqueConstraint(
            "id", "district_id", "department_id", name="uq_wings_id_scope"
        ),
    )

    district_id: uuid.UUID = Field(foreign_key="districts.id", index=True)
    department_id: uuid.UUID = Field(foreign_key="departments.id", index=True)
    name: str = Field(index=True)
    code: str = Field(default="", index=True)


class Tehsil(MasterDataRecord, table=True):
    __tablename__ = "tehsils"
    __table_args__ = (
        UniqueConstraint("district_id", "name", name="uq_tehsils_district_name"),
        UniqueConstraint("id", "district_id", name="uq_tehsils_id_district"),
    )

    district_id: uuid.UUID = Field(foreign_key="districts.id", index=True)
    name: str = Field(index=True)


class Markaz(MasterDataRecord, table=True):
    __tablename__ = "markazes"
    __table_args__ = (
        UniqueConstraint(
            "wing_id", "tehsil_id", "name", name="uq_markazes_scope_name"
        ),
        UniqueConstraint(
            "id", "wing_id", "tehsil_id", name="uq_markazes_id_scope"
        ),
    )

    wing_id: uuid.UUID = Field(foreign_key="wings.id", index=True)
    tehsil_id: uuid.UUID = Field(foreign_key="tehsils.id", index=True)
    name: str = Field(index=True)


class School(MasterDataRecord, table=True):
    __tablename__ = "schools"
    __table_args__ = (
        UniqueConstraint("id", "wing_id", name="uq_schools_id_wing"),
        ForeignKeyConstraint(
            ["wing_id", "district_id", "department_id"],
            ["wings.id", "wings.district_id", "wings.department_id"],
            name="fk_schools_wing_scope",
        ),
        ForeignKeyConstraint(
            ["tehsil_id", "district_id"],
            ["tehsils.id", "tehsils.district_id"],
            name="fk_schools_tehsil_scope",
        ),
        ForeignKeyConstraint(
            ["markaz_id", "wing_id", "tehsil_id"],
            ["markazes.id", "markazes.wing_id", "markazes.tehsil_id"],
            name="fk_schools_markaz_scope",
        ),
    )

    emis: str = Field(unique=True, index=True)
    name: str = Field(index=True)
    district_id: uuid.UUID = Field(index=True)
    department_id: uuid.UUID = Field(index=True)
    wing_id: uuid.UUID = Field(index=True)
    tehsil_id: uuid.UUID = Field(index=True)
    # Secondary-wing markaz data is not authoritative yet.
    markaz_id: uuid.UUID | None = Field(
        default=None, index=True
    )
    shift: str = ""
    school_type: str = ""
    school_level: str = Field(default="", index=True)
    deos_wise: str = ""
    source: str = ""
    source_row_hash: str = ""
    notes: str = Field(default="", sa_column=Column(Text))


class SchoolHead(MasterDataRecord, table=True):
    __tablename__ = "school_heads"

    school_id: uuid.UUID = Field(foreign_key="schools.id", unique=True, index=True)
    name: str = Field(index=True)
    mobile: str = ""
    normalized_mobile: str = Field(default="", index=True)


class Officer(MasterDataRecord, table=True):
    __tablename__ = "officers"
    __table_args__ = (
        UniqueConstraint("role", "normalized_mobile", name="uq_officers_role_mobile"),
        CheckConstraint("role IN ('aeo', 'ddeo')", name="ck_officers_role"),
        UniqueConstraint("id", "wing_id", name="uq_officers_id_wing"),
        ForeignKeyConstraint(
            ["wing_id", "district_id", "department_id"],
            ["wings.id", "wings.district_id", "wings.department_id"],
            name="fk_officers_wing_scope",
        ),
        ForeignKeyConstraint(
            ["tehsil_id", "district_id"],
            ["tehsils.id", "tehsils.district_id"],
            name="fk_officers_tehsil_scope",
        ),
        ForeignKeyConstraint(
            ["markaz_id", "wing_id", "tehsil_id"],
            ["markazes.id", "markazes.wing_id", "markazes.tehsil_id"],
            name="fk_officers_markaz_scope",
        ),
    )

    role: str = Field(index=True)
    name: str = Field(index=True)
    mobile: str = ""
    normalized_mobile: str = Field(index=True)
    district_id: uuid.UUID = Field(index=True)
    department_id: uuid.UUID = Field(index=True)
    wing_id: uuid.UUID = Field(index=True)
    tehsil_id: uuid.UUID = Field(index=True)
    markaz_id: uuid.UUID | None = Field(
        default=None, index=True
    )


class OfficerJurisdiction(MasterDataRecord, table=True):
    __tablename__ = "officer_jurisdictions"
    __table_args__ = (
        UniqueConstraint(
            "officer_id",
            "wing_id",
            "tehsil_id",
            "markaz_id",
            name="uq_officer_jurisdictions_scope",
        ),
        CheckConstraint(
            "role IN ('aeo', 'ddeo')", name="ck_officer_jurisdictions_role"
        ),
        ForeignKeyConstraint(
            ["officer_id", "wing_id"],
            ["officers.id", "officers.wing_id"],
            name="fk_jurisdictions_officer_wing",
        ),
        ForeignKeyConstraint(
            ["wing_id", "district_id", "department_id"],
            ["wings.id", "wings.district_id", "wings.department_id"],
            name="fk_jurisdictions_wing_scope",
        ),
        ForeignKeyConstraint(
            ["tehsil_id", "district_id"],
            ["tehsils.id", "tehsils.district_id"],
            name="fk_jurisdictions_tehsil_scope",
        ),
        ForeignKeyConstraint(
            ["markaz_id", "wing_id", "tehsil_id"],
            ["markazes.id", "markazes.wing_id", "markazes.tehsil_id"],
            name="fk_jurisdictions_markaz_scope",
        ),
    )

    role: str = Field(index=True)
    officer_id: uuid.UUID = Field(index=True)
    district_id: uuid.UUID = Field(index=True)
    department_id: uuid.UUID = Field(index=True)
    wing_id: uuid.UUID = Field(index=True)
    tehsil_id: uuid.UUID = Field(index=True)
    markaz_id: uuid.UUID | None = Field(
        default=None, index=True
    )
    effective_from: str = ""
    effective_to: str = ""
    notes: str = Field(default="", sa_column=Column(Text))


class SchoolOfficerOverride(MasterDataRecord, table=True):
    __tablename__ = "school_officer_overrides"
    __table_args__ = (
        UniqueConstraint(
            "school_id", "officer_id", "role", name="uq_school_officer_overrides"
        ),
        CheckConstraint(
            "role IN ('aeo', 'ddeo')", name="ck_school_officer_overrides_role"
        ),
        ForeignKeyConstraint(
            ["school_id", "wing_id"],
            ["schools.id", "schools.wing_id"],
            name="fk_overrides_school_wing",
        ),
        ForeignKeyConstraint(
            ["officer_id", "wing_id"],
            ["officers.id", "officers.wing_id"],
            name="fk_overrides_officer_wing",
        ),
    )

    role: str = Field(index=True)
    school_id: uuid.UUID = Field(index=True)
    officer_id: uuid.UUID = Field(index=True)
    # Stored explicitly so routing can reject cross-wing assignments cheaply.
    wing_id: uuid.UUID = Field(foreign_key="wings.id", index=True)
    effective_from: str = ""
    effective_to: str = ""
    reason: str = Field(default="", sa_column=Column(Text))

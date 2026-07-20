from __future__ import annotations

from typing import Literal

from sqlmodel import Field, SQLModel


class MasterContactWrite(SQLModel):
    name: str = Field(min_length=1, max_length=200)
    mobile: str = Field(default="", max_length=30)
    notes: str = Field(default="", max_length=2000)
    active: bool = True


class SchoolWrite(SQLModel):
    emis: str = Field(min_length=1, max_length=30)
    name: str = Field(min_length=1, max_length=300)
    district_ref: str
    department_ref: str
    wing_ref: str
    tehsil_ref: str
    # Secondary-wing schools can be imported before their markaz mapping exists.
    markaz_ref: str = ""
    head_name: str = ""
    head_contact: str = ""
    shift: str = "Single"
    school_type: str = "Male"
    school_level: str = "Primary"
    deos_wise: str = "M-EE"
    notes: str = ""
    active: bool = True


class OfficerWrite(SQLModel):
    role: Literal["aeo", "ddeo"]
    name: str = Field(min_length=1, max_length=200)
    mobile: str = Field(min_length=1, max_length=30)
    district_ref: str
    department_ref: str
    wing_ref: str
    tehsil_ref: str
    markaz_ref: str = ""
    # None preserves compatibility with older callers. The web form sends the
    # complete desired active scope list so removals and additional charges are
    # reconciled transactionally.
    jurisdiction_refs: list[str] | None = None
    replace_conflicts: bool = False
    helpdesk_user_email: str = Field(default="", max_length=255)
    helpdesk_enabled: bool = False
    active: bool = True


class JurisdictionAssignmentWrite(SQLModel):
    role: Literal["aeo", "ddeo"]
    officer_ref: str
    scope_ref: str
    make_primary: bool = False
    replace_conflicts: bool = False
    notes: str = Field(default="", max_length=2000)


class SchoolImportCommit(SQLModel):
    rows: list[SchoolWrite]

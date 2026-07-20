#!/usr/bin/env python3
"""Audit and repair officer rows that are not represented in live jurisdictions.

The officer form historically wrote officers.markaz_id/tehsil_id only. AntiDengue
routing, Markazes, Hierarchy and Jurisdictions read officer_jurisdictions instead.
This utility safely converts those orphaned form submissions into authoritative
assignments and transfers the scope from the previous temporary/additional-charge
holder when necessary.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import uuid

from sqlmodel import Session, select

from automation_core.database import engine
from master_data.models import (
    Department,
    District,
    Markaz,
    MasterDataAudit,
    Officer,
    OfficerJurisdiction,
    Tehsil,
    Wing,
)
from master_data.postgres_repository import PostgresMasterDataRepository
from master_data.schemas import OfficerWrite


def _json_row(row: Any) -> dict[str, Any]:
    return row.model_dump(mode="json")


def _write_backup(path: Path) -> None:
    if path.exists():
        raise SystemExit(f"Backup already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with Session(engine) as session:
        payload = {
            "created_at": datetime.now(UTC).isoformat(),
            "officers": [_json_row(row) for row in session.exec(select(Officer)).all()],
            "jurisdictions": [
                _json_row(row) for row in session.exec(select(OfficerJurisdiction)).all()
            ],
        }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _restore(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    officer_rows = {row["id"]: row for row in payload["officers"]}
    jurisdiction_rows = {row["id"]: row for row in payload["jurisdictions"]}
    with Session(engine) as session:
        for current in session.exec(select(OfficerJurisdiction)).all():
            key = str(current.id)
            if key not in jurisdiction_rows:
                session.delete(current)
        session.flush()
        for key, raw in jurisdiction_rows.items():
            record = session.get(OfficerJurisdiction, uuid.UUID(key))
            restored = OfficerJurisdiction.model_validate(raw)
            if record is None:
                session.add(restored)
                continue
            for field, value in restored.model_dump().items():
                if field != "id":
                    setattr(record, field, value)
            session.add(record)
        for key, raw in officer_rows.items():
            record = session.get(Officer, uuid.UUID(key))
            if record is None:
                raise RuntimeError(f"Officer disappeared during repair rollback: {key}")
            restored = Officer.model_validate(raw)
            for field, value in restored.model_dump().items():
                if field != "id":
                    setattr(record, field, value)
            session.add(record)
        session.commit()
    print(f"Restored officer and jurisdiction rows from {path}")


def _scope_name(session: Session, officer: Officer) -> str:
    if officer.role == "aeo" and officer.markaz_id:
        markaz = session.get(Markaz, officer.markaz_id)
        return markaz.name if markaz else str(officer.markaz_id)
    tehsil = session.get(Tehsil, officer.tehsil_id)
    return tehsil.name if tehsil else str(officer.tehsil_id)


def _active_scope_owner(session: Session, officer: Officer) -> Officer | None:
    query = select(OfficerJurisdiction).where(
        OfficerJurisdiction.active.is_(True),
        OfficerJurisdiction.role == officer.role,
        OfficerJurisdiction.wing_id == officer.wing_id,
        OfficerJurisdiction.tehsil_id == officer.tehsil_id,
    )
    if officer.role == "aeo":
        query = query.where(OfficerJurisdiction.markaz_id == officer.markaz_id)
    else:
        query = query.where(OfficerJurisdiction.markaz_id.is_(None))
    assignment = session.exec(query).first()
    return session.get(Officer, assignment.officer_id) if assignment else None


def _orphaned_officers(session: Session) -> list[Officer]:
    candidates: list[Officer] = []
    for officer in session.exec(
        select(Officer).where(Officer.active.is_(True)).order_by(Officer.created_at)
    ).all():
        active_count = len(
            session.exec(
                select(OfficerJurisdiction).where(
                    OfficerJurisdiction.officer_id == officer.id,
                    OfficerJurisdiction.active.is_(True),
                )
            ).all()
        )
        if active_count:
            continue
        if officer.role == "aeo" and not officer.markaz_id:
            continue
        candidates.append(officer)
    return candidates


def _officer_write(session: Session, officer: Officer) -> OfficerWrite:
    district = session.get(District, officer.district_id)
    department = session.get(Department, officer.department_id)
    wing = session.get(Wing, officer.wing_id)
    tehsil = session.get(Tehsil, officer.tehsil_id)
    if not all((district, department, wing, tehsil)):
        raise RuntimeError(f"Incomplete administrative placement for {officer.name}")
    primary_ref = str(officer.markaz_id or officer.tehsil_id)
    return OfficerWrite(
        role=officer.role,
        name=officer.name,
        mobile=officer.mobile,
        district_ref=str(district.id),
        department_ref=str(department.id),
        wing_ref=str(wing.id),
        tehsil_ref=str(tehsil.id),
        markaz_ref=str(officer.markaz_id or ""),
        jurisdiction_refs=[primary_ref],
        replace_conflicts=True,
        helpdesk_user_email=officer.helpdesk_user_email,
        helpdesk_enabled=officer.helpdesk_enabled,
        active=officer.active,
    )


def audit() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    claimed_scopes: dict[tuple[str, str, str], str] = {}
    with Session(engine) as session:
        for officer in _orphaned_officers(session):
            scope_ref = str(officer.markaz_id or officer.tehsil_id)
            scope_key = (officer.role, str(officer.wing_id), scope_ref)
            prior = claimed_scopes.get(scope_key)
            if prior:
                raise RuntimeError(
                    f"Unsafe repair ambiguity: {prior} and {officer.name} both claim "
                    f"{_scope_name(session, officer)}. Resolve the intended officer manually."
                )
            claimed_scopes[scope_key] = officer.name
            owner = _active_scope_owner(session, officer)
            rows.append(
                {
                    "officer_id": str(officer.id),
                    "officer": officer.name,
                    "role": officer.role.upper(),
                    "scope": _scope_name(session, officer),
                    "current_owner": owner.name if owner and owner.id != officer.id else "Unassigned",
                    "action": "transfer" if owner and owner.id != officer.id else "create",
                }
            )
    return rows


def apply(backup: Path) -> None:
    plan = audit()
    _write_backup(backup)
    repository = PostgresMasterDataRepository(engine)
    changed: list[dict[str, str]] = []
    for item in plan:
        with Session(engine) as session:
            officer = session.get(Officer, uuid.UUID(item["officer_id"]))
            if officer is None:
                raise RuntimeError(f"Officer vanished during repair: {item['officer']}")
            payload = _officer_write(session, officer)
        repository.save_officer(payload, item["officer_id"], jurisdiction_mode="replace")
        changed.append(item)
        print(
            f"[{item['action']}] {item['role']} {item['officer']} -> {item['scope']}"
            + (f" (replaced {item['current_owner']})" if item["action"] == "transfer" else "")
        )
    if changed:
        with Session(engine) as session:
            session.add(
                MasterDataAudit(
                    entity_type="officer_jurisdiction_repair",
                    entity_id="bundle-4.3",
                    action="reconciled",
                    summary=f"Reconciled {len(changed)} officer jurisdiction assignment(s)",
                    before={"plan": plan},
                    after={"changed": changed},
                )
            )
            session.commit()
    print(f"Changed assignments: {len(changed)}")
    print(f"Rollback data backup: {backup}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Apply the audited repair")
    parser.add_argument("--backup", type=Path, help="Required JSON backup path with --apply")
    parser.add_argument("--restore", type=Path, help="Restore a prior --apply backup")
    args = parser.parse_args()
    if args.restore:
        _restore(args.restore)
        return
    plan = audit()
    if not plan:
        print("No active officers are missing authoritative jurisdictions.")
        return
    print("Officer jurisdiction repair plan:")
    for item in plan:
        suffix = f"; current owner: {item['current_owner']}" if item["current_owner"] != "Unassigned" else ""
        print(f"- {item['role']} {item['officer']} -> {item['scope']} [{item['action']}]{suffix}")
    if not args.apply:
        print("Dry run only. Use --apply --backup <path> after review.")
        return
    if not args.backup:
        raise SystemExit("--backup is required with --apply")
    apply(args.backup)


if __name__ == "__main__":
    main()

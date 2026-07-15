from __future__ import annotations

import argparse
import sqlite3
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, inspect
from sqlmodel import Session

from automation_core.config import get_settings
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


NAMESPACE = uuid.UUID("86990825-b4ce-4b35-969d-c536f2efb43a")
REQUIRED_TABLES = {
    "departments",
    "districts",
    "markazes",
    "officers",
    "officer_jurisdictions",
    "school_heads",
    "school_officer_overrides",
    "schools",
    "tehsils",
    "wings",
}


def stable_id(entity: str, legacy_id: str) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, f"{entity}:{legacy_id}")


def rows(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(f"SELECT * FROM {table}")]


def target_id(entity: str, legacy_id: str) -> uuid.UUID:
    if not legacy_id:
        raise ValueError(f"Missing required {entity} reference")
    return stable_id(entity, legacy_id)


def optional_target_id(entity: str, legacy_id: str) -> uuid.UUID | None:
    return stable_id(entity, legacy_id) if legacy_id else None


def prefixed_legacy(entity: str, legacy_id: str) -> str:
    return f"{entity}:{legacy_id}"


def upsert(session: Session, model: type[Any], record_id: uuid.UUID, values: dict[str, Any]) -> None:
    record = session.get(model, record_id)
    if record is None:
        session.add(model(id=record_id, **values))
        return
    for key, value in values.items():
        setattr(record, key, value)
    session.add(record)


def validate_source(connection: sqlite3.Connection) -> None:
    checks = {
        "school district": """
            SELECT count(*) FROM schools s
            LEFT JOIN districts d ON d.id = s.district_ref
            WHERE d.id IS NULL
        """,
        "school department": """
            SELECT count(*) FROM schools s
            LEFT JOIN departments d ON d.id = s.department_ref
            WHERE d.id IS NULL
        """,
        "school wing": """
            SELECT count(*) FROM schools s
            LEFT JOIN wings w ON w.id = s.wing_ref
            WHERE w.id IS NULL
        """,
        "school tehsil": """
            SELECT count(*) FROM schools s
            LEFT JOIN tehsils t ON t.id = s.tehsil_ref
            WHERE t.id IS NULL
        """,
        "school markaz": """
            SELECT count(*) FROM schools s
            LEFT JOIN markazes m ON m.id = s.markaz_ref
            WHERE trim(s.markaz_ref) <> '' AND m.id IS NULL
        """,
    }
    failures: dict[str, int] = {}
    for label, sql in checks.items():
        count = connection.execute(sql).fetchone()[0]
        if count:
            failures[label] = count
    if failures:
        details = ", ".join(f"{label}={count}" for label, count in failures.items())
        raise ValueError(f"PocketBase reference validation failed: {details}")


def import_hierarchy(session: Session, source: sqlite3.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows(source, "districts"):
        upsert(
            session,
            District,
            stable_id("district", row["id"]),
            {
                "legacy_id": row["id"],
                "name": row["name"],
                "code": row["code"],
                "active": bool(row["active"]),
            },
        )
    counts["districts"] = source.execute("SELECT count(*) FROM districts").fetchone()[0]

    for row in rows(source, "departments"):
        upsert(
            session,
            Department,
            stable_id("department", row["id"]),
            {
                "legacy_id": row["id"],
                "name": row["name"],
                "code": row["code"],
                "active": bool(row["active"]),
            },
        )
    counts["departments"] = source.execute("SELECT count(*) FROM departments").fetchone()[0]
    session.flush()

    for row in rows(source, "wings"):
        upsert(
            session,
            Wing,
            stable_id("wing", row["id"]),
            {
                "legacy_id": row["id"],
                "district_id": target_id("district", row["district_ref"]),
                "department_id": target_id("department", row["department_ref"]),
                "name": row["name"],
                "code": row["code"],
                "active": bool(row["active"]),
            },
        )
    counts["wings"] = source.execute("SELECT count(*) FROM wings").fetchone()[0]

    for row in rows(source, "tehsils"):
        upsert(
            session,
            Tehsil,
            stable_id("tehsil", row["id"]),
            {
                "legacy_id": row["id"],
                "district_id": target_id("district", row["district_ref"]),
                "name": row["name"],
                "active": bool(row["active"]),
            },
        )
    counts["tehsils"] = source.execute("SELECT count(*) FROM tehsils").fetchone()[0]
    session.flush()

    for row in rows(source, "markazes"):
        upsert(
            session,
            Markaz,
            stable_id("markaz", row["id"]),
            {
                "legacy_id": row["id"],
                "wing_id": target_id("wing", row["wing_ref"]),
                "tehsil_id": target_id("tehsil", row["tehsil_ref"]),
                "name": row["name"],
                "active": bool(row["active"]),
            },
        )
    counts["markazes"] = source.execute("SELECT count(*) FROM markazes").fetchone()[0]
    session.flush()
    return counts


def import_schools(session: Session, source: sqlite3.Connection) -> dict[str, int]:
    school_count = 0
    head_count = 0
    for row in rows(source, "schools"):
        school_id = stable_id("school", row["id"])
        upsert(
            session,
            School,
            school_id,
            {
                "legacy_id": row["id"],
                "emis": row["emis"],
                "name": row["name"],
                "district_id": target_id("district", row["district_ref"]),
                "department_id": target_id("department", row["department_ref"]),
                "wing_id": target_id("wing", row["wing_ref"]),
                "tehsil_id": target_id("tehsil", row["tehsil_ref"]),
                "markaz_id": optional_target_id("markaz", row["markaz_ref"]),
                "shift": row["shift"],
                "school_type": row["school_type"],
                "school_level": row["school_level"],
                "deos_wise": row["deos_wise"],
                "source": row["source"],
                "source_row_hash": row["source_row_hash"],
                "notes": row["notes"],
                "active": bool(row["active"]),
            },
        )
        school_count += 1
        if row["head_name"].strip() or row["head_contact"].strip():
            upsert(
                session,
                SchoolHead,
                stable_id("school_head", row["id"]),
                {
                    "legacy_id": prefixed_legacy("school_head", row["id"]),
                    "school_id": school_id,
                    "name": row["head_name"],
                    "mobile": row["head_contact"],
                    "normalized_mobile": normalize_phone(row["head_contact"]),
                    "active": bool(row["active"]),
                },
            )
            head_count += 1
    session.flush()
    return {"schools": school_count, "school_heads": head_count}


def officer_rows(source: sqlite3.Connection) -> Iterable[tuple[str, dict[str, Any]]]:
    for role in ("aeo", "ddeo"):
        for row in rows(source, f"{role}_officers"):
            yield role, row


def import_officers(session: Session, source: sqlite3.Connection) -> dict[str, int]:
    counts = {"officers": 0, "officer_jurisdictions": 0, "school_officer_overrides": 0}
    for role, row in officer_rows(source):
        upsert(
            session,
            Officer,
            stable_id(f"{role}_officer", row["id"]),
            {
                "legacy_id": prefixed_legacy(f"{role}_officer", row["id"]),
                "role": role,
                "name": row["name"],
                "mobile": row["mobile"],
                "normalized_mobile": row["normalized_mobile"],
                "district_id": target_id("district", row["district_ref"]),
                "department_id": target_id("department", row["department_ref"]),
                "wing_id": target_id("wing", row["wing_ref"]),
                "tehsil_id": target_id("tehsil", row["tehsil_ref"]),
                "markaz_id": optional_target_id("markaz", row.get("markaz_ref", "")),
                "active": bool(row["active"]),
            },
        )
        counts["officers"] += 1
    session.flush()

    for role in ("aeo", "ddeo"):
        officer_ref = f"{role}_ref"
        for row in rows(source, f"{role}_jurisdictions"):
            upsert(
                session,
                OfficerJurisdiction,
                stable_id(f"{role}_jurisdiction", row["id"]),
                {
                    "legacy_id": prefixed_legacy(f"{role}_jurisdiction", row["id"]),
                    "role": role,
                    "officer_id": target_id(f"{role}_officer", row[officer_ref]),
                    "district_id": target_id("district", row["district_ref"]),
                    "department_id": target_id("department", row["department_ref"]),
                    "wing_id": target_id("wing", row["wing_ref"]),
                    "tehsil_id": target_id("tehsil", row["tehsil_ref"]),
                    "markaz_id": optional_target_id("markaz", row.get("markaz_ref", "")),
                    "effective_from": row["effective_from"],
                    "effective_to": row["effective_to"],
                    "notes": row["notes"],
                    "active": bool(row["active"]),
                },
            )
            counts["officer_jurisdictions"] += 1
    session.flush()

    for role in ("aeo", "ddeo"):
        officer_ref = f"{role}_ref"
        for row in rows(source, f"school_{role}_overrides"):
            school = source.execute(
                "SELECT wing_ref FROM schools WHERE id = ?", (row["school_ref"],)
            ).fetchone()
            if school is None:
                raise ValueError(f"Override references unknown school {row['school_ref']}")
            upsert(
                session,
                SchoolOfficerOverride,
                stable_id(f"school_{role}_override", row["id"]),
                {
                    "legacy_id": prefixed_legacy(f"school_{role}_override", row["id"]),
                    "role": role,
                    "school_id": target_id("school", row["school_ref"]),
                    "officer_id": target_id(f"{role}_officer", row[officer_ref]),
                    "wing_id": target_id("wing", school["wing_ref"]),
                    "effective_from": row["effective_from"],
                    "effective_to": row["effective_to"],
                    "reason": row["reason"],
                    "active": bool(row["active"]),
                },
            )
            counts["school_officer_overrides"] += 1
    session.flush()
    return counts


def migrate(source_path: Path, database_url: str, *, commit: bool) -> dict[str, int]:
    if not source_path.exists():
        raise FileNotFoundError(f"PocketBase database not found: {source_path}")
    engine = create_engine(database_url)
    missing = REQUIRED_TABLES - set(inspect(engine).get_table_names())
    if missing:
        raise RuntimeError(
            "Destination is not migrated; run `uv run alembic upgrade head`. "
            f"Missing: {', '.join(sorted(missing))}"
        )

    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    source.row_factory = sqlite3.Row
    try:
        validate_source(source)
        with Session(engine) as session:
            counts = import_hierarchy(session, source)
            counts.update(import_schools(session, source))
            counts.update(import_officers(session, source))
            if commit:
                session.commit()
            else:
                session.rollback()
            return counts
    finally:
        source.close()
        engine.dispose()


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Migrate PocketBase master data into the platform database."
    )
    parser.add_argument("--source", type=Path, default=settings.pocketbase_db_path)
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--commit", action="store_true", help="Commit the import; default is rollback preview.")
    parser.add_argument(
        "--allow-sqlite",
        action="store_true",
        help="Allow a SQLite destination for automated tests only.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.database_url.startswith(("postgresql://", "postgresql+psycopg://")):
        if not args.allow_sqlite:
            raise SystemExit("Refusing non-PostgreSQL destination (use --allow-sqlite for tests).")
    counts = migrate(args.source.resolve(), args.database_url, commit=args.commit)
    mode = "COMMITTED" if args.commit else "PREVIEW (rolled back)"
    print(mode)
    for entity, count in counts.items():
        print(f"{entity}: {count}")


if __name__ == "__main__":
    main()

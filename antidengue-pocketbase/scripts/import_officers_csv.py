#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV = ROOT.parent / "antidengue" / "officers_list.csv"
DEFAULT_RECIPIENTS_CSV = ROOT.parent / "antidengue" / "whatsapp_recipients.csv"
DEFAULT_DB = ROOT / "pb_data" / "data.db"
DEFAULT_DEPARTMENT = "School Education Department"
OFFICERS_IMPORT_NOTE = "Imported from officers_list.csv"


def record_id(prefix: str, key: str) -> str:
    return hashlib.sha1(f"{prefix}:{key}".encode("utf-8")).hexdigest()[:15]


def cell(row: dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def normalized_mobile(value: str) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return ""
    if digits.startswith("0092"):
        digits = digits[2:]
    if digits.startswith("92"):
        return digits
    if digits.startswith("0"):
        return "92" + digits[1:]
    if len(digits) == 10 and digits.startswith("3"):
        return "92" + digits
    return digits


def row_hash(row: dict[str, str]) -> str:
    parts = [f"{key}={cell(row, key)}" for key in sorted(row)]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def natural_key(value: str) -> str:
    return " ".join(str(value or "").strip().upper().split())


def parse_bool(value: str, default: bool = True) -> int:
    text = str(value or "").strip().lower()
    if not text:
        return 1 if default else 0
    return 0 if text in {"0", "false", "no", "off"} else 1


def parse_int(value: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    return int(float(text))


def school_level_from_name(name: str) -> str:
    normalized = natural_key(name)
    prefix = normalized.split(" ", 1)[0] if normalized else ""
    if prefix == "GPS":
        return "Primary"
    if prefix == "GES":
        return "Middle"
    if prefix == "GMMS":
        return "sMosque"
    return ""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    last_error: UnicodeDecodeError | None = None
    for encoding in ("utf-8-sig", "cp1252", "latin1"):
        try:
            with path.open(newline="", encoding=encoding) as handle:
                return list(csv.DictReader(handle))
        except UnicodeDecodeError as exc:
            last_error = exc
    assert last_error is not None
    raise last_error


def table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type = 'table'"
        ).fetchall()
    }


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"pragma table_info({table})").fetchall()}


def upsert_values(
    conn: sqlite3.Connection,
    table: str,
    values: dict[str, object],
    *,
    conflict_columns: tuple[str, ...],
    immutable_columns: set[str] | None = None,
) -> None:
    if table not in table_names(conn):
        return

    columns_in_table = table_columns(conn, table)
    filtered = {
        key: value
        for key, value in values.items()
        if key in columns_in_table
    }
    if not filtered or not set(conflict_columns).issubset(filtered):
        return

    immutable = set(immutable_columns or set())
    columns = ", ".join(filtered)
    placeholders = ", ".join(":" + key for key in filtered)
    update_columns = [
        key
        for key in filtered
        if key not in immutable and key not in conflict_columns and key != "id"
    ]
    if update_columns:
        updates = ", ".join(f"{key}=excluded.{key}" for key in update_columns)
        conflict = ", ".join(conflict_columns)
        sql = f"""
            insert into {table} ({columns})
            values ({placeholders})
            on conflict({conflict}) do update set {updates}
        """
    else:
        conflict = ", ".join(conflict_columns)
        sql = f"""
            insert into {table} ({columns})
            values ({placeholders})
            on conflict({conflict}) do nothing
        """
    conn.execute(sql, filtered)


def deactivate_stale_imported_rows(
    conn: sqlite3.Connection,
    table: str,
    current_ids: set[str],
) -> int:
    if table not in table_names(conn):
        return 0

    if current_ids:
        placeholders = ", ".join("?" for _ in current_ids)
        sql = f"""
            update {table}
            set active = 0
            where active = 1
              and notes = ?
              and id not in ({placeholders})
        """
        params: list[object] = [OFFICERS_IMPORT_NOTE, *sorted(current_ids)]
    else:
        sql = f"""
            update {table}
            set active = 0
            where active = 1
              and notes = ?
        """
        params = [OFFICERS_IMPORT_NOTE]

    cursor = conn.execute(sql, params)
    return int(cursor.rowcount or 0)


def deactivate_stale_unlinked_officers(
    conn: sqlite3.Connection,
    table: str,
    *,
    current_ids: set[str],
    jurisdiction_table: str,
    jurisdiction_ref_column: str,
    override_table: str,
    override_ref_column: str,
) -> int:
    if table not in table_names(conn):
        return 0

    required_tables = {jurisdiction_table, override_table}
    if not required_tables.issubset(table_names(conn)):
        return 0

    if current_ids:
        placeholders = ", ".join("?" for _ in current_ids)
        stale_clause = f"and officer.id not in ({placeholders})"
        params: list[object] = sorted(current_ids)
    else:
        stale_clause = ""
        params = []

    sql = f"""
        update {table} as officer
        set active = 0
        where officer.active = 1
          {stale_clause}
          and not exists (
            select 1
            from {jurisdiction_table} j
            where j.{jurisdiction_ref_column} = officer.id
              and j.active = 1
          )
          and not exists (
            select 1
            from {override_table} o
            where o.{override_ref_column} = officer.id
              and o.active = 1
          )
    """
    cursor = conn.execute(sql, params)
    return int(cursor.rowcount or 0)


def ensure_tables_exist(conn: sqlite3.Connection) -> None:
    required = {
        "districts",
        "departments",
        "wings",
        "tehsils",
        "markazes",
        "schools",
        "ddeo_officers",
        "aeo_officers",
        "ddeo_jurisdictions",
        "aeo_jurisdictions",
        "whatsapp_recipients",
        "officer_import_batches",
    }
    existing = table_names(conn)
    missing = sorted(required - existing)
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(
            f"PocketBase schema is missing: {joined}. Run './pocketbase migrate up' first."
        )


def upsert_district(conn: sqlite3.Connection, name: str) -> str:
    name = cell({"value": name}, "value")
    if not name or "districts" not in table_names(conn):
        return ""
    district_id = record_id("district", natural_key(name))
    upsert_values(
        conn,
        "districts",
        {
            "id": district_id,
            "name": name,
            "code": natural_key(name),
            "active": 1,
        },
        conflict_columns=("name",),
        immutable_columns={"id"},
    )
    return district_id


def upsert_department(conn: sqlite3.Connection, name: str = DEFAULT_DEPARTMENT) -> str:
    name = cell({"value": name}, "value") or DEFAULT_DEPARTMENT
    if "departments" not in table_names(conn):
        return ""
    department_id = record_id("department", natural_key(name))
    upsert_values(
        conn,
        "departments",
        {
            "id": department_id,
            "name": name,
            "code": natural_key(name),
            "active": 1,
        },
        conflict_columns=("name",),
        immutable_columns={"id"},
    )
    return department_id


def upsert_wing(
    conn: sqlite3.Connection,
    *,
    name: str,
    district_id: str,
    department_id: str,
) -> str:
    name = cell({"value": name}, "value")
    if not name or "wings" not in table_names(conn):
        return ""
    wing_id = record_id("wing", f"{district_id}:{department_id}:{natural_key(name)}")
    upsert_values(
        conn,
        "wings",
        {
            "id": wing_id,
            "name": name,
            "code": natural_key(name),
            "district_ref": district_id,
            "department_ref": department_id,
            "active": 1,
        },
        conflict_columns=("name", "district_ref", "department_ref"),
        immutable_columns={"id"},
    )
    return wing_id


def upsert_tehsil(
    conn: sqlite3.Connection,
    *,
    name: str,
    district_id: str,
) -> str:
    name = cell({"value": name}, "value")
    if not name or "tehsils" not in table_names(conn):
        return ""
    tehsil_id = record_id("tehsil", f"{district_id}:{natural_key(name)}")
    upsert_values(
        conn,
        "tehsils",
        {
            "id": tehsil_id,
            "name": name,
            "district_ref": district_id,
            "active": 1,
        },
        conflict_columns=("name", "district_ref"),
        immutable_columns={"id"},
    )
    return tehsil_id


def upsert_markaz(
    conn: sqlite3.Connection,
    *,
    name: str,
    tehsil_id: str,
    wing_id: str,
) -> str:
    name = cell({"value": name}, "value")
    if not name or "markazes" not in table_names(conn):
        return ""
    markaz_id = record_id("markaz", f"{tehsil_id}:{wing_id}:{natural_key(name)}")
    upsert_values(
        conn,
        "markazes",
        {
            "id": markaz_id,
            "name": name,
            "tehsil_ref": tehsil_id,
            "wing_ref": wing_id,
            "active": 1,
        },
        conflict_columns=("name", "tehsil_ref", "wing_ref"),
        immutable_columns={"id"},
    )
    return markaz_id


def upsert_hierarchy(conn: sqlite3.Connection, row: dict[str, str]) -> dict[str, str]:
    district_id = upsert_district(conn, cell(row, "District"))
    department_id = upsert_department(conn)
    wing_id = upsert_wing(
        conn,
        name=cell(row, "Wing"),
        district_id=district_id,
        department_id=department_id,
    )
    tehsil_id = upsert_tehsil(
        conn,
        name=cell(row, "Tehsil"),
        district_id=district_id,
    )
    markaz_id = upsert_markaz(
        conn,
        name=cell(row, "Markaz"),
        tehsil_id=tehsil_id,
        wing_id=wing_id,
    )
    return {
        "district_ref": district_id,
        "department_ref": department_id,
        "wing_ref": wing_id,
        "tehsil_ref": tehsil_id,
        "markaz_ref": markaz_id,
    }


def upsert_school(
    conn: sqlite3.Connection,
    row: dict[str, str],
    hierarchy: dict[str, str],
) -> str:
    emis = cell(row, "EMIS")
    if not emis:
        return ""
    values = {
        "id": record_id("school", emis),
        "emis": emis,
        "name": cell(row, "School Name"),
        "shift": cell(row, "Shift"),
        "head_name": cell(row, "Head Name"),
        "head_contact": normalized_mobile(cell(row, "Head Contact No")),
        "school_type": "Male",
        "deos_wise": "M-EE",
        "school_level": school_level_from_name(cell(row, "School Name")),
        "active": 1,
        "source_row_hash": row_hash(row),
        "source": "officers_list_csv",
        "notes": OFFICERS_IMPORT_NOTE,
        **hierarchy,
    }
    upsert_values(
        conn,
        "schools",
        values,
        conflict_columns=("emis",),
        immutable_columns={"id"},
    )
    return str(values["id"])


def upsert_officer(
    conn: sqlite3.Connection,
    *,
    name: str,
    role: str,
    mobile: str,
    row: dict[str, str],
    hierarchy: dict[str, str],
) -> str:
    phone = normalized_mobile(mobile)
    if not name or not phone:
        return ""
    values = {
        "id": record_id("officer", f"{role}:{phone}"),
        "name": name,
        "role": role,
        "mobile": phone,
        "normalized_mobile": phone,
        "district": cell(row, "District"),
        "wing": cell(row, "Wing"),
        "tehsil": cell(row, "Tehsil"),
        "markaz": cell(row, "Markaz") if role != "DDEO" else "",
        "active": 1,
        "district_ref": hierarchy.get("district_ref", ""),
        "department_ref": hierarchy.get("department_ref", ""),
        "wing_ref": hierarchy.get("wing_ref", ""),
        "tehsil_ref": hierarchy.get("tehsil_ref", ""),
        "markaz_ref": hierarchy.get("markaz_ref", "") if role != "DDEO" else "",
    }
    upsert_values(
        conn,
        "officers",
        values,
        conflict_columns=("normalized_mobile", "role"),
        immutable_columns={"id"},
    )
    return phone


def upsert_whatsapp_recipient(conn: sqlite3.Connection, row: dict[str, str]) -> str:
    target = cell(row, "target")
    if not target:
        return ""

    recipient_type = cell(row, "type").lower()
    if recipient_type not in {"group", "contact"}:
        recipient_type = "group" if target.endswith("@g.us") else "contact"

    attachment_mode = cell(row, "attachment_text_mode").lower()
    if attachment_mode not in {"caption", "separate"}:
        attachment_mode = ""

    values = {
        "id": record_id("whatsapp_recipient", target),
        "enabled": parse_bool(cell(row, "enabled"), default=True),
        "name": cell(row, "name") or target,
        "type": recipient_type,
        "target": target,
        "text": cell(row, "text"),
        "image_path": cell(row, "image_path"),
        "excel_path": cell(row, "excel_path"),
        "excel_filename": cell(row, "excel_filename"),
        "attachment_text_mode": attachment_mode,
        "delay_ms": parse_int(cell(row, "delay_ms")),
        "notes": "Imported from whatsapp_recipients.csv",
    }
    upsert_values(
        conn,
        "whatsapp_recipients",
        values,
        conflict_columns=("target",),
        immutable_columns={"id"},
    )
    return str(values["id"])


def upsert_role_officer(
    conn: sqlite3.Connection,
    *,
    name: str,
    role: str,
    mobile: str,
    hierarchy: dict[str, str],
) -> str:
    phone = normalized_mobile(mobile)
    if not name or not phone:
        return ""

    table = "ddeo_officers" if role == "DDEO" else "aeo_officers"
    if table not in table_names(conn):
        return ""

    values = {
        "id": record_id(role.lower(), phone),
        "name": name,
        "mobile": phone,
        "normalized_mobile": phone,
        "district_ref": hierarchy.get("district_ref", ""),
        "department_ref": hierarchy.get("department_ref", ""),
        "wing_ref": hierarchy.get("wing_ref", ""),
        "tehsil_ref": hierarchy.get("tehsil_ref", ""),
        "markaz_ref": hierarchy.get("markaz_ref", "") if role == "AEO" else "",
        "active": 1,
    }
    upsert_values(
        conn,
        table,
        values,
        conflict_columns=("normalized_mobile",),
        immutable_columns={"id"},
    )
    return str(values["id"])


def upsert_ddeo_jurisdiction(
    conn: sqlite3.Connection,
    *,
    ddeo_id: str,
    hierarchy: dict[str, str],
) -> str:
    if not ddeo_id:
        return ""
    key = f"{ddeo_id}:{hierarchy.get('wing_ref', '')}:{hierarchy.get('tehsil_ref', '')}"
    values = {
        "id": record_id("ddeo_jurisdiction", key),
        "ddeo_ref": ddeo_id,
        "district_ref": hierarchy.get("district_ref", ""),
        "department_ref": hierarchy.get("department_ref", ""),
        "wing_ref": hierarchy.get("wing_ref", ""),
        "tehsil_ref": hierarchy.get("tehsil_ref", ""),
        "active": 1,
        "notes": OFFICERS_IMPORT_NOTE,
    }
    upsert_values(
        conn,
        "ddeo_jurisdictions",
        values,
        conflict_columns=("id",),
        immutable_columns={"id"},
    )
    return str(values["id"])


def upsert_aeo_jurisdiction(
    conn: sqlite3.Connection,
    *,
    aeo_id: str,
    hierarchy: dict[str, str],
) -> str:
    if not aeo_id:
        return ""
    key = f"{aeo_id}:{hierarchy.get('wing_ref', '')}:{hierarchy.get('markaz_ref', '')}"
    values = {
        "id": record_id("aeo_jurisdiction", key),
        "aeo_ref": aeo_id,
        "district_ref": hierarchy.get("district_ref", ""),
        "department_ref": hierarchy.get("department_ref", ""),
        "wing_ref": hierarchy.get("wing_ref", ""),
        "tehsil_ref": hierarchy.get("tehsil_ref", ""),
        "markaz_ref": hierarchy.get("markaz_ref", ""),
        "active": 1,
        "notes": OFFICERS_IMPORT_NOTE,
    }
    upsert_values(
        conn,
        "aeo_jurisdictions",
        values,
        conflict_columns=("id",),
        immutable_columns={"id"},
    )
    return str(values["id"])


def import_csv(csv_path: Path, db_path: Path) -> dict[str, int | str]:
    return import_sources(csv_path, DEFAULT_RECIPIENTS_CSV, db_path)


def import_sources(
    csv_path: Path,
    recipients_path: Path,
    db_path: Path,
) -> dict[str, int | str]:
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)
    if not db_path.is_file():
        raise FileNotFoundError(
            f"{db_path} not found. Run './pocketbase migrate up' from {ROOT} first."
        )

    file_hash = sha256_file(csv_path)
    row_count = 0
    schools: set[str] = set()
    officers: set[tuple[str, str]] = set()
    ddeo_officers: set[str] = set()
    aeo_officers: set[str] = set()
    ddeo_jurisdictions: set[str] = set()
    aeo_jurisdictions: set[str] = set()
    whatsapp_recipients: set[str] = set()

    with sqlite3.connect(db_path) as conn:
        ensure_tables_exist(conn)
        conn.execute("pragma foreign_keys = on")
        for row in read_csv_rows(csv_path):
            row_count += 1
            school_emis = cell(row, "EMIS")
            hierarchy = upsert_hierarchy(conn, row)
            upsert_school(conn, row, hierarchy)
            if school_emis:
                schools.add(school_emis)

            ddeo_id = upsert_role_officer(
                conn,
                name=cell(row, "DDEO Name"),
                role="DDEO",
                mobile=cell(row, "DDEO CELL NUMBER"),
                hierarchy=hierarchy,
            )
            aeo_id = upsert_role_officer(
                conn,
                name=cell(row, "AEO NAME"),
                role="AEO",
                mobile=cell(row, "AEO CELL NUMBER"),
                hierarchy=hierarchy,
            )

            if ddeo_id:
                ddeo_officers.add(ddeo_id)
                ddeo_jurisdiction_id = upsert_ddeo_jurisdiction(
                    conn,
                    ddeo_id=ddeo_id,
                    hierarchy=hierarchy,
                )
                if ddeo_jurisdiction_id:
                    ddeo_jurisdictions.add(ddeo_jurisdiction_id)
            if aeo_id:
                aeo_officers.add(aeo_id)
                aeo_jurisdiction_id = upsert_aeo_jurisdiction(
                    conn,
                    aeo_id=aeo_id,
                    hierarchy=hierarchy,
                )
                if aeo_jurisdiction_id:
                    aeo_jurisdictions.add(aeo_jurisdiction_id)

            ddeo_phone = normalized_mobile(cell(row, "DDEO CELL NUMBER"))
            aeo_phone = normalized_mobile(cell(row, "AEO CELL NUMBER"))
            if ddeo_phone:
                officers.add(("DDEO", ddeo_phone))
            if aeo_phone:
                officers.add(("AEO", aeo_phone))

        if recipients_path.is_file():
            for row in read_csv_rows(recipients_path):
                recipient_id = upsert_whatsapp_recipient(conn, row)
                if recipient_id:
                    whatsapp_recipients.add(recipient_id)

        stale_ddeo_jurisdictions = deactivate_stale_imported_rows(
            conn,
            "ddeo_jurisdictions",
            ddeo_jurisdictions,
        )
        stale_aeo_jurisdictions = deactivate_stale_imported_rows(
            conn,
            "aeo_jurisdictions",
            aeo_jurisdictions,
        )
        stale_ddeo_officers = deactivate_stale_unlinked_officers(
            conn,
            "ddeo_officers",
            current_ids=ddeo_officers,
            jurisdiction_table="ddeo_jurisdictions",
            jurisdiction_ref_column="ddeo_ref",
            override_table="school_ddeo_overrides",
            override_ref_column="ddeo_ref",
        )
        stale_aeo_officers = deactivate_stale_unlinked_officers(
            conn,
            "aeo_officers",
            current_ids=aeo_officers,
            jurisdiction_table="aeo_jurisdictions",
            jurisdiction_ref_column="aeo_ref",
            override_table="school_aeo_overrides",
            override_ref_column="aeo_ref",
        )

        batch = {
            "id": record_id("batch", file_hash),
            "file_name": csv_path.name,
            "sha256": file_hash,
            "row_count": row_count,
            "school_count": len(schools),
            "officer_count": len(officers),
            "assignment_count": len(ddeo_jurisdictions) + len(aeo_jurisdictions),
            "status": "imported",
            "notes": (
                "Imported from officers_list.csv and whatsapp_recipients.csv "
                "by local PocketBase importer. Deactivated stale CSV-imported "
                f"jurisdictions: {stale_ddeo_jurisdictions} DDEO, "
                f"{stale_aeo_jurisdictions} AEO. Deactivated stale unlinked "
                f"officers: {stale_ddeo_officers} DDEO, {stale_aeo_officers} AEO."
            ),
        }
        columns = ", ".join(batch)
        placeholders = ", ".join(":" + key for key in batch)
        conn.execute(
            f"insert or replace into officer_import_batches ({columns}) values ({placeholders})",
            batch,
        )

    return {
        "csv": str(csv_path),
        "database": str(db_path),
        "rows": row_count,
        "schools": len(schools),
        "officers": len(officers),
        "ddeo_officers": len(ddeo_officers),
        "aeo_officers": len(aeo_officers),
        "ddeo_jurisdictions": len(ddeo_jurisdictions),
        "aeo_jurisdictions": len(aeo_jurisdictions),
        "jurisdictions": len(ddeo_jurisdictions) + len(aeo_jurisdictions),
        "whatsapp_recipients": len(whatsapp_recipients),
        "stale_ddeo_jurisdictions_deactivated": stale_ddeo_jurisdictions,
        "stale_aeo_jurisdictions_deactivated": stale_aeo_jurisdictions,
        "stale_ddeo_officers_deactivated": stale_ddeo_officers,
        "stale_aeo_officers_deactivated": stale_aeo_officers,
    }


def main(argv: list[str]) -> int:
    csv_path = Path(argv[1]).expanduser().resolve() if len(argv) > 1 else DEFAULT_CSV
    db_path = Path(argv[2]).expanduser().resolve() if len(argv) > 2 else DEFAULT_DB
    recipients_path = (
        Path(argv[3]).expanduser().resolve()
        if len(argv) > 3
        else DEFAULT_RECIPIENTS_CSV
    )
    try:
        result = import_sources(csv_path, recipients_path, db_path)
    except Exception as exc:
        print(f"Import failed: {exc}", file=sys.stderr)
        return 1

    print("PocketBase AntiDengue import complete")
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

from __future__ import annotations

import argparse
import hashlib
import json
import secrets
import sqlite3
from datetime import datetime
from pathlib import Path

import openpyxl


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKBOOK = ROOT / "raw-data" / "LAHORE ALL WING SCHOOL LIST.xlsx"
DEFAULT_DATABASE = ROOT.parent / "antidengue-pocketbase" / "pb_data" / "data.db"

SHEETS = {
    "DEO WEE": {"wing": "DEO WEE", "code": "DEO WEE", "deos_wise": "W-EE"},
    "DEO SE ": {"wing": "DEO SE", "code": "DEO SE", "deos_wise": "SE"},
}


def record_id() -> str:
    return secrets.token_hex(8)[:15]


def clean(value: object) -> str:
    return str(value or "").strip()


def emis(value: object) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return clean(value)


def backup_database(database: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    target = database.with_name(f"{database.stem}.before-wing-import-{stamp}{database.suffix}")
    with sqlite3.connect(database) as source, sqlite3.connect(target) as destination:
        source.backup(destination)
    return target


def import_workbook(workbook: Path, database: Path, *, commit: bool) -> dict[str, object]:
    wb = openpyxl.load_workbook(workbook, read_only=True, data_only=True)
    connection = sqlite3.connect(database, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("BEGIN IMMEDIATE")

    district = connection.execute(
        "SELECT id FROM districts WHERE active = 1 AND upper(name) = 'LAHORE' LIMIT 1"
    ).fetchone()
    department = connection.execute(
        "SELECT id FROM departments WHERE active = 1 ORDER BY name LIMIT 1"
    ).fetchone()
    if not district or not department:
        raise RuntimeError("Active Lahore district and department records are required")

    tehsils = {
        row["name"].strip().upper(): row["id"]
        for row in connection.execute("SELECT id, name FROM tehsils WHERE active = 1")
    }
    summary: dict[str, object] = {
        "committed": commit,
        "created": 0,
        "updated": 0,
        "wings_created": 0,
        "markazes_created": 0,
        "by_wing": {},
    }

    for sheet_name, config in SHEETS.items():
        existing_wing = connection.execute(
            """
            SELECT id FROM wings
            WHERE name = ? AND district_ref = ? AND department_ref = ?
            """,
            (config["wing"], district["id"], department["id"]),
        ).fetchone()
        if existing_wing:
            wing_id = existing_wing["id"]
            connection.execute("UPDATE wings SET active = 1, code = ? WHERE id = ?", (config["code"], wing_id))
        else:
            wing_id = record_id()
            connection.execute(
                "INSERT INTO wings (id, name, code, district_ref, department_ref, active) VALUES (?, ?, ?, ?, ?, 1)",
                (wing_id, config["wing"], config["code"], district["id"], department["id"]),
            )
            summary["wings_created"] = int(summary["wings_created"]) + 1

        wing_created = wing_updated = 0
        for row_number, row in enumerate(wb[sheet_name].iter_rows(min_row=2, values_only=True), start=2):
            school_emis = emis(row[0])
            school_name = clean(row[4])
            tehsil_name = clean(row[2]).upper()
            if not school_emis or not school_name:
                raise ValueError(f"{sheet_name} row {row_number}: EMIS and school name are required")
            if tehsil_name not in tehsils:
                raise ValueError(f"{sheet_name} row {row_number}: unknown tehsil {tehsil_name!r}")
            tehsil_id = tehsils[tehsil_name]

            markaz_id = ""
            if config["code"] == "DEO WEE":
                markaz_name = clean(row[3])
                existing_markaz = connection.execute(
                    "SELECT id FROM markazes WHERE name = ? AND tehsil_ref = ? AND wing_ref = ?",
                    (markaz_name, tehsil_id, wing_id),
                ).fetchone()
                if existing_markaz:
                    markaz_id = existing_markaz["id"]
                    connection.execute("UPDATE markazes SET active = 1 WHERE id = ?", (markaz_id,))
                else:
                    markaz_id = record_id()
                    connection.execute(
                        "INSERT INTO markazes (id, name, tehsil_ref, wing_ref, active) VALUES (?, ?, ?, ?, 1)",
                        (markaz_id, markaz_name, tehsil_id, wing_id),
                    )
                    summary["markazes_created"] = int(summary["markazes_created"]) + 1

            source_values = [school_emis, school_name, tehsil_name, clean(row[3]), clean(row[6]), clean(row[7]), config["code"]]
            source_hash = hashlib.sha256("|".join(source_values).encode()).hexdigest()
            existing_school = connection.execute(
                "SELECT id FROM schools WHERE emis = ?", (school_emis,)
            ).fetchone()
            if existing_school:
                connection.execute(
                    """
                    UPDATE schools SET name = ?, district_ref = ?, department_ref = ?,
                      wing_ref = ?, tehsil_ref = ?, markaz_ref = ?, school_type = ?,
                      school_level = ?, deos_wise = ?, source = ?, source_row_hash = ?,
                      notes = ?, active = 1
                    WHERE id = ?
                    """,
                    (
                        school_name,
                        district["id"],
                        department["id"],
                        wing_id,
                        tehsil_id,
                        markaz_id,
                        clean(row[6]),
                        clean(row[7]),
                        config["deos_wise"],
                        "lahore_all_wing_school_list_xlsx",
                        source_hash,
                        "Markaz assignment pending" if config["code"] == "DEO SE" else "Imported from Lahore all-wing workbook",
                        existing_school["id"],
                    ),
                )
                wing_updated += 1
                summary["updated"] = int(summary["updated"]) + 1
            else:
                connection.execute(
                    """
                    INSERT INTO schools (
                      id, emis, name, district_ref, department_ref, wing_ref, tehsil_ref,
                      markaz_ref, head_name, head_contact, shift, school_type, school_level,
                      deos_wise, source, source_row_hash, notes, active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', '', ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        record_id(),
                        school_emis,
                        school_name,
                        district["id"],
                        department["id"],
                        wing_id,
                        tehsil_id,
                        markaz_id,
                        clean(row[6]),
                        clean(row[7]),
                        config["deos_wise"],
                        "lahore_all_wing_school_list_xlsx",
                        source_hash,
                        "Markaz assignment pending" if config["code"] == "DEO SE" else "Imported from Lahore all-wing workbook",
                    ),
                )
                wing_created += 1
                summary["created"] = int(summary["created"]) + 1

        summary["by_wing"][config["code"]] = {"created": wing_created, "updated": wing_updated}

    if commit:
        connection.commit()
    else:
        connection.rollback()
    connection.close()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Import Lahore WEE and Secondary school master data")
    parser.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--commit", action="store_true", help="Persist the import; otherwise preview and roll back")
    args = parser.parse_args()

    if not args.workbook.exists():
        raise SystemExit(f"Workbook not found: {args.workbook}")
    if not args.database.exists():
        raise SystemExit(f"Database not found: {args.database}")
    if args.commit:
        backup = backup_database(args.database)
        print(f"Backup: {backup}")
    print(json.dumps(import_workbook(args.workbook, args.database, commit=args.commit), indent=2))


if __name__ == "__main__":
    main()

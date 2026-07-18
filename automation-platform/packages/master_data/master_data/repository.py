from __future__ import annotations

import csv
import io
import re
import secrets
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from master_data.schemas import OfficerWrite, SchoolWrite


def _record_id() -> str:
    return secrets.token_hex(8)[:15]


def normalize_phone(value: object) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if digits.startswith("0092"):
        digits = digits[2:]
    if digits.startswith("03"):
        digits = "92" + digits[1:]
    elif digits.startswith("3") and len(digits) == 10:
        digits = "92" + digits
    return digits


class MasterDataRepository:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        if not self.db_path.exists():
            raise FileNotFoundError(f"PocketBase database not found: {self.db_path}")
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 15000")
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row is not None else None

    def dashboard(self) -> dict[str, Any]:
        with self.connect() as conn:
            scalar = lambda sql, args=(): conn.execute(sql, args).fetchone()[0]
            counts = {
                "schools": scalar("SELECT count(*) FROM schools WHERE active = 1"),
                "inactive_schools": scalar("SELECT count(*) FROM schools WHERE active = 0"),
                "aeos": scalar("SELECT count(*) FROM aeo_officers WHERE active = 1"),
                "ddeos": scalar("SELECT count(*) FROM ddeo_officers WHERE active = 1"),
                "school_heads": scalar(
                    "SELECT count(*) FROM schools WHERE active = 1 AND trim(head_name) <> ''"
                ),
                "missing_heads": scalar(
                    "SELECT count(*) FROM schools WHERE active = 1 AND trim(head_name) = ''"
                ),
                "wings": scalar("SELECT count(*) FROM wings WHERE active = 1"),
                "tehsils": scalar("SELECT count(*) FROM tehsils WHERE active = 1"),
                "markazes": scalar("SELECT count(*) FROM markazes WHERE active = 1"),
                "unmapped_aeo": scalar(
                    """
                    SELECT count(*) FROM schools s
                    WHERE s.active = 1 AND trim(s.markaz_ref) <> '' AND NOT EXISTS (
                      SELECT 1 FROM school_aeo_overrides o
                      WHERE o.school_ref = s.id AND o.active = 1
                    ) AND NOT EXISTS (
                      SELECT 1 FROM aeo_jurisdictions j
                      WHERE j.markaz_ref = s.markaz_ref
                        AND j.wing_ref = s.wing_ref AND j.active = 1
                    )
                    """
                ),
                "pending_markaz": scalar(
                    "SELECT count(*) FROM schools WHERE active = 1 AND trim(markaz_ref) = ''"
                ),
                "unmapped_ddeo": scalar(
                    """
                    SELECT count(*) FROM schools s
                    WHERE s.active = 1 AND NOT EXISTS (
                      SELECT 1 FROM school_ddeo_overrides o
                      WHERE o.school_ref = s.id AND o.active = 1
                    ) AND NOT EXISTS (
                      SELECT 1 FROM ddeo_jurisdictions j
                      WHERE j.tehsil_ref = s.tehsil_ref
                        AND j.wing_ref = s.wing_ref AND j.active = 1
                    )
                    """
                ),
            }
            tehsils = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT t.id, t.name,
                      count(DISTINCT s.id) AS schools,
                      count(DISTINCT CASE WHEN s.head_name <> '' THEN s.id END) AS school_heads,
                      count(DISTINCT a.id) AS aeos,
                      count(DISTINCT d.id) AS ddeos,
                      count(DISTINCT CASE WHEN s.markaz_ref = '' THEN s.id END) AS missing_markaz
                    FROM tehsils t
                    LEFT JOIN schools s ON s.tehsil_ref = t.id AND s.active = 1
                    LEFT JOIN aeo_officers a ON a.tehsil_ref = t.id AND a.active = 1
                    LEFT JOIN ddeo_officers d ON d.tehsil_ref = t.id AND d.active = 1
                    WHERE t.active = 1
                    GROUP BY t.id, t.name
                    ORDER BY t.name
                    """
                )
            ]
            wings = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT w.id, w.name,
                      count(DISTINCT s.id) AS schools,
                      count(DISTINCT a.id) AS aeos,
                      count(DISTINCT d.id) AS ddeos
                    FROM wings w
                    LEFT JOIN schools s ON s.wing_ref = w.id AND s.active = 1
                    LEFT JOIN aeo_officers a ON a.wing_ref = w.id AND a.active = 1
                    LEFT JOIN ddeo_officers d ON d.wing_ref = w.id AND d.active = 1
                    WHERE w.active = 1
                    GROUP BY w.id, w.name
                    ORDER BY w.name
                    """
                )
            ]
        return {"counts": counts, "tehsils": tehsils, "wings": wings}

    def options(self) -> dict[str, list[dict[str, Any]]]:
        with self.connect() as conn:
            result: dict[str, list[dict[str, Any]]] = {}
            for table in ("districts", "departments", "wings", "tehsils", "markazes"):
                result[table] = [
                    dict(row)
                    for row in conn.execute(
                        f"SELECT * FROM {table} WHERE active = 1 ORDER BY name"
                    )
                ]
        return result

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
        where: list[str] = []
        args: list[Any] = []
        if search:
            where.append("(s.emis LIKE ? OR s.name LIKE ? OR s.head_name LIKE ?)")
            needle = f"%{search}%"
            args.extend([needle, needle, needle])
        if tehsil_ref:
            where.append("s.tehsil_ref = ?")
            args.append(tehsil_ref)
        if markaz_ref:
            where.append("s.markaz_ref = ?")
            args.append(markaz_ref)
        if active is not None:
            where.append("s.active = ?")
            args.append(int(active))
        clause = "WHERE " + " AND ".join(where) if where else ""
        offset = (page - 1) * page_size
        with self.connect() as conn:
            total = conn.execute(
                f"SELECT count(*) FROM schools s {clause}", args
            ).fetchone()[0]
            rows = conn.execute(
                f"""
                SELECT s.*, d.name AS district_name, dep.name AS department_name,
                  w.name AS wing_name, t.name AS tehsil_name, m.name AS markaz_name
                FROM schools s
                LEFT JOIN districts d ON d.id = s.district_ref
                LEFT JOIN departments dep ON dep.id = s.department_ref
                LEFT JOIN wings w ON w.id = s.wing_ref
                LEFT JOIN tehsils t ON t.id = s.tehsil_ref
                LEFT JOIN markazes m ON m.id = s.markaz_ref
                {clause}
                ORDER BY s.name
                LIMIT ? OFFSET ?
                """,
                [*args, page_size, offset],
            )
            items = [dict(row) for row in rows]
        return {"items": items, "total": total, "page": page, "page_size": page_size}

    def get_school(self, school_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            return self._dict(conn.execute("SELECT * FROM schools WHERE id = ?", (school_id,)).fetchone())

    def school_id_by_emis(self, emis: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute("SELECT id FROM schools WHERE emis = ?", (emis,)).fetchone()
            return str(row["id"]) if row else None

    @staticmethod
    def _validate_ref(conn: sqlite3.Connection, table: str, record_id: str, label: str) -> None:
        if not record_id or conn.execute(
            f"SELECT 1 FROM {table} WHERE id = ? AND active = 1", (record_id,)
        ).fetchone() is None:
            raise ValueError(f"Select a valid {label}")

    def save_school(self, data: SchoolWrite, school_id: str | None = None) -> tuple[dict | None, dict]:
        payload = data.model_dump()
        payload["emis"] = re.sub(r"\s+", "", payload["emis"])
        payload["head_contact"] = normalize_phone(payload["head_contact"])
        with self.connect() as conn:
            self._validate_ref(conn, "districts", data.district_ref, "district")
            self._validate_ref(conn, "departments", data.department_ref, "department")
            self._validate_ref(conn, "wings", data.wing_ref, "wing")
            self._validate_ref(conn, "tehsils", data.tehsil_ref, "tehsil")
            if data.markaz_ref:
                self._validate_ref(conn, "markazes", data.markaz_ref, "markaz")
            duplicate = conn.execute(
                "SELECT id FROM schools WHERE emis = ? AND id <> ?",
                (payload["emis"], school_id or ""),
            ).fetchone()
            if duplicate:
                raise ValueError(f"EMIS {payload['emis']} already exists")
            before = self._dict(
                conn.execute("SELECT * FROM schools WHERE id = ?", (school_id,)).fetchone()
            ) if school_id else None
            if school_id:
                if before is None:
                    raise LookupError("School not found")
                columns = list(payload)
                conn.execute(
                    f"UPDATE schools SET {', '.join(f'{key} = ?' for key in columns)} WHERE id = ?",
                    [*(payload[key] for key in columns), school_id],
                )
                record_id = school_id
            else:
                record_id = _record_id()
                payload.update({"source": "web_platform", "source_row_hash": ""})
                columns = ["id", *payload]
                conn.execute(
                    f"INSERT INTO schools ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
                    [record_id, *(payload[key] for key in payload)],
                )
            conn.commit()
            after = self._dict(conn.execute("SELECT * FROM schools WHERE id = ?", (record_id,)).fetchone())
        assert after is not None
        return before, after

    def set_school_active(self, school_id: str, active: bool) -> tuple[dict, dict]:
        with self.connect() as conn:
            before = self._dict(conn.execute("SELECT * FROM schools WHERE id = ?", (school_id,)).fetchone())
            if before is None:
                raise LookupError("School not found")
            conn.execute("UPDATE schools SET active = ? WHERE id = ?", (int(active), school_id))
            conn.commit()
            after = self._dict(conn.execute("SELECT * FROM schools WHERE id = ?", (school_id,)).fetchone())
        assert after is not None
        return before, after

    def list_officers(
        self,
        role: str,
        *,
        search: str = "",
        active: bool | None = True,
    ) -> list[dict[str, Any]]:
        table = self._officer_table(role)
        where: list[str] = []
        args: list[Any] = []
        if search:
            where.append("(o.name LIKE ? OR o.mobile LIKE ?)")
            args.extend([f"%{search}%", f"%{search}%"])
        if active is not None:
            where.append("o.active = ?")
            args.append(int(active))
        clause = "WHERE " + " AND ".join(where) if where else ""
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT o.*, t.name AS tehsil_name, m.name AS markaz_name,
                  w.name AS wing_name
                FROM {table} o
                LEFT JOIN tehsils t ON t.id = o.tehsil_ref
                LEFT JOIN markazes m ON m.id = {('o.markaz_ref' if role == 'aeo' else "''")}
                LEFT JOIN wings w ON w.id = o.wing_ref
                {clause}
                ORDER BY o.name
                """,
                args,
            )
            return [{**dict(row), "role": role} for row in rows]

    @staticmethod
    def _officer_table(role: str) -> str:
        if role == "aeo":
            return "aeo_officers"
        if role == "ddeo":
            return "ddeo_officers"
        raise ValueError("Role must be aeo or ddeo")

    def get_officer(self, role: str, officer_id: str) -> dict[str, Any] | None:
        table = self._officer_table(role)
        with self.connect() as conn:
            row = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (officer_id,)).fetchone()
            return {**dict(row), "role": role} if row else None

    def officer_id_by_mobile(self, role: str, mobile: str) -> str | None:
        table = self._officer_table(role)
        normalized = normalize_phone(mobile)
        with self.connect() as conn:
            row = conn.execute(
                f"SELECT id FROM {table} WHERE normalized_mobile = ?", (normalized,)
            ).fetchone()
            return str(row["id"]) if row else None

    def save_officer(self, data: OfficerWrite, officer_id: str | None = None) -> tuple[dict | None, dict]:
        table = self._officer_table(data.role)
        payload = data.model_dump(exclude={"role"})
        payload["normalized_mobile"] = normalize_phone(payload["mobile"])
        payload["mobile"] = payload["normalized_mobile"]
        if not re.fullmatch(r"923\d{9}", payload["normalized_mobile"]):
            raise ValueError("Enter a valid Pakistani mobile number")
        if data.role == "ddeo":
            payload.pop("markaz_ref", None)
        with self.connect() as conn:
            self._validate_ref(conn, "districts", data.district_ref, "district")
            self._validate_ref(conn, "departments", data.department_ref, "department")
            self._validate_ref(conn, "wings", data.wing_ref, "wing")
            self._validate_ref(conn, "tehsils", data.tehsil_ref, "tehsil")
            if data.role == "aeo":
                self._validate_ref(conn, "markazes", data.markaz_ref, "markaz")
            before = self._dict(
                conn.execute(f"SELECT * FROM {table} WHERE id = ?", (officer_id,)).fetchone()
            ) if officer_id else None
            if officer_id:
                if before is None:
                    raise LookupError("Officer not found")
                columns = list(payload)
                conn.execute(
                    f"UPDATE {table} SET {', '.join(f'{key} = ?' for key in columns)} WHERE id = ?",
                    [*(payload[key] for key in columns), officer_id],
                )
                record_id = officer_id
            else:
                record_id = _record_id()
                columns = ["id", *payload]
                conn.execute(
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
                    [record_id, *(payload[key] for key in payload)],
                )
            conn.commit()
            after = self._dict(conn.execute(f"SELECT * FROM {table} WHERE id = ?", (record_id,)).fetchone())
        assert after is not None
        return before, {**after, "role": data.role}

    def set_officer_active(self, role: str, officer_id: str, active: bool) -> tuple[dict, dict]:
        table = self._officer_table(role)
        with self.connect() as conn:
            before = self._dict(conn.execute(f"SELECT * FROM {table} WHERE id = ?", (officer_id,)).fetchone())
            if before is None:
                raise LookupError("Officer not found")
            conn.execute(f"UPDATE {table} SET active = ? WHERE id = ?", (int(active), officer_id))
            conn.commit()
            after = self._dict(conn.execute(f"SELECT * FROM {table} WHERE id = ?", (officer_id,)).fetchone())
        assert after is not None
        return before, after

    def heads(
        self,
        *,
        search: str = "",
        missing_only: bool = False,
        page: int = 1,
        page_size: int = 10,
    ) -> dict[str, Any]:
        where = ["s.active = 1"]
        args: list[Any] = []
        if missing_only:
            where.append("(trim(s.head_name) = '' OR trim(s.head_contact) = '')")
        if search:
            where.append(
                "(s.emis LIKE ? OR s.name LIKE ? OR s.head_name LIKE ? OR s.head_contact LIKE ?)"
            )
            needle = f"%{search}%"
            args.extend([needle, needle, needle, needle])
        clause = " AND ".join(where)
        offset = (page - 1) * page_size
        with self.connect() as conn:
            total = conn.execute(
                f"SELECT count(*) FROM schools s WHERE {clause}", args
            ).fetchone()[0]
            items = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT s.id, s.emis, s.name AS school_name, s.head_name,
                      s.head_contact, t.name AS tehsil_name, m.name AS markaz_name
                    FROM schools s
                    LEFT JOIN tehsils t ON t.id = s.tehsil_ref
                    LEFT JOIN markazes m ON m.id = s.markaz_ref
                    WHERE {clause}
                    ORDER BY s.name
                    LIMIT ? OFFSET ?
                    """,
                    [*args, page_size, offset],
                )
            ]
        return {"items": items, "total": total, "page": page, "page_size": page_size}

    def areas(self) -> dict[str, list[dict[str, Any]]]:
        return self.options()

    def jurisdictions(
        self,
        *,
        active: bool | None = True,
        wing_ref: str = "",
    ) -> dict[str, list[dict[str, Any]]]:
        filters: list[str] = []
        args: list[Any] = []
        if active is not None:
            filters.extend(["j.active = ?", "o.active = ?"])
            args.extend([int(active), int(active)])
        if wing_ref:
            filters.append("j.wing_ref = ?")
            args.append(wing_ref)
        clause = "WHERE " + " AND ".join(filters) if filters else ""
        with self.connect() as conn:
            aeo = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT j.id, j.active, o.name AS officer_name, o.mobile,
                      j.tehsil_ref, t.name AS tehsil_name, j.markaz_ref,
                      m.name AS markaz_name, w.name AS wing_name
                    FROM aeo_jurisdictions j
                    LEFT JOIN aeo_officers o ON o.id = j.aeo_ref
                    LEFT JOIN tehsils t ON t.id = j.tehsil_ref
                    LEFT JOIN markazes m ON m.id = j.markaz_ref
                    LEFT JOIN wings w ON w.id = j.wing_ref
                    {clause}
                    ORDER BY t.name, m.name
                    """,
                    args,
                )
            ]
            ddeo = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT j.id, j.active, o.name AS officer_name, o.mobile,
                      j.tehsil_ref, t.name AS tehsil_name, '' AS markaz_ref,
                      w.name AS wing_name
                    FROM ddeo_jurisdictions j
                    LEFT JOIN ddeo_officers o ON o.id = j.ddeo_ref
                    LEFT JOIN tehsils t ON t.id = j.tehsil_ref
                    LEFT JOIN wings w ON w.id = j.wing_ref
                    {clause}
                    ORDER BY t.name
                    """,
                    args,
                )
            ]
        return {"aeo": aeo, "ddeo": ddeo}

    def quality(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            checks = [
                {
                    "key": "missing_heads",
                    "title": "Schools missing head details",
                    "count": conn.execute(
                        "SELECT count(*) FROM schools WHERE active = 1 AND (trim(head_name) = '' OR trim(head_contact) = '')"
                    ).fetchone()[0],
                    "severity": "warning",
                    "href": "/master-data/heads?missing=true",
                },
                {
                    "key": "pending_secondary_markaz",
                    "title": "Secondary markaz assignments pending",
                    "count": conn.execute(
                        """
                        SELECT count(*) FROM schools s
                        JOIN wings w ON w.id = s.wing_ref
                        WHERE s.active = 1 AND trim(s.markaz_ref) = '' AND w.code = 'DEO SE'
                        """
                    ).fetchone()[0],
                    "severity": "info",
                    "href": "/master-data/schools",
                },
                {
                    "key": "missing_operational_markaz",
                    "title": "Non-secondary schools without markaz",
                    "count": conn.execute(
                        """
                        SELECT count(*) FROM schools s
                        JOIN wings w ON w.id = s.wing_ref
                        WHERE s.active = 1 AND trim(s.markaz_ref) = '' AND w.code <> 'DEO SE'
                        """
                    ).fetchone()[0],
                    "severity": "error",
                    "href": "/master-data/schools",
                },
                {
                    "key": "invalid_head_mobile",
                    "title": "Invalid head mobile numbers",
                    "count": conn.execute(
                        "SELECT count(*) FROM schools WHERE active = 1 AND head_contact <> '' AND head_contact NOT GLOB '923[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'"
                    ).fetchone()[0],
                    "severity": "warning",
                    "href": "/master-data/heads",
                },
            ]
        return checks

    def export_schools_csv(self) -> str:
        data = self.list_schools(active=None, page_size=100000)["items"]
        return self._csv(data)

    def export_officers_csv(self) -> str:
        data = [*self.list_officers("aeo", active=None), *self.list_officers("ddeo", active=None)]
        return self._csv(data)

    @staticmethod
    def _csv(rows: list[dict[str, Any]]) -> str:
        output = io.StringIO()
        if not rows:
            return ""
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue()

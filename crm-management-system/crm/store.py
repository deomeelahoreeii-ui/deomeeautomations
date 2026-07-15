from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CRM_CACHE_DB = "crm-cache.sqlite"


@dataclass(frozen=True)
class ExtractedComplaint:
    source_path: str
    source_sha256: str
    complaint_number: str = ""
    applicant_text: str = ""
    applicant_clean: str = ""
    remarks_text: str = ""
    remarks_clean: str = ""
    raw_text: str = ""
    ocr_text: str = ""
    extraction_method: str = ""
    confidence: float = 0.0
    needs_review: bool = False
    error: str = ""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS local_complaints (
            source_sha256 TEXT PRIMARY KEY,
            source_path TEXT NOT NULL,
            complaint_number TEXT NOT NULL DEFAULT '',
            applicant_text TEXT NOT NULL DEFAULT '',
            applicant_clean TEXT NOT NULL DEFAULT '',
            remarks_text TEXT NOT NULL DEFAULT '',
            remarks_clean TEXT NOT NULL DEFAULT '',
            raw_text TEXT NOT NULL DEFAULT '',
            ocr_text TEXT NOT NULL DEFAULT '',
            extraction_method TEXT NOT NULL DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0,
            needs_review INTEGER NOT NULL DEFAULT 0,
            error TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_local_complaints_number
        ON local_complaints(complaint_number);

        CREATE TABLE IF NOT EXISTS paperless_complaints (
            document_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            complaint_number TEXT NOT NULL DEFAULT '',
            applicant_text TEXT NOT NULL DEFAULT '',
            applicant_clean TEXT NOT NULL DEFAULT '',
            remarks_text TEXT NOT NULL DEFAULT '',
            remarks_clean TEXT NOT NULL DEFAULT '',
            content TEXT NOT NULL DEFAULT '',
            custom_fields_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_paperless_complaints_number
        ON paperless_complaints(complaint_number);

        CREATE TABLE IF NOT EXISTS duplicate_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_sha256 TEXT NOT NULL,
            source_path TEXT NOT NULL,
            paperless_document_id INTEGER,
            decision TEXT NOT NULL,
            score REAL NOT NULL DEFAULT 0,
            reason TEXT NOT NULL DEFAULT '',
            details_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


def save_local_complaint(conn: sqlite3.Connection, item: ExtractedComplaint) -> None:
    data = asdict(item)
    data["needs_review"] = 1 if item.needs_review else 0
    data["updated_at"] = utc_now()
    conn.execute(
        """
        INSERT INTO local_complaints (
            source_sha256, source_path, complaint_number, applicant_text,
            applicant_clean, remarks_text, remarks_clean, raw_text, ocr_text,
            extraction_method, confidence, needs_review, error, updated_at
        )
        VALUES (
            :source_sha256, :source_path, :complaint_number, :applicant_text,
            :applicant_clean, :remarks_text, :remarks_clean, :raw_text, :ocr_text,
            :extraction_method, :confidence, :needs_review, :error, :updated_at
        )
        ON CONFLICT(source_sha256) DO UPDATE SET
            source_path=excluded.source_path,
            complaint_number=excluded.complaint_number,
            applicant_text=excluded.applicant_text,
            applicant_clean=excluded.applicant_clean,
            remarks_text=excluded.remarks_text,
            remarks_clean=excluded.remarks_clean,
            raw_text=excluded.raw_text,
            ocr_text=excluded.ocr_text,
            extraction_method=excluded.extraction_method,
            confidence=excluded.confidence,
            needs_review=excluded.needs_review,
            error=excluded.error,
            updated_at=excluded.updated_at
        """,
        data,
    )
    conn.commit()


def get_local_by_sha256(
    conn: sqlite3.Connection, source_sha256: str
) -> ExtractedComplaint | None:
    row = conn.execute(
        "SELECT * FROM local_complaints WHERE source_sha256 = ?", [source_sha256]
    ).fetchone()
    if not row:
        return None
    values = dict(row)
    values["needs_review"] = bool(values["needs_review"])
    values.pop("updated_at", None)
    return ExtractedComplaint(**values)


def save_paperless_complaint(
    conn: sqlite3.Connection,
    *,
    document_id: int,
    title: str,
    complaint_number: str,
    applicant_text: str,
    applicant_clean: str,
    remarks_text: str,
    remarks_clean: str,
    content: str,
    custom_fields: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO paperless_complaints (
            document_id, title, complaint_number, applicant_text, applicant_clean,
            remarks_text, remarks_clean, content, custom_fields_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(document_id) DO UPDATE SET
            title=excluded.title,
            complaint_number=excluded.complaint_number,
            applicant_text=excluded.applicant_text,
            applicant_clean=excluded.applicant_clean,
            remarks_text=excluded.remarks_text,
            remarks_clean=excluded.remarks_clean,
            content=excluded.content,
            custom_fields_json=excluded.custom_fields_json,
            updated_at=excluded.updated_at
        """,
        [
            document_id,
            title,
            complaint_number,
            applicant_text,
            applicant_clean,
            remarks_text,
            remarks_clean,
            content,
            json.dumps(custom_fields, ensure_ascii=False, sort_keys=True),
            utc_now(),
        ],
    )
    conn.commit()


def paperless_candidates(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM paperless_complaints
        WHERE complaint_number <> '' OR applicant_clean <> '' OR remarks_clean <> ''
        ORDER BY document_id DESC
        """
    ).fetchall()


def record_decision(
    conn: sqlite3.Connection,
    *,
    source_sha256: str,
    source_path: str,
    paperless_document_id: int | None,
    decision: str,
    score: float,
    reason: str,
    details: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO duplicate_decisions (
            source_sha256, source_path, paperless_document_id, decision, score,
            reason, details_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            source_sha256,
            source_path,
            paperless_document_id,
            decision,
            score,
            reason,
            json.dumps(details, ensure_ascii=False, sort_keys=True),
            utc_now(),
        ],
    )
    conn.commit()

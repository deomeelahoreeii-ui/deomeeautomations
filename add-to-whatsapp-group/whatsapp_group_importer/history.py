from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .contacts import Contact


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class AttemptRecord:
    phone: str
    status: str
    attempted_at: str
    error_code: str
    error_message: str


class AttemptStore:
    """At-most-once contact attempt ledger scoped by source file and group."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path.resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.database_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self._migrate()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "AttemptStore":
        return self

    def __exit__(self, *_args) -> None:
        self.close()

    def _migrate(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS contact_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_path TEXT NOT NULL,
                source_name TEXT NOT NULL,
                source_sha256 TEXT NOT NULL,
                group_jid TEXT NOT NULL,
                phone TEXT NOT NULL,
                excel_row INTEGER NOT NULL,
                status TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                attempted_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error_code TEXT NOT NULL DEFAULT '',
                error_message TEXT NOT NULL DEFAULT '',
                UNIQUE(source_path, group_jid, phone)
            );
            CREATE INDEX IF NOT EXISTS idx_contact_attempts_lookup
                ON contact_attempts(source_path, group_jid, phone);
            CREATE INDEX IF NOT EXISTS idx_contact_attempts_batch
                ON contact_attempts(batch_id);
            """
        )
        self.connection.commit()

    @staticmethod
    def canonical_source_path(source_file: Path) -> str:
        return str(source_file.expanduser().resolve())

    def attempted_contacts(
        self,
        source_file: Path,
        group_jid: str,
        contacts: list[Contact],
    ) -> dict[str, AttemptRecord]:
        if not contacts:
            return {}
        source_path = self.canonical_source_path(source_file)
        phones = [contact.phone for contact in contacts]
        placeholders = ",".join("?" for _ in phones)
        rows = self.connection.execute(
            f"""
            SELECT phone, status, attempted_at, error_code, error_message
            FROM contact_attempts
            WHERE source_path = ? AND group_jid = ? AND phone IN ({placeholders})
            """,
            (source_path, group_jid, *phones),
        ).fetchall()
        return {
            str(row["phone"]): AttemptRecord(
                phone=str(row["phone"]),
                status=str(row["status"]),
                attempted_at=str(row["attempted_at"]),
                error_code=str(row["error_code"]),
                error_message=str(row["error_message"]),
            )
            for row in rows
        }

    def reserve_attempt(
        self,
        *,
        source_file: Path,
        source_digest: str,
        group_jid: str,
        contact: Contact,
        batch_id: str,
        job_id: str,
    ) -> bool:
        now = utc_now()
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO contact_attempts (
                source_path, source_name, source_sha256, group_jid, phone,
                excel_row, status, batch_id, job_id, attempted_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'reserved', ?, ?, ?, ?)
            """,
            (
                self.canonical_source_path(source_file),
                source_file.name,
                source_digest,
                group_jid,
                contact.phone,
                contact.source_row,
                batch_id,
                job_id,
                now,
                now,
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def update_attempt(
        self,
        *,
        source_file: Path,
        group_jid: str,
        phone: str,
        status: str,
        error_code: str = "",
        error_message: str = "",
    ) -> None:
        self.connection.execute(
            """
            UPDATE contact_attempts
            SET status = ?, error_code = ?, error_message = ?, updated_at = ?
            WHERE source_path = ? AND group_jid = ? AND phone = ?
            """,
            (
                status,
                error_code,
                error_message,
                utc_now(),
                self.canonical_source_path(source_file),
                group_jid,
                phone,
            ),
        )
        self.connection.commit()

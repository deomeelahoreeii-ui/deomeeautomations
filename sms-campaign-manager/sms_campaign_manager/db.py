from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS campaigns (
 id INTEGER PRIMARY KEY, campaign_key TEXT NOT NULL UNIQUE, source_file TEXT NOT NULL,
 campaign_name TEXT,
 sheet TEXT NOT NULL, number_column TEXT NOT NULL, message_hash TEXT NOT NULL,
 message TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft',
 description TEXT, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS recipients (
 id INTEGER PRIMARY KEY, campaign_id INTEGER NOT NULL REFERENCES campaigns(id),
 phone TEXT NOT NULL, source_row INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
 gateway_message_id TEXT, error TEXT, reserved_at TEXT, submitted_at TEXT, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(campaign_id, phone)
);
CREATE TABLE IF NOT EXISTS submissions (
 id INTEGER PRIMARY KEY, recipient_id INTEGER NOT NULL REFERENCES recipients(id),
 phone TEXT NOT NULL, status TEXT NOT NULL, gateway_message_id TEXT, units INTEGER NOT NULL DEFAULT 1,
 message_hash TEXT,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS submissions_created ON submissions(created_at);
"""


class Ledger:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.executescript(SCHEMA)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(submissions)")}
            if "units" not in columns:
                connection.execute("ALTER TABLE submissions ADD COLUMN units INTEGER NOT NULL DEFAULT 1")
            if "message_hash" not in columns:
                connection.execute("ALTER TABLE submissions ADD COLUMN message_hash TEXT")
            campaign_columns = {row[1] for row in connection.execute("PRAGMA table_info(campaigns)")}
            if "campaign_name" not in campaign_columns:
                connection.execute("ALTER TABLE campaigns ADD COLUMN campaign_name TEXT")
            if "status" not in campaign_columns:
                connection.execute("ALTER TABLE campaigns ADD COLUMN status TEXT NOT NULL DEFAULT 'draft'")
            if "description" not in campaign_columns:
                connection.execute("ALTER TABLE campaigns ADD COLUMN description TEXT")
            if "updated_at" not in campaign_columns:
                connection.execute("ALTER TABLE campaigns ADD COLUMN updated_at TEXT")
                connection.execute("UPDATE campaigns SET updated_at=COALESCE(created_at,CURRENT_TIMESTAMP) WHERE updated_at IS NULL")
            connection.execute(
                """UPDATE campaigns
                   SET status='needs_attention'
                   WHERE status='draft'
                     AND EXISTS (SELECT 1 FROM recipients r WHERE r.campaign_id=campaigns.id AND r.status IN ('failed','unknown'))"""
            )
            connection.execute(
                """UPDATE campaigns
                   SET status='verified'
                   WHERE status='draft'
                     AND EXISTS (SELECT 1 FROM recipients r WHERE r.campaign_id=campaigns.id)
                     AND NOT EXISTS (SELECT 1 FROM recipients r WHERE r.campaign_id=campaigns.id AND r.status IN ('pending','submitting','queued','processed','unknown','failed'))"""
            )
            connection.execute(
                """UPDATE campaigns
                   SET status='submitted'
                   WHERE status='draft'
                     AND EXISTS (SELECT 1 FROM recipients r WHERE r.campaign_id=campaigns.id AND r.status IN ('queued','processed','sent','delivered','submitting'))
                     AND EXISTS (SELECT 1 FROM recipients r WHERE r.campaign_id=campaigns.id AND r.status='pending')"""
            )

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def prepare(self, key: str, campaign_name: str, source: str, sheet: str, column: str, message_hash: str, message: str, contacts) -> tuple[int, int]:
        with self.connect() as db:
            db.execute("INSERT OR IGNORE INTO campaigns(campaign_key,campaign_name,source_file,sheet,number_column,message_hash,message) VALUES(?,?,?,?,?,?,?)", (key, campaign_name, source, sheet, column, message_hash, message))
            campaign_id = int(db.execute("SELECT id FROM campaigns WHERE campaign_key=?", (key,)).fetchone()[0])
            db.execute("UPDATE campaigns SET campaign_name=?,source_file=?,sheet=?,number_column=?,message_hash=?,message=?,status=CASE WHEN status='deleted' THEN 'draft' ELSE status END,updated_at=CURRENT_TIMESTAMP WHERE id=?", (campaign_name, source, sheet, column, message_hash, message, campaign_id))
            db.executemany("INSERT OR IGNORE INTO recipients(campaign_id,phone,source_row) VALUES(?,?,?)", ((campaign_id, item.phone, item.row) for item in contacts))
            recovered = 0
            for item in contacts:
                current = db.execute("SELECT status FROM recipients WHERE campaign_id=? AND phone=?", (campaign_id, item.phone)).fetchone()
                if current is None or current["status"] != "pending":
                    continue
                previous = db.execute(
                    """
                    SELECT r.status,r.gateway_message_id,r.error,r.reserved_at,r.submitted_at
                    FROM recipients r JOIN campaigns c ON c.id=r.campaign_id
                    WHERE r.phone=? AND r.campaign_id<>? AND r.status<>'pending'
                      AND (c.source_file=? OR c.campaign_name=?)
                    ORDER BY COALESCE(r.submitted_at,r.reserved_at,r.updated_at) DESC,r.id DESC LIMIT 1
                    """,
                    (item.phone, campaign_id, source, campaign_name),
                ).fetchone()
                if previous is None:
                    continue
                db.execute(
                    """UPDATE recipients SET status=?,gateway_message_id=?,error=?,reserved_at=?,submitted_at=?,updated_at=CURRENT_TIMESTAMP
                       WHERE campaign_id=? AND phone=? AND status='pending'""",
                    (previous["status"], previous["gateway_message_id"], previous["error"], previous["reserved_at"], previous["submitted_at"], campaign_id, item.phone),
                )
                recovered += 1
            return campaign_id, recovered

    def create_or_update_campaign(self, key: str, campaign_name: str, source: str, sheet: str, column: str, message_hash: str, message: str, description: str = "") -> int:
        with self.connect() as db:
            db.execute(
                """INSERT INTO campaigns(campaign_key,campaign_name,source_file,sheet,number_column,message_hash,message,description,status)
                   VALUES(?,?,?,?,?,?,?,?, 'draft')
                   ON CONFLICT(campaign_key) DO UPDATE SET
                     campaign_name=excluded.campaign_name,
                     source_file=excluded.source_file,
                     sheet=excluded.sheet,
                     number_column=excluded.number_column,
                     message_hash=excluded.message_hash,
                     message=excluded.message,
                     description=excluded.description,
                     status=CASE WHEN campaigns.status='deleted' THEN 'draft' ELSE campaigns.status END,
                     updated_at=CURRENT_TIMESTAMP""",
                (key, campaign_name, source, sheet, column, message_hash, message, description or None),
            )
            return int(db.execute("SELECT id FROM campaigns WHERE campaign_key=?", (key,)).fetchone()[0])

    def list_campaigns(self, include_deleted: bool = False):
        with self.connect() as db:
            where = "" if include_deleted else "WHERE c.status<>'deleted'"
            return db.execute(
                f"""SELECT c.*,
                          COUNT(r.id) AS recipients,
                          SUM(CASE WHEN r.status='pending' THEN 1 ELSE 0 END) AS pending,
                          SUM(CASE WHEN r.status='queued' THEN 1 ELSE 0 END) AS queued,
                          SUM(CASE WHEN r.status='processed' THEN 1 ELSE 0 END) AS processed,
                          SUM(CASE WHEN r.status='sent' THEN 1 ELSE 0 END) AS sent,
                          SUM(CASE WHEN r.status='delivered' THEN 1 ELSE 0 END) AS delivered,
                          SUM(CASE WHEN r.status='failed' THEN 1 ELSE 0 END) AS failed,
                          SUM(CASE WHEN r.status='unknown' THEN 1 ELSE 0 END) AS unknown,
                          SUM(CASE WHEN r.status='submitting' THEN 1 ELSE 0 END) AS submitting
                   FROM campaigns c LEFT JOIN recipients r ON r.campaign_id=c.id
                   {where}
                   GROUP BY c.id
                   ORDER BY COALESCE(c.updated_at,c.created_at) DESC,c.id DESC"""
            ).fetchall()

    def campaign_by_name(self, campaign_name: str, include_deleted: bool = False):
        with self.connect() as db:
            deleted_filter = "" if include_deleted else "AND status<>'deleted'"
            return db.execute(f"SELECT * FROM campaigns WHERE campaign_name=? {deleted_filter} ORDER BY id DESC LIMIT 1", (campaign_name,)).fetchone()

    def campaign_details(self, campaign_name: str, include_deleted: bool = False) -> tuple[sqlite3.Row | None, dict[str, int]]:
        with self.connect() as db:
            deleted_filter = "" if include_deleted else "AND status<>'deleted'"
            campaign = db.execute(f"SELECT * FROM campaigns WHERE campaign_name=? {deleted_filter} ORDER BY id DESC LIMIT 1", (campaign_name,)).fetchone()
            if campaign is None:
                return None, {}
            rows = db.execute("SELECT status,COUNT(*) AS total FROM recipients WHERE campaign_id=? GROUP BY status", (campaign["id"],)).fetchall()
            return campaign, {str(row["status"]): int(row["total"]) for row in rows}

    def campaign_failures(self, campaign_name: str):
        with self.connect() as db:
            return db.execute(
                """SELECT r.source_row,r.phone,r.status,r.gateway_message_id,r.error,r.updated_at,
                          (SELECT COUNT(*) FROM submissions s WHERE s.recipient_id=r.id) AS attempts
                   FROM recipients r JOIN campaigns c ON c.id=r.campaign_id
                   WHERE c.campaign_name=? AND r.status='failed'
                   ORDER BY r.source_row""",
                (campaign_name,),
            ).fetchall()

    def archive_campaign(self, campaign_name: str) -> bool:
        with self.connect() as db:
            changed = db.execute(
                "UPDATE campaigns SET status='deleted',updated_at=CURRENT_TIMESTAMP WHERE campaign_name=? AND status<>'deleted'",
                (campaign_name,),
            ).rowcount
            return bool(changed)

    def hard_delete_campaign(self, campaign_name: str) -> bool:
        with self.connect() as db:
            campaigns = db.execute("SELECT id FROM campaigns WHERE campaign_name=?", (campaign_name,)).fetchall()
            if not campaigns:
                return False
            campaign_ids = [int(row["id"]) for row in campaigns]
            placeholders = ",".join("?" for _ in campaign_ids)
            recipient_ids = [
                int(row["id"])
                for row in db.execute(f"SELECT id FROM recipients WHERE campaign_id IN ({placeholders})", campaign_ids).fetchall()
            ]
            if recipient_ids:
                recipient_placeholders = ",".join("?" for _ in recipient_ids)
                db.execute(f"DELETE FROM submissions WHERE recipient_id IN ({recipient_placeholders})", recipient_ids)
            db.execute(f"DELETE FROM recipients WHERE campaign_id IN ({placeholders})", campaign_ids)
            db.execute(f"DELETE FROM campaigns WHERE id IN ({placeholders})", campaign_ids)
            return True

    def pending(self, campaign_id: int, retry_failed: bool = False):
        with self.connect() as db:
            status = "failed" if retry_failed else "pending"
            return db.execute(
                """SELECT r.*,
                     (SELECT COUNT(*) FROM submissions s
                      JOIN recipients prior_r ON prior_r.id=s.recipient_id
                      JOIN campaigns prior_c ON prior_c.id=prior_r.campaign_id
                      JOIN campaigns current_c ON current_c.id=r.campaign_id
                      WHERE prior_r.phone=r.phone
                        AND (prior_c.campaign_name=current_c.campaign_name OR prior_c.source_file=current_c.source_file)
                     ) AS attempt_count
                   FROM recipients r WHERE r.campaign_id=? AND r.status=? ORDER BY r.source_row""",
                (campaign_id, status),
            ).fetchall()

    def counts_since(self, periods: tuple[str, str, str]) -> tuple[int, int, int]:
        with self.connect() as db:
            return tuple(int(db.execute("SELECT COALESCE(SUM(units),0) FROM submissions WHERE created_at >= ?", (period,)).fetchone()[0]) for period in periods)  # type: ignore[return-value]

    def next_attempt_number(self, recipient_id: int) -> int:
        with self.connect() as db:
            count = int(db.execute("SELECT COUNT(*) FROM submissions WHERE recipient_id=?", (recipient_id,)).fetchone()[0])
            return count + 1

    def reserve(self, recipient_id: int, phone: str, units: int = 1, message_hash: str = "", expected_status: str = "pending", gateway_message_id: str = "") -> bool:
        with self.connect() as db:
            changed = db.execute(
                "UPDATE recipients SET status='submitting',gateway_message_id=?,reserved_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE id=? AND status=?",
                (gateway_message_id or None, recipient_id, expected_status),
            ).rowcount
            if changed:
                db.execute(
                    "INSERT INTO submissions(recipient_id,phone,status,gateway_message_id,units,message_hash) VALUES(?,?,'reserved',?,?,?)",
                    (recipient_id, phone, gateway_message_id or None, units, message_hash or None),
                )
            return bool(changed)

    def complete(self, recipient_id: int, status: str, message_id: str = "", error: str = "") -> None:
        with self.connect() as db:
            db.execute("UPDATE recipients SET status=?,gateway_message_id=?,error=?,submitted_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE id=?", (status, message_id or None, error or None, recipient_id))
            db.execute(
                "UPDATE submissions SET status=?,gateway_message_id=COALESCE(?,gateway_message_id) WHERE id=(SELECT MAX(id) FROM submissions WHERE recipient_id=?)",
                (status, message_id or None, recipient_id),
            )

    def awaiting_status(self, campaign_name: str = "", campaign_id: int | None = None):
        with self.connect() as db:
            filters = ["r.status IN ('submitting','queued','processed','sent','failed','unknown')", "r.gateway_message_id IS NOT NULL"]
            params: list[object] = []
            if campaign_name:
                filters.append("c.campaign_name=?")
                params.append(campaign_name)
            if campaign_id is not None:
                filters.append("r.campaign_id=?")
                params.append(campaign_id)
            return db.execute(
                f"""SELECT MIN(r.phone) AS phone,r.gateway_message_id
                    FROM recipients r JOIN campaigns c ON c.id=r.campaign_id
                    WHERE {' AND '.join(filters)}
                    GROUP BY r.gateway_message_id ORDER BY MIN(r.updated_at)""",
                params,
            ).fetchall()

    def update_gateway_status(self, message_id: str, status: str, detail: str = "") -> None:
        with self.connect() as db:
            db.execute("UPDATE recipients SET status=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE gateway_message_id=?", (status, detail or None, message_id))
            db.execute("UPDATE submissions SET status=? WHERE gateway_message_id=?", (status, message_id))

    def mark_gateway_missing(self, message_id: str, detail: str = "") -> None:
        with self.connect() as db:
            db.execute(
                """UPDATE recipients
                   SET status=CASE WHEN status='submitting' THEN 'pending' ELSE 'unknown' END,
                       error=?,gateway_message_id=CASE WHEN status='submitting' THEN NULL ELSE gateway_message_id END,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE gateway_message_id=?""",
                (detail or "Gateway has no record for this deterministic SMS id", message_id),
            )
            db.execute("UPDATE submissions SET status='missing_gateway' WHERE gateway_message_id=?", (message_id,))

    def campaign_summary(self, campaign_id: int) -> dict[str, int]:
        with self.connect() as db:
            rows = db.execute("SELECT status,COUNT(*) AS total FROM recipients WHERE campaign_id=? GROUP BY status", (campaign_id,)).fetchall()
            return {str(row["status"]): int(row["total"]) for row in rows}

    def update_campaign_status(self, campaign_id: int, status: str) -> None:
        with self.connect() as db:
            db.execute("UPDATE campaigns SET status=?,updated_at=CURRENT_TIMESTAMP WHERE id=?", (status, campaign_id))

    def refresh_campaign_status(self, campaign_id: int) -> str:
        summary = self.campaign_summary(campaign_id)
        if not summary:
            status = "draft"
        elif summary.get("failed", 0) or summary.get("unknown", 0):
            status = "needs_attention"
        elif any(summary.get(item, 0) for item in ("pending", "submitting", "queued", "processed")):
            status = "submitted"
        else:
            status = "verified"
        self.update_campaign_status(campaign_id, status)
        return status

    def has_open_recipients(self, campaign_id: int) -> bool:
        with self.connect() as db:
            total = int(
                db.execute(
                    "SELECT COUNT(*) FROM recipients WHERE campaign_id=? AND status IN ('pending','submitting','queued','processed','unknown')",
                    (campaign_id,),
                ).fetchone()[0]
            )
            return total > 0

    def status_summary(self) -> dict[str, int]:
        with self.connect() as db:
            rows = db.execute("SELECT status,COUNT(*) AS total FROM recipients GROUP BY status").fetchall()
            return {str(row["status"]): int(row["total"]) for row in rows}

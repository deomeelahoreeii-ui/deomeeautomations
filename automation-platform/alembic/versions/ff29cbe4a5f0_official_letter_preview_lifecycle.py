"""Enforce one active official-letter preview and one current finalized letter.

Revision ID: ff29cbe4a5f0
Revises: fd07e9a1c408
Create Date: 2026-07-21
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "ff29cbe4a5f0"
down_revision: Union[str, None] = "fd07e9a1c408"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _sort_time(value: Any) -> datetime:
    return value if isinstance(value, datetime) else datetime.min


def _repair_existing_rows() -> None:
    connection = op.get_bind()
    rows = list(
        connection.execute(
            sa.text(
                """
                SELECT id, complaint_case_id, status, revision, created_at,
                       generated_at, finalized_at, supersedes_letter_id
                FROM crm_official_letters
                WHERE status IN ('preview', 'finalized')
                """
            )
        ).mappings()
    )
    grouped: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["complaint_case_id"]].append(dict(row))

    for case_rows in grouped.values():
        finalized = sorted(
            (row for row in case_rows if row["status"] == "finalized"),
            key=lambda row: (
                _sort_time(row.get("finalized_at")),
                _sort_time(row.get("created_at")),
                int(row.get("revision") or 0),
            ),
        )
        current_finalized = finalized[-1] if finalized else None
        if len(finalized) > 1:
            older = finalized[:-1]
            for row in older:
                connection.execute(
                    sa.text(
                        """
                        UPDATE crm_official_letters
                        SET status = 'superseded'
                        WHERE id = :id
                        """
                    ),
                    {"id": row["id"]},
                )
            predecessor = older[-1]
            if current_finalized and current_finalized.get("supersedes_letter_id") is None:
                connection.execute(
                    sa.text(
                        """
                        UPDATE crm_official_letters
                        SET supersedes_letter_id = :predecessor_id
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": current_finalized["id"],
                        "predecessor_id": predecessor["id"],
                    },
                )

        previews = sorted(
            (row for row in case_rows if row["status"] == "preview"),
            key=lambda row: (
                _sort_time(row.get("generated_at")),
                _sort_time(row.get("created_at")),
                int(row.get("revision") or 0),
            ),
            reverse=True,
        )
        keep_preview_id = None
        if current_finalized is None and previews:
            keep_preview_id = previews[0]["id"]
        elif current_finalized is not None:
            for row in previews:
                if row.get("supersedes_letter_id") == current_finalized["id"]:
                    keep_preview_id = row["id"]
                    break

        for row in previews:
            if row["id"] == keep_preview_id:
                continue
            connection.execute(
                sa.text(
                    """
                    UPDATE crm_official_letters
                    SET status = 'discarded',
                        error = :reason
                    WHERE id = :id
                    """
                ),
                {
                    "id": row["id"],
                    "reason": (
                        "Discarded during B3.6.1 lifecycle repair because another "
                        "preview/finalized record is authoritative for this complaint."
                    ),
                },
            )


def upgrade() -> None:
    with op.batch_alter_table("crm_official_letters") as batch:
        batch.drop_constraint("ck_crm_official_letters_status", type_="check")
        batch.create_check_constraint(
            "ck_crm_official_letters_status",
            "status IN ('preview', 'finalized', 'superseded', 'failed', 'discarded')",
        )

    _repair_existing_rows()

    op.create_index(
        "uq_crm_official_letters_active_preview",
        "crm_official_letters",
        ["complaint_case_id"],
        unique=True,
        postgresql_where=sa.text("status = 'preview'"),
        sqlite_where=sa.text("status = 'preview'"),
    )
    op.create_index(
        "uq_crm_official_letters_current_finalized",
        "crm_official_letters",
        ["complaint_case_id"],
        unique=True,
        postgresql_where=sa.text("status = 'finalized'"),
        sqlite_where=sa.text("status = 'finalized'"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_crm_official_letters_current_finalized",
        table_name="crm_official_letters",
    )
    op.drop_index(
        "uq_crm_official_letters_active_preview",
        table_name="crm_official_letters",
    )
    op.execute(
        sa.text(
            """
            UPDATE crm_official_letters
            SET status = 'failed',
                error = COALESCE(error, 'Discarded preview restored as failed during downgrade')
            WHERE status = 'discarded'
            """
        )
    )
    with op.batch_alter_table("crm_official_letters") as batch:
        batch.drop_constraint("ck_crm_official_letters_status", type_="check")
        batch.create_check_constraint(
            "ck_crm_official_letters_status",
            "status IN ('preview', 'finalized', 'superseded', 'failed')",
        )

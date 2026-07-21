"""Add batch tracking for CRM exports, reply imports and formal letters.

Revision ID: f1a5c7e9b306
Revises: e9b3c5d7f104
Create Date: 2026-07-21
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f1a5c7e9b306"
down_revision: Union[str, None] = "e9b3c5d7f104"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_HISTORICAL_IMPORT_ID = uuid.UUID("7876bdc4-7461-5a3e-a2c8-683d729cc71d")
_HISTORICAL_LETTER_ID = uuid.UUID("4c97a4ca-4143-529d-8f01-5c0ad2f08062")


def _index(table: str, *names: str) -> None:
    for name in names:
        op.create_index(f"ix_{table}_{name}", table, [name])


def upgrade() -> None:
    op.create_table(
        "crm_bulk_operation_batches",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_number", sa.String(length=48), nullable=False),
        sa.Column("operation_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="draft"),
        sa.Column("parent_batch_id", sa.Uuid(), nullable=True),
        sa.Column("source_filename", sa.String(length=255), nullable=True),
        sa.Column("source_sha256", sa.String(length=64), nullable=True),
        sa.Column("scope_json", sa.JSON(), nullable=False),
        sa.Column("settings_json", sa.JSON(), nullable=False),
        sa.Column("total_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("valid_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("successful_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failed_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skipped_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicate_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_by", sa.String(length=120), nullable=False, server_default="web-operator"),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "operation_type IN ('reply_export', 'reply_import', 'formal_letters')",
            name="ck_crm_bulk_operation_batches_type",
        ),
        sa.CheckConstraint(
            "status IN ('draft', 'validating', 'ready', 'processing', 'completed', "
            "'completed_with_errors', 'failed', 'cancelled')",
            name="ck_crm_bulk_operation_batches_status",
        ),
        sa.ForeignKeyConstraint(
            ["parent_batch_id"],
            ["crm_bulk_operation_batches.id"],
            name="fk_crm_bulk_operation_batches_parent",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("batch_number", name="uq_crm_bulk_operation_batch_number"),
    )
    _index(
        "crm_bulk_operation_batches",
        "batch_number",
        "operation_type",
        "status",
        "parent_batch_id",
        "source_filename",
        "source_sha256",
        "created_by",
        "created_at",
        "updated_at",
        "started_at",
        "completed_at",
    )

    op.create_table(
        "crm_bulk_operation_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=True),
        sa.Column("complaint_number_snapshot", sa.String(length=80), nullable=False, server_default=""),
        sa.Column("source_row", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("error_code", sa.String(length=80), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("reply_version_before", sa.Integer(), nullable=True),
        sa.Column("reply_version_after", sa.Integer(), nullable=True),
        sa.Column("reply_content_hash", sa.String(length=64), nullable=True),
        sa.Column("output_filename", sa.String(length=500), nullable=True),
        sa.Column("output_sha256", sa.String(length=64), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("processed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'exported', 'valid', 'invalid', 'imported', 'updated', "
            "'unchanged', 'generated', 'failed', 'skipped', 'missing')",
            name="ck_crm_bulk_operation_items_status",
        ),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            ["crm_bulk_operation_batches.id"],
            name="fk_crm_bulk_operation_items_batch",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["complaint_case_id"],
            ["crm_complaint_cases.id"],
            name="fk_crm_bulk_operation_items_case",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    _index(
        "crm_bulk_operation_items",
        "batch_id",
        "complaint_case_id",
        "complaint_number_snapshot",
        "source_row",
        "status",
        "error_code",
        "reply_content_hash",
        "output_sha256",
        "created_at",
        "processed_at",
    )

    op.create_table(
        "crm_bulk_operation_artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=80), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("content_type", sa.String(length=160), nullable=False, server_default="application/octet-stream"),
        sa.Column("size_bytes", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sha256", sa.String(length=64), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            ["crm_bulk_operation_batches.id"],
            name="fk_crm_bulk_operation_artifacts_batch",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "batch_id", "kind", "name", name="uq_crm_bulk_operation_artifact"
        ),
    )
    _index(
        "crm_bulk_operation_artifacts",
        "batch_id",
        "kind",
        "sha256",
        "created_at",
    )

    connection = op.get_bind()
    now = datetime.utcnow()
    batch_table = sa.table(
        "crm_bulk_operation_batches",
        sa.column("id", sa.Uuid()),
        sa.column("batch_number", sa.String()),
        sa.column("operation_type", sa.String()),
        sa.column("status", sa.String()),
        sa.column("parent_batch_id", sa.Uuid()),
        sa.column("source_filename", sa.String()),
        sa.column("source_sha256", sa.String()),
        sa.column("scope_json", sa.JSON()),
        sa.column("settings_json", sa.JSON()),
        sa.column("total_items", sa.Integer()),
        sa.column("valid_items", sa.Integer()),
        sa.column("successful_items", sa.Integer()),
        sa.column("failed_items", sa.Integer()),
        sa.column("skipped_items", sa.Integer()),
        sa.column("duplicate_items", sa.Integer()),
        sa.column("created_by", sa.String()),
        sa.column("error_summary", sa.Text()),
        sa.column("created_at", sa.DateTime()),
        sa.column("updated_at", sa.DateTime()),
        sa.column("started_at", sa.DateTime()),
        sa.column("completed_at", sa.DateTime()),
    )
    item_table = sa.table(
        "crm_bulk_operation_items",
        sa.column("id", sa.Uuid()),
        sa.column("batch_id", sa.Uuid()),
        sa.column("complaint_case_id", sa.Uuid()),
        sa.column("complaint_number_snapshot", sa.String()),
        sa.column("source_row", sa.Integer()),
        sa.column("status", sa.String()),
        sa.column("error_code", sa.String()),
        sa.column("error_message", sa.Text()),
        sa.column("reply_version_before", sa.Integer()),
        sa.column("reply_version_after", sa.Integer()),
        sa.column("reply_content_hash", sa.String()),
        sa.column("output_filename", sa.String()),
        sa.column("output_sha256", sa.String()),
        sa.column("details_json", sa.JSON()),
        sa.column("created_at", sa.DateTime()),
        sa.column("processed_at", sa.DateTime()),
    )
    replies = list(
        connection.execute(
            sa.text(
                "SELECT r.id, r.complaint_case_id, r.source_filename, r.source_row, "
                "r.version, r.imported_at, r.generated_at, c.complaint_number "
                "FROM crm_complaint_replies r "
                "JOIN crm_complaint_cases c ON c.id = r.complaint_case_id "
                "ORDER BY r.imported_at, r.id"
            )
        ).mappings()
    )
    if replies:
        earliest = min((row["imported_at"] or now) for row in replies)
        connection.execute(
            batch_table.insert(),
            {
                "id": _HISTORICAL_IMPORT_ID,
                "batch_number": "CRM-IMP-HISTORICAL",
                "operation_type": "reply_import",
                "status": "completed",
                "parent_batch_id": None,
                "source_filename": None,
                "source_sha256": None,
                "scope_json": {"source": "pre_batch_tracking"},
                "settings_json": {"backfilled": True},
                "total_items": len(replies),
                "valid_items": len(replies),
                "successful_items": len(replies),
                "failed_items": 0,
                "skipped_items": 0,
                "duplicate_items": 0,
                "created_by": "migration",
                "error_summary": None,
                "created_at": earliest,
                "updated_at": now,
                "started_at": earliest,
                "completed_at": now,
            },
        )
        connection.execute(
            item_table.insert(),
            [
                {
                    "id": uuid.uuid4(),
                    "batch_id": _HISTORICAL_IMPORT_ID,
                    "complaint_case_id": row["complaint_case_id"],
                    "complaint_number_snapshot": row["complaint_number"] or "",
                    "source_row": row["source_row"],
                    "status": "imported",
                    "error_code": None,
                    "error_message": None,
                    "reply_version_before": None,
                    "reply_version_after": row["version"],
                    "reply_content_hash": None,
                    "output_filename": None,
                    "output_sha256": None,
                    "details_json": {
                        "source_filename": row["source_filename"],
                        "backfilled": True,
                    },
                    "created_at": row["imported_at"] or earliest,
                    "processed_at": row["imported_at"] or earliest,
                }
                for row in replies
            ],
        )

        generated = [row for row in replies if row["generated_at"] is not None]
        if generated:
            generated_at = max(row["generated_at"] for row in generated)
            connection.execute(
                batch_table.insert(),
                {
                    "id": _HISTORICAL_LETTER_ID,
                    "batch_number": "CRM-LET-HISTORICAL",
                    "operation_type": "formal_letters",
                    "status": "completed",
                    "parent_batch_id": _HISTORICAL_IMPORT_ID,
                    "source_filename": None,
                    "source_sha256": None,
                    "scope_json": {"source": "pre_batch_tracking"},
                    "settings_json": {"backfilled": True, "artifact_unavailable": True},
                    "total_items": len(generated),
                    "valid_items": len(generated),
                    "successful_items": len(generated),
                    "failed_items": 0,
                    "skipped_items": 0,
                    "duplicate_items": 0,
                    "created_by": "migration",
                    "error_summary": None,
                    "created_at": generated_at,
                    "updated_at": now,
                    "started_at": generated_at,
                    "completed_at": generated_at,
                },
            )
            connection.execute(
                item_table.insert(),
                [
                    {
                        "id": uuid.uuid4(),
                        "batch_id": _HISTORICAL_LETTER_ID,
                        "complaint_case_id": row["complaint_case_id"],
                        "complaint_number_snapshot": row["complaint_number"] or str(row["complaint_case_id"]),
                        "source_row": None,
                        "status": "generated",
                        "error_code": None,
                        "error_message": None,
                        "reply_version_before": row["version"],
                        "reply_version_after": row["version"],
                        "reply_content_hash": None,
                        "output_filename": (
                            f"{row['complaint_number'] or row['complaint_case_id']}/"
                            f"{row['complaint_number'] or row['complaint_case_id']} - DEO Report.odt"
                        ),
                        "output_sha256": None,
                        "details_json": {"backfilled": True, "artifact_unavailable": True},
                        "created_at": row["generated_at"],
                        "processed_at": row["generated_at"],
                    }
                    for row in generated
                ],
            )


def downgrade() -> None:
    op.drop_table("crm_bulk_operation_artifacts")
    op.drop_table("crm_bulk_operation_items")
    op.drop_table("crm_bulk_operation_batches")

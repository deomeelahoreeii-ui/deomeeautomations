"""Add exclusive lifecycle claims for upward compliance submissions.

Revision ID: f4a6c8e0b215
Revises: e9b2c4d6f801
Create Date: 2026-07-22
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import uuid

from alembic import op
import sqlalchemy as sa


revision = "f4a6c8e0b215"
down_revision = "e9b2c4d6f801"
branch_labels = None
depends_on = None


ATTEMPTED_TARGET_STATUSES = {
    "queued",
    "sent_pending_confirmation",
    "delivered",
    "failed",
    "timed_out",
}
TARGET_PRIORITY = {
    "delivered": 60,
    "sent_pending_confirmation": 50,
    "queued": 40,
    "failed": 30,
    "timed_out": 30,
    "approved": 20,
    "planned": 10,
    "blocked": 5,
    "cancelled": 0,
    "excluded": 0,
}
BATCH_PRIORITY = {
    "completed": 80,
    "completed_with_errors": 70,
    "sending": 60,
    "queued": 50,
    "approved": 40,
    "ready": 30,
    "review_required": 20,
    "failed": 10,
}


def _backfill_claims() -> None:
    bind = op.get_bind()
    rows = list(
        bind.execute(
            sa.text(
                """
                SELECT item.id AS item_id, item.official_letter_id, item.complaint_case_id,
                       item.compliance_status, item.created_at AS item_created_at,
                       batch.id AS batch_id, batch.batch_number, batch.status AS batch_status,
                       batch.created_at AS batch_created_at, batch.completed_at
                FROM crm_dispatch_items AS item
                JOIN crm_dispatch_batches AS batch ON batch.id = item.batch_id
                WHERE batch.direction = 'upward'
                  AND item.official_letter_id IS NOT NULL
                  AND item.excluded = false
                """
            )
        ).mappings()
    )
    if not rows:
        return

    targets: dict[object, list[dict]] = defaultdict(list)
    item_ids = [row["item_id"] for row in rows]
    target_rows = bind.execute(
        sa.text(
            """
            SELECT dispatch_item_id, business_status, sent_at, completed_at
            FROM crm_dispatch_targets
            WHERE dispatch_item_id IN :item_ids
            """
        ).bindparams(sa.bindparam("item_ids", expanding=True)),
        {"item_ids": item_ids},
    ).mappings()
    for target in target_rows:
        targets[target["dispatch_item_id"]].append(dict(target))

    claim_table = sa.table(
        "crm_upward_submission_claims",
        sa.column("id", sa.Uuid()),
        sa.column("official_letter_id", sa.Uuid()),
        sa.column("dispatch_item_id", sa.Uuid()),
        sa.column("status", sa.String()),
        sa.column("claimed_by", sa.String()),
        sa.column("release_reason", sa.Text()),
        sa.column("claimed_at", sa.DateTime()),
        sa.column("sent_at", sa.DateTime()),
        sa.column("released_at", sa.DateTime()),
        sa.column("updated_at", sa.DateTime()),
    )
    audit_table = sa.table(
        "crm_complaint_audit_events",
        sa.column("id", sa.Uuid()),
        sa.column("complaint_case_id", sa.Uuid()),
        sa.column("entity_type", sa.String()),
        sa.column("entity_id", sa.String()),
        sa.column("event_type", sa.String()),
        sa.column("state", sa.String()),
        sa.column("actor", sa.String()),
        sa.column("before_json", sa.JSON()),
        sa.column("after_json", sa.JSON()),
        sa.column("details_json", sa.JSON()),
        sa.column("error", sa.Text()),
        sa.column("created_at", sa.DateTime()),
    )
    grouped: dict[object, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["official_letter_id"]].append(dict(row))

    now = datetime.utcnow()
    for letter_id, candidates in grouped.items():

        def priority(row: dict) -> tuple:
            item_targets = targets.get(row["item_id"], [])
            target_score = max(
                (TARGET_PRIORITY.get(target["business_status"], 0) for target in item_targets),
                default=0,
            )
            return (
                100 if row["compliance_status"] == "submitted" else 0,
                target_score,
                BATCH_PRIORITY.get(row["batch_status"], 0),
                row["batch_created_at"],
                str(row["item_id"]),
            )

        canonical = max(candidates, key=priority)
        for row in candidates:
            item_targets = targets.get(row["item_id"], [])
            attempted = row["compliance_status"] == "submitted" or any(
                target["business_status"] in ATTEMPTED_TARGET_STATUSES for target in item_targets
            )
            sent_values = [
                target["sent_at"] or target["completed_at"]
                for target in item_targets
                if target["sent_at"] or target["completed_at"]
            ]
            sent_at = (
                min(sent_values) if sent_values else (row["completed_at"] if attempted else None)
            )
            is_canonical = row["item_id"] == canonical["item_id"]
            released_at = None if is_canonical else now
            bind.execute(
                claim_table.insert().values(
                    id=uuid.uuid4(),
                    official_letter_id=letter_id,
                    dispatch_item_id=row["item_id"],
                    status=("sent" if attempted else "reserved") if is_canonical else "released",
                    claimed_by="dispatch-claim-backfill",
                    release_reason=None
                    if is_canonical
                    else (f"Superseded by canonical dispatch batch {canonical['batch_number']}"),
                    claimed_at=row["item_created_at"],
                    sent_at=sent_at,
                    released_at=released_at,
                    updated_at=now,
                )
            )
            if is_canonical:
                continue
            reason = f"Superseded by canonical dispatch batch {canonical['batch_number']}"
            bind.execute(
                sa.text(
                    """
                    UPDATE crm_dispatch_items
                    SET excluded = true, route_status = 'excluded',
                        exclusion_reason = :reason, updated_at = :now
                    WHERE id = :item_id
                    """
                ),
                {"reason": reason, "now": now, "item_id": row["item_id"]},
            )
            bind.execute(
                audit_table.insert().values(
                    id=uuid.uuid4(),
                    complaint_case_id=row["complaint_case_id"],
                    entity_type="crm_dispatch_item",
                    entity_id=str(row["item_id"]),
                    event_type="upward_submission_claim_reconciled",
                    state="succeeded",
                    actor="dispatch-claim-backfill",
                    before_json={"claim": "active"},
                    after_json={"claim": "released", "excluded": True},
                    details_json={
                        "official_letter_id": str(letter_id),
                        "batch_id": str(row["batch_id"]),
                        "canonical_batch_id": str(canonical["batch_id"]),
                        "canonical_batch_number": canonical["batch_number"],
                    },
                    error=None,
                    created_at=now,
                )
            )

    bind.execute(
        sa.text(
            """
            UPDATE crm_dispatch_batches AS batch
            SET status = 'stale', completed_at = COALESCE(completed_at, :now),
                updated_at = :now,
                error_summary = 'Superseded by a canonical upward compliance submission.'
            WHERE batch.direction = 'upward'
              AND batch.status NOT IN ('completed', 'completed_with_errors', 'cancelled', 'stale')
              AND EXISTS (
                  SELECT 1 FROM crm_dispatch_items AS item
                  WHERE item.batch_id = batch.id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM crm_dispatch_items AS item
                  WHERE item.batch_id = batch.id AND item.excluded = false
              )
            """
        ),
        {"now": now},
    )


def upgrade() -> None:
    op.create_table(
        "crm_upward_submission_claims",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("official_letter_id", sa.Uuid(), nullable=False),
        sa.Column("dispatch_item_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("claimed_by", sa.String(length=120), nullable=False),
        sa.Column("release_reason", sa.Text(), nullable=True),
        sa.Column("claimed_at", sa.DateTime(), nullable=False),
        sa.Column("sent_at", sa.DateTime(), nullable=True),
        sa.Column("released_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('reserved', 'sent', 'released')",
            name="ck_crm_upward_submission_claims_status",
        ),
        sa.ForeignKeyConstraint(["official_letter_id"], ["crm_official_letters.id"]),
        sa.ForeignKeyConstraint(
            ["dispatch_item_id"], ["crm_dispatch_items.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dispatch_item_id", name="uq_crm_upward_submission_claims_item"),
    )
    for column in (
        "official_letter_id",
        "dispatch_item_id",
        "status",
        "claimed_by",
        "claimed_at",
        "sent_at",
        "released_at",
        "updated_at",
    ):
        op.create_index(
            f"ix_crm_upward_submission_claims_{column}",
            "crm_upward_submission_claims",
            [column],
        )
    op.create_index(
        "uq_crm_upward_submission_claims_active_letter",
        "crm_upward_submission_claims",
        ["official_letter_id"],
        unique=True,
        postgresql_where=sa.text("released_at IS NULL"),
        sqlite_where=sa.text("released_at IS NULL"),
    )
    _backfill_claims()


def downgrade() -> None:
    op.drop_table("crm_upward_submission_claims")

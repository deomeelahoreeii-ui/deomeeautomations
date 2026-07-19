"""Separate delivery state counts and audit partial preview approvals.

Revision ID: a1c3e5f7b902
Revises: a8d9e0f1b2c3
Create Date: 2026-07-19
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "a1c3e5f7b902"
down_revision = "a8d9e0f1b2c3"
branch_labels = None
depends_on = None


_ACTIVE_BLOCKED_ARTIFACT = """
EXISTS (
    SELECT 1
    FROM whatsapp_dispatch_preview_artifacts AS artifact
    WHERE artifact.preview_id = preview.id
      AND artifact.status = 'blocked'
      AND EXISTS (
          SELECT 1
          FROM whatsapp_dispatch_preview_deliveries AS delivery
          CROSS JOIN LATERAL json_array_elements_text(
              COALESCE(delivery.attachment_ids, '[]'::json)
          ) AS attachment_entry(value)
          WHERE delivery.preview_id = preview.id
            AND delivery.status IN ('ready', 'warning')
            AND attachment_entry.value = artifact.id::text
      )
)
"""


def upgrade() -> None:
    op.add_column(
        "whatsapp_dispatch_approvals",
        sa.Column("exclusions_acknowledged", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "whatsapp_dispatch_approvals",
        sa.Column(
            "approved_content_sha256", sa.String(), nullable=False, server_default=sa.text("''"),
        ),
    )
    op.add_column(
        "whatsapp_dispatch_approvals",
        sa.Column(
            "approved_delivery_ids",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
    )
    op.add_column(
        "whatsapp_dispatch_approvals",
        sa.Column(
            "excluded_deliveries",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
    )
    op.add_column(
        "whatsapp_dispatch_approvals",
        sa.Column("excluded_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "whatsapp_dispatch_approvals",
        sa.Column("partial", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index(
        "ix_whatsapp_dispatch_approvals_approved_content_sha256",
        "whatsapp_dispatch_approvals",
        ["approved_content_sha256"],
        unique=False,
    )

    # These columns now represent mutually exclusive delivery states. Issue
    # totals remain derived API metadata and are never mixed into these counts.
    op.execute(
        """
        UPDATE whatsapp_dispatch_previews AS preview
        SET
            ready_count = (
                SELECT COUNT(*) FROM whatsapp_dispatch_preview_deliveries AS delivery
                WHERE delivery.preview_id = preview.id AND delivery.status = 'ready'
            ),
            warning_count = (
                SELECT COUNT(*) FROM whatsapp_dispatch_preview_deliveries AS delivery
                WHERE delivery.preview_id = preview.id AND delivery.status = 'warning'
            ),
            blocked_count = (
                SELECT COUNT(*) FROM whatsapp_dispatch_preview_deliveries AS delivery
                WHERE delivery.preview_id = preview.id AND delivery.status = 'blocked'
            ),
            skipped_count = (
                SELECT COUNT(*) FROM whatsapp_dispatch_preview_deliveries AS delivery
                WHERE delivery.preview_id = preview.id AND delivery.status = 'skipped'
            ),
            delivery_count = (
                SELECT COUNT(*) FROM whatsapp_dispatch_preview_deliveries AS delivery
                WHERE delivery.preview_id = preview.id
            )
        """
    )
    op.execute(
        f"""
        UPDATE whatsapp_dispatch_previews AS preview
        SET status = CASE
            WHEN EXISTS (
                SELECT 1
                FROM json_array_elements(COALESCE(preview.issues, '[]'::json)) AS issue
                WHERE issue ->> 'severity' = 'blocked'
            ) THEN 'blocked'
            WHEN {_ACTIVE_BLOCKED_ARTIFACT} THEN 'blocked'
            WHEN preview.ready_count + preview.warning_count > 0 THEN 'ready'
            WHEN preview.blocked_count > 0 THEN 'blocked'
            ELSE 'ready'
        END
        """
    )


def downgrade() -> None:
    # Restore the historical meaning used by the pre-patch source: warning and
    # blocked columns were issue counts (delivery + batch), not delivery states.
    op.execute(
        """
        UPDATE whatsapp_dispatch_previews AS preview
        SET
            ready_count = (
                SELECT COUNT(*) FROM whatsapp_dispatch_preview_deliveries AS delivery
                WHERE delivery.preview_id = preview.id AND delivery.status = 'ready'
            ),
            warning_count = (
                SELECT COUNT(*)
                FROM whatsapp_dispatch_preview_deliveries AS delivery
                CROSS JOIN LATERAL json_array_elements(COALESCE(delivery.issues, '[]'::json)) AS issue
                WHERE delivery.preview_id = preview.id
                  AND issue ->> 'severity' = 'warning'
            ) + (
                SELECT COUNT(*)
                FROM json_array_elements(COALESCE(preview.issues, '[]'::json)) AS issue
                WHERE issue ->> 'severity' = 'warning'
            ),
            blocked_count = (
                SELECT COUNT(*)
                FROM whatsapp_dispatch_preview_deliveries AS delivery
                CROSS JOIN LATERAL json_array_elements(COALESCE(delivery.issues, '[]'::json)) AS issue
                WHERE delivery.preview_id = preview.id
                  AND issue ->> 'severity' = 'blocked'
            ) + (
                SELECT COUNT(*)
                FROM json_array_elements(COALESCE(preview.issues, '[]'::json)) AS issue
                WHERE issue ->> 'severity' = 'blocked'
            ),
            skipped_count = (
                SELECT COUNT(*) FROM whatsapp_dispatch_preview_deliveries AS delivery
                WHERE delivery.preview_id = preview.id AND delivery.status = 'skipped'
            ),
            delivery_count = (
                SELECT COUNT(*) FROM whatsapp_dispatch_preview_deliveries AS delivery
                WHERE delivery.preview_id = preview.id
            )
        """
    )
    op.execute(
        f"""
        UPDATE whatsapp_dispatch_previews AS preview
        SET status = CASE
            WHEN preview.blocked_count > 0 OR {_ACTIVE_BLOCKED_ARTIFACT}
            THEN 'blocked'
            ELSE 'ready'
        END
        """
    )

    op.drop_index(
        "ix_whatsapp_dispatch_approvals_approved_content_sha256",
        table_name="whatsapp_dispatch_approvals",
    )
    op.drop_column("whatsapp_dispatch_approvals", "partial")
    op.drop_column("whatsapp_dispatch_approvals", "excluded_count")
    op.drop_column("whatsapp_dispatch_approvals", "excluded_deliveries")
    op.drop_column("whatsapp_dispatch_approvals", "approved_delivery_ids")
    op.drop_column("whatsapp_dispatch_approvals", "approved_content_sha256")
    op.drop_column("whatsapp_dispatch_approvals", "exclusions_acknowledged")

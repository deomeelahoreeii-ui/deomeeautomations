"""zero-result acknowledgement templates and daily claims

Revision ID: f1a2b3c4d5e6
Revises: e7a1c3d5f902
Create Date: 2026-07-18 06:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f1a2b3c4d5e6"
down_revision: str | None = "e7a1c3d5f902"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TEMPLATES = (
    (
        "67f1487e-0e13-4daf-b12f-bbc2eb167101",
        "antidengue_zero_result_ack_1",
        "Zero dormant acknowledgement 1",
        "✅ Today’s Anti-Dengue review found no dormant {{wing}} schools in {{tehsil}}. "
        "Thank you for keeping portal activities updated.",
    ),
    (
        "67f1487e-0e13-4daf-b12f-bbc2eb167102",
        "antidengue_zero_result_ack_2",
        "Zero dormant acknowledgement 2",
        "Good update for {{tehsil}}: the latest report shows zero dormant {{wing}} schools today. "
        "Please continue timely reporting.",
    ),
    (
        "67f1487e-0e13-4daf-b12f-bbc2eb167103",
        "antidengue_zero_result_ack_3",
        "Zero dormant acknowledgement 3",
        "Today’s status: no dormant {{wing}} schools were identified in {{tehsil}}. "
        "Thank you for the prompt updates.",
    ),
)


def upgrade() -> None:
    op.create_table(
        "whatsapp_daily_message_claims",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("semantic_key", sa.String(length=64), nullable=False),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("purpose", sa.String(length=80), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("application_id", sa.Uuid(), nullable=False),
        sa.Column("report_type_id", sa.Uuid(), nullable=False),
        sa.Column("template_id", sa.Uuid(), nullable=True),
        sa.Column("preview_id", sa.Uuid(), nullable=False),
        sa.Column("preview_delivery_id", sa.Uuid(), nullable=False),
        sa.Column("approval_id", sa.Uuid(), nullable=False),
        sa.Column("target_jid", sa.String(length=160), nullable=False),
        sa.Column("scope_key", sa.String(length=100), nullable=False),
        sa.Column("scope_value", sa.String(length=100), nullable=False),
        sa.Column("scope_label", sa.String(length=200), nullable=False, server_default=""),
        sa.Column("template_key", sa.String(length=100), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "purpose IN ('zero_result_acknowledgement')",
            name="ck_whatsapp_daily_message_claim_purpose",
        ),
        sa.ForeignKeyConstraint(["account_id"], ["whatsapp_accounts.id"]),
        sa.ForeignKeyConstraint(["application_id"], ["whatsapp_applications.id"]),
        sa.ForeignKeyConstraint(["report_type_id"], ["whatsapp_report_types.id"]),
        sa.ForeignKeyConstraint(
            ["template_id"], ["whatsapp_templates.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["preview_id"], ["whatsapp_dispatch_previews.id"]),
        sa.ForeignKeyConstraint(
            ["preview_delivery_id"], ["whatsapp_dispatch_preview_deliveries.id"]
        ),
        sa.ForeignKeyConstraint(["approval_id"], ["whatsapp_dispatch_approvals.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "semantic_key", name="uq_whatsapp_daily_message_claim_semantic_key"
        ),
    )
    for column in (
        "semantic_key",
        "business_date",
        "purpose",
        "account_id",
        "application_id",
        "report_type_id",
        "template_id",
        "preview_id",
        "preview_delivery_id",
        "approval_id",
        "target_jid",
        "scope_key",
        "scope_value",
        "created_at",
    ):
        op.create_index(
            f"ix_whatsapp_daily_message_claims_{column}",
            "whatsapp_daily_message_claims",
            [column],
        )

    connection = op.get_bind()
    for template_id, key, name, body in TEMPLATES:
        connection.execute(
            sa.text(
                """
                INSERT INTO whatsapp_templates
                    (id, application_id, report_type_id, recipient_scope_id,
                     recipient_channel, key, name, category, body, enabled,
                     created_at, updated_at)
                SELECT CAST(:id AS UUID), app.id, report.id, scope.id,
                       'group', :key, :name, 'zero_result_acknowledgement',
                       :body, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                FROM whatsapp_applications app
                JOIN whatsapp_report_types report
                  ON report.application_id = app.id
                 AND report.key = 'tehsil_dormant_summary'
                JOIN whatsapp_recipient_scopes scope
                  ON scope.application_id = app.id
                 AND scope.channel = 'group'
                 AND scope.key = 'tehsil'
                WHERE app.key = 'antidengue'
                ON CONFLICT (key) DO NOTHING
                """
            ),
            {"id": template_id, "key": key, "name": name, "body": body},
        )


def downgrade() -> None:
    keys = ", ".join(f"'{item[1]}'" for item in TEMPLATES)
    op.execute(f"DELETE FROM whatsapp_templates WHERE key IN ({keys})")
    op.drop_table("whatsapp_daily_message_claims")

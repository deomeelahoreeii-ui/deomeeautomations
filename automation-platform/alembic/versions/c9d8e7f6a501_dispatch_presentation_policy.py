"""dispatch presentation policy

Revision ID: c9d8e7f6a501
Revises: b7e4d9a1c203
Create Date: 2026-07-14 15:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c9d8e7f6a501"
down_revision: str | None = "b7e4d9a1c203"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "whatsapp_dispatch_profiles",
        sa.Column(
            "presentation_policy",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
    )
    op.execute(
        """
        UPDATE whatsapp_dispatch_profiles AS profile
        SET presentation_policy = CASE report.key
            WHEN 'tehsil_dormant_summary' THEN
                '{"message_style":"detailed","attachment_mode":"none","image_content":"details"}'::json
            WHEN 'wing_summary' THEN
                '{"message_style":"summary","attachment_mode":"excel","image_content":"summary"}'::json
            ELSE '{}'::json
        END
        FROM whatsapp_report_types AS report
        WHERE profile.report_type_id = report.id
        """
    )


def downgrade() -> None:
    op.drop_column("whatsapp_dispatch_profiles", "presentation_policy")

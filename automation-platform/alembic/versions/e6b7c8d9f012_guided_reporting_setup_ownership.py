"""guided reporting setup ownership

Revision ID: e6b7c8d9f012
Revises: a8c4e2f91b60
Create Date: 2026-07-14 00:55:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e6b7c8d9f012"
down_revision: str | None = "a8c4e2f91b60"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "whatsapp_dispatch_profiles",
        sa.Column("guided_setup", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "whatsapp_dispatch_profiles",
        sa.Column("owns_audience", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "whatsapp_dispatch_profiles",
        sa.Column("owns_template", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index(
        "ix_whatsapp_dispatch_profiles_guided_setup",
        "whatsapp_dispatch_profiles",
        ["guided_setup"],
    )
    op.execute(
        """
        UPDATE whatsapp_dispatch_profiles AS profile
        SET guided_setup = TRUE,
            owns_audience = TRUE
        FROM whatsapp_audiences AS audience
        WHERE profile.audience_id = audience.id
          AND profile.notes = 'Created by the guided reporting-route setup.'
          AND audience.description = 'Created by the guided reporting-route setup.'
        """
    )
    op.execute(
        """
        UPDATE whatsapp_dispatch_profiles AS profile
        SET owns_template = TRUE
        FROM whatsapp_templates AS template
        WHERE profile.guided_setup = TRUE
          AND profile.template_id = template.id
          AND ABS(EXTRACT(EPOCH FROM (profile.created_at - template.created_at))) <= 10
          AND NOT EXISTS (
              SELECT 1
              FROM whatsapp_dispatch_profiles AS other
              WHERE other.template_id = template.id
                AND other.id <> profile.id
          )
        """
    )


def downgrade() -> None:
    op.drop_index(
        "ix_whatsapp_dispatch_profiles_guided_setup",
        table_name="whatsapp_dispatch_profiles",
    )
    op.drop_column("whatsapp_dispatch_profiles", "owns_template")
    op.drop_column("whatsapp_dispatch_profiles", "owns_audience")
    op.drop_column("whatsapp_dispatch_profiles", "guided_setup")

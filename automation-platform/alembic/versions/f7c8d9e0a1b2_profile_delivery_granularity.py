"""Profile-level recipient or jurisdiction delivery granularity.

Revision ID: f7c8d9e0a1b2
Revises: e6f7a8b9c0d1
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f7c8d9e0a1b2"
down_revision: str | None = "e6f7a8b9c0d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "whatsapp_dispatch_profiles",
        sa.Column(
            "delivery_granularity", sa.String(), nullable=False,
            server_default="recipient",
        ),
    )
    op.create_check_constraint(
        "ck_whatsapp_profiles_delivery_granularity",
        "whatsapp_dispatch_profiles",
        "delivery_granularity IN ('recipient', 'scope')",
    )
    op.create_index(
        "ix_whatsapp_dispatch_profiles_delivery_granularity",
        "whatsapp_dispatch_profiles", ["delivery_granularity"],
    )
    op.execute(sa.text("""
        UPDATE whatsapp_dispatch_profiles AS profile
        SET delivery_granularity = 'scope',
            version = version + 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE profile.recipient_channel = 'individual'
          AND EXISTS (
              SELECT 1
              FROM whatsapp_audience_sources AS source
              WHERE source.audience_id = profile.audience_id
                AND source.enabled = true
                AND source.recipient_role = 'aeo'
                AND source.route_scope_key = 'markaz'
          )
    """))


def downgrade() -> None:
    op.drop_index(
        "ix_whatsapp_dispatch_profiles_delivery_granularity",
        table_name="whatsapp_dispatch_profiles",
    )
    op.drop_constraint(
        "ck_whatsapp_profiles_delivery_granularity",
        "whatsapp_dispatch_profiles", type_="check",
    )
    op.drop_column("whatsapp_dispatch_profiles", "delivery_granularity")

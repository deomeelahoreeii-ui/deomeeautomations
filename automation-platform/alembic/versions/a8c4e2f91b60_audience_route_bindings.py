"""audience route bindings

Revision ID: a8c4e2f91b60
Revises: f2b6c8d1e304
Create Date: 2026-07-13 22:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a8c4e2f91b60"
down_revision: str | None = "f2b6c8d1e304"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "whatsapp_audience_members",
        sa.Column("route_scope_key", sa.String(), nullable=False, server_default=""),
    )
    op.add_column(
        "whatsapp_audience_members",
        sa.Column("route_scope_value", sa.String(), nullable=False, server_default=""),
    )
    op.add_column(
        "whatsapp_audience_members",
        sa.Column("route_scope_label", sa.String(), nullable=False, server_default=""),
    )
    op.create_index(
        "ix_whatsapp_audience_members_route_scope_key",
        "whatsapp_audience_members",
        ["route_scope_key"],
    )
    op.create_index(
        "ix_whatsapp_audience_members_route_scope_value",
        "whatsapp_audience_members",
        ["route_scope_value"],
    )
    op.execute(
        "UPDATE whatsapp_report_types "
        "SET key = 'tehsil_dormant_summary', name = 'Tehsil Dormant Summary' "
        "WHERE key = 'tehsil_dormant_summery'"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE whatsapp_report_types "
        "SET key = 'tehsil_dormant_summery', name = 'Tehsil Dormant Summery' "
        "WHERE key = 'tehsil_dormant_summary'"
    )
    op.drop_index(
        "ix_whatsapp_audience_members_route_scope_value",
        table_name="whatsapp_audience_members",
    )
    op.drop_index(
        "ix_whatsapp_audience_members_route_scope_key",
        table_name="whatsapp_audience_members",
    )
    op.drop_column("whatsapp_audience_members", "route_scope_label")
    op.drop_column("whatsapp_audience_members", "route_scope_value")
    op.drop_column("whatsapp_audience_members", "route_scope_key")

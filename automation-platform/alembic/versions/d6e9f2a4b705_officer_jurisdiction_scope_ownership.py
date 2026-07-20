"""Enforce one active responsible officer per administrative scope.

Revision ID: d6e9f2a4b705
Revises: c3d8e1f4a702
Create Date: 2026-07-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "d6e9f2a4b705"
down_revision = "c3d8e1f4a702"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Fail loudly rather than silently choosing one of two live recipients. The
    # repair utility bundled with this revision can resolve intended transfers.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM officer_jurisdictions
            WHERE active = TRUE AND role = 'aeo'
            GROUP BY wing_id, tehsil_id, markaz_id
            HAVING COUNT(*) > 1
          ) THEN
            RAISE EXCEPTION 'Duplicate active AEO scope ownership exists; run the jurisdiction audit before upgrading';
          END IF;
          IF EXISTS (
            SELECT 1
            FROM officer_jurisdictions
            WHERE active = TRUE AND role = 'ddeo'
            GROUP BY wing_id, tehsil_id
            HAVING COUNT(*) > 1
          ) THEN
            RAISE EXCEPTION 'Duplicate active DDEO scope ownership exists; run the jurisdiction audit before upgrading';
          END IF;
        END $$;
        """
    )
    op.create_index(
        "uq_officer_jurisdictions_active_aeo_scope",
        "officer_jurisdictions",
        ["wing_id", "tehsil_id", "markaz_id"],
        unique=True,
        postgresql_where=sa.text("active = TRUE AND role = 'aeo'"),
    )
    op.create_index(
        "uq_officer_jurisdictions_active_ddeo_scope",
        "officer_jurisdictions",
        ["wing_id", "tehsil_id"],
        unique=True,
        postgresql_where=sa.text("active = TRUE AND role = 'ddeo'"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_officer_jurisdictions_active_ddeo_scope",
        table_name="officer_jurisdictions",
    )
    op.drop_index(
        "uq_officer_jurisdictions_active_aeo_scope",
        table_name="officer_jurisdictions",
    )

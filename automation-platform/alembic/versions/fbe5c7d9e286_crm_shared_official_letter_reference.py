"""Allow a shared official-letter number and date across complaint letters.

Revision ID: fbe5c7d9e286
Revises: f9e3a5c7d064
Create Date: 2026-07-21
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "fbe5c7d9e286"
down_revision: Union[str, None] = "f9e3a5c7d064"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "crm_official_letter_settings",
        sa.Column("current_letter_number", sa.String(length=180), nullable=True),
    )
    op.add_column(
        "crm_official_letter_settings",
        sa.Column("current_letter_date", sa.Date(), nullable=True),
    )
    op.execute(
        sa.text(
            """
            UPDATE crm_official_letter_settings AS settings
            SET current_letter_number = COALESCE(
                    (
                        SELECT letter.letter_number
                        FROM crm_official_letters AS letter
                        WHERE letter.status IN ('finalized', 'superseded')
                        ORDER BY COALESCE(letter.finalized_at, letter.generated_at, letter.created_at) DESC
                        LIMIT 1
                    ),
                    CASE
                        WHEN TRIM(settings.numbering_prefix) <> ''
                            THEN CAST(settings.last_numeric_number + 1 AS VARCHAR) || '/' || TRIM(settings.numbering_prefix)
                        ELSE CAST(settings.last_numeric_number + 1 AS VARCHAR)
                    END
                ),
                current_letter_date = COALESCE(
                    (
                        SELECT letter.letter_date
                        FROM crm_official_letters AS letter
                        WHERE letter.status IN ('finalized', 'superseded')
                        ORDER BY COALESCE(letter.finalized_at, letter.generated_at, letter.created_at) DESC
                        LIMIT 1
                    ),
                    CURRENT_DATE
                ),
                require_unique_number = FALSE
            WHERE settings.current_letter_number IS NULL OR settings.current_letter_date IS NULL
            """
        )
    )
    op.alter_column(
        "crm_official_letter_settings",
        "current_letter_number",
        existing_type=sa.String(length=180),
        nullable=False,
    )
    op.alter_column(
        "crm_official_letter_settings",
        "current_letter_date",
        existing_type=sa.Date(),
        nullable=False,
    )
    op.create_index(
        "ix_crm_official_letter_settings_current_letter_date",
        "crm_official_letter_settings",
        ["current_letter_date"],
    )
    op.drop_constraint(
        "uq_crm_official_letters_number",
        "crm_official_letters",
        type_="unique",
    )


def downgrade() -> None:
    connection = op.get_bind()
    duplicate = connection.execute(
        sa.text(
            """
            SELECT letter_number, COUNT(*) AS total
            FROM crm_official_letters
            GROUP BY letter_number
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        )
    ).first()
    if duplicate is not None:
        raise RuntimeError(
            "Cannot downgrade shared official-letter references while duplicate "
            f"letter number {duplicate[0]!r} exists. Preserve the B3.5.3 schema "
            "or consolidate those records before downgrading."
        )
    op.create_unique_constraint(
        "uq_crm_official_letters_number",
        "crm_official_letters",
        ["letter_number"],
    )
    op.drop_index(
        "ix_crm_official_letter_settings_current_letter_date",
        table_name="crm_official_letter_settings",
    )
    op.drop_column("crm_official_letter_settings", "current_letter_date")
    op.drop_column("crm_official_letter_settings", "current_letter_number")
    op.execute(
        sa.text(
            "UPDATE crm_official_letter_settings SET require_unique_number = TRUE"
        )
    )

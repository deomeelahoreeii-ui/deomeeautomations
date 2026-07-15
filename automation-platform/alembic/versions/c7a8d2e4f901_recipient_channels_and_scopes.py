"""recipient channels and configurable scopes

Revision ID: c7a8d2e4f901
Revises: d41c7e9a2b10
Create Date: 2026-07-13 19:05:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


revision: str = "c7a8d2e4f901"
down_revision: Union[str, Sequence[str], None] = "d41c7e9a2b10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "whatsapp_recipient_scopes",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("application_id", sa.Uuid(), nullable=False),
        sa.Column("parent_id", sa.Uuid(), nullable=True),
        sa.Column("channel", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("key", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("hierarchy_level", sa.Integer(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "channel IN ('individual', 'group')",
            name="ck_whatsapp_recipient_scopes_channel",
        ),
        sa.ForeignKeyConstraint(["application_id"], ["whatsapp_applications.id"]),
        sa.ForeignKeyConstraint(["parent_id"], ["whatsapp_recipient_scopes.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "application_id",
            "channel",
            "key",
            name="uq_whatsapp_recipient_scopes_application_channel_key",
        ),
    )
    for column in (
        "application_id",
        "parent_id",
        "channel",
        "key",
        "name",
        "hierarchy_level",
        "enabled",
    ):
        op.create_index(
            op.f(f"ix_whatsapp_recipient_scopes_{column}"),
            "whatsapp_recipient_scopes",
            [column],
            unique=False,
        )

    op.add_column(
        "whatsapp_templates",
        sa.Column(
            "recipient_channel",
            sqlmodel.sql.sqltypes.AutoString(),
            server_default="any",
            nullable=False,
        ),
    )
    op.add_column(
        "whatsapp_templates",
        sa.Column("recipient_scope_id", sa.Uuid(), nullable=True),
    )
    op.create_check_constraint(
        "ck_whatsapp_templates_recipient_channel",
        "whatsapp_templates",
        "recipient_channel IN ('individual', 'group', 'any')",
    )
    op.create_foreign_key(
        "fk_whatsapp_templates_recipient_scope",
        "whatsapp_templates",
        "whatsapp_recipient_scopes",
        ["recipient_scope_id"],
        ["id"],
    )
    op.create_index(
        op.f("ix_whatsapp_templates_recipient_channel"),
        "whatsapp_templates",
        ["recipient_channel"],
        unique=False,
    )
    op.create_index(
        op.f("ix_whatsapp_templates_recipient_scope_id"),
        "whatsapp_templates",
        ["recipient_scope_id"],
        unique=False,
    )

    op.add_column(
        "whatsapp_dispatch_profiles",
        sa.Column(
            "recipient_channel",
            sqlmodel.sql.sqltypes.AutoString(),
            server_default="group",
            nullable=False,
        ),
    )
    op.add_column(
        "whatsapp_dispatch_profiles",
        sa.Column("recipient_scope_id", sa.Uuid(), nullable=True),
    )
    op.execute(
        "UPDATE whatsapp_dispatch_profiles "
        "SET recipient_channel = CASE "
        "WHEN delivery_mode = 'individuals' THEN 'individual' ELSE 'group' END"
    )
    op.create_check_constraint(
        "ck_whatsapp_profiles_recipient_channel",
        "whatsapp_dispatch_profiles",
        "recipient_channel IN ('individual', 'group')",
    )
    op.create_foreign_key(
        "fk_whatsapp_profiles_recipient_scope",
        "whatsapp_dispatch_profiles",
        "whatsapp_recipient_scopes",
        ["recipient_scope_id"],
        ["id"],
    )
    op.create_index(
        op.f("ix_whatsapp_dispatch_profiles_recipient_channel"),
        "whatsapp_dispatch_profiles",
        ["recipient_channel"],
        unique=False,
    )
    op.create_index(
        op.f("ix_whatsapp_dispatch_profiles_recipient_scope_id"),
        "whatsapp_dispatch_profiles",
        ["recipient_scope_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_whatsapp_dispatch_profiles_recipient_scope_id"),
        table_name="whatsapp_dispatch_profiles",
    )
    op.drop_index(
        op.f("ix_whatsapp_dispatch_profiles_recipient_channel"),
        table_name="whatsapp_dispatch_profiles",
    )
    op.drop_constraint(
        "fk_whatsapp_profiles_recipient_scope",
        "whatsapp_dispatch_profiles",
        type_="foreignkey",
    )
    op.drop_constraint(
        "ck_whatsapp_profiles_recipient_channel",
        "whatsapp_dispatch_profiles",
        type_="check",
    )
    op.drop_column("whatsapp_dispatch_profiles", "recipient_scope_id")
    op.drop_column("whatsapp_dispatch_profiles", "recipient_channel")

    op.drop_index(
        op.f("ix_whatsapp_templates_recipient_scope_id"),
        table_name="whatsapp_templates",
    )
    op.drop_index(
        op.f("ix_whatsapp_templates_recipient_channel"),
        table_name="whatsapp_templates",
    )
    op.drop_constraint(
        "fk_whatsapp_templates_recipient_scope",
        "whatsapp_templates",
        type_="foreignkey",
    )
    op.drop_constraint(
        "ck_whatsapp_templates_recipient_channel",
        "whatsapp_templates",
        type_="check",
    )
    op.drop_column("whatsapp_templates", "recipient_scope_id")
    op.drop_column("whatsapp_templates", "recipient_channel")
    op.drop_table("whatsapp_recipient_scopes")

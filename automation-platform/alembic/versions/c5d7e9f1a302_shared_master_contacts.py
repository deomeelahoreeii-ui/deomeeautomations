"""Add shared master contacts and map WhatsApp identities to them.

Revision ID: c5d7e9f1a302
Revises: b3e5f7a9c102
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa
import sqlmodel


revision = "c5d7e9f1a302"
down_revision = "b3e5f7a9c102"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "master_contacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("legacy_id", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("normalized_mobile", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("name_source", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("name_verified", sa.Boolean(), nullable=False),
        sa.Column("last_observed_at", sa.DateTime(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "name_source IN ('unknown', 'whatsapp_push', 'whatsapp_profile', "
            "'verified_entity', 'manual', 'import')",
            name="ck_master_contacts_name_source",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_master_contacts_active", "master_contacts", ["active"])
    op.create_index("ix_master_contacts_legacy_id", "master_contacts", ["legacy_id"], unique=True)
    op.create_index("ix_master_contacts_name", "master_contacts", ["name"])
    op.create_index("ix_master_contacts_name_source", "master_contacts", ["name_source"])
    op.create_index("ix_master_contacts_name_verified", "master_contacts", ["name_verified"])
    op.create_index("ix_master_contacts_last_observed_at", "master_contacts", ["last_observed_at"])
    op.create_index(
        "ix_master_contacts_normalized_mobile",
        "master_contacts",
        ["normalized_mobile"],
        unique=True,
    )

    op.add_column(
        "whatsapp_directory_contacts",
        sa.Column("master_contact_id", sa.Uuid(), nullable=True),
    )
    op.create_index(
        "ix_whatsapp_directory_contacts_master_contact_id",
        "whatsapp_directory_contacts",
        ["master_contact_id"],
    )
    op.create_foreign_key(
        "fk_whatsapp_directory_contacts_master_contact_id",
        "whatsapp_directory_contacts",
        "master_contacts",
        ["master_contact_id"],
        ["id"],
        ondelete="SET NULL",
    )

    # One shared master identity per normalized WhatsApp phone. Existing channel
    # names are retained; otherwise the newest inbound push name seeds an
    # explicitly unverified master name.
    op.execute(
        """
        WITH ranked AS (
            SELECT c.*,
                   regexp_replace(split_part(coalesce(c.phone_jid, ''), '@', 1), '\\D', '', 'g') AS mobile,
                   row_number() OVER (
                       PARTITION BY nullif(regexp_replace(split_part(coalesce(c.phone_jid, ''), '@', 1), '\\D', '', 'g'), '')
                       ORDER BY c.first_seen_at, c.id
                   ) AS mobile_rank
            FROM whatsapp_directory_contacts c
        ), seeds AS (
            SELECT r.*,
                   (
                       SELECT nullif(btrim(m.push_name), '')
                       FROM whatsapp_inbound_messages m
                       WHERE m.directory_contact_id = r.id
                         AND m.from_me IS FALSE
                         AND nullif(btrim(m.push_name), '') IS NOT NULL
                       ORDER BY m.message_timestamp DESC, m.id DESC
                       LIMIT 1
                   ) AS push_name
            FROM ranked r
            WHERE r.mobile IS NULL OR r.mobile = '' OR r.mobile_rank = 1
        ), cleaned AS (
            SELECT seeds.*,
                   regexp_replace(
                       coalesce(nullif(btrim(display_name), ''), ''),
                       '^[^[:alnum:]]+|[^[:alnum:]]+$', '', 'g'
                   ) AS profile_name,
                   regexp_replace(
                       coalesce(push_name, ''),
                       '^[^[:alnum:]]+|[^[:alnum:]]+$', '', 'g'
                   ) AS clean_push_name
            FROM seeds
        )
        INSERT INTO master_contacts (
            id, legacy_id, active, created_at, updated_at, name,
            normalized_mobile, name_source, name_verified, last_observed_at, notes
        )
        SELECT id, NULL, active, first_seen_at, last_seen_at,
               coalesce(nullif(profile_name, ''), nullif(clean_push_name, ''), ''),
               nullif(mobile, ''),
               CASE
                   WHEN nullif(profile_name, '') IS NOT NULL THEN 'whatsapp_profile'
                   WHEN nullif(clean_push_name, '') IS NOT NULL THEN 'whatsapp_push'
                   ELSE 'unknown'
               END,
               false, last_seen_at, ''
        FROM cleaned
        """
    )
    op.execute(
        """
        UPDATE whatsapp_directory_contacts c
        SET master_contact_id = mc.id
        FROM master_contacts mc
        WHERE (
            mc.normalized_mobile IS NOT NULL
            AND mc.normalized_mobile = regexp_replace(split_part(coalesce(c.phone_jid, ''), '@', 1), '\\D', '', 'g')
        ) OR (mc.normalized_mobile IS NULL AND mc.id = c.id)
        """
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_whatsapp_directory_contacts_master_contact_id",
        "whatsapp_directory_contacts",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_whatsapp_directory_contacts_master_contact_id",
        table_name="whatsapp_directory_contacts",
    )
    op.drop_column("whatsapp_directory_contacts", "master_contact_id")
    op.drop_table("master_contacts")

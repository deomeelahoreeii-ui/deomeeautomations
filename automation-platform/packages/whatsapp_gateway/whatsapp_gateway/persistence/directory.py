from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    JSON,
    Text,
    UniqueConstraint,
)
from sqlmodel import Field, SQLModel

from automation_core.time import utcnow

class WhatsAppDirectoryGroup(SQLModel, table=True):
    """A group observed by the gateway, not yet authorized for dispatch."""

    __tablename__ = "whatsapp_directory_groups"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "jid", name="uq_whatsapp_directory_groups_account_jid"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    jid: str = Field(index=True)
    name: str = Field(default="", index=True)
    description: str = Field(default="", sa_column=Column(Text))
    owner_jid: str | None = Field(default=None, index=True)
    participant_count: int = 0
    available: bool = Field(default=True, index=True)
    discovered_at: datetime = Field(default_factory=utcnow, index=True)
    last_seen_at: datetime = Field(default_factory=utcnow, index=True)
    last_synced_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppDirectoryContact(SQLModel, table=True):
    """A normalized WhatsApp identity discovered by the gateway."""

    __tablename__ = "whatsapp_directory_contacts"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "canonical_key", name="uq_whatsapp_contacts_account_key"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    canonical_key: str = Field(index=True)
    phone_jid: str | None = Field(default=None, index=True)
    primary_lid_jid: str | None = Field(default=None, index=True)
    display_name: str = Field(default="", index=True)
    source: str = Field(default="gateway", index=True)
    confidence: float = 0.0
    active: bool = Field(default=True, index=True)
    token_status: str = Field(default="unknown", index=True)
    token_checked_at: datetime | None = Field(default=None, index=True)
    token_issued_at: datetime | None = None
    first_seen_at: datetime = Field(default_factory=utcnow, index=True)
    last_seen_at: datetime = Field(default_factory=utcnow, index=True)
    last_synced_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppIdentityAlias(SQLModel, table=True):
    __tablename__ = "whatsapp_identity_aliases"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "lid_jid", name="uq_whatsapp_identity_aliases_account_lid"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    contact_id: uuid.UUID = Field(
        foreign_key="whatsapp_directory_contacts.id", index=True
    )
    lid_jid: str = Field(index=True)
    source: str = Field(default="gateway", index=True)
    confidence: float = 0.0
    first_seen_at: datetime = Field(default_factory=utcnow)
    last_seen_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppGroupMember(SQLModel, table=True):
    __tablename__ = "whatsapp_group_members"
    __table_args__ = (
        UniqueConstraint(
            "directory_group_id",
            "member_key",
            name="uq_whatsapp_group_members_group_key",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    directory_group_id: uuid.UUID = Field(
        foreign_key="whatsapp_directory_groups.id", index=True
    )
    contact_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_directory_contacts.id", index=True
    )
    member_key: str = Field(index=True)
    member_jid: str | None = Field(default=None, index=True)
    phone_jid: str | None = Field(default=None, index=True)
    lid_jid: str | None = Field(default=None, index=True)
    display_name: str = Field(default="", index=True)
    admin_role: str | None = Field(default=None, index=True)
    active: bool = Field(default=True, index=True)
    first_seen_at: datetime = Field(default_factory=utcnow)
    last_seen_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppGroup(SQLModel, table=True):
    __tablename__ = "whatsapp_groups"
    __table_args__ = (
        UniqueConstraint("account_id", "jid", name="uq_whatsapp_groups_account_jid"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    directory_group_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_directory_groups.id", index=True
    )
    wing_id: uuid.UUID = Field(foreign_key="wings.id", index=True)
    name: str = Field(index=True)
    jid: str = Field(index=True)
    purpose: str = Field(default="reports", index=True)
    enabled: bool = Field(default=True, index=True)
    verified_at: datetime | None = None
    notes: str = Field(default="", sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class WhatsAppContactLink(SQLModel, table=True):
    """Verified link between a discovered WhatsApp identity and master data."""

    __tablename__ = "whatsapp_contact_links"
    __table_args__ = (
        UniqueConstraint(
            "directory_contact_id",
            "entity_type",
            "entity_key",
            name="uq_whatsapp_contact_links_target",
        ),
        CheckConstraint(
            "entity_type IN ('officer', 'school_head')",
            name="ck_whatsapp_contact_links_entity_type",
        ),
        CheckConstraint(
            "status IN ('suggested', 'verified')",
            name="ck_whatsapp_contact_links_status",
        ),
        CheckConstraint(
            "(entity_type = 'officer' AND officer_id IS NOT NULL AND school_head_id IS NULL) "
            "OR (entity_type = 'school_head' AND school_head_id IS NOT NULL AND officer_id IS NULL)",
            name="ck_whatsapp_contact_links_entity",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    directory_contact_id: uuid.UUID = Field(
        foreign_key="whatsapp_directory_contacts.id", index=True
    )
    entity_type: str = Field(index=True)
    entity_key: str = Field(index=True)
    officer_id: uuid.UUID | None = Field(
        default=None, foreign_key="officers.id", index=True
    )
    school_head_id: uuid.UUID | None = Field(
        default=None, foreign_key="school_heads.id", index=True
    )
    wing_id: uuid.UUID = Field(foreign_key="wings.id", index=True)
    status: str = Field(default="verified", index=True)
    source: str = Field(default="manual", index=True)
    confidence: float = 1.0
    active: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


__all__ = [
    'WhatsAppDirectoryGroup',
    'WhatsAppDirectoryContact',
    'WhatsAppIdentityAlias',
    'WhatsAppGroupMember',
    'WhatsAppGroup',
    'WhatsAppContactLink',
]

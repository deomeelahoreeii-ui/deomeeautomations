from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from whatsapp_gateway.inbound.schemas import InboundMessageEvent
from whatsapp_gateway.models import WhatsAppInboundAttachment, WhatsAppInboundMessage


def insert_idempotently(
    session: Session,
    *,
    table: Any,
    values: dict[str, Any],
    constraint_name: str,
    conflict_columns: list[str],
) -> uuid.UUID | None:
    """Insert once and return the new id, or ``None`` after a conflict."""

    dialect_name = session.get_bind().dialect.name
    if dialect_name == "postgresql":
        statement = (
            postgresql_insert(table)
            .values(**values)
            .on_conflict_do_nothing(constraint=constraint_name)
            .returning(table.c.id)
        )
        return session.execute(statement).scalar_one_or_none()

    if dialect_name == "sqlite":
        statement = (
            sqlite_insert(table)
            .values(**values)
            .on_conflict_do_nothing(index_elements=conflict_columns)
            .returning(table.c.id)
        )
        return session.execute(statement).scalar_one_or_none()

    try:
        with session.begin_nested():
            session.execute(table.insert().values(**values))
        return values["id"]
    except IntegrityError:
        return None


def upsert_inbound_message(
    session: Session,
    *,
    account_id: uuid.UUID,
    event: InboundMessageEvent,
    values: dict[str, Any],
    now: datetime,
) -> tuple[uuid.UUID, bool]:
    table = WhatsAppInboundMessage.__table__
    inserted_id = insert_idempotently(
        session,
        table=table,
        values={
            "id": uuid.uuid4(),
            "account_id": account_id,
            "message_id": event.messageId,
            "remote_jid": event.remoteJid,
            "first_ingested_at": now,
            **values,
        },
        constraint_name="uq_whatsapp_inbound_message_identity",
        conflict_columns=["account_id", "remote_jid", "message_id"],
    )
    if inserted_id is not None:
        return inserted_id, True

    existing_id = session.execute(
        update(table)
        .where(
            table.c.account_id == account_id,
            table.c.remote_jid == event.remoteJid,
            table.c.message_id == event.messageId,
        )
        .values(**values)
        .returning(table.c.id)
    ).scalar_one_or_none()
    if existing_id is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The inbound message conflicted but could not be reloaded",
        )
    return existing_id, False


def upsert_inbound_attachment(
    session: Session,
    *,
    message_id: uuid.UUID,
    mapped: dict[str, Any],
    now: datetime,
) -> uuid.UUID:
    table = WhatsAppInboundAttachment.__table__
    inserted_id = insert_idempotently(
        session,
        table=table,
        values={
            "id": uuid.uuid4(),
            "message_id": message_id,
            "created_at": now,
            **mapped,
        },
        constraint_name="uq_whatsapp_inbound_attachment_message",
        conflict_columns=["message_id"],
    )
    if inserted_id is not None:
        return inserted_id

    existing_id = session.execute(
        update(table)
        .where(table.c.message_id == message_id)
        .values(**mapped)
        .returning(table.c.id)
    ).scalar_one_or_none()
    if existing_id is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The inbound attachment conflicted but could not be reloaded",
        )
    return existing_id

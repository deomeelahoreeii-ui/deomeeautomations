import uuid
from datetime import datetime, timezone

from sqlmodel import Session, SQLModel, create_engine

from master_data.models import MasterContact
from whatsapp_gateway.directory.master_contacts import (
    ensure_master_contact,
    resolved_contact_dict,
    set_directory_master_name,
)
from whatsapp_gateway.inbound.ingestion import message_values
from whatsapp_gateway.inbound.schemas import InboundMessageEvent
from whatsapp_gateway.models import WhatsAppDirectoryContact


def test_master_contact_name_precedence_and_phone_reuse() -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        first = WhatsAppDirectoryContact(
            account_id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
            canonical_key="923001234567@s.whatsapp.net",
            phone_jid="923001234567@s.whatsapp.net",
        )
        session.add(first)
        session.flush()

        observed = ensure_master_contact(
            session,
            first,
            observed_name="  *WhatsApp   Alias* ",
            name_source="whatsapp_push",
        )
        assert observed.name == "WhatsApp Alias"
        assert observed.name_verified is False
        assert first.master_contact_id == observed.id

        ensure_master_contact(
            session,
            first,
            observed_name=".",
            name_source="whatsapp_push",
        )
        assert observed.name == "WhatsApp Alias"

        set_directory_master_name(session, first, "Verified Person")
        ensure_master_contact(
            session,
            first,
            observed_name="Changed Sender Name",
            name_source="whatsapp_push",
        )
        session.flush()

        stored = session.get(MasterContact, observed.id)
        assert stored is not None
        assert stored.name == "Verified Person"
        assert stored.name_source == "manual"
        assert stored.name_verified is True

        second = WhatsAppDirectoryContact(
            account_id=uuid.UUID("00000000-0000-0000-0000-000000000002"),
            canonical_key="923001234567@s.whatsapp.net",
            phone_jid="923001234567@s.whatsapp.net",
        )
        session.add(second)
        session.flush()
        reused = ensure_master_contact(
            session,
            second,
            observed_name="Another Weak Alias",
            name_source="whatsapp_profile",
        )

        assert reused.id == stored.id
        assert reused.name == "Verified Person"
        assert resolved_contact_dict(second, reused)["display_name"] == "Verified Person"


def test_outgoing_push_name_is_not_contact_name_evidence() -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    account_id = uuid.UUID("00000000-0000-0000-0000-000000000001")
    with Session(engine) as session:
        contact = WhatsAppDirectoryContact(
            account_id=account_id,
            canonical_key="923360249999@s.whatsapp.net",
            phone_jid="923360249999@s.whatsapp.net",
        )
        session.add(contact)
        session.flush()
        event = InboundMessageEvent(
            workerId="default",
            messageId="outgoing-1",
            remoteJid=contact.phone_jid,
            senderJid=contact.phone_jid,
            fromMe=True,
            chatScope="direct",
            messageTimestamp=datetime.now(timezone.utc),
            pushName="Operator Name",
            messageType="conversation",
            ingestionSource="live",
            payloadSha256="a" * 64,
        )
        values = message_values(
            session,
            event=event,
            account_id=account_id,
            now=datetime.now(timezone.utc),
        )
        assert values["directory_contact_id"] == contact.id
        assert values["push_name"] is None

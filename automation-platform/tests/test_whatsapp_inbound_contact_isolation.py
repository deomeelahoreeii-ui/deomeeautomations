from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.time import utcnow
from whatsapp_gateway.identity_repair import repair_inbound_message_identities
from whatsapp_gateway.inbound_service import find_matching_attachments
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
    WhatsAppIdentityAlias,
    WhatsAppInboundAttachment,
    WhatsAppInboundMessage,
)


def _engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _add_file(
    session: Session,
    *,
    account_id,
    message_id: str,
    remote_jid: str,
    sender_jid: str,
    participant_jid: str | None,
    scope: str,
    assigned_contact_id=None,
    offset_minutes: int = 0,
) -> WhatsAppInboundMessage:
    message = WhatsAppInboundMessage(
        account_id=account_id,
        worker_key="default",
        message_id=message_id,
        remote_jid=remote_jid,
        participant_jid=participant_jid,
        sender_jid=sender_jid,
        directory_contact_id=assigned_contact_id,
        from_me=False,
        chat_scope=scope,
        message_timestamp=utcnow() + timedelta(minutes=offset_minutes),
        message_type="documentMessage",
        ingestion_source="live",
        payload_sha256=(message_id[0] if message_id else "0") * 64,
        raw_payload={},
    )
    session.add(message)
    session.flush()
    session.add(
        WhatsAppInboundAttachment(
            message_id=message.id,
            media_kind="document",
            message_key="documentMessage",
            original_filename=f"{message_id}.pdf",
            mime_type="application/pdf",
        )
    )
    return message


def test_contact_scan_is_account_scoped_sender_scoped_and_read_only() -> None:
    engine = _engine()
    with Session(engine) as session:
        account = WhatsAppAccount(name="Primary", worker_key="default")
        other_account = WhatsAppAccount(name="Other", worker_key="other")
        session.add(account)
        session.add(other_account)
        session.flush()

        contact_a = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key="contact-a",
            phone_jid="111@s.whatsapp.net",
            primary_lid_jid="111@lid",
            display_name="Contact A",
        )
        contact_b = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key="contact-b",
            phone_jid="222@s.whatsapp.net",
            primary_lid_jid="222@lid",
            display_name="Contact B",
        )
        other_contact = WhatsAppDirectoryContact(
            account_id=other_account.id,
            canonical_key="other-a",
            phone_jid="111@s.whatsapp.net",
            display_name="Other Account A",
        )
        session.add(contact_a)
        session.add(contact_b)
        session.add(other_contact)
        session.flush()
        session.add(
            WhatsAppIdentityAlias(
                account_id=account.id,
                contact_id=contact_a.id,
                lid_jid="111-alias@lid",
            )
        )

        direct_a = _add_file(
            session,
            account_id=account.id,
            message_id="a-direct",
            remote_jid="111@s.whatsapp.net",
            sender_jid="111@s.whatsapp.net",
            participant_jid=None,
            scope="direct",
        )
        _add_file(
            session,
            account_id=account.id,
            message_id="b-direct",
            remote_jid="222@s.whatsapp.net",
            sender_jid="222@s.whatsapp.net",
            participant_jid=None,
            scope="direct",
        )
        _add_file(
            session,
            account_id=account.id,
            message_id="a-group",
            remote_jid="group-1@g.us",
            sender_jid="111-alias@lid",
            participant_jid="111-alias@lid",
            scope="group",
        )
        # Deliberately contaminated assignment: identity must win over the
        # previously stored directory_contact_id.
        _add_file(
            session,
            account_id=account.id,
            message_id="b-group-wrongly-assigned-a",
            remote_jid="group-1@g.us",
            sender_jid="222@lid",
            participant_jid="222@lid",
            scope="group",
            assigned_contact_id=contact_a.id,
        )
        _add_file(
            session,
            account_id=account.id,
            message_id="unknown-group",
            remote_jid="111@s.whatsapp.net",
            sender_jid="999@lid",
            participant_jid="999@lid",
            scope="group",
        )
        _add_file(
            session,
            account_id=other_account.id,
            message_id="other-account-a",
            remote_jid="111@s.whatsapp.net",
            sender_jid="111@s.whatsapp.net",
            participant_jid=None,
            scope="direct",
        )
        session.commit()

        direct = find_matching_attachments(
            session,
            contact_id=contact_a.id,
            date_from=None,
            date_to=None,
            chat_scope="direct",
            media_types=["pdf"],
        )
        all_scopes = find_matching_attachments(
            session,
            contact_id=contact_a.id,
            date_from=None,
            date_to=None,
            chat_scope="direct_and_groups",
            media_types=["pdf"],
        )
        session.refresh(direct_a)

    assert [message.message_id for _, message, _ in direct] == ["a-direct"]
    assert {message.message_id for _, message, _ in all_scopes} == {
        "a-direct",
        "a-group",
    }
    assert direct_a.directory_contact_id is None


def test_identity_repair_corrects_assigns_and_clears_unproved_ownership(
    tmp_path: Path,
) -> None:
    engine = _engine()
    with Session(engine) as session:
        account = WhatsAppAccount(name="Primary", worker_key="default")
        session.add(account)
        session.flush()
        contact_a = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key="a",
            phone_jid="111@s.whatsapp.net",
        )
        contact_b = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key="b",
            primary_lid_jid="222@lid",
        )
        contact_c = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key="c",
            phone_jid="shared@s.whatsapp.net",
        )
        contact_d = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key="d",
            primary_lid_jid="shared@s.whatsapp.net",
        )
        session.add(contact_a)
        session.add(contact_b)
        session.add(contact_c)
        session.add(contact_d)
        session.flush()

        corrected = _add_file(
            session,
            account_id=account.id,
            message_id="corrected",
            remote_jid="111@s.whatsapp.net",
            sender_jid="111@s.whatsapp.net",
            participant_jid=None,
            scope="direct",
            assigned_contact_id=contact_b.id,
        )
        assigned = _add_file(
            session,
            account_id=account.id,
            message_id="assigned",
            remote_jid="group@g.us",
            sender_jid="222@lid",
            participant_jid="222@lid",
            scope="group",
        )
        cleared = _add_file(
            session,
            account_id=account.id,
            message_id="cleared",
            remote_jid="group@g.us",
            sender_jid="unknown@lid",
            participant_jid="unknown@lid",
            scope="group",
            assigned_contact_id=contact_a.id,
        )
        ambiguous = _add_file(
            session,
            account_id=account.id,
            message_id="ambiguous",
            remote_jid="shared@s.whatsapp.net",
            sender_jid="shared@s.whatsapp.net",
            participant_jid=None,
            scope="direct",
            assigned_contact_id=contact_c.id,
        )
        session.commit()

        result = repair_inbound_message_identities(
            session,
            apply=True,
            output_dir=tmp_path,
        )
        session.refresh(corrected)
        session.refresh(assigned)
        session.refresh(cleared)
        session.refresh(ambiguous)

    assert corrected.directory_contact_id == contact_a.id
    assert assigned.directory_contact_id == contact_b.id
    assert cleared.directory_contact_id is None
    assert ambiguous.directory_contact_id is None
    assert result["counts"]["corrected"] == 1
    assert result["counts"]["assigned"] == 1
    assert result["counts"]["cleared"] == 2
    assert result["counts"]["ambiguous"] == 1
    assert Path(result["backup_path"]).is_file()
    assert Path(result["report_json"]).is_file()
    assert Path(result["report_csv"]).is_file()

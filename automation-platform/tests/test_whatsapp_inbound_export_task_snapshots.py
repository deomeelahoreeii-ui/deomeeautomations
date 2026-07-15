from __future__ import annotations

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.time import utcnow
from whatsapp_gateway.inbound_tasks import (
    _snapshot_account,
    _snapshot_attachment,
    _snapshot_message,
)
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppInboundAttachment,
    WhatsAppInboundMessage,
)


def test_export_task_snapshots_survive_session_close() -> None:
    test_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(test_engine)

    with Session(test_engine) as session:
        account = WhatsAppAccount(
            name="Default",
            worker_key="default",
            health_subject="whatsapp.worker.health",
        )
        session.add(account)
        session.flush()

        message = WhatsAppInboundMessage(
            account_id=account.id,
            worker_key="default",
            message_id="message-detached",
            remote_jid="923360249999@s.whatsapp.net",
            sender_jid="923360249999@s.whatsapp.net",
            from_me=False,
            chat_scope="direct",
            message_timestamp=utcnow(),
            message_type="documentMessage",
            ingestion_source="live",
            payload_sha256="3" * 64,
            raw_payload={},
        )
        session.add(message)
        session.flush()

        attachment = WhatsAppInboundAttachment(
            message_id=message.id,
            media_kind="document",
            message_key="documentMessage",
            original_filename="complaint.pdf",
            mime_type="application/pdf",
        )
        session.add(attachment)
        session.commit()

        account_snapshot = _snapshot_account(account)
        attachment_snapshot = _snapshot_attachment(attachment)
        message_snapshot = _snapshot_message(message)
        expected_attachment_id = attachment.id

    # Accessing ORM objects here would raise DetachedInstanceError because the
    # Session is closed.  Value snapshots must remain safe for the Celery task.
    assert account_snapshot.worker_key == "default"
    assert account_snapshot.health_subject == "whatsapp.worker.health"
    assert attachment_snapshot.id == expected_attachment_id
    assert attachment_snapshot.original_filename == "complaint.pdf"
    assert attachment_snapshot.mime_type == "application/pdf"
    assert message_snapshot.remote_jid == "923360249999@s.whatsapp.net"
    assert message_snapshot.message_id == "message-detached"

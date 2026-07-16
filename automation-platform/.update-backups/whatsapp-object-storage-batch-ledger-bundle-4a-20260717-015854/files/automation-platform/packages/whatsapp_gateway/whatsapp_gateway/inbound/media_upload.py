from __future__ import annotations

import hashlib
import hmac
import os
import uuid
from pathlib import Path
from typing import Any

from fastapi import Depends, Header, HTTPException, Request, status
from sqlmodel import Session

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.time import utcnow
from whatsapp_gateway.inbound.media_types import detect_file_type
from whatsapp_gateway.models import WhatsAppInboundAttachment, WhatsAppInboundMessage


async def upload_attachment_content(
    attachment_id: uuid.UUID,
    request: Request,
    x_content_sha256: str | None = Header(default=None),
    x_declared_mime_type: str | None = Header(default=None),
    x_whatsapp_worker_id: str | None = Header(default=None),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    attachment = session.get(WhatsAppInboundAttachment, attachment_id)
    if attachment is None:
        raise HTTPException(status_code=404, detail="Inbound attachment not found")
    message = session.get(WhatsAppInboundMessage, attachment.message_id)
    if message is None:
        raise HTTPException(status_code=409, detail="Inbound message is missing")
    if not x_whatsapp_worker_id:
        raise HTTPException(status_code=400, detail="X-WhatsApp-Worker-ID is required")
    if not hmac.compare_digest(x_whatsapp_worker_id, message.worker_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The worker is not authorized for this inbound message",
        )
    if not x_content_sha256 or len(x_content_sha256) != 64:
        raise HTTPException(
            status_code=400,
            detail="A valid X-Content-SHA256 header is required",
        )

    root = settings.whatsapp_inbound_media_root / str(attachment.id)
    root.mkdir(parents=True, exist_ok=True)
    temporary = root / f"upload-{uuid.uuid4().hex}.part"
    digest = hashlib.sha256()
    size = 0
    try:
        with temporary.open("wb") as handle:
            async for chunk in request.stream():
                if not chunk:
                    continue
                size += len(chunk)
                if size > settings.whatsapp_inbound_media_max_bytes:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=(
                            "Inbound attachment exceeds the configured maximum of "
                            f"{settings.whatsapp_inbound_media_max_bytes} bytes"
                        ),
                    )
                digest.update(chunk)
                handle.write(chunk)
        if size == 0:
            raise HTTPException(status_code=400, detail="Worker uploaded an empty file")
        actual_sha256 = digest.hexdigest()
        if x_content_sha256 and not hmac.compare_digest(
            x_content_sha256.lower(), actual_sha256
        ):
            raise HTTPException(status_code=409, detail="Inbound media checksum mismatch")
        try:
            detected_mime, category, extension = detect_file_type(
                temporary,
                declared_mime=x_declared_mime_type or attachment.mime_type,
                original_filename=attachment.original_filename,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=str(exc),
            ) from exc

        final_path = root / f"attachment-{attachment.id}{extension}"
        os.replace(temporary, final_path)
        previous_path = Path(attachment.stored_path) if attachment.stored_path else None
        if previous_path and previous_path != final_path and previous_path.is_file():
            previous_path.unlink(missing_ok=True)
        attachment.detected_mime_type = detected_mime
        attachment.media_category = category
        attachment.safe_extension = extension
        attachment.actual_size = size
        attachment.actual_sha256 = actual_sha256
        attachment.stored_path = str(final_path)
        attachment.download_status = "archived"
        attachment.last_error = None
        attachment.archived_at = utcnow()
        attachment.updated_at = utcnow()
        session.add(attachment)
        session.commit()
        return {
            "uploaded": True,
            "attachment_id": str(attachment.id),
            "size_bytes": size,
            "sha256": actual_sha256,
            "detected_mime_type": detected_mime,
            "media_category": category,
        }
    finally:
        temporary.unlink(missing_ok=True)

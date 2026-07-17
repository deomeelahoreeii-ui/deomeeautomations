from __future__ import annotations

from pathlib import Path

from sqlmodel import Session

from automation_core.config import Settings
from automation_core.object_storage import ObjectStorageError, S3ObjectStorage
from crm_filters.paperless import PaperlessClient
from whatsapp_gateway.inbound.processing import classification_document
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatchItem,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundStoredObject,
)


def paperless_client(settings: Settings) -> PaperlessClient:
    return PaperlessClient(
        base_url=settings.paperless_url,
        username=settings.paperless_username,
        password=settings.paperless_password,
        token=settings.paperless_token,
        verify_ssl=settings.paperless_verify_ssl,
        ca_bundle=settings.paperless_ca_bundle,
        allow_insecure_fallback=settings.paperless_allow_insecure_fallback,
        timeout_seconds=settings.paperless_timeout_seconds,
        document_type_name=settings.paperless_document_type_complaint,
        attachment_type_name=settings.paperless_document_type_attachment,
        correspondent_name=settings.paperless_correspondent_name,
        max_pages=settings.paperless_max_pages,
        task_timeout_seconds=settings.paperless_task_timeout_seconds,
    )


def paperless_configured(settings: Settings) -> bool:
    return bool(
        settings.paperless_url
        and (
            settings.paperless_token
            or (settings.paperless_username and settings.paperless_password)
        )
    )


def materialize_source(
    *,
    session: Session,
    item: WhatsAppInboundProcessingItem,
    destination: Path,
    settings: Settings,
    storage: S3ObjectStorage,
) -> Path:
    attachment = session.get(WhatsAppInboundAttachment, item.attachment_id)
    batch_item = session.get(WhatsAppInboundBatchItem, item.batch_item_id)
    if attachment is None or batch_item is None:
        raise RuntimeError("The inbound attachment or batch item is unavailable")
    if item.stored_object_id:
        stored = session.get(WhatsAppInboundStoredObject, item.stored_object_id)
        if stored is None:
            raise RuntimeError("The RustFS object ledger record is unavailable")
        result = storage.download_file(
            bucket=stored.bucket,
            object_key=stored.object_key,
            destination=destination,
        )
        if result["size_bytes"] != stored.size_bytes:
            raise ObjectStorageError("Downloaded object size does not match the storage ledger")
        if stored.sha256 and result["sha256"] != stored.sha256:
            raise ObjectStorageError("Downloaded object checksum does not match the storage ledger")
        return destination
    if attachment.stored_path:
        source = Path(attachment.stored_path)
        if source.is_file():
            destination.write_bytes(source.read_bytes())
            return destination
    raise RuntimeError("The file is unavailable in RustFS and the compatibility archive")


def write_derived_record(
    *,
    session: Session,
    item: WhatsAppInboundProcessingItem,
    settings: Settings,
    storage: S3ObjectStorage,
) -> None:
    if not settings.object_storage_enabled:
        return
    key = f"classification/{item.run_id}/{item.id}.json"
    storage.put_bytes(
        bucket=settings.object_storage_derived_bucket,
        object_key=key,
        body=classification_document(item),
        content_type="application/json",
        metadata={
            "processing-item-id": str(item.id),
            "attachment-id": str(item.attachment_id),
            "category": item.primary_category,
            "complaint-number": item.detected_complaint_number or "",
        },
    )
    item.derived_object_key = key
    session.add(item)

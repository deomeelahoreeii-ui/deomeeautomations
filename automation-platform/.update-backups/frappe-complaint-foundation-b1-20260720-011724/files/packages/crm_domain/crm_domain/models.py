from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, Column, JSON, Text, UniqueConstraint
from sqlmodel import Field, SQLModel

from automation_core.time import utcnow


class ComplaintCase(SQLModel, table=True):
    __tablename__ = "crm_complaint_cases"
    __table_args__ = (
        UniqueConstraint("source_system", "complaint_number", name="uq_crm_case_source_number"),
        CheckConstraint(
            "state IN ('candidate', 'review_required', 'fresh', 'existing', 'publishing', 'published', 'rejected')",
            name="ck_crm_complaint_cases_state",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    source_system: str = Field(default="crm_portal", index=True)
    complaint_number: str | None = Field(default=None, index=True)
    state: str = Field(default="candidate", index=True)
    complainant_name: str | None = Field(default=None, index=True)
    complainant_mobile: str | None = Field(default=None, index=True)
    complainant_cnic: str | None = Field(default=None, index=True)
    complainant_address: str | None = Field(default=None, sa_column=Column(Text))
    district: str | None = Field(default=None, index=True)
    tehsil: str | None = Field(default=None, index=True)
    department: str | None = Field(default=None, index=True)
    category: str | None = Field(default=None, index=True)
    sub_category: str | None = Field(default=None, index=True)
    remarks: str | None = Field(default=None, sa_column=Column(Text))
    canonical_paperless_document_id: int | None = Field(default=None, index=True)
    version: int = 1
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class ComplaintDocument(SQLModel, table=True):
    __tablename__ = "crm_complaint_documents"
    __table_args__ = (
        UniqueConstraint("source_processing_item_id", name="uq_crm_document_processing_item"),
        CheckConstraint(
            "role IN ('main_complaint', 'complaint_details', 'attachment', 'reply', 'report', 'unclassified')",
            name="ck_crm_complaint_documents_role",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_cases.id", index=True)
    source_processing_item_id: uuid.UUID = Field(foreign_key="whatsapp_inbound_processing_items.id", index=True)
    source_attachment_id: uuid.UUID | None = Field(default=None, foreign_key="whatsapp_inbound_attachments.id", index=True)
    source_message_id: uuid.UUID | None = Field(default=None, foreign_key="whatsapp_inbound_messages.id", index=True)
    source_sha256: str | None = Field(default=None, index=True)
    original_filename: str | None = Field(default=None, index=True)
    mime_type: str | None = Field(default=None, index=True)
    role: str = Field(default="unclassified", index=True)
    relationship_confidence: float = 0.0
    relationship_reason: str | None = Field(default=None, sa_column=Column(Text))
    paperless_document_id: int | None = Field(default=None, index=True)
    review_state: str = Field(default="pending", index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class ComplaintDocumentCaseLink(SQLModel, table=True):
    __tablename__ = "crm_complaint_document_case_links"
    __table_args__ = (
        UniqueConstraint(
            "complaint_document_id", "complaint_case_id", "source_locator",
            name="uq_crm_document_case_source",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_document_id: uuid.UUID = Field(
        foreign_key="crm_complaint_documents.id", index=True
    )
    complaint_case_id: uuid.UUID = Field(foreign_key="crm_complaint_cases.id", index=True)
    role: str = Field(default="evidence", index=True)
    review_state: str = Field(default="pending", index=True)
    confidence: float = 0.0
    reason: str | None = Field(default=None, sa_column=Column(Text))
    source_locator: str = Field(default="document", index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class DocumentExtraction(SQLModel, table=True):
    __tablename__ = "crm_document_extractions"
    __table_args__ = (
        UniqueConstraint(
            "complaint_document_id", "extractor_name", "extractor_version", "content_sha256",
            name="uq_crm_document_extraction_version",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_document_id: uuid.UUID = Field(foreign_key="crm_complaint_documents.id", index=True)
    extractor_name: str = Field(index=True)
    extractor_version: str = Field(index=True)
    extraction_method: str = Field(index=True)
    content_sha256: str = Field(index=True)
    raw_text: str = Field(default="", sa_column=Column(Text, nullable=False))
    normalized_text: str = Field(default="", sa_column=Column(Text, nullable=False))
    metadata_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    quality_score: float = 0.0
    error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)


class ComplaintFieldObservation(SQLModel, table=True):
    __tablename__ = "crm_complaint_field_observations"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_cases.id", index=True)
    extraction_id: uuid.UUID = Field(foreign_key="crm_document_extractions.id", index=True)
    field_name: str = Field(index=True)
    raw_value: str = Field(sa_column=Column(Text, nullable=False))
    normalized_value: str = Field(sa_column=Column(Text, nullable=False))
    confidence: float = 0.0
    source_locator: str | None = Field(default=None, index=True)
    decision: str = Field(default="observed", index=True)
    reviewed_by: str | None = Field(default=None, index=True)
    reviewed_at: datetime | None = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class ComplaintMatch(SQLModel, table=True):
    __tablename__ = "crm_complaint_matches"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID = Field(foreign_key="crm_complaint_cases.id", index=True)
    processing_item_id: uuid.UUID | None = Field(default=None, foreign_key="whatsapp_inbound_processing_items.id", index=True)
    matched_case_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_cases.id", index=True)
    paperless_document_id: int | None = Field(default=None, index=True)
    proposed_decision: str = Field(index=True)
    final_decision: str | None = Field(default=None, index=True)
    score: float = 0.0
    reason: str = Field(default="", sa_column=Column(Text, nullable=False))
    signals_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    created_at: datetime = Field(default_factory=utcnow, index=True)


class PaperlessPublication(SQLModel, table=True):
    __tablename__ = "crm_paperless_publications"
    __table_args__ = (UniqueConstraint("idempotency_key", name="uq_crm_paperless_publication_key"),)

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID = Field(foreign_key="crm_complaint_cases.id", index=True)
    complaint_document_id: uuid.UUID = Field(foreign_key="crm_complaint_documents.id", index=True)
    idempotency_key: str = Field(index=True)
    state: str = Field(default="pending", index=True)
    intended_fields_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    paperless_task_id: str | None = Field(default=None, index=True)
    paperless_document_id: int | None = Field(default=None, index=True)
    attempts: int = 0
    last_error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class ComplaintReply(SQLModel, table=True):
    __tablename__ = "crm_complaint_replies"
    __table_args__ = (
        UniqueConstraint("complaint_case_id", name="uq_crm_reply_case"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID = Field(
        foreign_key="crm_complaint_cases.id", index=True
    )
    reply_text: str = Field(sa_column=Column(Text, nullable=False))
    source_filename: str = Field(index=True)
    source_row: int
    version: int = 1
    imported_at: datetime = Field(default_factory=utcnow, index=True)
    generated_at: datetime | None = Field(default=None, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)

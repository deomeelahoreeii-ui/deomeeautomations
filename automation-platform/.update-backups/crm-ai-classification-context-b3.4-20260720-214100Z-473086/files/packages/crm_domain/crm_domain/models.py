from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, CheckConstraint, Column, JSON, Text, UniqueConstraint
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
        CheckConstraint(
            "frappe_sync_status IN ('not_synced', 'pending', 'synchronized', 'update_pending', 'failed')",
            name="ck_crm_complaint_cases_frappe_sync_status",
        ),
        CheckConstraint(
            "frappe_routing_status IN ('not_routed', 'planned', 'routed', 'manual_preserved', 'failed')",
            name="ck_crm_complaint_cases_frappe_routing_status",
        ),
        CheckConstraint(
            "classification_sync_status IN ('not_synced', 'pending', 'synchronized', 'failed')",
            name="ck_crm_complaint_cases_classification_sync_status",
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
    category_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_complaint_categories.id", index=True
    )
    sub_category_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_complaint_subcategories.id", index=True
    )
    classification_sync_status: str = Field(default="not_synced", index=True)
    classification_last_synced_at: datetime | None = Field(default=None, index=True)
    classification_last_error: str | None = Field(default=None, sa_column=Column(Text))
    remarks: str | None = Field(default=None, sa_column=Column(Text))
    canonical_paperless_document_id: int | None = Field(default=None, index=True)
    frappe_ticket_id: str | None = Field(default=None, index=True)
    frappe_ticket_url: str | None = Field(default=None, sa_column=Column(Text))
    frappe_sync_status: str = Field(default="not_synced", index=True)
    frappe_last_synced_at: datetime | None = Field(default=None, index=True)
    frappe_last_error: str | None = Field(default=None, sa_column=Column(Text))
    frappe_remote_modified_at: datetime | None = Field(default=None, index=True)
    frappe_sync_version: int = 0
    frappe_payload_hash: str | None = Field(default=None, index=True)
    frappe_workflow_status: str | None = Field(default=None, index=True)
    frappe_agent_group: str | None = Field(default=None, index=True)
    frappe_assigned_users_json: list[str] = Field(
        default_factory=list, sa_column=Column(JSON, nullable=False)
    )
    frappe_assigned_officer_id: uuid.UUID | None = Field(
        default=None, foreign_key="officers.id", index=True
    )
    frappe_assigned_officer_name: str | None = Field(default=None, index=True)
    frappe_assigned_officer_role: str | None = Field(default=None, index=True)
    frappe_routing_status: str = Field(default="not_routed", index=True)
    frappe_routing_reason: str | None = Field(default=None, sa_column=Column(Text))
    frappe_last_workflow_pull_at: datetime | None = Field(default=None, index=True)
    frappe_reply_approval_status: str | None = Field(default=None, index=True)
    frappe_disposal_outcome: str | None = Field(default=None, index=True)
    frappe_ai_eligible: bool = Field(
        default=False, sa_column=Column(Boolean, nullable=False, default=False)
    )
    frappe_workflow_hash: str | None = Field(default=None, index=True)
    frappe_reply_hash: str | None = Field(default=None, index=True)
    frappe_reply_text_snapshot: str | None = Field(default=None, sa_column=Column(Text))
    frappe_inquiry_findings: str | None = Field(default=None, sa_column=Column(Text))
    frappe_school_version: str | None = Field(default=None, sa_column=Column(Text))
    frappe_applicable_policy: str | None = Field(default=None, sa_column=Column(Text))
    version: int = 1
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class ComplaintCategory(SQLModel, table=True):
    __tablename__ = "crm_complaint_categories"
    __table_args__ = (
        UniqueConstraint("normalized_name", name="uq_crm_complaint_categories_normalized_name"),
        CheckConstraint(
            "frappe_sync_status IN ('pending', 'synchronized', 'failed', 'not_required')",
            name="ck_crm_complaint_categories_frappe_sync_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    name: str = Field(index=True, max_length=140)
    normalized_name: str = Field(index=True, max_length=140)
    description: str | None = Field(default=None, sa_column=Column(Text))
    display_order: int = Field(default=100, index=True)
    active: bool = Field(default=True, sa_column=Column(Boolean, nullable=False, default=True, index=True))
    default_ai_eligible: bool = Field(
        default=False, sa_column=Column(Boolean, nullable=False, default=False)
    )
    reply_guidance: str | None = Field(default=None, sa_column=Column(Text))
    policy_notes: str | None = Field(default=None, sa_column=Column(Text))
    frappe_ticket_type_name: str = Field(default="", index=True, max_length=140)
    frappe_sync_status: str = Field(default="pending", index=True)
    frappe_last_synced_at: datetime | None = Field(default=None, index=True)
    frappe_last_error: str | None = Field(default=None, sa_column=Column(Text))
    merged_into_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_complaint_categories.id", index=True
    )
    version: int = 1
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class ComplaintSubcategory(SQLModel, table=True):
    __tablename__ = "crm_complaint_subcategories"
    __table_args__ = (
        UniqueConstraint(
            "category_id", "normalized_name", name="uq_crm_complaint_subcategories_category_name"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    category_id: uuid.UUID = Field(
        foreign_key="crm_complaint_categories.id", index=True
    )
    name: str = Field(index=True, max_length=140)
    normalized_name: str = Field(index=True, max_length=140)
    description: str | None = Field(default=None, sa_column=Column(Text))
    display_order: int = Field(default=100, index=True)
    active: bool = Field(default=True, sa_column=Column(Boolean, nullable=False, default=True, index=True))
    reply_guidance: str | None = Field(default=None, sa_column=Column(Text))
    policy_notes: str | None = Field(default=None, sa_column=Column(Text))
    merged_into_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_complaint_subcategories.id", index=True
    )
    version: int = 1
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class ComplaintAuditEvent(SQLModel, table=True):
    __tablename__ = "crm_complaint_audit_events"
    __table_args__ = (
        CheckConstraint(
            "state IN ('started', 'succeeded', 'failed', 'partial')",
            name="ck_crm_complaint_audit_events_state",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_complaint_cases.id", index=True
    )
    entity_type: str = Field(index=True, max_length=80)
    entity_id: str = Field(default="", index=True, max_length=140)
    event_type: str = Field(index=True, max_length=120)
    state: str = Field(default="succeeded", index=True)
    actor: str = Field(default="web-operator", index=True, max_length=120)
    before_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    after_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    details_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)


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


class CrmBulkOperationBatch(SQLModel, table=True):
    __tablename__ = "crm_bulk_operation_batches"
    __table_args__ = (
        UniqueConstraint("batch_number", name="uq_crm_bulk_operation_batch_number"),
        CheckConstraint(
            "operation_type IN ('reply_export', 'reply_import', 'formal_letters')",
            name="ck_crm_bulk_operation_batches_type",
        ),
        CheckConstraint(
            "status IN ('draft', 'validating', 'ready', 'processing', 'completed', "
            "'completed_with_errors', 'failed', 'cancelled')",
            name="ck_crm_bulk_operation_batches_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    batch_number: str = Field(index=True, max_length=48)
    operation_type: str = Field(index=True, max_length=32)
    status: str = Field(default="draft", index=True, max_length=32)
    parent_batch_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_bulk_operation_batches.id", index=True
    )
    source_filename: str | None = Field(default=None, index=True, max_length=255)
    source_sha256: str | None = Field(default=None, index=True, max_length=64)
    scope_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    settings_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    total_items: int = 0
    valid_items: int = 0
    successful_items: int = 0
    failed_items: int = 0
    skipped_items: int = 0
    duplicate_items: int = 0
    created_by: str = Field(default="web-operator", index=True, max_length=120)
    error_summary: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)
    started_at: datetime | None = Field(default=None, index=True)
    completed_at: datetime | None = Field(default=None, index=True)


class CrmBulkOperationItem(SQLModel, table=True):
    __tablename__ = "crm_bulk_operation_items"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'exported', 'valid', 'invalid', 'imported', 'updated', "
            "'unchanged', 'generated', 'failed', 'skipped', 'missing')",
            name="ck_crm_bulk_operation_items_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    batch_id: uuid.UUID = Field(
        foreign_key="crm_bulk_operation_batches.id", index=True
    )
    complaint_case_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_complaint_cases.id", index=True
    )
    complaint_number_snapshot: str = Field(default="", index=True, max_length=80)
    source_row: int | None = Field(default=None, index=True)
    status: str = Field(default="pending", index=True, max_length=32)
    error_code: str | None = Field(default=None, index=True, max_length=80)
    error_message: str | None = Field(default=None, sa_column=Column(Text))
    reply_version_before: int | None = None
    reply_version_after: int | None = None
    reply_content_hash: str | None = Field(default=None, index=True, max_length=64)
    output_filename: str | None = Field(default=None, max_length=500)
    output_sha256: str | None = Field(default=None, index=True, max_length=64)
    details_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    created_at: datetime = Field(default_factory=utcnow, index=True)
    processed_at: datetime | None = Field(default=None, index=True)


class CrmBulkOperationArtifact(SQLModel, table=True):
    __tablename__ = "crm_bulk_operation_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "batch_id", "kind", "name", name="uq_crm_bulk_operation_artifact"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    batch_id: uuid.UUID = Field(
        foreign_key="crm_bulk_operation_batches.id", index=True
    )
    kind: str = Field(index=True, max_length=80)
    name: str = Field(max_length=255)
    path: str = Field(sa_column=Column(Text, nullable=False))
    content_type: str = Field(default="application/octet-stream", max_length=160)
    size_bytes: int = 0
    sha256: str = Field(default="", index=True, max_length=64)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class ComplaintReplyRevision(SQLModel, table=True):
    __tablename__ = "crm_complaint_reply_revisions"
    __table_args__ = (
        UniqueConstraint(
            "complaint_case_id", "content_hash", name="uq_crm_reply_revision_case_hash"
        ),
        CheckConstraint(
            "approval_status IN ('Approved', 'Issued')",
            name="ck_crm_reply_revision_approval_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID = Field(
        foreign_key="crm_complaint_cases.id", index=True
    )
    reply_text: str = Field(sa_column=Column(Text, nullable=False))
    content_hash: str = Field(index=True)
    source_system: str = Field(default="frappe_helpdesk", index=True)
    source_reference: str = Field(default="", index=True)
    approval_status: str = Field(index=True)
    remote_modified_at: datetime | None = Field(default=None, index=True)
    captured_at: datetime = Field(default_factory=utcnow, index=True)
    is_current: bool = Field(
        default=True, sa_column=Column(Boolean, nullable=False, default=True)
    )


class FrappeSyncEvent(SQLModel, table=True):
    __tablename__ = "crm_frappe_sync_events"
    __table_args__ = (
        CheckConstraint(
            "direction IN ('crm_to_frappe', 'frappe_to_crm')",
            name="ck_crm_frappe_sync_events_direction",
        ),
        CheckConstraint(
            "state IN ('started', 'succeeded', 'failed', 'skipped')",
            name="ck_crm_frappe_sync_events_state",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID = Field(
        foreign_key="crm_complaint_cases.id", index=True
    )
    direction: str = Field(default="crm_to_frappe", index=True)
    operation: str = Field(index=True)
    state: str = Field(default="started", index=True)
    remote_ticket_id: str | None = Field(default=None, index=True)
    request_fingerprint: str | None = Field(default=None, index=True)
    http_status: int | None = Field(default=None, index=True)
    error: str | None = Field(default=None, sa_column=Column(Text))
    details_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    created_at: datetime = Field(default_factory=utcnow, index=True)
    completed_at: datetime | None = Field(default=None, index=True)

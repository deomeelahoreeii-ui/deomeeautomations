from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import Boolean, CheckConstraint, Column, Index, JSON, Text, UniqueConstraint, text
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
        CheckConstraint(
            "source_kind IN ('legacy', 'bulk_import', 'manual_editor', 'frappe_helpdesk')",
            name="ck_crm_complaint_replies_source_kind",
        ),
        CheckConstraint(
            "workspace_status IN ('Imported Draft', 'Not Prepared', 'Draft', 'Pending Approval', 'Approved', 'Issued', 'Rejected')",
            name="ck_crm_complaint_replies_workspace_status",
        ),
        CheckConstraint(
            "sync_status IN ('not_synced', 'pending', 'synchronized', 'failed', 'conflict')",
            name="ck_crm_complaint_replies_sync_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID = Field(
        foreign_key="crm_complaint_cases.id", index=True
    )
    reply_text: str = Field(sa_column=Column(Text, nullable=False))
    inquiry_findings: str | None = Field(default=None, sa_column=Column(Text))
    school_version: str | None = Field(default=None, sa_column=Column(Text))
    applicable_policy: str | None = Field(default=None, sa_column=Column(Text))
    disposal_outcome: str | None = Field(default=None, sa_column=Column(Text))
    ai_eligible: bool = Field(
        default=False, sa_column=Column(Boolean, nullable=False, default=False)
    )
    source_filename: str = Field(index=True)
    source_row: int
    source_kind: str = Field(default="legacy", index=True, max_length=32)
    workspace_status: str = Field(default="Imported Draft", index=True, max_length=40)
    sync_status: str = Field(default="not_synced", index=True, max_length=32)
    sync_error: str | None = Field(default=None, sa_column=Column(Text))
    source_batch_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_bulk_operation_batches.id", index=True
    )
    source_item_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_bulk_operation_items.id", index=True
    )
    last_synced_at: datetime | None = Field(default=None, index=True)
    version: int = 1
    imported_at: datetime = Field(default_factory=utcnow, index=True)
    generated_at: datetime | None = Field(default=None, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class CrmBulkOperationBatch(SQLModel, table=True):
    __tablename__ = "crm_bulk_operation_batches"
    __table_args__ = (
        UniqueConstraint("batch_number", name="uq_crm_bulk_operation_batch_number"),
        CheckConstraint(
            "operation_type IN ('classification_export', 'classification_import', 'reply_context_export', 'reply_export', 'reply_import', 'formal_letters')",
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
            "'unchanged', 'generated', 'classified', 'review_required', 'approved', 'rejected', 'suggested', 'failed', 'skipped', 'missing')",
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


class CrmPromptProfile(SQLModel, table=True):
    __tablename__ = "crm_prompt_profiles"
    __table_args__ = (
        UniqueConstraint("slug", name="uq_crm_prompt_profiles_slug"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    slug: str = Field(index=True, max_length=120)
    name: str = Field(index=True, max_length=180)
    description: str | None = Field(default=None, sa_column=Column(Text))
    active: bool = Field(default=True, sa_column=Column(Boolean, nullable=False, default=True, index=True))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class CrmPromptProfileVersion(SQLModel, table=True):
    __tablename__ = "crm_prompt_profile_versions"
    __table_args__ = (
        UniqueConstraint("profile_id", "version_label", name="uq_crm_prompt_profile_versions_label"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    profile_id: uuid.UUID = Field(foreign_key="crm_prompt_profiles.id", index=True)
    version_label: str = Field(index=True, max_length=80)
    content: str = Field(sa_column=Column(Text, nullable=False))
    content_sha256: str = Field(index=True, max_length=64)
    structured_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    is_active: bool = Field(default=False, sa_column=Column(Boolean, nullable=False, default=False, index=True))
    effective_from: datetime | None = Field(default=None, index=True)
    created_by: str = Field(default="system", index=True, max_length=120)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class CrmComplaintTagGroup(SQLModel, table=True):
    __tablename__ = "crm_complaint_tag_groups"
    __table_args__ = (
        UniqueConstraint("slug", name="uq_crm_complaint_tag_groups_slug"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    slug: str = Field(index=True, max_length=80)
    name: str = Field(index=True, max_length=120)
    description: str | None = Field(default=None, sa_column=Column(Text))
    display_order: int = Field(default=100, index=True)
    active: bool = Field(default=True, sa_column=Column(Boolean, nullable=False, default=True, index=True))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class CrmComplaintTag(SQLModel, table=True):
    __tablename__ = "crm_complaint_tags"
    __table_args__ = (
        UniqueConstraint("normalized_name", name="uq_crm_complaint_tags_normalized_name"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    name: str = Field(index=True, max_length=120)
    display_name: str = Field(default="", index=True, max_length=140)
    normalized_name: str = Field(index=True, max_length=120)
    group_name: str = Field(default="issue", index=True, max_length=80)
    description: str | None = Field(default=None, sa_column=Column(Text))
    aliases_json: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    ai_available: bool = Field(default=True, sa_column=Column(Boolean, nullable=False, default=True, index=True))
    active: bool = Field(default=True, sa_column=Column(Boolean, nullable=False, default=True, index=True))
    merged_into_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_complaint_tags.id", index=True
    )
    version: int = 1
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class CrmComplaintTagLink(SQLModel, table=True):
    __tablename__ = "crm_complaint_tag_links"
    __table_args__ = (
        UniqueConstraint("complaint_case_id", "tag_id", name="uq_crm_complaint_tag_links_case_tag"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID = Field(foreign_key="crm_complaint_cases.id", index=True)
    tag_id: uuid.UUID = Field(foreign_key="crm_complaint_tags.id", index=True)
    classification_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_classifications.id", index=True)
    source: str = Field(default="manual", index=True, max_length=40)
    created_by: str = Field(default="web-operator", index=True, max_length=120)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class CrmComplaintClassification(SQLModel, table=True):
    __tablename__ = "crm_complaint_classifications"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'auto_accepted', 'review_required', 'approved', 'rejected', 'superseded')",
            name="ck_crm_complaint_classifications_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID = Field(foreign_key="crm_complaint_cases.id", index=True)
    batch_id: uuid.UUID | None = Field(default=None, foreign_key="crm_bulk_operation_batches.id", index=True)
    suggested_category_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_categories.id", index=True)
    suggested_subcategory_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_subcategories.id", index=True)
    suggested_category_name: str | None = Field(default=None, index=True, max_length=140)
    suggested_subcategory_name: str | None = Field(default=None, index=True, max_length=140)
    approved_category_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_categories.id", index=True)
    approved_subcategory_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_subcategories.id", index=True)
    confidence: float = 0.0
    reason: str | None = Field(default=None, sa_column=Column(Text))
    tags_json: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    status: str = Field(default="pending", index=True, max_length=32)
    decision_source: str = Field(default="ai_csv", index=True, max_length=40)
    taxonomy_snapshot_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    decided_by: str | None = Field(default=None, index=True, max_length=120)
    decided_at: datetime | None = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class CrmTaxonomySuggestion(SQLModel, table=True):
    __tablename__ = "crm_taxonomy_suggestions"
    __table_args__ = (
        CheckConstraint(
            "proposal_type IN ('category', 'subcategory', 'tag')",
            name="ck_crm_taxonomy_suggestions_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'approved', 'merged', 'rejected', 'deferred')",
            name="ck_crm_taxonomy_suggestions_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    classification_id: uuid.UUID = Field(foreign_key="crm_complaint_classifications.id", index=True)
    proposal_type: str = Field(index=True, max_length=24)
    parent_category_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_categories.id", index=True)
    proposed_name: str = Field(index=True, max_length=140)
    normalized_name: str = Field(index=True, max_length=140)
    proposed_group_name: str | None = Field(default=None, index=True, max_length=80)
    reason: str | None = Field(default=None, sa_column=Column(Text))
    supporting_count: int = 1
    status: str = Field(default="pending", index=True, max_length=24)
    resolved_category_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_categories.id", index=True)
    resolved_subcategory_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_subcategories.id", index=True)
    resolved_tag_id: uuid.UUID | None = Field(default=None, foreign_key="crm_complaint_tags.id", index=True)
    decided_by: str | None = Field(default=None, index=True, max_length=120)
    decided_at: datetime | None = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class CrmReplyContextExample(SQLModel, table=True):
    __tablename__ = "crm_reply_context_examples"
    __table_args__ = (
        UniqueConstraint(
            "batch_id", "complaint_case_id", "reply_revision_id",
            name="uq_crm_reply_context_examples_batch_case_revision",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    batch_id: uuid.UUID = Field(foreign_key="crm_bulk_operation_batches.id", index=True)
    complaint_case_id: uuid.UUID = Field(foreign_key="crm_complaint_cases.id", index=True)
    example_case_id: uuid.UUID = Field(foreign_key="crm_complaint_cases.id", index=True)
    reply_revision_id: uuid.UUID = Field(foreign_key="crm_complaint_reply_revisions.id", index=True)
    rank: int = 1
    score: float = 0.0
    matched_by: str = Field(default="subcategory", index=True, max_length=80)
    snapshot_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
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


class CrmOfficialLetterSettings(SQLModel, table=True):
    __tablename__ = "crm_official_letter_settings"
    __table_args__ = (
        UniqueConstraint("singleton_key", name="uq_crm_official_letter_settings_singleton"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    singleton_key: str = Field(default="default", index=True, max_length=40)
    office_title: str = Field(
        default="OFFICE OF THE\nDISTRICT EDUCATION OFFICER (M-EE)\nLAHORE",
        sa_column=Column(Text, nullable=False),
    )
    default_recipient_name: str = Field(
        default="The Chief Executive Officer (DEA),", max_length=240
    )
    default_recipient_location: str = Field(default="Lahore", max_length=160)
    default_subject_prefix: str = Field(default="COMPLAINT NO.", max_length=160)
    default_cc_entries: str = Field(default="Office Record.", sa_column=Column(Text, nullable=False))
    date_format: str = Field(default="DD/MM/YYYY", max_length=40)
    numbering_prefix: str = Field(default="PMDU/CRM", max_length=120)
    last_numeric_number: int = 1510
    current_letter_number: str = Field(default="1511/PMDU/CRM", max_length=180)
    current_letter_date: date = Field(default_factory=date.today, index=True)
    allow_manual_override: bool = Field(
        default=True, sa_column=Column(Boolean, nullable=False, default=True)
    )
    require_unique_number: bool = Field(
        default=False, sa_column=Column(Boolean, nullable=False, default=False)
    )
    default_template_id: uuid.UUID | None = Field(default=None, index=True)
    default_signature_id: uuid.UUID | None = Field(default=None, index=True)
    updated_by: str = Field(default="system", index=True, max_length=120)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class CrmOfficialLetterTemplate(SQLModel, table=True):
    __tablename__ = "crm_official_letter_templates"
    __table_args__ = (
        UniqueConstraint(
            "template_key", "version", name="uq_crm_official_letter_template_version"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    template_key: str = Field(default="deo-crm-official-letter", index=True, max_length=120)
    name: str = Field(index=True, max_length=200)
    version: int = 1
    description: str | None = Field(default=None, sa_column=Column(Text))
    file_path: str = Field(sa_column=Column(Text, nullable=False))
    sha256: str = Field(index=True, max_length=64)
    size_bytes: int = 0
    signature_target_path: str = Field(
        default="Pictures/1000000100000196000000E8FB647BB3.png", max_length=500
    )
    active: bool = Field(
        default=True, sa_column=Column(Boolean, nullable=False, default=True, index=True)
    )
    is_default: bool = Field(
        default=False, sa_column=Column(Boolean, nullable=False, default=False, index=True)
    )
    created_by: str = Field(default="system", index=True, max_length=120)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class CrmOfficialLetterSignatureProfile(SQLModel, table=True):
    __tablename__ = "crm_official_letter_signature_profiles"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    name: str = Field(index=True, max_length=200)
    officer_name: str | None = Field(default=None, max_length=200)
    designation: str = Field(default="DISTRICT EDUCATION OFFICER (M-EE)", max_length=240)
    office_location: str = Field(default="LAHORE", max_length=160)
    image_path: str = Field(sa_column=Column(Text, nullable=False))
    image_sha256: str = Field(index=True, max_length=64)
    image_size_bytes: int = 0
    active: bool = Field(
        default=True, sa_column=Column(Boolean, nullable=False, default=True, index=True)
    )
    is_default: bool = Field(
        default=False, sa_column=Column(Boolean, nullable=False, default=False, index=True)
    )
    effective_from: date | None = Field(default=None, index=True)
    effective_to: date | None = Field(default=None, index=True)
    created_by: str = Field(default="system", index=True, max_length=120)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class CrmOfficialLetter(SQLModel, table=True):
    __tablename__ = "crm_official_letters"
    __table_args__ = (
        UniqueConstraint(
            "complaint_case_id", "revision", name="uq_crm_official_letters_case_revision"
        ),
        CheckConstraint(
            "status IN ('preview', 'finalized', 'superseded', 'failed', 'discarded')",
            name="ck_crm_official_letters_status",
        ),
        Index(
            "uq_crm_official_letters_active_preview",
            "complaint_case_id",
            unique=True,
            postgresql_where=text("status = 'preview'"),
            sqlite_where=text("status = 'preview'"),
        ),
        Index(
            "uq_crm_official_letters_current_finalized",
            "complaint_case_id",
            unique=True,
            postgresql_where=text("status = 'finalized'"),
            sqlite_where=text("status = 'finalized'"),
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    complaint_case_id: uuid.UUID = Field(
        foreign_key="crm_complaint_cases.id", index=True
    )
    reply_revision_id: uuid.UUID = Field(
        foreign_key="crm_complaint_reply_revisions.id", index=True
    )
    template_id: uuid.UUID = Field(
        foreign_key="crm_official_letter_templates.id", index=True
    )
    signature_profile_id: uuid.UUID = Field(
        foreign_key="crm_official_letter_signature_profiles.id", index=True
    )
    letter_number: str = Field(index=True, max_length=180)
    letter_date: date = Field(index=True)
    recipient_name: str = Field(max_length=240)
    recipient_location: str = Field(max_length=160)
    subject_prefix: str = Field(default="COMPLAINT NO.", max_length=160)
    cc_entries: str = Field(default="Office Record.", sa_column=Column(Text, nullable=False))
    status: str = Field(default="preview", index=True, max_length=24)
    revision: int = 1
    supersedes_letter_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_official_letters.id", index=True
    )
    complaint_number_snapshot: str = Field(index=True, max_length=80)
    complaint_text_snapshot: str = Field(sa_column=Column(Text, nullable=False))
    reply_text_snapshot: str = Field(sa_column=Column(Text, nullable=False))
    configuration_snapshot_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    generated_by: str = Field(default="web-operator", index=True, max_length=120)
    error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    generated_at: datetime | None = Field(default=None, index=True)
    finalized_at: datetime | None = Field(default=None, index=True)


class CrmOfficialLetterArtifact(SQLModel, table=True):
    __tablename__ = "crm_official_letter_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "official_letter_id", "kind", name="uq_crm_official_letter_artifact_kind"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    official_letter_id: uuid.UUID = Field(
        foreign_key="crm_official_letters.id", index=True
    )
    kind: str = Field(index=True, max_length=40)
    name: str = Field(max_length=255)
    path: str = Field(sa_column=Column(Text, nullable=False))
    content_type: str = Field(max_length=160)
    size_bytes: int = 0
    sha256: str = Field(index=True, max_length=64)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class CrmDispatchRule(SQLModel, table=True):
    __tablename__ = "crm_dispatch_rules"
    __table_args__ = (
        UniqueConstraint("name", name="uq_crm_dispatch_rules_name"),
        CheckConstraint(
            "selection_mode IN ('suggested', 'manual_only', 'fallback')",
            name="ck_crm_dispatch_rules_selection_mode",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    name: str = Field(index=True, max_length=200)
    description: str | None = Field(default=None, sa_column=Column(Text))
    priority: int = Field(default=100, index=True)
    selection_mode: str = Field(default="suggested", index=True, max_length=24)
    conditions_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    dispatch_profile_ids_json: list[str] = Field(
        default_factory=list, sa_column=Column(JSON, nullable=False)
    )
    stop_after_match: bool = Field(
        default=True, sa_column=Column(Boolean, nullable=False, default=True)
    )
    enabled: bool = Field(
        default=True, sa_column=Column(Boolean, nullable=False, default=True, index=True)
    )
    effective_from: datetime | None = Field(default=None, index=True)
    effective_to: datetime | None = Field(default=None, index=True)
    version: int = 1
    created_by: str = Field(default="web-operator", index=True, max_length=120)
    updated_by: str = Field(default="web-operator", index=True, max_length=120)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class CrmDispatchBatch(SQLModel, table=True):
    __tablename__ = "crm_dispatch_batches"
    __table_args__ = (
        UniqueConstraint("batch_number", name="uq_crm_dispatch_batches_number"),
        CheckConstraint(
            "status IN ('draft', 'resolving_routes', 'review_required', 'ready', "
            "'approved', 'queued', 'sending', 'completed', 'completed_with_errors', "
            "'failed', 'cancelled', 'stale')",
            name="ck_crm_dispatch_batches_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    batch_number: str = Field(index=True, max_length=80)
    status: str = Field(default="draft", index=True, max_length=40)
    source_mode: str = Field(default="official_letters", index=True, max_length=40)
    purpose: str = Field(default="complaint_forwarding", index=True, max_length=80)
    total_items: int = 0
    ready_items: int = 0
    blocked_items: int = 0
    excluded_items: int = 0
    queued_items: int = 0
    successful_items: int = 0
    failed_items: int = 0
    response_due_at: datetime | None = Field(default=None, index=True)
    created_by: str = Field(default="web-operator", index=True, max_length=120)
    approved_by: str | None = Field(default=None, index=True, max_length=120)
    error_summary: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)
    approved_at: datetime | None = Field(default=None, index=True)
    completed_at: datetime | None = Field(default=None, index=True)


class CrmDispatchItem(SQLModel, table=True):
    __tablename__ = "crm_dispatch_items"
    __table_args__ = (
        UniqueConstraint(
            "batch_id", "official_letter_id", name="uq_crm_dispatch_items_batch_letter"
        ),
        CheckConstraint(
            "route_status IN ('ready', 'needs_review', 'conflict', 'blocked', 'excluded')",
            name="ck_crm_dispatch_items_route_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    batch_id: uuid.UUID = Field(foreign_key="crm_dispatch_batches.id", index=True)
    complaint_case_id: uuid.UUID = Field(foreign_key="crm_complaint_cases.id", index=True)
    official_letter_id: uuid.UUID = Field(foreign_key="crm_official_letters.id", index=True)
    complete_pdf_artifact_id: uuid.UUID = Field(
        foreign_key="crm_official_letter_artifacts.id", index=True
    )
    complaint_number_snapshot: str = Field(index=True, max_length=80)
    letter_number_snapshot: str = Field(index=True, max_length=180)
    letter_date_snapshot: date = Field(index=True)
    packet_sha256: str = Field(index=True, max_length=64)
    packet_size_bytes: int = 0
    packet_page_count: int = 0
    route_status: str = Field(default="needs_review", index=True, max_length=24)
    route_summary_json: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON, nullable=False)
    )
    excluded: bool = Field(
        default=False, sa_column=Column(Boolean, nullable=False, default=False, index=True)
    )
    exclusion_reason: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class CrmDispatchTarget(SQLModel, table=True):
    __tablename__ = "crm_dispatch_targets"
    __table_args__ = (
        UniqueConstraint(
            "dispatch_item_id", "dispatch_profile_id",
            name="uq_crm_dispatch_targets_item_profile",
        ),
        CheckConstraint(
            "selection_source IN ('rule', 'manual', 'fallback')",
            name="ck_crm_dispatch_targets_selection_source",
        ),
        CheckConstraint(
            "business_status IN ('planned', 'blocked', 'excluded', 'approved', 'queued', "
            "'sent_pending_confirmation', 'delivered', 'failed', 'timed_out', 'cancelled')",
            name="ck_crm_dispatch_targets_business_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    dispatch_item_id: uuid.UUID = Field(foreign_key="crm_dispatch_items.id", index=True)
    routing_rule_id: uuid.UUID | None = Field(
        default=None, foreign_key="crm_dispatch_rules.id", index=True
    )
    dispatch_profile_id: uuid.UUID = Field(
        foreign_key="whatsapp_dispatch_profiles.id", index=True
    )
    selection_source: str = Field(default="rule", index=True, max_length=20)
    manual_override_reason: str | None = Field(default=None, sa_column=Column(Text))
    recipient_snapshot_json: list[dict[str, Any]] = Field(
        default_factory=list, sa_column=Column(JSON, nullable=False)
    )
    message_snapshot: str = Field(default="", sa_column=Column(Text, nullable=False))
    message_sha256: str = Field(default="", index=True, max_length=64)
    preview_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_dispatch_previews.id", index=True
    )
    preview_delivery_ids_json: list[str] = Field(
        default_factory=list, sa_column=Column(JSON, nullable=False)
    )
    whatsapp_delivery_ids_json: list[str] = Field(
        default_factory=list, sa_column=Column(JSON, nullable=False)
    )
    business_status: str = Field(default="planned", index=True, max_length=40)
    response_due_at: datetime | None = Field(default=None, index=True)
    error: str | None = Field(default=None, sa_column=Column(Text))
    sent_at: datetime | None = Field(default=None, index=True)
    completed_at: datetime | None = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


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

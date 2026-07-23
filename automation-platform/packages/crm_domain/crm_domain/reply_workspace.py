from __future__ import annotations

import hashlib
import mimetypes
import uuid
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import func, or_
from sqlmodel import Session, select

from automation_core.config import Settings
from automation_core.time import utcnow
from crm_domain.json_safe import json_safe
from crm_domain.case_scopes import reply_case_eligibility_clause
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintCategory,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintReply,
    ComplaintReplyRevision,
    ComplaintSubcategory,
    CrmBulkOperationBatch,
    CrmBulkOperationItem,
    CrmOfficialLetter,
    CrmOfficialLetterArtifact,
    FrappeSyncEvent,
)
from crm_domain.taxonomy import clean_name
from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskClient, FrappeHelpdeskError
from crm_integrations.frappe_helpdesk.service import build_client
from crm_integrations.frappe_helpdesk.workflow import ComplaintHelpdeskWorkflowService


REPLY_STATUSES = (
    "Not Prepared",
    "Draft",
    "Pending Approval",
    "Approved",
    "Issued",
    "Rejected",
)
APPROVED_REPLY_STATUSES = {"Approved", "Issued"}
STATUS_TO_HELPDESK = {
    "Draft": "Reply Under Preparation",
    "Pending Approval": "Pending Approval",
    "Approved": "Approved",
    "Issued": "Reply Issued",
    "Rejected": "Reply Under Preparation",
}


class ReplyWorkspaceError(ValueError):
    """Base error for the operator-facing reply workspace."""


class ReplyValidationError(ReplyWorkspaceError):
    """The requested state transition or selection is invalid."""


class ReplyNotFoundError(ReplyWorkspaceError):
    """A requested complaint or taxonomy record does not exist."""


class ReplyRemoteError(ReplyWorkspaceError):
    """Frappe write/read-back verification failed."""


def _text(value: object) -> str:
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "yes", "on"}


class ComplaintReplyWorkspaceService:
    """Operator-facing classification and reply workflow over Frappe Helpdesk."""

    def __init__(
        self,
        session: Session,
        settings: Settings,
        client: FrappeHelpdeskClient | None = None,
    ) -> None:
        self.session = session
        self.settings = settings
        self.client = client or build_client(settings)

    def _audit(
        self,
        *,
        case: ComplaintCase | None,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor: str,
        state: str = "succeeded",
        before: dict[str, Any] | None = None,
        after: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        self.session.add(
            ComplaintAuditEvent(
                complaint_case_id=case.id if case else None,
                entity_type=entity_type,
                entity_id=entity_id,
                event_type=event_type,
                actor=actor,
                state=state,
                before_json=json_safe(before or {}),
                after_json=json_safe(after or {}),
                details_json=json_safe(details or {}),
                error=error,
            )
        )

    def _frappe_event(
        self,
        case: ComplaintCase,
        *,
        operation: str,
        state: str,
        details: dict[str, Any],
        error: str | None = None,
    ) -> None:
        self.session.add(
            FrappeSyncEvent(
                complaint_case_id=case.id,
                direction="crm_to_frappe",
                operation=operation,
                state=state,
                remote_ticket_id=case.frappe_ticket_id,
                request_fingerprint=case.frappe_payload_hash,
                error=error,
                details_json=json_safe(details),
                completed_at=utcnow(),
            )
        )

    def _require_case(self, case_id: uuid.UUID) -> ComplaintCase:
        case = self.session.get(ComplaintCase, case_id)
        if case is None:
            raise ReplyNotFoundError("Complaint case was not found")
        return case

    def _require_category(
        self, category_id: uuid.UUID, *, active: bool = True
    ) -> ComplaintCategory:
        category = self.session.get(ComplaintCategory, category_id)
        if category is None:
            raise ReplyNotFoundError("Complaint category was not found")
        if active and (not category.active or category.merged_into_id):
            raise ReplyValidationError("Select an active complaint category")
        return category

    def _require_subcategory(
        self,
        subcategory_id: uuid.UUID,
        *,
        category_id: uuid.UUID,
        active: bool = True,
    ) -> ComplaintSubcategory:
        subcategory = self.session.get(ComplaintSubcategory, subcategory_id)
        if subcategory is None:
            raise ReplyNotFoundError("Complaint subcategory was not found")
        if subcategory.category_id != category_id:
            raise ReplyValidationError("The selected subcategory does not belong to the category")
        if active and (not subcategory.active or subcategory.merged_into_id):
            raise ReplyValidationError("Select an active complaint subcategory")
        return subcategory

    @staticmethod
    def _subject(case: ComplaintCase, category_name: str) -> str:
        number = case.complaint_number or str(case.id)
        return f"Complaint {number} — {category_name}"[:140]

    def _classification_snapshot(self, case: ComplaintCase) -> dict[str, Any]:
        return {
            "category_id": str(case.category_id) if case.category_id else None,
            "subcategory_id": str(case.sub_category_id) if case.sub_category_id else None,
            "category": case.category or "",
            "subcategory": case.sub_category or "",
            "sync_status": case.classification_sync_status,
            "last_synced_at": case.classification_last_synced_at,
            "last_error": case.classification_last_error,
        }

    def sync_classification(
        self,
        case_id: uuid.UUID,
        *,
        actor: str = "web-operator",
    ) -> dict[str, Any]:
        case = self._require_case(case_id)
        if case.category_id is None:
            raise ReplyValidationError("Classify the complaint before synchronizing it")
        category = self._require_category(case.category_id, active=False)
        subcategory = (
            self._require_subcategory(
                case.sub_category_id,
                category_id=category.id,
                active=False,
            )
            if case.sub_category_id
            else None
        )
        if not case.frappe_ticket_id:
            case.classification_sync_status = "not_synced"
            case.classification_last_error = "Complaint is not linked to Helpdesk"
            case.updated_at = utcnow()
            self.session.add(case)
            self.session.commit()
            return {
                "case_id": str(case.id),
                "status": "not_linked",
                "synchronized": False,
                "error": case.classification_last_error,
            }
        before = self._classification_snapshot(case)
        payload = {
            "ticket_type": category.name,
            "custom_sub_category": subcategory.name if subcategory else "",
            "subject": self._subject(case, category.name),
        }
        try:
            try:
                ticket_type = self.client.get_resource("HD Ticket Type", category.name)
            except FrappeHelpdeskError as exc:
                if exc.status_code != 404:
                    raise
                ticket_type = None
            if ticket_type is None:
                self.client.create_resource(
                    "HD Ticket Type",
                    {
                        "name": category.name,
                        "description": category.description
                        or "Complaint category synchronized from Deomee CRM.",
                        "disabled": 0,
                        "is_system": 0,
                    },
                )
            self.client.update_resource("HD Ticket", case.frappe_ticket_id, payload)
            verified = self.client.get_resource("HD Ticket", case.frappe_ticket_id)
            verified_category = clean_name(str(verified.get("ticket_type") or ""))
            verified_subcategory = clean_name(str(verified.get("custom_sub_category") or ""))
            expected_subcategory = subcategory.name if subcategory else ""
            if verified_category != category.name or verified_subcategory != expected_subcategory:
                raise ReplyWorkspaceError(
                    "Helpdesk classification read-back did not match the saved selection"
                )
            case.category = category.name
            case.sub_category = expected_subcategory or None
            case.classification_sync_status = "synchronized"
            case.classification_last_synced_at = utcnow()
            case.classification_last_error = None
            case.updated_at = utcnow()
            after = self._classification_snapshot(case)
            self._frappe_event(
                case,
                operation="sync_classification",
                state="succeeded",
                details={"payload": payload, "verified": after},
            )
            self._audit(
                case=case,
                entity_type="complaint_case",
                entity_id=str(case.id),
                event_type="classification_synchronized",
                actor=actor,
                before=before,
                after=after,
            )
            self.session.add(case)
            self.session.commit()
            return {
                "case_id": str(case.id),
                "status": "synchronized",
                "synchronized": True,
                "classification": after,
            }
        except Exception as exc:
            error = str(exc)
            case_key = case.id
            self.session.rollback()
            failed_case = self.session.get(ComplaintCase, case_key)
            if failed_case is None:
                raise ReplyRemoteError(error) from exc
            failed_case.classification_sync_status = "failed"
            failed_case.classification_last_error = error
            failed_case.updated_at = utcnow()
            failed_snapshot = self._classification_snapshot(failed_case)
            self._frappe_event(
                failed_case,
                operation="sync_classification",
                state="failed",
                details={"payload": payload},
                error=error,
            )
            self._audit(
                case=failed_case,
                entity_type="complaint_case",
                entity_id=str(failed_case.id),
                event_type="classification_sync_failed",
                actor=actor,
                state="failed",
                before=before,
                after=failed_snapshot,
                error=error,
            )
            self.session.add(failed_case)
            self.session.commit()
            return {
                "case_id": str(failed_case.id),
                "status": "failed",
                "synchronized": False,
                "error": error,
                "classification": failed_snapshot,
            }

    def save_classification(
        self,
        case_id: uuid.UUID,
        *,
        category_id: uuid.UUID,
        subcategory_id: uuid.UUID | None = None,
        actor: str = "web-operator",
        synchronize: bool = True,
    ) -> dict[str, Any]:
        case = self._require_case(case_id)
        category = self._require_category(category_id)
        subcategory = (
            self._require_subcategory(
                subcategory_id,
                category_id=category.id,
            )
            if subcategory_id
            else None
        )
        before = self._classification_snapshot(case)
        case.category_id = category.id
        case.sub_category_id = subcategory.id if subcategory else None
        case.category = category.name
        case.sub_category = subcategory.name if subcategory else None
        case.classification_sync_status = "pending" if case.frappe_ticket_id else "not_synced"
        case.classification_last_error = None
        case.version += 1
        case.updated_at = utcnow()
        after = self._classification_snapshot(case)
        self._audit(
            case=case,
            entity_type="complaint_case",
            entity_id=str(case.id),
            event_type="classification_saved",
            actor=actor,
            before=before,
            after=after,
        )
        self.session.add(case)
        self.session.commit()
        result = {
            "case_id": str(case.id),
            "saved": True,
            "classification": after,
            "synchronized": False,
        }
        if synchronize and case.frappe_ticket_id:
            result.update(self.sync_classification(case.id, actor=actor))
        return result

    def bulk_classify(
        self,
        case_ids: Iterable[uuid.UUID],
        *,
        category_id: uuid.UUID,
        subcategory_id: uuid.UUID | None = None,
        actor: str = "web-operator",
        synchronize: bool = True,
    ) -> dict[str, Any]:
        unique_ids = list(dict.fromkeys(case_ids))
        if not unique_ids:
            raise ReplyValidationError("Select at least one complaint")
        results: list[dict[str, Any]] = []
        for case_id in unique_ids:
            try:
                results.append(
                    self.save_classification(
                        case_id,
                        category_id=category_id,
                        subcategory_id=subcategory_id,
                        actor=actor,
                        synchronize=synchronize,
                    )
                )
            except Exception as exc:
                results.append(
                    {
                        "case_id": str(case_id),
                        "saved": False,
                        "synchronized": False,
                        "status": "failed",
                        "error": str(exc),
                    }
                )
        return {
            "results": results,
            "counts": dict(
                Counter(
                    str(
                        item.get("status")
                        or ("synchronized" if item.get("synchronized") else None)
                        or ("saved" if item.get("saved") else "failed")
                    )
                    for item in results
                )
            ),
            "requested": len(unique_ids),
        }

    def _reply_snapshot(self, case: ComplaintCase) -> dict[str, Any]:
        return {
            "inquiry_findings": case.frappe_inquiry_findings or "",
            "school_version": case.frappe_school_version or "",
            "applicable_policy": case.frappe_applicable_policy or "",
            "final_reply": case.frappe_reply_text_snapshot or "",
            "reply_status": case.frappe_reply_approval_status or "Not Prepared",
            "disposal_outcome": case.frappe_disposal_outcome or "",
            "ai_eligible": bool(case.frappe_ai_eligible),
            "workflow_status": case.frappe_workflow_status or "",
            "last_pulled_at": case.frappe_last_workflow_pull_at,
        }

    @staticmethod
    def _reply_values(
        *,
        inquiry_findings: str,
        school_version: str,
        applicable_policy: str,
        final_reply: str,
        reply_status: str,
        disposal_outcome: str,
        ai_eligible: bool,
    ) -> dict[str, Any]:
        values: dict[str, Any] = {
            "custom_inquiry_findings": inquiry_findings,
            "custom_school_version": school_version,
            "custom_applicable_policy": applicable_policy,
            "custom_final_approved_reply": final_reply,
            "custom_reply_approval_status": reply_status,
            "custom_disposal_outcome": disposal_outcome,
            "custom_ai_eligible": 1 if ai_eligible else 0,
        }
        workflow_status = STATUS_TO_HELPDESK.get(reply_status)
        if workflow_status:
            values["status"] = workflow_status
        return values

    def _workspace_reply(self, case_id: uuid.UUID) -> ComplaintReply | None:
        return self.session.exec(
            select(ComplaintReply).where(ComplaintReply.complaint_case_id == case_id)
        ).first()

    @staticmethod
    def _workspace_reply_snapshot(reply: ComplaintReply | None) -> dict[str, Any]:
        if reply is None:
            return {}
        return {
            "id": str(reply.id),
            "reply_text": reply.reply_text,
            "inquiry_findings": reply.inquiry_findings or "",
            "school_version": reply.school_version or "",
            "applicable_policy": reply.applicable_policy or "",
            "disposal_outcome": reply.disposal_outcome or "",
            "ai_eligible": bool(reply.ai_eligible),
            "source_filename": reply.source_filename,
            "source_row": reply.source_row,
            "source_kind": reply.source_kind,
            "workspace_status": reply.workspace_status,
            "sync_status": reply.sync_status,
            "sync_error": reply.sync_error,
            "source_batch_id": str(reply.source_batch_id) if reply.source_batch_id else None,
            "source_item_id": str(reply.source_item_id) if reply.source_item_id else None,
            "version": reply.version,
            "imported_at": reply.imported_at,
            "last_synced_at": reply.last_synced_at,
            "updated_at": reply.updated_at,
        }

    def _store_local_workspace(
        self,
        case: ComplaintCase,
        *,
        inquiry_findings: str,
        school_version: str,
        applicable_policy: str,
        final_reply: str,
        reply_status: str,
        disposal_outcome: str,
        ai_eligible: bool,
        actor: str,
        sync_status: str,
    ) -> ComplaintReply | None:
        existing = self._workspace_reply(case.id)
        if existing is None and not final_reply:
            return None
        now = utcnow()
        before = self._workspace_reply_snapshot(existing)
        if existing is None:
            existing = ComplaintReply(
                complaint_case_id=case.id,
                reply_text=final_reply,
                inquiry_findings=inquiry_findings or None,
                school_version=school_version or None,
                applicable_policy=applicable_policy or None,
                disposal_outcome=disposal_outcome or None,
                ai_eligible=ai_eligible,
                source_filename=f"reply-editor/{case.id}.txt",
                source_row=0,
                source_kind="manual_editor",
                workspace_status=reply_status,
                sync_status=sync_status,
                imported_at=now,
                updated_at=now,
            )
        else:
            changed = any(
                (
                    existing.reply_text != final_reply,
                    (existing.inquiry_findings or "") != inquiry_findings,
                    (existing.school_version or "") != school_version,
                    (existing.applicable_policy or "") != applicable_policy,
                    (existing.disposal_outcome or "") != disposal_outcome,
                    bool(existing.ai_eligible) != bool(ai_eligible),
                    existing.workspace_status != reply_status,
                )
            )
            if changed:
                existing.version += 1
            existing.reply_text = final_reply
            existing.inquiry_findings = inquiry_findings or None
            existing.school_version = school_version or None
            existing.applicable_policy = applicable_policy or None
            existing.disposal_outcome = disposal_outcome or None
            existing.ai_eligible = ai_eligible
            existing.source_kind = "manual_editor"
            existing.workspace_status = reply_status
            existing.sync_status = sync_status
            existing.sync_error = None
            existing.updated_at = now
        self.session.add(existing)
        self.session.flush()
        self._audit(
            case=case,
            entity_type="complaint_reply_workspace",
            entity_id=str(existing.id),
            event_type="reply_local_saved",
            actor=actor,
            before=before,
            after=self._workspace_reply_snapshot(existing),
            details={"remote_sync_pending": sync_status == "pending"},
        )
        return existing

    def save_reply(
        self,
        case_id: uuid.UUID,
        *,
        inquiry_findings: str = "",
        school_version: str = "",
        applicable_policy: str = "",
        final_reply: str = "",
        reply_status: str = "Draft",
        disposal_outcome: str = "",
        ai_eligible: bool = False,
        actor: str = "web-operator",
    ) -> dict[str, Any]:
        case = self._require_case(case_id)
        status = clean_name(reply_status)
        if status == "Not Started":
            status = "Not Prepared"
        if status not in REPLY_STATUSES:
            raise ReplyValidationError(f"Unsupported reply status: {status}")
        final_text = _text(final_reply)
        inquiry_text = _text(inquiry_findings)
        school_text = _text(school_version)
        policy_text = _text(applicable_policy)
        disposal_text = _text(disposal_outcome)
        if status in {"Pending Approval", "Approved", "Issued"} and not final_text:
            raise ReplyValidationError("Enter the final reply before using this status")
        if ai_eligible and status not in APPROVED_REPLY_STATUSES:
            raise ReplyValidationError(
                "AI eligibility is allowed only for Approved or Issued replies"
            )
        if not case.frappe_ticket_id and status in {"Pending Approval", "Approved", "Issued"}:
            raise ReplyValidationError(
                "This complaint is not linked to Frappe Helpdesk; save it as a local Draft first"
            )

        before = self._reply_snapshot(case)
        local = self._store_local_workspace(
            case,
            inquiry_findings=inquiry_text,
            school_version=school_text,
            applicable_policy=policy_text,
            final_reply=final_text,
            reply_status=status,
            disposal_outcome=disposal_text,
            ai_eligible=ai_eligible,
            actor=actor,
            sync_status="pending" if case.frappe_ticket_id else "not_synced",
        )
        self.session.commit()

        if not case.frappe_ticket_id:
            return {
                "case_id": str(case.id),
                "ticket_id": None,
                "saved": True,
                "saved_locally": True,
                "write_verified": False,
                "reply": self._workspace_reply_snapshot(local),
                "archive": None,
            }

        values = self._reply_values(
            inquiry_findings=inquiry_text,
            school_version=school_text,
            applicable_policy=policy_text,
            final_reply=final_text,
            reply_status=status,
            disposal_outcome=disposal_text,
            ai_eligible=ai_eligible,
        )
        try:
            self.client.update_resource("HD Ticket", case.frappe_ticket_id, values)
            verified = self.client.get_resource("HD Ticket", case.frappe_ticket_id)
            checks = {
                "custom_inquiry_findings": _text(verified.get("custom_inquiry_findings")),
                "custom_school_version": _text(verified.get("custom_school_version")),
                "custom_applicable_policy": _text(verified.get("custom_applicable_policy")),
                "custom_final_approved_reply": _text(verified.get("custom_final_approved_reply")),
                "custom_reply_approval_status": clean_name(
                    str(verified.get("custom_reply_approval_status") or "")
                ),
                "custom_disposal_outcome": _text(verified.get("custom_disposal_outcome")),
                "custom_ai_eligible": _truthy(verified.get("custom_ai_eligible")),
            }
            expected = {
                "custom_inquiry_findings": values["custom_inquiry_findings"],
                "custom_school_version": values["custom_school_version"],
                "custom_applicable_policy": values["custom_applicable_policy"],
                "custom_final_approved_reply": values["custom_final_approved_reply"],
                "custom_reply_approval_status": values["custom_reply_approval_status"],
                "custom_disposal_outcome": values["custom_disposal_outcome"],
                "custom_ai_eligible": bool(ai_eligible),
            }
            mismatches = {
                key: {"expected": expected[key], "actual": checks[key]}
                for key in expected
                if checks[key] != expected[key]
            }
            if mismatches:
                raise ReplyWorkspaceError(
                    f"Helpdesk reply read-back mismatch: {', '.join(sorted(mismatches))}"
                )
            pull_result = ComplaintHelpdeskWorkflowService(
                self.session, self.settings, self.client
            ).pull_case(case.id)
            if pull_result.get("status") != "pulled":
                raise ReplyWorkspaceError(
                    str(
                        pull_result.get("error")
                        or "Helpdesk write succeeded but CRM pull-back failed"
                    )
                )
            refreshed = self._require_case(case.id)
            local = self._workspace_reply(case.id)
            if local is not None:
                local.workspace_status = status
                local.sync_status = "synchronized"
                local.sync_error = None
                local.last_synced_at = utcnow()
                local.updated_at = utcnow()
                self.session.add(local)
            after = self._reply_snapshot(refreshed)
            self._frappe_event(
                refreshed,
                operation="save_reply_workspace",
                state="succeeded",
                details={
                    "status": status,
                    "verified": True,
                    "reply": pull_result.get("reply"),
                    "local_workspace_id": str(local.id) if local else None,
                },
            )
            self._audit(
                case=refreshed,
                entity_type="complaint_reply",
                entity_id=str(local.id if local else refreshed.id),
                event_type="reply_saved",
                actor=actor,
                before=before,
                after=after,
                details={
                    "write_verified": True,
                    "pull_result": pull_result,
                    "local_workspace": self._workspace_reply_snapshot(local),
                },
            )
            self.session.commit()
            return {
                "case_id": str(refreshed.id),
                "ticket_id": refreshed.frappe_ticket_id,
                "saved": True,
                "saved_locally": True,
                "write_verified": True,
                "reply": after,
                "workspace": self._workspace_reply_snapshot(local),
                "archive": pull_result.get("reply"),
            }
        except Exception as exc:
            error = str(exc)
            case_key = case.id
            self.session.rollback()
            failed_case = self.session.get(ComplaintCase, case_key)
            failed_local = self._workspace_reply(case_key)
            if failed_local is not None:
                failed_local.sync_status = "failed"
                failed_local.sync_error = error[:4000]
                failed_local.updated_at = utcnow()
                self.session.add(failed_local)
            if failed_case is not None:
                self._frappe_event(
                    failed_case,
                    operation="save_reply_workspace",
                    state="failed",
                    details={
                        "status": status,
                        "values_present": sorted(values),
                        "local_draft_preserved": failed_local is not None,
                    },
                    error=error,
                )
                self._audit(
                    case=failed_case,
                    entity_type="complaint_reply",
                    entity_id=str(failed_local.id if failed_local else failed_case.id),
                    event_type="reply_save_failed",
                    actor=actor,
                    state="failed",
                    before=before,
                    after=self._workspace_reply_snapshot(failed_local),
                    details={
                        "write_verified": False,
                        "local_draft_preserved": failed_local is not None,
                    },
                    error=error,
                )
                self.session.commit()
            raise ReplyRemoteError(error) from exc

    def _paperless_url(self, case: ComplaintCase) -> str:
        root = str(self.settings.paperless_url or "").strip().rstrip("/")
        for suffix in ("/dashboard", "/api"):
            if root.endswith(suffix):
                root = root[: -len(suffix)]
        if not root or not case.canonical_paperless_document_id:
            return ""
        return f"{root}/documents/{case.canonical_paperless_document_id}/details"

    def upload_case_document(
        self,
        case_id: uuid.UUID,
        *,
        filename: str,
        content: bytes,
        content_type: str | None,
        role: str,
        actor: str,
        dispatch_batch_id: uuid.UUID | None = None,
        dispatch_item_id: uuid.UUID | None = None,
    ) -> dict[str, Any]:
        case = self._require_case(case_id)
        allowed_roles = {"reply", "report", "policy", "attachment"}
        if role not in allowed_roles:
            raise ReplyValidationError(
                "Choose reply, report, policy or attachment as the file role"
            )
        if not content:
            raise ReplyValidationError("The uploaded case file is empty")
        if len(content) > 50 * 1024 * 1024:
            raise ReplyValidationError("Case files may not exceed 50 MB")
        safe_name = Path(filename or "case-file").name.replace("\x00", "").strip() or "case-file"
        digest = hashlib.sha256(content).hexdigest()
        existing = self.session.exec(
            select(ComplaintDocument, ComplaintDocumentCaseLink)
            .join(
                ComplaintDocumentCaseLink,
                ComplaintDocumentCaseLink.complaint_document_id == ComplaintDocument.id,
            )
            .where(
                ComplaintDocumentCaseLink.complaint_case_id == case.id,
                ComplaintDocument.source_sha256 == digest,
            )
        ).first()
        if existing:
            document, link = existing
            return {
                "id": str(document.id),
                "filename": document.original_filename,
                "role": link.role,
                "review_state": link.review_state,
                "paperless_document_id": document.paperless_document_id,
                "source_kind": document.source_kind,
                "download_url": f"/api/v1/crm/reply-workspace/documents/{document.id}/download",
                "duplicate": True,
            }
        root = (
            self.settings.artifact_root.expanduser().resolve() / "crm-case-documents" / str(case.id)
        )
        root.mkdir(parents=True, exist_ok=True)
        suffix = Path(safe_name).suffix
        destination = root / f"{uuid.uuid4().hex}{suffix}"
        destination.write_bytes(content)
        detected = content_type or mimetypes.guess_type(safe_name)[0] or "application/octet-stream"
        now = utcnow()
        document = ComplaintDocument(
            complaint_case_id=case.id,
            source_processing_item_id=None,
            source_kind="manual_upload",
            storage_path=str(destination),
            uploaded_by=actor,
            source_dispatch_batch_id=dispatch_batch_id,
            source_dispatch_item_id=dispatch_item_id,
            source_sha256=digest,
            original_filename=safe_name,
            mime_type=detected,
            role=role,
            relationship_confidence=1.0,
            relationship_reason=f"Manually uploaded by {actor}",
            review_state="accepted",
            created_at=now,
            updated_at=now,
        )
        self.session.add(document)
        self.session.flush()
        link = ComplaintDocumentCaseLink(
            complaint_document_id=document.id,
            complaint_case_id=case.id,
            role=role,
            review_state="accepted",
            confidence=1.0,
            reason=f"Manually uploaded by {actor}",
            source_locator="manual_upload",
        )
        self.session.add(link)
        invalidated_packets = 0
        finalized_letter_ids = select(CrmOfficialLetter.id).where(
            CrmOfficialLetter.complaint_case_id == case.id,
            CrmOfficialLetter.status == "finalized",
        )
        existing_packets = list(
            self.session.exec(
                select(CrmOfficialLetterArtifact).where(
                    CrmOfficialLetterArtifact.official_letter_id.in_(finalized_letter_ids),
                    CrmOfficialLetterArtifact.kind == "complete_pdf",
                )
            ).all()
        )
        for artifact in existing_packets:
            try:
                Path(artifact.path).expanduser().resolve().unlink(missing_ok=True)
            except OSError:
                pass
            self.session.delete(artifact)
            invalidated_packets += 1
        self._audit(
            case=case,
            entity_type="complaint_document",
            entity_id=str(document.id),
            event_type="case_file_uploaded",
            state="succeeded",
            actor=actor,
            after={
                "filename": safe_name,
                "role": role,
                "sha256": digest,
                "source_dispatch_batch_id": str(dispatch_batch_id) if dispatch_batch_id else None,
                "source_dispatch_item_id": str(dispatch_item_id) if dispatch_item_id else None,
                "invalidated_complete_pdf_count": invalidated_packets,
            },
        )
        self.session.commit()
        return {
            "id": str(document.id),
            "filename": safe_name,
            "role": role,
            "review_state": "accepted",
            "paperless_document_id": None,
            "source_kind": "manual_upload",
            "download_url": f"/api/v1/crm/reply-workspace/documents/{document.id}/download",
            "duplicate": False,
        }

    def case_document_path(self, document_id: uuid.UUID) -> tuple[ComplaintDocument, Path]:
        document = self.session.get(ComplaintDocument, document_id)
        if document is None or document.source_kind != "manual_upload" or not document.storage_path:
            raise ReplyNotFoundError("The manually uploaded case file was not found")
        path = Path(document.storage_path).expanduser().resolve()
        if not path.is_file():
            raise ReplyNotFoundError("The manually uploaded case file is unavailable")
        return document, path

    def editor(
        self, case_id: uuid.UUID, *, refresh_remote: bool = True
    ) -> dict[str, Any]:
        case = self._require_case(case_id)
        category = (
            self.session.get(ComplaintCategory, case.category_id) if case.category_id else None
        )
        subcategory = (
            self.session.get(ComplaintSubcategory, case.sub_category_id)
            if case.sub_category_id
            else None
        )
        live_error = ""
        live_ticket: dict[str, Any] = {}
        if refresh_remote and case.frappe_ticket_id:
            try:
                live_ticket = self.client.get_resource("HD Ticket", case.frappe_ticket_id)
            except Exception as exc:
                live_error = str(exc)
        reply = self.session.exec(
            select(ComplaintReply).where(ComplaintReply.complaint_case_id == case.id)
        ).first()
        revisions = list(
            self.session.exec(
                select(ComplaintReplyRevision)
                .where(ComplaintReplyRevision.complaint_case_id == case.id)
                .order_by(ComplaintReplyRevision.captured_at.desc())
            ).all()
        )
        documents = list(
            self.session.exec(
                select(ComplaintDocument, ComplaintDocumentCaseLink)
                .join(
                    ComplaintDocumentCaseLink,
                    ComplaintDocumentCaseLink.complaint_document_id == ComplaintDocument.id,
                )
                .where(ComplaintDocumentCaseLink.complaint_case_id == case.id)
                .order_by(ComplaintDocument.created_at)
            ).all()
        )
        remote_reply = {
            "inquiry_findings": _text(
                live_ticket.get("custom_inquiry_findings")
                if live_ticket
                else case.frappe_inquiry_findings
            ),
            "school_version": _text(
                live_ticket.get("custom_school_version")
                if live_ticket
                else case.frappe_school_version
            ),
            "applicable_policy": _text(
                live_ticket.get("custom_applicable_policy")
                if live_ticket
                else case.frappe_applicable_policy
            ),
            "final_reply": _text(
                live_ticket.get("custom_final_approved_reply")
                if live_ticket
                else case.frappe_reply_text_snapshot
            ),
            "reply_status": clean_name(
                str(
                    live_ticket.get("custom_reply_approval_status")
                    if live_ticket
                    else case.frappe_reply_approval_status or "Not Prepared"
                )
            )
            or "Not Prepared",
            "disposal_outcome": _text(
                live_ticket.get("custom_disposal_outcome")
                if live_ticket
                else case.frappe_disposal_outcome
            ),
            "ai_eligible": _truthy(
                live_ticket.get("custom_ai_eligible") if live_ticket else case.frappe_ai_eligible
            ),
            "workflow_status": clean_name(
                str(live_ticket.get("status") if live_ticket else case.frappe_workflow_status or "")
            ),
        }
        local_snapshot = self._workspace_reply_snapshot(reply)
        local_text = _text(reply.reply_text) if reply else ""
        remote_text = remote_reply["final_reply"]
        local_pending = bool(
            reply
            and reply.source_kind in {"bulk_import", "manual_editor"}
            and reply.sync_status in {"not_synced", "pending", "failed", "conflict"}
        )
        conflict = bool(local_pending and local_text and remote_text and local_text != remote_text)
        use_local = bool(reply and local_text and (local_pending or not remote_text or live_error))
        if use_local and reply is not None:
            operational_status = (
                "Draft" if reply.workspace_status == "Imported Draft" else reply.workspace_status
            )
            current_reply = {
                "inquiry_findings": _text(reply.inquiry_findings)
                or remote_reply["inquiry_findings"],
                "school_version": _text(reply.school_version) or remote_reply["school_version"],
                "applicable_policy": _text(reply.applicable_policy)
                or remote_reply["applicable_policy"],
                "final_reply": local_text,
                "reply_status": operational_status or "Draft",
                "display_status": reply.workspace_status or operational_status or "Draft",
                "disposal_outcome": _text(reply.disposal_outcome)
                or remote_reply["disposal_outcome"],
                "ai_eligible": bool(reply.ai_eligible),
                "workflow_status": remote_reply["workflow_status"],
                "source": "local_workspace",
                "source_label": (
                    "Imported CSV draft"
                    if reply.source_batch_id
                    or reply.source_filename.lower().endswith((".csv", ".xlsx"))
                    else "Local workspace draft"
                ),
                "sync_status": "conflict" if conflict else reply.sync_status,
                "sync_error": reply.sync_error,
                "conflict": conflict,
                "remote_final_reply": remote_text,
                "local_final_reply": local_text,
                "last_pulled_at": case.frappe_last_workflow_pull_at,
            }
        else:
            current_reply = {
                **remote_reply,
                "display_status": remote_reply["reply_status"],
                "source": "live_helpdesk" if live_ticket else "crm_snapshot",
                "source_label": "Live Helpdesk" if live_ticket else "PostgreSQL Helpdesk snapshot",
                "sync_status": "synchronized" if remote_text or live_ticket else "not_synced",
                "sync_error": live_error or None,
                "conflict": False,
                "remote_final_reply": remote_text,
                "local_final_reply": local_text,
                "last_pulled_at": case.frappe_last_workflow_pull_at,
            }
        source_batch = (
            self.session.get(CrmBulkOperationBatch, reply.source_batch_id)
            if reply and reply.source_batch_id
            else None
        )
        source_item = (
            self.session.get(CrmBulkOperationItem, reply.source_item_id)
            if reply and reply.source_item_id
            else None
        )
        current_reply["source_filename"] = reply.source_filename if reply else None
        current_reply["source_row"] = reply.source_row if reply else None
        current_reply["source_kind"] = reply.source_kind if reply else None
        current_reply["workspace_version"] = reply.version if reply else None
        current_reply["workspace_updated_at"] = reply.updated_at if reply else None
        current_reply["last_synced_at"] = reply.last_synced_at if reply else None
        current_reply["source_item_id"] = str(source_item.id) if source_item else None
        current_reply["source_batch"] = (
            {
                "id": str(source_batch.id),
                "batch_number": source_batch.batch_number,
                "operation_type": source_batch.operation_type,
                "status": source_batch.status,
                "url": f"/crm/replies/bulk/batches/{source_batch.id}/",
            }
            if source_batch
            else None
        )
        latest_revision = next(
            (row for row in revisions if row.is_current), revisions[0] if revisions else None
        )
        revision_payloads = [
            {**row.model_dump(mode="json"), "version": len(revisions) - index}
            for index, row in enumerate(revisions)
        ]
        latest_revision_version = (
            len(revisions) - revisions.index(latest_revision)
            if latest_revision is not None
            else None
        )
        return {
            "case": {
                "id": str(case.id),
                "complaint_number": case.complaint_number,
                "state": case.state,
                "source_system": case.source_system,
                "complaint_text": case.remarks or "",
                "category_id": str(category.id) if category else None,
                "category": category.name if category else case.category or "",
                "subcategory_id": str(subcategory.id) if subcategory else None,
                "subcategory": subcategory.name if subcategory else case.sub_category or "",
                "classification_sync_status": case.classification_sync_status,
                "classification_last_error": case.classification_last_error,
                "helpdesk_ticket_id": case.frappe_ticket_id,
                "helpdesk_ticket_url": case.frappe_ticket_url,
                "paperless_document_id": case.canonical_paperless_document_id,
                "paperless_url": self._paperless_url(case),
                "created_at": case.created_at,
                "updated_at": case.updated_at,
            },
            "reply": current_reply,
            "workspace": local_snapshot,
            "approved_archive": {
                "reply_text": latest_revision.reply_text if latest_revision else "",
                "version": latest_revision_version,
                "approval_status": latest_revision.approval_status if latest_revision else None,
                "captured_at": latest_revision.captured_at if latest_revision else None,
            },
            "revisions": revision_payloads,
            "documents": [
                {
                    "id": str(document.id),
                    "filename": document.original_filename,
                    "role": link.role,
                    "review_state": link.review_state,
                    "paperless_document_id": document.paperless_document_id,
                    "source_kind": document.source_kind,
                    "uploaded_by": document.uploaded_by,
                    "created_at": document.created_at,
                    "download_url": (
                        f"/api/v1/crm/reply-workspace/documents/{document.id}/download"
                        if document.source_kind == "manual_upload"
                        else None
                    ),
                }
                for document, link in documents
            ],
            "reply_statuses": list(REPLY_STATUSES),
            "live_helpdesk": bool(live_ticket),
            "live_helpdesk_error": live_error,
        }

    def statistics(self) -> dict[str, Any]:
        cases = list(
            self.session.exec(select(ComplaintCase).where(reply_case_eligibility_clause())).all()
        )
        replies = {
            row.complaint_case_id: row for row in self.session.exec(select(ComplaintReply)).all()
        }
        effective_statuses: list[str] = []
        awaiting_reply = 0
        for case in cases:
            local = replies.get(case.id)
            local_pending = bool(
                local
                and local.reply_text.strip()
                and local.source_kind in {"bulk_import", "manual_editor"}
                and local.sync_status in {"not_synced", "pending", "failed", "conflict"}
            )
            status = (
                local.workspace_status
                if local_pending and local is not None
                else case.frappe_reply_approval_status or "Not Prepared"
            )
            effective_statuses.append(status)
            if status == "Not Prepared" and not _text(case.frappe_reply_text_snapshot):
                awaiting_reply += 1
        statuses = Counter(effective_statuses)
        return {
            "published_cases": sum(case.state == "published" for case in cases),
            "reply_eligible_cases": len(cases),
            "awaiting_classification": sum(case.category_id is None for case in cases),
            "awaiting_reply": awaiting_reply,
            "imported_drafts": statuses.get("Imported Draft", 0),
            "draft_replies": statuses.get("Draft", 0),
            "pending_approval": statuses.get("Pending Approval", 0),
            "approved": statuses.get("Approved", 0),
            "issued": statuses.get("Issued", 0),
            "rejected": statuses.get("Rejected", 0),
            "sync_failed": sum(row.sync_status == "failed" for row in replies.values()),
            "sync_conflicts": sum(row.sync_status == "conflict" for row in replies.values()),
            "ai_ready": sum(
                case.frappe_ai_eligible
                and bool(case.frappe_reply_hash)
                and (case.frappe_reply_approval_status or "") in APPROVED_REPLY_STATUSES
                for case in cases
            ),
            "statuses": dict(statuses),
        }

    def list_cases(
        self,
        *,
        category_id: uuid.UUID | None = None,
        subcategory_id: uuid.UUID | None = None,
        reply_status: str = "",
        source_system: str = "",
        search: str = "",
        ai_eligible: bool | None = None,
        date_from: date | None = None,
        date_to: date | None = None,
        scope: str = "all",
        source_batch_id: uuid.UUID | None = None,
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        if scope not in {"actionable", "all"}:
            raise ReplyValidationError("Reply queue scope must be actionable or all")
        filters: list[Any] = [reply_case_eligibility_clause()]
        pending_local_ids = select(ComplaintReply.complaint_case_id).where(
            ComplaintReply.sync_status.in_(("not_synced", "pending", "failed", "conflict")),
            ComplaintReply.source_kind.in_(("bulk_import", "manual_editor")),
            ComplaintReply.reply_text != "",
        )
        if scope == "actionable":
            filters.append(
                or_(
                    ComplaintCase.id.in_(pending_local_ids),
                    ComplaintCase.frappe_reply_approval_status.is_(None),
                    ComplaintCase.frappe_reply_approval_status.not_in(
                        tuple(APPROVED_REPLY_STATUSES)
                    ),
                )
            )
        if source_batch_id:
            filters.append(
                ComplaintCase.id.in_(
                    select(CrmBulkOperationItem.complaint_case_id).where(
                        CrmBulkOperationItem.batch_id == source_batch_id,
                        CrmBulkOperationItem.complaint_case_id.is_not(None),
                    )
                )
            )
        if category_id:
            filters.append(ComplaintCase.category_id == category_id)
        if subcategory_id:
            filters.append(ComplaintCase.sub_category_id == subcategory_id)
        if reply_status:
            normalized = "Not Prepared" if reply_status == "Not Started" else reply_status
            if normalized == "Not Prepared":
                filters.extend(
                    [
                        or_(
                            ComplaintCase.frappe_reply_approval_status.is_(None),
                            ComplaintCase.frappe_reply_approval_status == "Not Prepared",
                        ),
                        ~ComplaintCase.id.in_(pending_local_ids),
                    ]
                )
            elif normalized == "Imported Draft":
                filters.append(
                    ComplaintCase.id.in_(
                        select(ComplaintReply.complaint_case_id).where(
                            ComplaintReply.workspace_status == "Imported Draft",
                            ComplaintReply.sync_status.in_(
                                ("not_synced", "pending", "failed", "conflict")
                            ),
                        )
                    )
                )
            else:
                filters.append(
                    or_(
                        ComplaintCase.frappe_reply_approval_status == normalized,
                        ComplaintCase.id.in_(
                            select(ComplaintReply.complaint_case_id).where(
                                ComplaintReply.workspace_status == normalized,
                                ComplaintReply.sync_status.in_(
                                    ("not_synced", "pending", "failed", "conflict")
                                ),
                            )
                        ),
                    )
                )
        if source_system:
            filters.append(ComplaintCase.source_system == source_system)
        if ai_eligible is not None:
            filters.append(ComplaintCase.frappe_ai_eligible == ai_eligible)
        if date_from:
            filters.append(
                ComplaintCase.created_at >= datetime.combine(date_from, datetime.min.time())
            )
        if date_to:
            filters.append(
                ComplaintCase.created_at <= datetime.combine(date_to, datetime.max.time())
            )
        if search.strip():
            term = f"%{search.strip()}%"
            filters.append(
                or_(
                    ComplaintCase.complaint_number.ilike(term),
                    ComplaintCase.remarks.ilike(term),
                    ComplaintCase.category.ilike(term),
                    ComplaintCase.sub_category.ilike(term),
                    ComplaintCase.frappe_reply_text_snapshot.ilike(term),
                    ComplaintCase.id.in_(
                        select(ComplaintReply.complaint_case_id).where(
                            ComplaintReply.reply_text.ilike(term)
                        )
                    ),
                )
            )
        total = int(
            self.session.exec(select(func.count()).select_from(ComplaintCase).where(*filters)).one()
        )
        rows = list(
            self.session.exec(
                select(ComplaintCase)
                .where(*filters)
                .order_by(ComplaintCase.updated_at.desc(), ComplaintCase.id)
                .offset((page - 1) * page_size)
                .limit(page_size)
            ).all()
        )
        reply_map = (
            {
                row.complaint_case_id: row
                for row in self.session.exec(
                    select(ComplaintReply).where(
                        ComplaintReply.complaint_case_id.in_([case.id for case in rows])
                    )
                ).all()
            }
            if rows
            else {}
        )
        items: list[dict[str, Any]] = []
        for case in rows:
            local = reply_map.get(case.id)
            local_pending = bool(
                local
                and local.reply_text.strip()
                and local.source_kind in {"bulk_import", "manual_editor"}
                and local.sync_status in {"not_synced", "pending", "failed", "conflict"}
            )
            reply_text = (
                local.reply_text
                if local_pending and local is not None
                else _text(case.frappe_reply_text_snapshot)
            )
            status = (
                local.workspace_status
                if local_pending and local is not None
                else case.frappe_reply_approval_status or "Not Prepared"
            )
            items.append(
                {
                    "case_id": str(case.id),
                    "complaint_number": case.complaint_number,
                    "source_system": case.source_system,
                    "category_id": str(case.category_id) if case.category_id else None,
                    "category": case.category or "Unclassified",
                    "subcategory_id": str(case.sub_category_id) if case.sub_category_id else None,
                    "subcategory": case.sub_category or "Unclassified",
                    "complaint_preview": _text(case.remarks)[:500],
                    "reply_preview": reply_text[:500],
                    "reply_status": status,
                    "reply_source": ("local_workspace" if local_pending else "helpdesk_snapshot"),
                    "reply_sync_status": local.sync_status if local else "synchronized",
                    "ai_eligible": bool(case.frappe_ai_eligible),
                    "archive_ready": bool(case.frappe_reply_hash),
                    "classification_sync_status": case.classification_sync_status,
                    "workflow_status": case.frappe_workflow_status,
                    "helpdesk_ticket_id": case.frappe_ticket_id,
                    "helpdesk_ticket_url": case.frappe_ticket_url,
                    "paperless_document_id": case.canonical_paperless_document_id,
                    "updated_at": max(
                        [
                            value
                            for value in (case.updated_at, local.updated_at if local else None)
                            if value
                        ]
                    ),
                }
            )
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "scope": scope,
            "source_batch_id": str(source_batch_id) if source_batch_id else None,
            "pages": max(1, (total + page_size - 1) // page_size),
        }

    def audit(self, *, case_id: uuid.UUID | None = None, limit: int = 100) -> dict[str, Any]:
        query = select(ComplaintAuditEvent)
        if case_id:
            query = query.where(ComplaintAuditEvent.complaint_case_id == case_id)
        rows = list(
            self.session.exec(
                query.order_by(ComplaintAuditEvent.created_at.desc()).limit(max(1, min(limit, 500)))
            ).all()
        )
        return {"items": [row.model_dump(mode="json") for row in rows], "count": len(rows)}

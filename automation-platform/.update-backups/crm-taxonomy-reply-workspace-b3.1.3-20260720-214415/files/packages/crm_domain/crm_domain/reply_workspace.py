from __future__ import annotations

import uuid
from collections import Counter
from datetime import date, datetime
from typing import Any, Iterable

from sqlalchemy import func, or_
from sqlmodel import Session, select

from automation_core.config import Settings
from automation_core.time import utcnow
from crm_domain.json_safe import json_safe
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintCategory,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintReply,
    ComplaintReplyRevision,
    ComplaintSubcategory,
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

    def _require_category(self, category_id: uuid.UUID, *, active: bool = True) -> ComplaintCategory:
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
        if not case.frappe_ticket_id:
            raise ReplyValidationError("This complaint is not linked to Frappe Helpdesk")
        status = clean_name(reply_status)
        if status == "Not Started":
            status = "Not Prepared"
        if status not in REPLY_STATUSES:
            raise ReplyValidationError(f"Unsupported reply status: {status}")
        final_text = _text(final_reply)
        if status in {"Pending Approval", "Approved", "Issued"} and not final_text:
            raise ReplyValidationError("Enter the final reply before using this status")
        if ai_eligible and status not in APPROVED_REPLY_STATUSES:
            raise ReplyValidationError("AI eligibility is allowed only for Approved or Issued replies")
        before = self._reply_snapshot(case)
        values = self._reply_values(
            inquiry_findings=_text(inquiry_findings),
            school_version=_text(school_version),
            applicable_policy=_text(applicable_policy),
            final_reply=final_text,
            reply_status=status,
            disposal_outcome=_text(disposal_outcome),
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
                    str(pull_result.get("error") or "Helpdesk write succeeded but CRM pull-back failed")
                )
            refreshed = self._require_case(case.id)
            after = self._reply_snapshot(refreshed)
            self._frappe_event(
                refreshed,
                operation="save_reply_workspace",
                state="succeeded",
                details={
                    "status": status,
                    "verified": True,
                    "reply": pull_result.get("reply"),
                },
            )
            self._audit(
                case=refreshed,
                entity_type="complaint_reply",
                entity_id=str(refreshed.id),
                event_type="reply_saved",
                actor=actor,
                before=before,
                after=after,
                details={"write_verified": True, "pull_result": pull_result},
            )
            self.session.commit()
            return {
                "case_id": str(refreshed.id),
                "ticket_id": refreshed.frappe_ticket_id,
                "saved": True,
                "write_verified": True,
                "reply": after,
                "archive": pull_result.get("reply"),
            }
        except Exception as exc:
            error = str(exc)
            case_key = case.id
            self.session.rollback()
            failed_case = self.session.get(ComplaintCase, case_key)
            if failed_case is not None:
                self._frappe_event(
                    failed_case,
                    operation="save_reply_workspace",
                    state="failed",
                    details={"status": status, "values_present": sorted(values)},
                    error=error,
                )
                self._audit(
                    case=failed_case,
                    entity_type="complaint_reply",
                    entity_id=str(failed_case.id),
                    event_type="reply_save_failed",
                    actor=actor,
                    state="failed",
                    before=before,
                    details={"write_verified": False},
                    error=error,
                )
                self.session.commit()
            if isinstance(exc, ReplyWorkspaceError):
                raise ReplyRemoteError(error) from exc
            raise ReplyRemoteError(error) from exc

    def _paperless_url(self, case: ComplaintCase) -> str:
        root = str(self.settings.paperless_url or "").strip().rstrip("/")
        for suffix in ("/dashboard", "/api"):
            if root.endswith(suffix):
                root = root[: -len(suffix)]
        if not root or not case.canonical_paperless_document_id:
            return ""
        return f"{root}/documents/{case.canonical_paperless_document_id}/details"

    def editor(self, case_id: uuid.UUID) -> dict[str, Any]:
        case = self._require_case(case_id)
        category = self.session.get(ComplaintCategory, case.category_id) if case.category_id else None
        subcategory = (
            self.session.get(ComplaintSubcategory, case.sub_category_id)
            if case.sub_category_id
            else None
        )
        live_error = ""
        live_ticket: dict[str, Any] = {}
        if case.frappe_ticket_id:
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
        current_reply = {
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
                live_ticket.get("custom_ai_eligible")
                if live_ticket
                else case.frappe_ai_eligible
            ),
            "workflow_status": clean_name(
                str(live_ticket.get("status") if live_ticket else case.frappe_workflow_status or "")
            ),
        }
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
            "approved_archive": {
                "reply_text": reply.reply_text if reply else "",
                "version": reply.version if reply else None,
                "imported_at": reply.imported_at if reply else None,
            },
            "revisions": [row.model_dump(mode="json") for row in revisions],
            "documents": [
                {
                    "id": str(document.id),
                    "filename": document.original_filename,
                    "role": link.role,
                    "review_state": link.review_state,
                    "paperless_document_id": document.paperless_document_id,
                }
                for document, link in documents
            ],
            "reply_statuses": list(REPLY_STATUSES),
            "live_helpdesk": bool(live_ticket),
            "live_helpdesk_error": live_error,
        }

    def statistics(self) -> dict[str, Any]:
        cases = list(
            self.session.exec(
                select(ComplaintCase).where(ComplaintCase.state == "published")
            ).all()
        )
        statuses = Counter(case.frappe_reply_approval_status or "Not Prepared" for case in cases)
        return {
            "published_cases": len(cases),
            "awaiting_classification": sum(case.category_id is None for case in cases),
            "awaiting_reply": sum(
                (case.frappe_reply_approval_status or "Not Prepared") == "Not Prepared"
                and not _text(case.frappe_reply_text_snapshot)
                for case in cases
            ),
            "draft_replies": statuses.get("Draft", 0),
            "pending_approval": statuses.get("Pending Approval", 0),
            "approved": statuses.get("Approved", 0),
            "issued": statuses.get("Issued", 0),
            "rejected": statuses.get("Rejected", 0),
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
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        filters: list[Any] = [ComplaintCase.state == "published"]
        if category_id:
            filters.append(ComplaintCase.category_id == category_id)
        if subcategory_id:
            filters.append(ComplaintCase.sub_category_id == subcategory_id)
        if reply_status:
            normalized = "Not Prepared" if reply_status == "Not Started" else reply_status
            if normalized == "Not Prepared":
                filters.append(
                    or_(
                        ComplaintCase.frappe_reply_approval_status.is_(None),
                        ComplaintCase.frappe_reply_approval_status == "Not Prepared",
                    )
                )
            else:
                filters.append(ComplaintCase.frappe_reply_approval_status == normalized)
        if source_system:
            filters.append(ComplaintCase.source_system == source_system)
        if ai_eligible is not None:
            filters.append(ComplaintCase.frappe_ai_eligible == ai_eligible)
        if date_from:
            filters.append(ComplaintCase.created_at >= datetime.combine(date_from, datetime.min.time()))
        if date_to:
            filters.append(ComplaintCase.created_at <= datetime.combine(date_to, datetime.max.time()))
        if search.strip():
            term = f"%{search.strip()}%"
            filters.append(
                or_(
                    ComplaintCase.complaint_number.ilike(term),
                    ComplaintCase.remarks.ilike(term),
                    ComplaintCase.category.ilike(term),
                    ComplaintCase.sub_category.ilike(term),
                    ComplaintCase.frappe_reply_text_snapshot.ilike(term),
                )
            )
        total = int(
            self.session.exec(
                select(func.count()).select_from(ComplaintCase).where(*filters)
            ).one()
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
        return {
            "items": [
                {
                    "case_id": str(case.id),
                    "complaint_number": case.complaint_number,
                    "source_system": case.source_system,
                    "category_id": str(case.category_id) if case.category_id else None,
                    "category": case.category or "Unclassified",
                    "subcategory_id": str(case.sub_category_id) if case.sub_category_id else None,
                    "subcategory": case.sub_category or "Unclassified",
                    "complaint_preview": _text(case.remarks)[:500],
                    "reply_preview": _text(case.frappe_reply_text_snapshot)[:500],
                    "reply_status": case.frappe_reply_approval_status or "Not Prepared",
                    "ai_eligible": bool(case.frappe_ai_eligible),
                    "archive_ready": bool(case.frappe_reply_hash),
                    "classification_sync_status": case.classification_sync_status,
                    "workflow_status": case.frappe_workflow_status,
                    "helpdesk_ticket_id": case.frappe_ticket_id,
                    "helpdesk_ticket_url": case.frappe_ticket_url,
                    "paperless_document_id": case.canonical_paperless_document_id,
                    "updated_at": case.updated_at,
                }
                for case in rows
            ],
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": max(1, (total + page_size - 1) // page_size),
        }

    def audit(self, *, case_id: uuid.UUID | None = None, limit: int = 100) -> dict[str, Any]:
        query = select(ComplaintAuditEvent)
        if case_id:
            query = query.where(ComplaintAuditEvent.complaint_case_id == case_id)
        rows = list(
            self.session.exec(
                query.order_by(ComplaintAuditEvent.created_at.desc()).limit(
                    max(1, min(limit, 500))
                )
            ).all()
        )
        return {"items": [row.model_dump(mode="json") for row in rows], "count": len(rows)}

from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from datetime import datetime
from typing import Any, Iterable

from sqlmodel import Session, select

from automation_core.config import Settings
from automation_core.time import utcnow
from crm_domain.models import (
    ComplaintCase,
    ComplaintReply,
    ComplaintReplyRevision,
    FrappeSyncEvent,
)
from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskClient, FrappeHelpdeskError
from crm_integrations.frappe_helpdesk.service import build_client
from master_data.models import Officer


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "yes", "on"}


def _remote_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


class ComplaintHelpdeskWorkflowService:
    """Pull operational workflow and approved replies from Frappe into PostgreSQL."""

    def __init__(
        self,
        session: Session,
        settings: Settings,
        client: FrappeHelpdeskClient | None = None,
    ) -> None:
        self.session = session
        self.settings = settings
        self.client = client or build_client(settings)

    @property
    def approved_statuses(self) -> set[str]:
        return set(self.settings.frappe_helpdesk_approved_reply_status_list or ["Approved", "Issued"])

    def _linked_cases(self, *, limit: int, case_ids: Iterable[uuid.UUID] | None = None) -> list[ComplaintCase]:
        query = select(ComplaintCase).where(ComplaintCase.frappe_ticket_id.is_not(None))
        values = list(case_ids) if case_ids is not None else None
        if values is not None:
            if not values:
                return []
            query = query.where(ComplaintCase.id.in_(values))
        rows = list(
            self.session.exec(
                query.order_by(ComplaintCase.created_at, ComplaintCase.id).limit(max(1, min(limit, 500)))
            ).all()
        )
        if values is not None:
            by_id = {row.id: row for row in rows}
            return [by_id[value] for value in values if value in by_id]
        return rows

    def preview(self, *, limit: int = 200) -> dict[str, Any]:
        items = [
            {
                "case_id": str(case.id),
                "complaint_number": case.complaint_number,
                "ticket_id": case.frappe_ticket_id,
                "last_pull_at": case.frappe_last_workflow_pull_at,
                "workflow_status": case.frappe_workflow_status,
                "reply_approval_status": case.frappe_reply_approval_status,
                "reply_imported": bool(case.frappe_reply_hash),
            }
            for case in self._linked_cases(limit=limit)
        ]
        return {"items": items, "selected_case_ids": [item["case_id"] for item in items], "selected_count": len(items)}

    def _find_officer(self, ticket: dict[str, Any], assigned_users: list[str]) -> Officer | None:
        value = _clean(ticket.get("custom_deomee_officer_id"))
        if value:
            try:
                officer = self.session.get(Officer, uuid.UUID(value))
                if officer is not None:
                    return officer
            except ValueError:
                pass
        lowered = {user.casefold() for user in assigned_users}
        if not lowered:
            return None
        officers = self.session.exec(select(Officer).where(Officer.helpdesk_enabled == True)).all()  # noqa: E712
        return next((row for row in officers if row.helpdesk_user_email.casefold() in lowered), None)

    @staticmethod
    def _hash_text(value: str) -> str:
        return hashlib.sha256(value.replace("\r\n", "\n").strip().encode()).hexdigest()

    def _import_approved_reply(
        self,
        case: ComplaintCase,
        ticket: dict[str, Any],
        *,
        approval_status: str,
    ) -> dict[str, Any]:
        reply_text = _text(ticket.get("custom_final_approved_reply"))
        if approval_status not in self.approved_statuses:
            return {"imported": False, "reason": "reply_not_approved"}
        if not reply_text:
            return {"imported": False, "reason": "approved_reply_is_empty"}
        content_hash = self._hash_text(reply_text)
        existing_revision = self.session.exec(
            select(ComplaintReplyRevision).where(
                ComplaintReplyRevision.complaint_case_id == case.id,
                ComplaintReplyRevision.content_hash == content_hash,
            )
        ).first()
        revision_created = existing_revision is None
        previous = self.session.exec(
            select(ComplaintReplyRevision).where(
                ComplaintReplyRevision.complaint_case_id == case.id,
                ComplaintReplyRevision.is_current == True,  # noqa: E712
            )
        ).all()
        for row in previous:
            if existing_revision is None or row.id != existing_revision.id:
                row.is_current = False
                self.session.add(row)
        if existing_revision is None:
            existing_revision = ComplaintReplyRevision(
                complaint_case_id=case.id,
                reply_text=reply_text,
                content_hash=content_hash,
                source_reference=str(case.frappe_ticket_id or ""),
                approval_status=approval_status,
                remote_modified_at=_remote_datetime(ticket.get("modified")),
                is_current=True,
            )
        else:
            existing_revision.is_current = True
            existing_revision.approval_status = approval_status
            existing_revision.remote_modified_at = _remote_datetime(ticket.get("modified"))
        self.session.add(existing_revision)

        current = self.session.exec(
            select(ComplaintReply).where(ComplaintReply.complaint_case_id == case.id)
        ).first()
        changed = current is None or self._hash_text(current.reply_text) != content_hash
        if current is None:
            current = ComplaintReply(
                complaint_case_id=case.id,
                reply_text=reply_text,
                source_filename=f"frappe-helpdesk/{case.frappe_ticket_id}.txt",
                source_row=0,
                version=1,
                imported_at=utcnow(),
            )
        elif changed:
            current.reply_text = reply_text
            current.source_filename = f"frappe-helpdesk/{case.frappe_ticket_id}.txt"
            current.source_row = 0
            current.version += 1
            current.imported_at = utcnow()
            current.updated_at = utcnow()
        self.session.add(current)
        case.frappe_reply_hash = content_hash
        return {
            "imported": True,
            "changed": changed,
            "content_hash": content_hash,
            "reply_version": current.version,
            "revision_created": revision_created,
        }

    def _event(self, case: ComplaintCase, *, state: str, details: dict[str, Any], error: str | None = None) -> None:
        self.session.add(
            FrappeSyncEvent(
                complaint_case_id=case.id,
                direction="frappe_to_crm",
                operation="pull_workflow",
                state=state,
                remote_ticket_id=case.frappe_ticket_id,
                request_fingerprint=case.frappe_workflow_hash,
                error=error,
                details_json=details,
                completed_at=utcnow(),
            )
        )

    def pull_case(self, case_id: uuid.UUID) -> dict[str, Any]:
        case = self.session.get(ComplaintCase, case_id)
        if case is None:
            raise KeyError(f"Complaint case {case_id} not found")
        if not case.frappe_ticket_id:
            return {"case_id": str(case.id), "status": "blocked", "reason": "case_not_linked_to_helpdesk"}
        try:
            ticket = self.client.get_resource("HD Ticket", case.frappe_ticket_id)
            assignments = self.client.open_assignments(case.frappe_ticket_id)
            assigned_users = sorted({_clean(row.get("allocated_to")).lower() for row in assignments if _clean(row.get("allocated_to"))})
            officer = self._find_officer(ticket, assigned_users)
            snapshot = {
                "status": _clean(ticket.get("status")),
                "agent_group": _clean(ticket.get("agent_group")),
                "assigned_users": assigned_users,
                "approval_status": _clean(ticket.get("custom_reply_approval_status")) or "Not Prepared",
                "disposal_outcome": _clean(ticket.get("custom_disposal_outcome")),
                "ai_eligible": _truthy(ticket.get("custom_ai_eligible")),
                "inquiry_findings": _text(ticket.get("custom_inquiry_findings")),
                "school_version": _text(ticket.get("custom_school_version")),
                "applicable_policy": _text(ticket.get("custom_applicable_policy")),
                "reply_text": _text(ticket.get("custom_final_approved_reply")),
                "modified": ticket.get("modified"),
            }
            workflow_hash = hashlib.sha256(json.dumps(snapshot, sort_keys=True, default=str).encode()).hexdigest()
            case.frappe_workflow_status = snapshot["status"] or None
            case.frappe_agent_group = snapshot["agent_group"] or None
            case.frappe_assigned_users_json = assigned_users
            case.frappe_assigned_officer_id = officer.id if officer else None
            case.frappe_assigned_officer_name = officer.name if officer else _clean(ticket.get("custom_deomee_officer_name")) or None
            case.frappe_assigned_officer_role = officer.role if officer else _clean(ticket.get("custom_deomee_officer_role")).lower() or None
            case.frappe_reply_approval_status = snapshot["approval_status"]
            case.frappe_disposal_outcome = snapshot["disposal_outcome"] or None
            case.frappe_ai_eligible = snapshot["ai_eligible"]
            case.frappe_inquiry_findings = snapshot["inquiry_findings"] or None
            case.frappe_school_version = snapshot["school_version"] or None
            case.frappe_applicable_policy = snapshot["applicable_policy"] or None
            case.frappe_reply_text_snapshot = snapshot["reply_text"] or None
            case.frappe_workflow_hash = workflow_hash
            case.frappe_remote_modified_at = _remote_datetime(ticket.get("modified"))
            case.frappe_last_workflow_pull_at = utcnow()
            case.frappe_last_error = None
            case.updated_at = utcnow()
            reply_result = self._import_approved_reply(
                case,
                ticket,
                approval_status=snapshot["approval_status"],
            )
            details = {"workflow": snapshot, "reply": reply_result}
            self._event(case, state="succeeded", details=details)
            self.session.add(case)
            self.session.commit()
            return {
                "case_id": str(case.id),
                "complaint_number": case.complaint_number,
                "ticket_id": case.frappe_ticket_id,
                "status": "pulled",
                "workflow_status": case.frappe_workflow_status,
                "agent_group": case.frappe_agent_group,
                "assigned_users": assigned_users,
                "reply": reply_result,
            }
        except Exception as exc:
            case.frappe_last_error = f"Helpdesk workflow pull failed: {exc}"
            case.updated_at = utcnow()
            self._event(case, state="failed", details={"error_type": type(exc).__name__}, error=str(exc))
            self.session.add(case)
            self.session.commit()
            return {
                "case_id": str(case.id),
                "complaint_number": case.complaint_number,
                "ticket_id": case.frappe_ticket_id,
                "status": "failed",
                "error": str(exc),
                "http_status": exc.status_code if isinstance(exc, FrappeHelpdeskError) else None,
            }

    def pull_many(
        self,
        *,
        case_ids: Iterable[uuid.UUID] | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        cases = self._linked_cases(limit=limit, case_ids=case_ids)
        results = [self.pull_case(case.id) for case in cases]
        return {
            "results": results,
            "counts": dict(Counter(item["status"] for item in results)),
            "executed_case_ids": [item["case_id"] for item in results],
        }

    def statistics(self) -> dict[str, Any]:
        linked = self._linked_cases(limit=500)
        return {
            "linked_cases": len(linked),
            "pulled_cases": sum(case.frappe_last_workflow_pull_at is not None for case in linked),
            "approved_replies": sum(bool(case.frappe_reply_hash) for case in linked),
            "workflow_statuses": dict(Counter(case.frappe_workflow_status or "not_pulled" for case in linked)),
            "approval_statuses": dict(Counter(case.frappe_reply_approval_status or "not_pulled" for case in linked)),
            "routing_statuses": dict(Counter(case.frappe_routing_status for case in linked)),
        }

    def audit(self, *, limit: int = 200) -> dict[str, Any]:
        cases = self._linked_cases(limit=limit)
        return {
            "count": len(cases),
            "items": [
                {
                    "case_id": str(case.id),
                    "complaint_number": case.complaint_number,
                    "ticket_id": case.frappe_ticket_id,
                    "ticket_url": case.frappe_ticket_url,
                    "workflow_status": case.frappe_workflow_status,
                    "team": case.frappe_agent_group,
                    "assigned_users": case.frappe_assigned_users_json,
                    "assigned_officer": case.frappe_assigned_officer_name,
                    "routing_status": case.frappe_routing_status,
                    "reply_approval_status": case.frappe_reply_approval_status,
                    "reply_imported": bool(case.frappe_reply_hash),
                    "last_pull_at": case.frappe_last_workflow_pull_at,
                    "last_error": case.frappe_last_error,
                }
                for case in cases
            ],
        }

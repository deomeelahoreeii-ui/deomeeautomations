from __future__ import annotations

import uuid
from collections import Counter
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import func
from sqlmodel import Session, col, select

from automation_core.config import Settings
from automation_core.time import utcnow
from crm_domain.models import ComplaintCase, FrappeSyncEvent
from crm_integrations.frappe_helpdesk.bootstrap import ensure_helpdesk_foundation
from crm_integrations.frappe_helpdesk.client import (
    FrappeHelpdeskClient,
    FrappeHelpdeskError,
)
from crm_integrations.frappe_helpdesk.mapping import (
    helpdesk_ticket_url,
    ticket_payloads,
)


def build_client(settings: Settings) -> FrappeHelpdeskClient:
    return FrappeHelpdeskClient(
        base_url=settings.frappe_helpdesk_url,
        api_key=settings.frappe_helpdesk_api_key,
        api_secret=settings.frappe_helpdesk_api_secret,
        timeout_seconds=settings.frappe_helpdesk_timeout_seconds,
        verify_ssl=settings.frappe_helpdesk_verify_ssl,
        ca_bundle=settings.frappe_helpdesk_ca_bundle,
    )


def _parse_remote_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


class ComplaintHelpdeskSyncService:
    """One-way, idempotent CRM-to-Frappe synchronization service."""

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
    def eligible_states(self) -> set[str]:
        return set(self.settings.frappe_helpdesk_sync_state_list or ["published"])

    def configuration_errors(self) -> list[str]:
        errors: list[str] = []
        if not self.settings.frappe_helpdesk_enabled:
            errors.append("Frappe Helpdesk integration is disabled")
        if not self.settings.frappe_helpdesk_url:
            errors.append("FRAPPE_HELPDESK_URL is not configured")
        if not self.settings.frappe_helpdesk_api_key:
            errors.append("FRAPPE_HELPDESK_API_KEY is not configured")
        if not self.settings.frappe_helpdesk_api_secret:
            errors.append("FRAPPE_HELPDESK_API_SECRET is not configured")
        return errors

    def health(self) -> dict[str, Any]:
        errors = self.configuration_errors()
        if errors:
            return {
                "status": "not_configured",
                "errors": errors,
                "base_url": self.settings.frappe_helpdesk_url,
                "site": self.settings.frappe_helpdesk_site,
            }
        result = self.client.health()
        return {**result, "site": self.settings.frappe_helpdesk_site}

    def case_blockers(self, case: ComplaintCase) -> list[str]:
        blockers: list[str] = []
        if case.state not in self.eligible_states:
            blockers.append(
                f"Case state '{case.state}' is not eligible; allowed: {', '.join(sorted(self.eligible_states))}"
            )
        if not str(case.complaint_number or "").strip():
            blockers.append("Complaint number is missing")
        if not str(case.remarks or "").strip():
            blockers.append("Complaint remarks are missing")
        if not case.canonical_paperless_document_id:
            blockers.append("Canonical Paperless document is missing")
        return blockers

    def statistics(self) -> dict[str, Any]:
        rows = self.session.exec(
            select(ComplaintCase.frappe_sync_status, func.count(ComplaintCase.id)).group_by(
                ComplaintCase.frappe_sync_status
            )
        ).all()
        statuses = {str(status): int(count) for status, count in rows}
        total = self.session.exec(select(func.count()).select_from(ComplaintCase)).one()
        linked = self.session.exec(
            select(func.count()).select_from(ComplaintCase).where(
                ComplaintCase.frappe_ticket_id.is_not(None)
            )
        ).one()
        eligible = self.session.exec(
            select(func.count()).select_from(ComplaintCase).where(
                ComplaintCase.state.in_(sorted(self.eligible_states))
            )
        ).one()
        return {
            "total_cases": int(total),
            "eligible_cases": int(eligible),
            "linked_cases": int(linked),
            "statuses": statuses,
            "enabled": self.settings.frappe_helpdesk_enabled,
            "eligible_states": sorted(self.eligible_states),
        }

    def preview(self, *, limit: int = 200) -> dict[str, Any]:
        cases = list(
            self.session.exec(
                select(ComplaintCase)
                .order_by(col(ComplaintCase.updated_at).desc())
                .limit(max(1, min(limit, 1000)))
            ).all()
        )
        items: list[dict[str, Any]] = []
        counts: Counter[str] = Counter()
        for case in cases:
            blockers = self.case_blockers(case)
            if case.frappe_ticket_id:
                disposition = "linked"
            elif blockers:
                disposition = "blocked"
            else:
                disposition = "ready"
            counts[disposition] += 1
            items.append(
                {
                    "case_id": str(case.id),
                    "complaint_number": case.complaint_number,
                    "state": case.state,
                    "category": case.category,
                    "sub_category": case.sub_category,
                    "paperless_document_id": case.canonical_paperless_document_id,
                    "frappe_ticket_id": case.frappe_ticket_id,
                    "frappe_sync_status": case.frappe_sync_status,
                    "disposition": disposition,
                    "blockers": blockers,
                }
            )
        return {
            "items": items,
            "counts": dict(counts),
            "eligible_states": sorted(self.eligible_states),
            "configuration_errors": self.configuration_errors(),
        }

    def categories(self) -> list[str]:
        rows = self.session.exec(
            select(ComplaintCase.category)
            .where(ComplaintCase.category.is_not(None))
            .distinct()
            .order_by(ComplaintCase.category)
        ).all()
        return sorted({" ".join(str(value).split()).strip() for value in rows if str(value or "").strip()})

    def bootstrap(self) -> dict[str, Any]:
        errors = self.configuration_errors()
        if errors:
            raise RuntimeError("; ".join(errors))
        return ensure_helpdesk_foundation(self.client, self.categories())

    def _new_event(
        self,
        case: ComplaintCase,
        *,
        operation: str,
        fingerprint: str | None = None,
    ) -> FrappeSyncEvent:
        event = FrappeSyncEvent(
            complaint_case_id=case.id,
            direction="crm_to_frappe",
            operation=operation,
            state="started",
            remote_ticket_id=case.frappe_ticket_id,
            request_fingerprint=fingerprint,
        )
        self.session.add(event)
        return event

    def _locate_remote(self, case: ComplaintCase) -> dict[str, Any] | None:
        if case.frappe_ticket_id:
            try:
                return self.client.get_resource("HD Ticket", case.frappe_ticket_id)
            except FrappeHelpdeskError as exc:
                if exc.status_code != 404:
                    raise
        return self.client.find_ticket_by_deomee_case(str(case.id))

    def sync_case(self, case_id: uuid.UUID, *, force: bool = False) -> dict[str, Any]:
        case = self.session.get(ComplaintCase, case_id)
        if case is None:
            raise KeyError(f"Complaint case not found: {case_id}")
        blockers = self.case_blockers(case)
        if blockers:
            event = self._new_event(case, operation="sync")
            event.state = "skipped"
            event.error = " · ".join(blockers)
            event.completed_at = utcnow()
            self.session.add(event)
            self.session.commit()
            return {
                "case_id": str(case.id),
                "status": "blocked",
                "blockers": blockers,
                "frappe_ticket_id": case.frappe_ticket_id,
            }

        config_errors = self.configuration_errors()
        if config_errors:
            raise RuntimeError("; ".join(config_errors))

        next_version = case.frappe_sync_version + 1
        create_payload, update_payload, fingerprint = ticket_payloads(
            case,
            self.settings,
            sync_version=next_version,
        )
        event = self._new_event(case, operation="sync", fingerprint=fingerprint)
        case.frappe_sync_status = "pending"
        case.frappe_last_error = None
        self.session.add(case)
        self.session.add(event)
        self.session.commit()

        try:
            remote = self._locate_remote(case)
            remote_hash = str((remote or {}).get("custom_deomee_payload_hash") or "")
            if remote and not force and remote_hash == fingerprint:
                action = "unchanged"
                ticket = remote
            elif remote:
                action = "updated"
                ticket = self.client.update_resource(
                    "HD Ticket",
                    str(remote["name"]),
                    update_payload,
                )
            else:
                action = "created"
                ticket = self.client.create_resource("HD Ticket", create_payload)

            ticket_id = str(ticket.get("name") or (remote or {}).get("name") or "")
            if not ticket_id:
                raise FrappeHelpdeskError("Frappe did not return the created ticket identifier")
            case.frappe_ticket_id = ticket_id
            case.frappe_ticket_url = helpdesk_ticket_url(ticket_id, self.settings)
            case.frappe_sync_status = "synchronized"
            case.frappe_last_synced_at = utcnow()
            case.frappe_last_error = None
            case.frappe_remote_modified_at = _parse_remote_datetime(ticket.get("modified"))
            case.frappe_sync_version = (
                case.frappe_sync_version if action == "unchanged" else next_version
            )
            case.frappe_payload_hash = fingerprint
            event.state = "succeeded"
            event.remote_ticket_id = ticket_id
            event.details_json = {"action": action, "ticket_url": case.frappe_ticket_url}
            event.completed_at = utcnow()
            self.session.add(case)
            self.session.add(event)
            self.session.commit()
            return {
                "case_id": str(case.id),
                "status": action,
                "frappe_ticket_id": ticket_id,
                "frappe_ticket_url": case.frappe_ticket_url,
                "sync_version": case.frappe_sync_version,
            }
        except FrappeHelpdeskError as exc:
            case.frappe_sync_status = "failed"
            case.frappe_last_error = str(exc)
            event.state = "failed"
            event.error = str(exc)
            event.http_status = exc.status_code
            event.completed_at = utcnow()
            self.session.add(case)
            self.session.add(event)
            self.session.commit()
            return {
                "case_id": str(case.id),
                "status": "failed",
                "error": str(exc),
                "http_status": exc.status_code,
                "frappe_ticket_id": case.frappe_ticket_id,
            }
        except Exception as exc:
            case.frappe_sync_status = "failed"
            case.frappe_last_error = f"{type(exc).__name__}: {exc}"
            event.state = "failed"
            event.error = case.frappe_last_error
            event.completed_at = utcnow()
            self.session.add(case)
            self.session.add(event)
            self.session.commit()
            return {
                "case_id": str(case.id),
                "status": "failed",
                "error": case.frappe_last_error,
                "frappe_ticket_id": case.frappe_ticket_id,
            }

    def sync_many(
        self,
        *,
        case_ids: Iterable[uuid.UUID] | None = None,
        limit: int = 100,
        force: bool = False,
    ) -> dict[str, Any]:
        if case_ids is None:
            cases = list(
                self.session.exec(
                    select(ComplaintCase)
                    .where(ComplaintCase.state.in_(sorted(self.eligible_states)))
                    .order_by(col(ComplaintCase.updated_at).asc())
                    .limit(max(1, min(limit, 1000)))
                ).all()
            )
            ids = [case.id for case in cases]
        else:
            ids = list(dict.fromkeys(case_ids))[: max(1, min(limit, 1000))]
        results = [self.sync_case(case_id, force=force) for case_id in ids]
        statuses = Counter(str(item.get("status")) for item in results)
        return {
            "results": results,
            "counts": dict(statuses),
            "requested": len(ids),
        }

    def recent_events(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.session.exec(
            select(FrappeSyncEvent)
            .order_by(col(FrappeSyncEvent.created_at).desc())
            .limit(max(1, min(limit, 500)))
        ).all()
        return [row.model_dump(mode="json") for row in rows]

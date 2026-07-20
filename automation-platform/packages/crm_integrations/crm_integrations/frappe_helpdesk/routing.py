from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from typing import Any

from sqlmodel import Session, select

from automation_core.config import Settings
from automation_core.time import utcnow
from crm_domain.models import ComplaintCase, FrappeSyncEvent
from crm_integrations.frappe_helpdesk.batch_tokens import SignedBatchCodec, SignedBatchError
from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskClient, FrappeHelpdeskError
from crm_integrations.frappe_helpdesk.service import build_client
from master_data.models import Officer, Tehsil


def _clean(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _key(value: Any) -> str:
    return _clean(value).casefold()


class RoutingBatchChangedError(SignedBatchError):
    def __init__(self, changes: list[dict[str, Any]]) -> None:
        self.changes = changes
        super().__init__("The approved routing preview changed; run preview again before routing")


class ComplaintHelpdeskRoutingService:
    """Route linked complaints to Helpdesk teams while keeping master data in PostgreSQL."""

    def __init__(
        self,
        session: Session,
        settings: Settings,
        client: FrappeHelpdeskClient | None = None,
    ) -> None:
        self.session = session
        self.settings = settings
        self.client = client or build_client(settings)
        self.codec = SignedBatchCodec(
            secret=settings.frappe_helpdesk_api_secret,
            purpose="complaint-routing-v1",
            ttl_seconds=settings.frappe_helpdesk_preview_token_ttl_seconds,
        )
        self._agent_cache: dict[str, bool] = {}

    @property
    def triage_team(self) -> str:
        return _clean(self.settings.frappe_helpdesk_triage_team) or "Complaint Triage"

    def team_name(self, tehsil_name: str) -> str:
        prefix = _clean(self.settings.frappe_helpdesk_team_prefix) or "Complaints"
        return f"{prefix} - {_clean(tehsil_name)}"

    def _all_tehsils(self) -> list[Tehsil]:
        return list(self.session.exec(select(Tehsil).where(Tehsil.active == True).order_by(Tehsil.name)).all())  # noqa: E712

    def _all_ddeos(self) -> list[Officer]:
        return list(
            self.session.exec(
                select(Officer)
                .where(Officer.role == "ddeo", Officer.active == True)  # noqa: E712
                .order_by(Officer.name, Officer.id)
            ).all()
        )

    def _resolve(self, case: ComplaintCase) -> tuple[str, Officer | None, str]:
        tehsil_by_name = {_key(row.name): row for row in self._all_tehsils()}
        tehsil = tehsil_by_name.get(_key(case.tehsil))
        if tehsil is None:
            return self.triage_team, None, "Complaint tehsil is missing or not matched; routed to triage"
        candidates = [officer for officer in self._all_ddeos() if officer.tehsil_id == tehsil.id]
        candidates.sort(key=lambda row: (not row.helpdesk_enabled, not bool(row.helpdesk_user_email), row.name.casefold(), str(row.id)))
        officer = candidates[0] if candidates else None
        if officer is None:
            return self.team_name(tehsil.name), None, f"No active DDEO is registered for {tehsil.name}; team routing only"
        if officer.helpdesk_enabled and officer.helpdesk_user_email:
            return self.team_name(tehsil.name), officer, f"Matched active DDEO for {tehsil.name} with a Helpdesk account"
        return self.team_name(tehsil.name), officer, f"Matched active DDEO for {tehsil.name}; individual Helpdesk account is not linked"

    def _linked_cases(self, *, limit: int) -> list[ComplaintCase]:
        return list(
            self.session.exec(
                select(ComplaintCase)
                .where(ComplaintCase.frappe_ticket_id.is_not(None))
                .order_by(ComplaintCase.created_at, ComplaintCase.id)
                .limit(max(1, min(limit, 500)))
            ).all()
        )

    def _agent_is_active(self, email: str) -> bool:
        normalized = email.strip().lower()
        if not normalized:
            return False
        if normalized in self._agent_cache:
            return self._agent_cache[normalized]
        try:
            agent = self.client.get_resource("HD Agent", normalized)
            active = bool(int(agent.get("is_active", 0)))
        except FrappeHelpdeskError as exc:
            if exc.status_code != 404:
                raise
            active = False
        self._agent_cache[normalized] = active
        return active

    def _remote_context(self, case: ComplaintCase) -> tuple[dict[str, Any], list[str]]:
        if not case.frappe_ticket_id:
            return {}, []
        ticket = self.client.get_resource("HD Ticket", case.frappe_ticket_id)
        assignments = self.client.open_assignments(case.frappe_ticket_id)
        users = sorted({_clean(item.get("allocated_to")).lower() for item in assignments if _clean(item.get("allocated_to"))})
        return ticket, users

    def plan_case(self, case: ComplaintCase) -> dict[str, Any]:
        team, officer, reason = self._resolve(case)
        ticket, assigned_users = self._remote_context(case)
        current_team = _clean(ticket.get("agent_group"))
        configured_user = (
            officer.helpdesk_user_email.strip().lower()
            if officer and officer.helpdesk_enabled and officer.helpdesk_user_email.strip()
            else ""
        )
        target_user = configured_user if configured_user and self._agent_is_active(configured_user) else ""
        if configured_user and not target_user:
            reason = f"{reason}; configured Helpdesk agent {configured_user} is missing or inactive, so team routing will be used"
        expected_users = {target_user} if target_user else set()
        manual_assignment = bool(
            (current_team and current_team != team)
            or (assigned_users and not set(assigned_users).issubset(expected_users))
        )
        already_routed = bool(
            current_team == team
            and not manual_assignment
            and (not target_user or target_user in assigned_users)
        )
        disposition = (
            "manual_assignment_preserved"
            if manual_assignment
            else "already_routed"
            if already_routed
            else "ready"
        )
        revision_data = {
            "case_id": str(case.id),
            "ticket": case.frappe_ticket_id,
            "tehsil": _clean(case.tehsil),
            "target_team": team,
            "officer_id": str(officer.id) if officer else "",
            "target_user": target_user,
            "remote_modified": ticket.get("modified"),
            "current_team": current_team,
            "assigned_users": assigned_users,
        }
        revision = hashlib.sha256(json.dumps(revision_data, sort_keys=True, default=str).encode()).hexdigest()
        return {
            "case_id": str(case.id),
            "complaint_number": case.complaint_number,
            "ticket_id": case.frappe_ticket_id,
            "tehsil": case.tehsil,
            "target_team": team,
            "officer_id": str(officer.id) if officer else None,
            "officer_name": officer.name if officer else None,
            "officer_role": officer.role if officer else None,
            "officer_mobile": officer.mobile if officer else None,
            "target_user": target_user or None,
            "current_team": current_team or None,
            "assigned_users": assigned_users,
            "routing_reason": reason,
            "disposition": disposition,
            "preview_revision": revision,
        }

    def preview(self, *, limit: int = 200) -> dict[str, Any]:
        items = [self.plan_case(case) for case in self._linked_cases(limit=limit)]
        payload = {
            "batch_id": uuid.uuid4().hex[:16],
            "case_ids": [item["case_id"] for item in items],
            "revisions": {item["case_id"]: item["preview_revision"] for item in items},
        }
        token = self.codec.encode(payload) if items else None
        return {
            "items": items,
            "counts": dict(Counter(item["disposition"] for item in items)),
            "selected_count": len(items),
            "selected_case_ids": payload["case_ids"],
            "batch_id": payload["batch_id"],
            "batch_token": token,
        }

    def _ensure_team(self, team_name: str) -> str:
        try:
            self.client.get_resource("HD Team", team_name)
            return "unchanged"
        except FrappeHelpdeskError as exc:
            if exc.status_code != 404:
                raise
        users: list[dict[str, str]] = []
        fallback = self.settings.frappe_helpdesk_fallback_agent_email.strip().lower()
        if fallback:
            users.append({"user": fallback})
        self.client.create_resource(
            "HD Team",
            {"name": team_name, "team_name": team_name, "disabled": 0, "users": users},
        )
        return "created"

    def bootstrap_teams(self) -> dict[str, Any]:
        fallback = self.settings.frappe_helpdesk_fallback_agent_email.strip().lower()
        if fallback:
            try:
                agent = self.client.get_resource("HD Agent", fallback)
            except FrappeHelpdeskError as exc:
                if exc.status_code == 404:
                    raise RuntimeError(
                        f"Fallback Helpdesk agent {fallback} does not exist; create/activate it first"
                    ) from exc
                raise
            if not int(agent.get("is_active", 0)):
                raise RuntimeError(f"Fallback Helpdesk agent {fallback} is inactive")
        teams = [self.triage_team, *(self.team_name(row.name) for row in self._all_tehsils())]
        counts = Counter(self._ensure_team(team) for team in dict.fromkeys(teams))
        officers = self._all_ddeos()
        return {
            "teams": dict(counts),
            "team_names": list(dict.fromkeys(teams)),
            "fallback_agent": fallback or None,
            "active_ddeos": len(officers),
            "helpdesk_linked_ddeos": sum(bool(row.helpdesk_enabled and row.helpdesk_user_email) for row in officers),
        }

    def _event(self, case: ComplaintCase, *, state: str, details: dict[str, Any], error: str | None = None) -> None:
        self.session.add(
            FrappeSyncEvent(
                complaint_case_id=case.id,
                direction="crm_to_frappe",
                operation="route_ticket",
                state=state,
                remote_ticket_id=case.frappe_ticket_id,
                request_fingerprint=details.get("preview_revision"),
                error=error,
                details_json=details,
                completed_at=utcnow(),
            )
        )

    def apply(self, preview_token: str, *, force: bool = False) -> dict[str, Any]:
        approved = self.codec.decode(preview_token)
        case_ids = [uuid.UUID(value) for value in approved.get("case_ids", [])]
        expected = approved.get("revisions") if isinstance(approved.get("revisions"), dict) else {}
        plans: list[tuple[ComplaintCase, dict[str, Any]]] = []
        changes: list[dict[str, Any]] = []
        for case_id in case_ids:
            case = self.session.get(ComplaintCase, case_id)
            if case is None or not case.frappe_ticket_id:
                changes.append({"case_id": str(case_id), "reason": "case_or_ticket_link_missing"})
                continue
            plan = self.plan_case(case)
            if plan["preview_revision"] != expected.get(str(case_id)):
                changes.append({"case_id": str(case_id), "complaint_number": case.complaint_number, "reason": "routing_inputs_changed"})
            plans.append((case, plan))
        if changes:
            raise RoutingBatchChangedError(changes)

        results: list[dict[str, Any]] = []
        for case, plan in plans:
            try:
                if plan["disposition"] == "already_routed" and not force:
                    case.frappe_routing_status = "routed"
                    case.frappe_routing_reason = str(plan["routing_reason"])
                    case.frappe_agent_group = str(plan["target_team"])
                    case.updated_at = utcnow()
                    self._event(case, state="skipped", details={**plan, "action": "unchanged"})
                    self.session.add(case)
                    self.session.commit()
                    results.append({**plan, "status": "unchanged"})
                    continue
                if plan["disposition"] == "manual_assignment_preserved" and not force:
                    case.frappe_routing_status = "manual_preserved"
                    case.frappe_routing_reason = "Existing manual Helpdesk assignment was preserved"
                    case.updated_at = utcnow()
                    self._event(case, state="skipped", details=plan)
                    self.session.add(case)
                    self.session.commit()
                    results.append({**plan, "status": "manual_preserved"})
                    continue
                self._ensure_team(str(plan["target_team"]))
                payload = {
                    "agent_group": plan["target_team"],
                    "custom_deomee_routing_status": "Routed",
                    "custom_deomee_target_team": plan["target_team"],
                    "custom_deomee_officer_id": plan["officer_id"] or "",
                    "custom_deomee_officer_name": plan["officer_name"] or "",
                    "custom_deomee_officer_role": (plan["officer_role"] or "").upper(),
                    "custom_deomee_officer_mobile": plan["officer_mobile"] or "",
                    "custom_deomee_routing_reason": plan["routing_reason"],
                }
                ticket = self.client.get_resource("HD Ticket", str(case.frappe_ticket_id))
                if _clean(ticket.get("status")) == "New":
                    payload["status"] = "Assigned"
                remote = self.client.update_resource("HD Ticket", str(case.frappe_ticket_id), payload)
                if plan.get("target_user") and plan["target_user"] not in plan["assigned_users"]:
                    self.client.assign_to_ticket(
                        str(case.frappe_ticket_id),
                        [str(plan["target_user"])],
                        description=f"Assigned from Deomee master data: {plan['routing_reason']}",
                    )
                assignments = self.client.open_assignments(str(case.frappe_ticket_id))
                assigned_users = sorted({_clean(row.get("allocated_to")).lower() for row in assignments if _clean(row.get("allocated_to"))})
                case.frappe_agent_group = str(plan["target_team"])
                case.frappe_assigned_users_json = assigned_users
                case.frappe_assigned_officer_id = uuid.UUID(plan["officer_id"]) if plan.get("officer_id") else None
                case.frappe_assigned_officer_name = plan.get("officer_name")
                case.frappe_assigned_officer_role = plan.get("officer_role")
                case.frappe_routing_status = "routed"
                case.frappe_routing_reason = str(plan["routing_reason"])
                case.frappe_workflow_status = _clean(remote.get("status")) or case.frappe_workflow_status
                case.frappe_last_error = None
                case.updated_at = utcnow()
                self._event(case, state="succeeded", details={**plan, "assigned_users_after": assigned_users})
                self.session.add(case)
                self.session.commit()
                results.append({**plan, "status": "routed", "assigned_users_after": assigned_users})
            except Exception as exc:
                case.frappe_routing_status = "failed"
                case.frappe_routing_reason = str(exc)
                case.frappe_last_error = f"Helpdesk routing failed: {exc}"
                case.updated_at = utcnow()
                details = {**plan, "error_type": type(exc).__name__}
                self._event(case, state="failed", details=details, error=str(exc))
                self.session.add(case)
                self.session.commit()
                results.append({**plan, "status": "failed", "error": str(exc)})
        return {
            "batch_id": approved.get("batch_id"),
            "results": results,
            "counts": dict(Counter(item["status"] for item in results)),
            "executed_case_ids": [item["case_id"] for item in results],
        }

    def statistics(self) -> dict[str, Any]:
        linked = self._linked_cases(limit=500)
        return {
            "linked_cases": len(linked),
            "routing_statuses": dict(Counter(case.frappe_routing_status for case in linked)),
            "teams": dict(Counter(case.frappe_agent_group or "unassigned" for case in linked)),
        }

from __future__ import annotations

import uuid
from collections import Counter
from datetime import datetime
from typing import Any, Iterable

from sqlmodel import Session, select

from crm_domain.case_scopes import (
    APPROVED_REPLY_STATUSES,
    SENT_DISPATCH_TARGET_STATUSES,
    effective_reply_status,
    is_reply_actionable,
    is_reply_awaiting,
    is_reply_case_eligible,
)
from crm_domain.models import (
    ComplaintCase,
    ComplaintReply,
    CrmDispatchBatch,
    CrmDispatchItem,
    CrmDispatchTarget,
    CrmUpwardSubmissionClaim,
)


ACTIVE_TARGET_STATUSES = frozenset({"approved", "queued"})
ATTENTION_ROUTE_STATUSES = frozenset({"needs_review", "conflict", "blocked"})


def _empty_direction() -> dict[str, Any]:
    return {
        "status": "not_started",
        "item_count": 0,
        "target_counts": {},
        "compliance_statuses": [],
        "submission_status": None,
        "sent_at": None,
        "updated_at": None,
        "attention": False,
    }


def _latest(values: Iterable[datetime | None]) -> datetime | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _direction_status(
    *,
    target_statuses: set[str],
    route_statuses: set[str],
    submission_status: str | None,
    excluded_only: bool,
) -> str:
    # Delivery is stronger evidence than a later stale planning row. Once a
    # dispatch reached the recipient, the complaint must never regress to a
    # generic pending/planned state in registry projections.
    if "delivered" in target_statuses:
        return "delivered"
    if "sent_pending_confirmation" in target_statuses:
        return "sent_pending_confirmation"
    if target_statuses.intersection(ACTIVE_TARGET_STATUSES):
        return "sending"
    if target_statuses.intersection({"failed", "timed_out"}):
        return "failed"
    if submission_status == "sent":
        return "sent"
    if submission_status == "reserved":
        return "reserved"
    if "blocked" in target_statuses or route_statuses.intersection(ATTENTION_ROUTE_STATUSES):
        return "blocked"
    if "planned" in target_statuses or "ready" in route_statuses:
        return "planned"
    if excluded_only:
        return "excluded"
    return "not_started"


def complaint_lifecycle_projection(
    session: Session,
    cases: Iterable[ComplaintCase],
) -> dict[uuid.UUID, dict[str, Any]]:
    """Return one authoritative reply/dispatch projection for each case.

    Intake/Paperless state, reply state and dispatch state are deliberately
    independent. This projection never infers a reply or delivery from
    ``ComplaintCase.state``; it reads the durable reply, dispatch targets and
    upward-submission claim records that own those transitions.
    """

    case_list = list(cases)
    case_ids = [case.id for case in case_list]
    if not case_ids:
        return {}

    replies = {
        reply.complaint_case_id: reply
        for reply in session.exec(
            select(ComplaintReply).where(ComplaintReply.complaint_case_id.in_(case_ids))
        ).all()
    }
    dispatch_rows = list(
        session.exec(
            select(
                CrmDispatchItem,
                CrmDispatchBatch,
                CrmDispatchTarget,
                CrmUpwardSubmissionClaim,
            )
            .join(CrmDispatchBatch, CrmDispatchBatch.id == CrmDispatchItem.batch_id)
            .join(
                CrmDispatchTarget,
                CrmDispatchTarget.dispatch_item_id == CrmDispatchItem.id,
                isouter=True,
            )
            .join(
                CrmUpwardSubmissionClaim,
                CrmUpwardSubmissionClaim.dispatch_item_id == CrmDispatchItem.id,
                isouter=True,
            )
            .where(CrmDispatchItem.complaint_case_id.in_(case_ids))
        ).all()
    )

    grouped: dict[uuid.UUID, dict[str, list[tuple[Any, ...]]]] = {}
    for item, batch, target, claim in dispatch_rows:
        grouped.setdefault(item.complaint_case_id, {}).setdefault(batch.direction, []).append(
            (item, batch, target, claim)
        )

    result: dict[uuid.UUID, dict[str, Any]] = {}
    for case in case_list:
        reply = replies.get(case.id)
        eligible = is_reply_case_eligible(case, has_local_reply=reply is not None)
        reply_status = effective_reply_status(case, reply) if eligible else "Not eligible"
        directions: dict[str, dict[str, Any]] = {}
        for direction in ("downward", "upward"):
            rows = grouped.get(case.id, {}).get(direction, [])
            if not rows:
                directions[direction] = _empty_direction()
                continue
            active_claim_item_ids = {
                item.id
                for item, _batch, _target, claim in rows
                if claim is not None and claim.released_at is None
            }
            if direction == "upward" and active_claim_item_ids:
                rows = [row for row in rows if row[0].id in active_claim_item_ids]
            else:
                latest_batch = max(
                    (batch for _item, batch, _target, _claim in rows),
                    key=lambda batch: (batch.created_at, str(batch.id)),
                )
                rows = [row for row in rows if row[1].id == latest_batch.id]
            items = {item.id: item for item, _batch, _target, _claim in rows}
            targets = {
                target.id: target for _item, _batch, target, _claim in rows if target is not None
            }
            claims = {
                claim.id: claim
                for _item, _batch, _target, claim in rows
                if claim is not None and claim.released_at is None
            }
            target_counts = Counter(target.business_status for target in targets.values())
            target_statuses = set(target_counts)
            route_statuses = {item.route_status for item in items.values() if not item.excluded}
            active_claims = list(claims.values())
            submission_status = (
                "sent"
                if any(claim.status == "sent" for claim in active_claims)
                else "reserved"
                if any(claim.status == "reserved" for claim in active_claims)
                else None
            )
            excluded_only = bool(items) and all(item.excluded for item in items.values())
            status = _direction_status(
                target_statuses=target_statuses,
                route_statuses=route_statuses,
                submission_status=submission_status,
                excluded_only=excluded_only,
            )
            directions[direction] = {
                "status": status,
                "item_count": len(items),
                "target_counts": dict(sorted(target_counts.items())),
                "compliance_statuses": sorted({item.compliance_status for item in items.values()}),
                "submission_status": submission_status,
                "sent_at": _latest(
                    [target.sent_at for target in targets.values()]
                    + [claim.sent_at for claim in active_claims]
                ),
                "updated_at": _latest(
                    [item.updated_at for item in items.values()]
                    + [target.updated_at for target in targets.values()]
                    + [claim.updated_at for claim in active_claims]
                ),
                # Rows above are limited to the current claim/latest batch, so a
                # partial destination failure remains visible while failures in
                # superseded attempts do not regress a later successful send.
                "attention": bool(
                    target_statuses.intersection({"blocked", "failed", "timed_out"})
                    or route_statuses.intersection(ATTENTION_ROUTE_STATUSES)
                ),
            }

        result[case.id] = {
            "reply": {
                "eligible": eligible,
                "status": reply_status,
                "awaiting": eligible and is_reply_awaiting(case, reply),
                "actionable": eligible and is_reply_actionable(case, reply),
                "completed": eligible and reply_status in APPROVED_REPLY_STATUSES,
            },
            "dispatch": {
                **directions,
                "attention": any(item["attention"] for item in directions.values()),
                # A claim marked ``sent`` means the immutable packet was
                # attempted and must not re-enter the queue. Delivery targets
                # own the actual send outcome, including terminal failures.
                "upward_attempted": directions["upward"]["submission_status"] == "sent",
                "upward_sent": directions["upward"]["status"] in SENT_DISPATCH_TARGET_STATUSES,
                "upward_in_progress": directions["upward"]["submission_status"] == "reserved",
                "downward_sent": directions["downward"]["status"] in SENT_DISPATCH_TARGET_STATUSES,
            },
        }
    return result

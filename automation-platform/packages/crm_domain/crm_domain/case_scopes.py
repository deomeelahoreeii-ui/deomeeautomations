from __future__ import annotations

from typing import Any

from sqlalchemy import and_, or_
from sqlmodel import select

from crm_domain.models import ComplaintCase, ComplaintReply


TERMINAL_CASE_STATES = frozenset({"publishing", "published", "rejected"})
DOWNWARD_DISPATCH_STATES = frozenset({"fresh", "existing", "published", "review_required"})


def reply_case_eligibility_clause() -> Any:
    """Cases with a durable reply-workspace identity.

    Paperless discovery state is intentionally absent. A case belongs to the
    reply workspace when it is active and was published by the platform, has a
    Helpdesk ticket, or has a durable local reply. Keeping the published branch
    preserves pre-Helpdesk workflows while preventing Paperless `existing`
    observations from hiding linked operational cases.
    """

    local_reply_ids = select(ComplaintReply.complaint_case_id)
    return and_(
        ComplaintCase.registry_status == "active",
        or_(
            ComplaintCase.state == "published",
            ComplaintCase.frappe_ticket_id.is_not(None),
            ComplaintCase.id.in_(local_reply_ids),
        ),
    )


def is_reply_case_eligible(case: ComplaintCase, *, has_local_reply: bool = False) -> bool:
    return bool(
        case.registry_status == "active"
        and (case.state == "published" or case.frappe_ticket_id or has_local_reply)
    )


def formal_letter_case_eligibility_clause() -> Any:
    return ComplaintCase.registry_status == "active"


def downward_dispatch_case_eligibility_clause() -> Any:
    return and_(
        ComplaintCase.registry_status == "active",
        ComplaintCase.state.in_(tuple(DOWNWARD_DISPATCH_STATES)),
    )


def paperless_match_target_state(current_state: str, proposed_state: str) -> str:
    """Apply intake evidence without regressing an operational terminal state."""

    if current_state in TERMINAL_CASE_STATES:
        return current_state
    if current_state == "existing" and proposed_state == "review_required":
        return current_state
    return proposed_state

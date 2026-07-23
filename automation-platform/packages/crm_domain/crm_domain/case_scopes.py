from __future__ import annotations

from typing import Any

from sqlalchemy import and_, func, not_, or_
from sqlmodel import select

from crm_domain.models import ComplaintCase, ComplaintReply, ComplaintReplyRevision


TERMINAL_CASE_STATES = frozenset({"publishing", "published", "rejected"})
DOWNWARD_DISPATCH_STATES = frozenset({"fresh", "existing", "published"})
REPLY_ACCEPTED_CASE_STATES = frozenset({"fresh", "publishing", "published"})
APPROVED_REPLY_STATUSES = frozenset({"Approved", "Issued"})
LOCAL_REPLY_SOURCE_KINDS = frozenset({"bulk_import", "manual_editor"})
PENDING_REPLY_SYNC_STATUSES = frozenset({"not_synced", "pending", "failed", "conflict"})
NOT_PREPARED_REPLY_STATUSES = frozenset({"", "Not Prepared"})
SENT_DISPATCH_TARGET_STATUSES = frozenset({"sent_pending_confirmation", "delivered"})
ATTEMPTED_UPWARD_TARGET_STATUSES = frozenset(
    {"queued", *SENT_DISPATCH_TARGET_STATUSES, "failed", "timed_out"}
)


def _nonempty(column: Any) -> Any:
    return func.length(func.trim(func.coalesce(column, ""))) > 0


def pending_local_reply_clause() -> Any:
    """A durable local reply that still owns the effective workspace state."""

    return and_(
        ComplaintReply.id.is_not(None),
        _nonempty(ComplaintReply.reply_text),
        ComplaintReply.source_kind.in_(tuple(LOCAL_REPLY_SOURCE_KINDS)),
        ComplaintReply.sync_status.in_(tuple(PENDING_REPLY_SYNC_STATUSES)),
    )


def reply_has_text_clause() -> Any:
    return or_(
        _nonempty(ComplaintReply.reply_text),
        _nonempty(ComplaintCase.frappe_reply_text_snapshot),
    )


def reply_awaiting_clause() -> Any:
    """Cases for which no reply has actually been prepared yet."""

    return and_(
        not_(reply_has_text_clause()),
        or_(
            ComplaintCase.frappe_reply_approval_status.is_(None),
            ComplaintCase.frappe_reply_approval_status.in_(tuple(NOT_PREPARED_REPLY_STATUSES)),
        ),
    )


def reply_actionable_clause() -> Any:
    """Cases that still require drafting, review, synchronization, or revision."""

    return or_(
        pending_local_reply_clause(),
        ComplaintCase.frappe_reply_approval_status.is_(None),
        ComplaintCase.frappe_reply_approval_status.not_in(tuple(APPROVED_REPLY_STATUSES)),
    )


def reply_actionable_case_clause() -> Any:
    """Actionable predicate for queries that select only ``ComplaintCase``."""

    pending_local_ids = select(ComplaintReply.complaint_case_id).where(pending_local_reply_clause())
    return or_(
        ComplaintCase.id.in_(pending_local_ids),
        ComplaintCase.frappe_reply_approval_status.is_(None),
        ComplaintCase.frappe_reply_approval_status.not_in(tuple(APPROVED_REPLY_STATUSES)),
    )


def reply_awaiting_case_clause() -> Any:
    """Awaiting predicate for queries that select only ``ComplaintCase``."""

    local_text_ids = select(ComplaintReply.complaint_case_id).where(
        _nonempty(ComplaintReply.reply_text)
    )
    return and_(
        ~ComplaintCase.id.in_(local_text_ids),
        ~_nonempty(ComplaintCase.frappe_reply_text_snapshot),
        or_(
            ComplaintCase.frappe_reply_approval_status.is_(None),
            ComplaintCase.frappe_reply_approval_status.in_(tuple(NOT_PREPARED_REPLY_STATUSES)),
        ),
    )


def reply_case_eligibility_clause() -> Any:
    """Cases with a durable reply-workspace identity.

    Paperless discovery state is intentionally absent. A case belongs to the
    reply workspace when it is active and has been accepted by intake, was
    published by the platform, has a Helpdesk ticket, or has a durable local
    reply. Bare Paperless discovery records remain absent: `existing` alone is
    evidence of a remote document, not proof that this platform accepted the
    case into its reply workflow.
    """

    local_reply_ids = select(ComplaintReply.complaint_case_id)
    return and_(
        ComplaintCase.registry_status == "active",
        ComplaintCase.state != "rejected",
        or_(
            ComplaintCase.state.in_(tuple(REPLY_ACCEPTED_CASE_STATES)),
            ComplaintCase.frappe_ticket_id.is_not(None),
            ComplaintCase.id.in_(local_reply_ids),
        ),
    )


def is_reply_case_eligible(case: ComplaintCase, *, has_local_reply: bool = False) -> bool:
    return bool(
        case.registry_status == "active"
        and case.state != "rejected"
        and (case.state in REPLY_ACCEPTED_CASE_STATES or case.frappe_ticket_id or has_local_reply)
    )


def has_pending_local_reply(reply: ComplaintReply | None) -> bool:
    return bool(
        reply
        and reply.reply_text.strip()
        and reply.source_kind in LOCAL_REPLY_SOURCE_KINDS
        and reply.sync_status in PENDING_REPLY_SYNC_STATUSES
    )


def effective_reply_status(
    case: ComplaintCase,
    reply: ComplaintReply | None,
) -> str:
    if has_pending_local_reply(reply):
        assert reply is not None
        return reply.workspace_status or "Draft"
    return (case.frappe_reply_approval_status or "Not Prepared").strip() or "Not Prepared"


def effective_reply_text(
    case: ComplaintCase,
    reply: ComplaintReply | None,
) -> str:
    if has_pending_local_reply(reply):
        assert reply is not None
        return reply.reply_text.strip()
    remote = (case.frappe_reply_text_snapshot or "").strip()
    if remote:
        return remote
    return (reply.reply_text if reply else "").strip()


def is_reply_awaiting(
    case: ComplaintCase,
    reply: ComplaintReply | None,
) -> bool:
    return effective_reply_status(case, reply) == "Not Prepared" and not effective_reply_text(
        case, reply
    )


def is_reply_actionable(
    case: ComplaintCase,
    reply: ComplaintReply | None,
) -> bool:
    return (
        has_pending_local_reply(reply)
        or effective_reply_status(case, reply) not in APPROVED_REPLY_STATUSES
    )


def formal_letter_case_eligibility_clause() -> Any:
    return and_(
        ComplaintCase.registry_status == "active",
        ComplaintCase.state != "rejected",
    )


def reply_completed_case_clause() -> Any:
    """A reply completed in either Helpdesk or the immutable revision ledger."""

    approved_revision_ids = select(ComplaintReplyRevision.complaint_case_id).where(
        ComplaintReplyRevision.is_current.is_(True),
        ComplaintReplyRevision.approval_status.in_(tuple(APPROVED_REPLY_STATUSES)),
    )
    return or_(
        func.coalesce(ComplaintCase.frappe_reply_approval_status, "").in_(
            tuple(APPROVED_REPLY_STATUSES)
        ),
        ComplaintCase.id.in_(approved_revision_ids),
    )


def downward_dispatch_case_eligibility_clause() -> Any:
    return and_(
        ComplaintCase.registry_status == "active",
        ComplaintCase.state.in_(tuple(DOWNWARD_DISPATCH_STATES)),
        not_(reply_completed_case_clause()),
    )


def is_downward_dispatch_case_eligible(
    case: ComplaintCase,
    *,
    has_approved_revision: bool = False,
) -> bool:
    return bool(
        case.registry_status == "active"
        and case.state in DOWNWARD_DISPATCH_STATES
        and case.frappe_reply_approval_status not in APPROVED_REPLY_STATUSES
        and not has_approved_revision
    )


def paperless_match_target_state(current_state: str, proposed_state: str) -> str:
    """Apply intake evidence without regressing an operational terminal state."""

    if current_state in TERMINAL_CASE_STATES:
        return current_state
    if current_state == "existing" and proposed_state == "review_required":
        return current_state
    return proposed_state

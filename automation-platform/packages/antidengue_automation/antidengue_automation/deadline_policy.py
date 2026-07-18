from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlmodel import Session

from antidengue_automation.models import AntiDengueDeadlinePolicy, AntiDengueSchedule
from automation_core.config import get_settings
from automation_core.time import utcnow

DEFAULT_POLICY_KEY = "default"


def normalize_deadline(value: str) -> str:
    raw = value.strip()
    for pattern in ("%H:%M", "%I:%M %p"):
        try:
            parsed = datetime.strptime(raw.upper(), pattern)
            return parsed.strftime("%H:%M")
        except ValueError:
            continue
    raise ValueError("Submission deadline must be a valid time in HH:MM format")


def deadline_label(value: str) -> str:
    return datetime.strptime(normalize_deadline(value), "%H:%M").strftime("%I:%M %p").lstrip("0")


def validate_timezone(value: str) -> str:
    timezone = value.strip()
    try:
        ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError("Select a valid IANA timezone") from exc
    return timezone


def get_or_create_deadline_policy(session: Session) -> AntiDengueDeadlinePolicy:
    policy = session.get(AntiDengueDeadlinePolicy, DEFAULT_POLICY_KEY)
    if policy is not None:
        return policy
    fallback = normalize_deadline(get_settings().antidengue_submission_deadline)
    policy = AntiDengueDeadlinePolicy(
        policy_key=DEFAULT_POLICY_KEY,
        submission_deadline=fallback,
        timezone="Asia/Karachi",
        updated_by="legacy-config-migration",
    )
    session.add(policy)
    session.flush()
    return policy


@dataclass(frozen=True, slots=True)
class ResolvedDeadline:
    time: str
    label: str
    timezone: str
    policy_version: int
    source: str

    def model_dump(self) -> dict[str, str | int]:
        return asdict(self)


def resolve_deadline_policy(
    session: Session,
    *,
    schedule: AntiDengueSchedule | None = None,
) -> ResolvedDeadline:
    policy = get_or_create_deadline_policy(session)
    override = schedule.submission_deadline_override if schedule is not None else None
    value = normalize_deadline(override or policy.submission_deadline)
    timezone = validate_timezone(schedule.timezone if override and schedule else policy.timezone)
    return ResolvedDeadline(
        time=value,
        label=deadline_label(value),
        timezone=timezone,
        policy_version=policy.version,
        source="schedule_override" if override else "global",
    )


def deadline_from_snapshot(value: object) -> ResolvedDeadline:
    if not isinstance(value, dict):
        raise ValueError("Deadline snapshot is missing or invalid")
    normalized = normalize_deadline(str(value.get("time") or ""))
    label = deadline_label(normalized)
    supplied_label = str(value.get("label") or label).strip()
    if supplied_label != label:
        raise ValueError("Deadline snapshot label does not match its canonical time")
    timezone = validate_timezone(str(value.get("timezone") or ""))
    try:
        policy_version = int(value.get("policy_version") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("Deadline snapshot policy version is invalid") from exc
    if policy_version < 1:
        raise ValueError("Deadline snapshot policy version is invalid")
    source = str(value.get("source") or "").strip()
    if source not in {"global", "schedule_override"}:
        raise ValueError("Deadline snapshot source is invalid")
    return ResolvedDeadline(normalized, label, timezone, policy_version, source)


def update_deadline_policy(
    session: Session,
    *,
    submission_deadline: str,
    timezone: str,
    updated_by: str,
) -> AntiDengueDeadlinePolicy:
    policy = get_or_create_deadline_policy(session)
    normalized_deadline = normalize_deadline(submission_deadline)
    normalized_timezone = validate_timezone(timezone)
    changed = (
        policy.submission_deadline != normalized_deadline
        or policy.timezone != normalized_timezone
    )
    policy.submission_deadline = normalized_deadline
    policy.timezone = normalized_timezone
    policy.updated_by = updated_by.strip() or "web-operator"
    policy.updated_at = utcnow()
    if changed:
        policy.version += 1
    session.add(policy)
    session.commit()
    session.refresh(policy)
    return policy


def policy_dict(policy: AntiDengueDeadlinePolicy) -> dict[str, object]:
    return {
        "policy_key": policy.policy_key,
        "submission_deadline": policy.submission_deadline,
        "submission_deadline_label": deadline_label(policy.submission_deadline),
        "timezone": policy.timezone,
        "version": policy.version,
        "updated_by": policy.updated_by,
        "created_at": policy.created_at,
        "updated_at": policy.updated_at,
    }


__all__ = [
    "ResolvedDeadline",
    "deadline_label",
    "deadline_from_snapshot",
    "get_or_create_deadline_policy",
    "normalize_deadline",
    "policy_dict",
    "resolve_deadline_policy",
    "update_deadline_policy",
    "validate_timezone",
]

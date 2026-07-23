from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from sqlmodel import Session

from antidengue_automation.models import AntiDengueRuntimeState
from automation_core.time import utcnow


LAST_SCRAPE_STATE_KEY = "last_scrape_metadata"


def materialize_runtime_state(
    session: Session,
    *,
    state_key: str,
    destination: Path,
) -> dict[str, Any]:
    """Materialize PostgreSQL state only for the lifetime of a legacy subprocess."""

    row = session.get(AntiDengueRuntimeState, state_key)
    value = dict(row.value_json) if row else {}
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(destination)
    return value


def persist_runtime_state(
    session: Session,
    *,
    state_key: str,
    source: Path,
    job_id: uuid.UUID,
) -> dict[str, Any]:
    """Persist legacy subprocess state back to PostgreSQL after validation."""

    if not source.is_file():
        return {}
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Runtime state {state_key!r} must be a JSON object")
    row = session.get(AntiDengueRuntimeState, state_key)
    if row is None:
        row = AntiDengueRuntimeState(state_key=state_key)
    row.value_json = payload
    row.updated_by_job_id = job_id
    row.updated_at = utcnow()
    session.add(row)
    session.commit()
    return payload


__all__ = [
    "LAST_SCRAPE_STATE_KEY",
    "materialize_runtime_state",
    "persist_runtime_state",
]

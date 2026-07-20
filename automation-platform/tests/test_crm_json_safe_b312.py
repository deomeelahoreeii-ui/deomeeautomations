from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from enum import Enum

from crm_domain.json_safe import json_safe


class SampleState(Enum):
    ready = "ready"


@dataclass
class SamplePayload:
    identifier: uuid.UUID
    happened_at: datetime


def test_json_safe_recursively_normalizes_audit_payload_values() -> None:
    identifier = uuid.uuid4()
    happened_at = datetime(2026, 7, 20, 16, 18, 52, tzinfo=timezone.utc)
    payload = {
        "uuid": identifier,
        "datetime": happened_at,
        "date": date(2026, 7, 20),
        "time": time(16, 18, 52),
        "decimal": Decimal("12.50"),
        "enum": SampleState.ready,
        "set": {"alpha", "beta"},
        "dataclass": SamplePayload(identifier=identifier, happened_at=happened_at),
        "nested": [{"when": happened_at}],
    }

    normalized = json_safe(payload)

    # The exact persistence contract: the value must be accepted by the
    # standard encoder used by SQLAlchemy JSON columns on SQLite/PostgreSQL.
    encoded = json.dumps(normalized, sort_keys=True)
    assert identifier.hex in encoded.replace("-", "")
    assert normalized["datetime"] == happened_at.isoformat()
    assert normalized["dataclass"]["happened_at"] == happened_at.isoformat()
    assert normalized["nested"][0]["when"] == happened_at.isoformat()
    assert normalized["decimal"] == "12.50"
    assert normalized["enum"] == "ready"

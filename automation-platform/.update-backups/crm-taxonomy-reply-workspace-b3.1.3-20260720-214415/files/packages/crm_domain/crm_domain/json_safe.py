from __future__ import annotations

import dataclasses
import enum
import uuid
from collections.abc import Mapping
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any


def json_safe(value: Any) -> Any:
    """Return a recursively JSON-serializable representation.

    Audit/event payloads are persisted in SQLAlchemy JSON columns.  Domain
    snapshots contain datetimes, UUIDs and occasionally enums or dataclasses,
    which the standard JSON encoder cannot serialize directly.  Conversion is
    intentionally performed at the persistence boundary so normal API/domain
    responses can retain their native Python types.
    """

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, enum.Enum):
        return json_safe(value.value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return json_safe(dataclasses.asdict(value))
    if hasattr(value, "model_dump"):
        try:
            return json_safe(value.model_dump(mode="json"))
        except TypeError:
            return json_safe(value.model_dump())
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [json_safe(item) for item in value]
    return str(value)

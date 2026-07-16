import uuid

from automation_core.time import utcnow


def preview_key() -> str:
    now = utcnow()
    return f"AD-{now:%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6].upper()}"


_preview_key = preview_key

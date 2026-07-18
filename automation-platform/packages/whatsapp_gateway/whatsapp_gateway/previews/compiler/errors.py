from contextlib import contextmanager
from typing import Any, Iterator

class PreviewCompileError(ValueError):
    pass

def issue(code: str, severity: str, message: str, **details: Any) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        **details,
    }


def extend_unique_issues(
    target: list[dict[str, Any]], additions: list[dict[str, Any]]
) -> None:
    """Append run-level findings once instead of copying them to each delivery."""
    existing = {
        (item.get("code"), item.get("severity"), item.get("message"))
        for item in target
    }
    for item in additions:
        identity = (item.get("code"), item.get("severity"), item.get("message"))
        if identity not in existing:
            target.append(item)
            existing.add(identity)


class IssueCollector(list[dict[str, Any]]):
    """Deduplicates batch findings while retaining their affected recipients."""

    def __init__(self) -> None:
        super().__init__()
        self._identities: dict[str, dict[str, Any]] = {}
        self._affected: dict[str, str] | None = None

    @contextmanager
    def affecting(self, *, key: str, label: str, entity_type: str) -> Iterator[None]:
        previous = self._affected
        self._affected = {"key": key, "label": label, "entity_type": entity_type}
        try:
            yield
        finally:
            self._affected = previous

    @staticmethod
    def _identity(item: dict[str, Any]) -> str:
        import json

        excluded = {
            "occurrence_count", "affected_count", "affected_items",
            "affected_entity_type",
        }
        canonical = {key: value for key, value in item.items() if key not in excluded}
        return json.dumps(canonical, sort_keys=True, ensure_ascii=False, default=str)

    def append(self, item: dict[str, Any]) -> None:
        value = dict(item)
        identity = self._identity(value)
        existing = self._identities.get(identity)
        affected = self._affected
        if existing is None:
            value.setdefault("occurrence_count", 1)
            if affected is not None:
                value["affected_count"] = 1
                value["affected_entity_type"] = affected["entity_type"]
                value["affected_items"] = [
                    {"key": affected["key"], "label": affected["label"]}
                ]
            super().append(value)
            self._identities[identity] = value
            return
        existing["occurrence_count"] = int(existing.get("occurrence_count") or 1) + 1
        if affected is None:
            return
        items = list(existing.get("affected_items") or [])
        keys = {str(item.get("key")) for item in items}
        existing["affected_count"] = int(existing.get("affected_count") or 0) + (
            0 if affected["key"] in keys else 1
        )
        existing["affected_entity_type"] = affected["entity_type"]
        if affected["key"] not in keys and len(items) < 10:
            items.append({"key": affected["key"], "label": affected["label"]})
            existing["affected_items"] = items

    def extend(self, values: Iterator[dict[str, Any]] | list[dict[str, Any]]) -> None:
        for item in values:
            self.append(item)

    def add_batch(
        self, item: dict[str, Any], *, affected_count: int = 0,
        affected_entity_type: str = "",
    ) -> None:
        value = dict(item)
        if affected_count:
            value["affected_count"] = affected_count
            value["affected_entity_type"] = affected_entity_type
        self.append(value)

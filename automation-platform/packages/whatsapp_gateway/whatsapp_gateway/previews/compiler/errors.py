from typing import Any

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

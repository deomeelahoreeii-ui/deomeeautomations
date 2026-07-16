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


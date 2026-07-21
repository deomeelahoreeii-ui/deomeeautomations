from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JsonLogFormatter(logging.Formatter):
    """Small dependency-free JSON formatter for service logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "event": record.getMessage(),
        }
        context = getattr(record, "context", None)
        if isinstance(context, dict):
            payload.update(context)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_service_logging(*, level: str = "INFO", log_format: str = "json") -> logging.Logger:
    """Configure the shared application logger once per process."""

    logger = logging.getLogger("automation_platform")
    logger.setLevel(level.upper())
    logger.propagate = False

    if not any(getattr(handler, "_automation_platform", False) for handler in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler._automation_platform = True  # type: ignore[attr-defined]
        if log_format.strip().lower() == "json":
            handler.setFormatter(JsonLogFormatter())
        else:
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
            )
        logger.addHandler(handler)

    return logger

from __future__ import annotations

import re
from datetime import datetime

from automation_core.models import Job
from master_data.models import Wing
from whatsapp_gateway.rendering.antidengue.models import PAKISTAN_TIME


def _school_count(count: int) -> str:
    return f"{count} {'school' if count == 1 else 'schools'}"


def _wing_label(wing: Wing) -> str:
    value = (wing.code or wing.name).strip()
    return re.sub(r"^DEO[\s_-]+", "", value, flags=re.IGNORECASE) or value


def _wing_display_name(wing: Wing) -> str:
    label = _wing_label(wing).upper().replace("-", "").replace(" ", "")
    if label == "SE":
        return "Secondary Wing"
    return f"{_wing_label(wing)} Wing"


def _report_time(source_job: Job) -> datetime:
    value = source_job.finished_at or source_job.started_at or source_job.created_at
    if value.tzinfo is None:
        # PostgreSQL returns the configured server-local timestamp without its
        # offset in this deployment. Treat it as Pakistan time rather than
        # shifting an already-local clock by another five hours.
        value = value.replace(tzinfo=PAKISTAN_TIME)
    return value.astimezone(PAKISTAN_TIME)

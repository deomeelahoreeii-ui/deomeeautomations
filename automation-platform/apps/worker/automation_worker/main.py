from __future__ import annotations

from automation_core.celery_app import celery_app
import antidengue_automation.tasks  # noqa: F401
import crm_filters.tasks  # noqa: F401

__all__ = ["celery_app"]

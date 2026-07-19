from __future__ import annotations

import os
from pathlib import Path

from automation_core.config import get_settings


def test_suite_cannot_publish_to_or_write_into_the_development_stack() -> None:
    settings = get_settings()

    assert os.environ["AUTOMATION_TEST_ISOLATED"] == "1"
    assert settings.database_url.startswith("sqlite:///")
    assert "automation-platform-tests-" in settings.database_url
    assert settings.celery_broker_url == "memory://"
    assert settings.celery_result_backend == "cache+memory://"
    assert settings.antidengue_scheduler_enabled is False
    assert settings.ntfy_enabled is False
    assert settings.object_storage_enabled is False
    assert Path(settings.artifact_root).is_relative_to(
        Path(settings.database_url.removeprefix("sqlite:///")).parent
    )

"""Hard isolation boundary between tests and the live development stack."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest


# This file is loaded before test modules import the application, which is the
# only safe point to configure its module-level SQLAlchemy engine and Celery app.
_runtime = tempfile.TemporaryDirectory(prefix="automation-platform-tests-")
_runtime_root = Path(_runtime.name)
os.environ["DATABASE_URL"] = f"sqlite:///{_runtime_root / 'test.db'}"
os.environ["CELERY_BROKER_URL"] = "memory://"
os.environ["CELERY_RESULT_BACKEND"] = "cache+memory://"
os.environ["ARTIFACT_ROOT"] = str(_runtime_root / "artifacts")
os.environ["ANTIDENGUE_SCHEDULER_ENABLED"] = "false"
os.environ["NTFY_ENABLED"] = "false"
os.environ["OBJECT_STORAGE_ENABLED"] = "false"
os.environ["AUTOMATION_TEST_ISOLATED"] = "1"

# Import every table family before creating the disposable schema.  The normal
# API lifespan does the same transitively, but many unit tests use the shared
# engine without starting TestClient first.
import antidengue_automation.models  # noqa: E402,F401
import automation_core.models  # noqa: E402,F401
import crm_domain.models  # noqa: E402,F401
import master_data.models  # noqa: E402,F401
import whatsapp_gateway.models  # noqa: E402,F401
import whatsapp_gateway.persistence.audience_sources  # noqa: E402,F401
from automation_core.database import engine  # noqa: E402
from sqlmodel import SQLModel  # noqa: E402

SQLModel.metadata.create_all(engine)


@pytest.fixture(scope="session", autouse=True)
def isolated_application_runtime():
    yield _runtime_root
    engine.dispose()
    _runtime.cleanup()

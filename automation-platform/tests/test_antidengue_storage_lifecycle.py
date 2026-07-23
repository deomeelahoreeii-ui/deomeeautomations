from __future__ import annotations

import hashlib
import uuid
from datetime import timedelta
from pathlib import Path

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import antidengue_automation.models  # noqa: F401
import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from antidengue_automation.models import AntiDengueRuntimeState
from antidengue_automation.runtime_state import (
    LAST_SCRAPE_STATE_KEY,
    materialize_runtime_state,
    persist_runtime_state,
)
from antidengue_automation.storage_lifecycle import (
    antidengue_storage_counts,
    antidengue_workspace,
    evict_verified_antidengue_cache,
)
from automation_core.config import Settings
from automation_core.models import (
    Artifact,
    Job,
    JobStatus,
    JobType,
    SourceFile,
    SourceFileRun,
    StoredObject,
)
from automation_core.storage_catalog import ensure_artifact_local
from automation_core.time import utcnow
from whatsapp_gateway.models import WhatsAppDelivery


def memory_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def lifecycle_settings(tmp_path: Path) -> Settings:
    return Settings(
        artifact_root=tmp_path / "cache",
        antidengue_project_root=tmp_path / "legacy",
        antidengue_local_retention_hours=0,
        antidengue_failed_workspace_retention_hours=168,
    )


def seed_durable_artifact(
    session: Session,
    path: Path,
    *,
    job_status: str = JobStatus.succeeded.value,
    verified: bool = True,
) -> tuple[Job, Artifact, StoredObject]:
    payload = f"durable antidengue artifact:{path}".encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    old = utcnow() - timedelta(days=10)
    job = Job(
        type=JobType.antidengue_report.value,
        title="Lifecycle test",
        status=job_status,
        created_at=old,
        updated_at=old,
        finished_at=old if job_status not in {"queued", "running"} else None,
    )
    stored = StoredObject(
        backend="rustfs",
        bucket="automation-derived-test",
        object_key=f"antidengue/derived/{digest}.xlsx",
        sha256=digest,
        size_bytes=len(payload),
        status="ready",
        verified_at=old if verified else None,
    )
    session.add_all([job, stored])
    session.flush()
    artifact = Artifact(
        job_id=job.id,
        module_key="antidengue",
        kind="report",
        name=path.name,
        path=str(path.resolve()),
        size_bytes=len(payload),
        sha256=digest,
        stored_object_id=stored.id,
        storage_status="ready",
        archived_at=old,
        created_at=old,
    )
    session.add(artifact)
    session.commit()
    return job, artifact, stored


def test_verified_expired_cache_is_evicted_but_catalog_remains(tmp_path: Path) -> None:
    engine = memory_engine()
    settings = lifecycle_settings(tmp_path)
    path = settings.artifact_root / "antidengue-workspaces" / "job" / "report.xlsx"
    with Session(engine) as session:
        _, artifact, _ = seed_durable_artifact(session, path)
        preview = evict_verified_antidengue_cache(
            session, settings=settings, apply=False
        )
        assert preview["eligible"] == 1
        assert path.is_file()

        applied = evict_verified_antidengue_cache(
            session, settings=settings, apply=True
        )
        session.refresh(artifact)
        assert applied["evicted"] == 1
        assert not path.exists()
        assert artifact.local_evicted_at is not None
        assert antidengue_storage_counts(session)["rustfs_ready"] == 1


def test_active_delivery_reference_blocks_eviction(tmp_path: Path) -> None:
    engine = memory_engine()
    settings = lifecycle_settings(tmp_path)
    path = settings.artifact_root / "antidengue-workspaces" / "job" / "report.xlsx"
    with Session(engine) as session:
        seed_durable_artifact(session, path)
        delivery = WhatsAppDelivery(
            account_id=uuid.uuid4(),
            target="test@g.us",
            message="test",
            attachments=[{"path": str(path)}],
            status="queued",
            status_subject=f"test.{uuid.uuid4()}",
        )
        session.add(delivery)
        session.commit()

        blocked = evict_verified_antidengue_cache(
            session, settings=settings, apply=True
        )
        assert blocked["protected"] == 1
        assert path.is_file()

        delivery.status = "delivered"
        session.add(delivery)
        session.commit()
        assert evict_verified_antidengue_cache(
            session, settings=settings, apply=True
        )["evicted"] == 1


def test_active_source_file_run_blocks_shared_path_eviction(tmp_path: Path) -> None:
    engine = memory_engine()
    settings = lifecycle_settings(tmp_path)
    path = settings.source_file_root / "input.xlsx"
    with Session(engine) as session:
        _, artifact, stored = seed_durable_artifact(session, path)
        source = SourceFile(
            module_key="antidengue",
            original_name="input.xlsx",
            stored_path=str(path.resolve()),
            extension=".xlsx",
            size_bytes=artifact.size_bytes,
            sha256=artifact.sha256,
            stored_object_id=stored.id,
            storage_status="ready",
            archived_at=artifact.archived_at,
            validation_status="valid",
            created_at=artifact.created_at,
        )
        active_job = Job(
            type=JobType.antidengue_report.value,
            title="Active source consumer",
            status=JobStatus.running.value,
        )
        session.add_all([source, active_job])
        session.flush()
        session.add(SourceFileRun(source_file_id=source.id, job_id=active_job.id))
        session.commit()

        result = evict_verified_antidengue_cache(session, settings=settings, apply=True)
        assert result["protected"] == 1
        assert path.is_file()


def test_unverified_object_and_checksum_mismatch_are_never_evicted(tmp_path: Path) -> None:
    engine = memory_engine()
    settings = lifecycle_settings(tmp_path)
    first = settings.artifact_root / "antidengue-workspaces" / "one" / "report.xlsx"
    second = settings.artifact_root / "antidengue-workspaces" / "two" / "report.xlsx"
    with Session(engine) as session:
        seed_durable_artifact(session, first, verified=False)
        seed_durable_artifact(session, second)
        second.write_bytes(b"mutated after durable archival")

        result = evict_verified_antidengue_cache(session, settings=settings, apply=True)
        assert result["not_fully_durable"] == 1
        assert result["integrity_mismatch"] == 1
        assert first.is_file() and second.is_file()


def test_failed_workspace_uses_longer_retention_window(tmp_path: Path) -> None:
    engine = memory_engine()
    settings = lifecycle_settings(tmp_path)
    settings.antidengue_failed_workspace_retention_hours = 24 * 30
    path = settings.artifact_root / "antidengue-workspaces" / "failed" / "report.xlsx"
    with Session(engine) as session:
        seed_durable_artifact(session, path, job_status=JobStatus.failed.value)
        result = evict_verified_antidengue_cache(session, settings=settings, apply=True)
        assert result["eligible"] == 0
        assert path.is_file()


def test_evicted_artifact_hydrates_on_demand_and_records_access(
    tmp_path: Path, monkeypatch
) -> None:
    engine = memory_engine()
    settings = lifecycle_settings(tmp_path)
    original = settings.artifact_root / "antidengue-workspaces" / "job" / "report.xlsx"
    with Session(engine) as session:
        _, artifact, stored = seed_durable_artifact(session, original)
        payload = original.read_bytes()
        original.unlink()
        artifact.local_evicted_at = utcnow()
        session.add(artifact)
        session.commit()

        def fake_hydrate(_stored, destination, **_kwargs):
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(payload)
            return destination

        monkeypatch.setattr(
            "automation_core.storage_catalog.hydrate_stored_object", fake_hydrate
        )
        monkeypatch.setattr(
            "automation_core.storage_catalog.get_settings", lambda: settings
        )
        hydrated = ensure_artifact_local(session, artifact)
        session.commit()
        assert hydrated.read_bytes() == payload
        assert hydrated.name == f"{stored.sha256}.xlsx"
        assert artifact.last_hydrated_at is not None
        assert artifact.local_evicted_at is None


def test_runtime_cursor_round_trips_through_postgresql_owned_state(tmp_path: Path) -> None:
    engine = memory_engine()
    job_id = uuid.uuid4()
    with Session(engine) as session:
        session.add(
            Job(
                id=job_id,
                type=JobType.antidengue_report.value,
                title="State owner",
                status=JobStatus.running.value,
            )
        )
        session.add(
            AntiDengueRuntimeState(
                state_key=LAST_SCRAPE_STATE_KEY,
                value_json={"cursor": "old"},
            )
        )
        session.commit()
        materialized = tmp_path / "workspace" / "last_scrape_metadata.json"
        assert materialize_runtime_state(
            session,
            state_key=LAST_SCRAPE_STATE_KEY,
            destination=materialized,
        ) == {"cursor": "old"}
        materialized.write_text('{"cursor": "new"}', encoding="utf-8")
        assert persist_runtime_state(
            session,
            state_key=LAST_SCRAPE_STATE_KEY,
            source=materialized,
            job_id=job_id,
        ) == {"cursor": "new"}
        row = session.get(AntiDengueRuntimeState, LAST_SCRAPE_STATE_KEY)
        assert row is not None
        assert row.value_json == {"cursor": "new"}
        assert row.updated_by_job_id == job_id


def test_workspace_is_job_scoped_and_operational_outputs_are_gitignored(
    tmp_path: Path,
) -> None:
    settings = lifecycle_settings(tmp_path)
    first = antidengue_workspace(settings, uuid.uuid4())
    second = antidengue_workspace(settings, uuid.uuid4())
    assert first != second
    assert first.parent == settings.artifact_root.resolve() / "antidengue-workspaces"

    ignore = Path("../antidengue/.gitignore").read_text(encoding="utf-8")
    for pattern in (
        "drop-raw-files/",
        "output-files/",
        "unmapped-officer-reports/",
        "archived-files/",
        "last_scrape_metadata.json",
    ):
        assert pattern in ignore

    task_source = Path(
        "packages/antidengue_automation/antidengue_automation/tasks.py"
    ).read_text(encoding="utf-8")
    for variable in (
        "ANTIDENGUE_WORK_ROOT",
        "ANTIDENGUE_OUTPUT_DIR",
        "ANTIDENGUE_RAW_DIR",
        "ANTIDENGUE_LAST_SCRAPE_METADATA_PATH",
    ):
        assert variable in task_source

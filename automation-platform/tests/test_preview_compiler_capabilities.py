from __future__ import annotations

import uuid
from datetime import timedelta
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import antidengue_automation.models  # noqa: F401
import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.models import (
    AutomationWorkerRuntime, Job, JobStatus, JobType, TaskOutbox,
)
from automation_core.time import utcnow
from automation_core.worker_runtime import (
    WorkerCapabilityUnavailable, register_worker_runtime, require_compatible_workers,
)
from whatsapp_gateway.configuration.dynamic_audiences import ResolvedAudienceMember
from whatsapp_gateway.models import (
    WhatsAppAccount, WhatsAppApplication, WhatsAppAudience,
    WhatsAppDispatchPreview, WhatsAppDispatchProfile, WhatsAppReportType,
)
from whatsapp_gateway.previews.compiler.capabilities import (
    PREVIEW_COMPILER_PROTOCOL, PREVIEW_COMPILER_QUEUE,
    UnsupportedPlannerCapability,
    compiler_build_id, compiler_capabilities, compiler_fingerprint,
    local_compiler_runtime, required_compiler_contract,
    resolve_planner_capability,
)
from whatsapp_gateway.previews.compiler.plans import build_dispatch_plan
from whatsapp_gateway.previews.compiler.errors import IssueCollector, issue
from whatsapp_gateway.previews.creation import create_preview
from whatsapp_gateway.previews.schemas import PreviewInput


def _engine():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _register(session: Session, name: str, *, capabilities: dict[str, int] | None = None):
    return register_worker_runtime(
        session, worker_name=name, queues=[PREVIEW_COMPILER_QUEUE],
        protocols={"antidengue_preview": PREVIEW_COMPILER_PROTOCOL},
        capabilities=capabilities if capabilities is not None else compiler_capabilities(),
        capability_fingerprint=compiler_fingerprint(), build_id=compiler_build_id(),
        database_fingerprint="test",
    )


def _legacy_profile(session: Session) -> tuple[Job, WhatsAppDispatchProfile]:
    application = WhatsAppApplication(key="antidengue", name="AntiDengue")
    account = WhatsAppAccount(name="Primary", worker_key="capability-test")
    session.add_all([application, account]); session.flush()
    report = WhatsAppReportType(
        application_id=application.id, key="school_activity", name="School activity"
    )
    audience = WhatsAppAudience(application_id=application.id, key="test", name="Test")
    session.add_all([report, audience]); session.flush()
    profile = WhatsAppDispatchProfile(
        application_id=application.id, key="test", name="Test profile",
        report_type_id=report.id, audience_id=audience.id, account_id=account.id,
        recipient_channel="group", delivery_mode="groups",
    )
    source = Job(
        type=JobType.antidengue_report.value, title="Source",
        status=JobStatus.succeeded.value, parameters={"dry_run": True},
    )
    session.add_all([profile, source]); session.commit()
    return source, profile


def test_registry_declares_dynamic_markaz_digest_capability() -> None:
    capability = resolve_planner_capability(
        report_key="consolidated_action_digest", audience_kind="dynamic",
        channel="individual", scope_key="aeo", delivery_granularity="scope",
    )
    assert capability is not None
    assert capability.planner_key == "dynamic_digest"
    assert capability.capability_id == "antidengue.digest.dynamic-aeo-markaz"
    assert capability.revision == 2
    assert set(capability.delivery_granularities) == {"recipient", "scope"}


def test_unsupported_dynamic_route_never_falls_back_to_legacy(monkeypatch) -> None:
    import whatsapp_gateway.previews.compiler.capabilities as capability_module

    with Session(_engine()) as session:
        _, profile = _legacy_profile(session)
        profile.recipient_channel = "individual"
        session.add(profile); session.commit()
        monkeypatch.setattr(capability_module, "active_audience_sources", lambda *_: [object()])

        with pytest.raises(UnsupportedPlannerCapability) as exc_info:
            required_compiler_contract(session, [profile])

        assert exc_info.value.detail["code"] == "unsupported_preview_planner_capability"
        assert "legacy-static" not in str(exc_info.value.detail)


def test_all_live_queue_consumers_must_support_required_contract() -> None:
    with Session(_engine()) as session:
        _, profile = _legacy_profile(session)
        required = required_compiler_contract(session, [profile])
        compatible = _register(session, "compatible")
        assert require_compatible_workers(session, required) == [compatible]

        _register(session, "stale", capabilities={})
        with pytest.raises(WorkerCapabilityUnavailable) as exc_info:
            require_compatible_workers(session, required)
        assert exc_info.value.workers[1]["problems"] == [
            "missing antidengue.preview.legacy-static.v1"
        ]


def test_expired_worker_is_not_considered_live() -> None:
    with Session(_engine()) as session:
        _, profile = _legacy_profile(session)
        worker = _register(session, "expired")
        worker.last_seen_at = utcnow() - timedelta(minutes=5)
        session.add(worker); session.commit()
        with pytest.raises(WorkerCapabilityUnavailable) as exc_info:
            require_compatible_workers(session, required_compiler_contract(session, [profile]))
        assert exc_info.value.workers == []


def test_api_preflight_creates_neither_job_nor_outbox_without_worker() -> None:
    with Session(_engine()) as session:
        source, profile = _legacy_profile(session)
        with pytest.raises(HTTPException) as exc_info:
            create_preview(
                PreviewInput(source_job_id=source.id, dispatch_profile_id=profile.id), session
            )
        assert exc_info.value.status_code == 503
        assert exc_info.value.detail["code"] == "preview_worker_capability_unavailable"
        assert len(list(session.scalars(select(Job)).all())) == 1
        assert list(session.scalars(select(TaskOutbox)).all()) == []


def test_worker_side_contract_guard_fails_job_without_preview(monkeypatch) -> None:
    import whatsapp_gateway.dispatch.task_entrypoints as task_module

    engine = _engine()
    runtime = local_compiler_runtime(worker_name="guard-test")
    required = {**runtime, "capabilities": {"not.installed": 1}}
    with Session(engine) as session:
        job = Job(
            type=JobType.whatsapp_dispatch_preview.value, title="Guarded preview",
            parameters={"compiler_contract": required},
        )
        session.add(job); session.commit(); job_id = job.id
    monkeypatch.setattr(task_module, "engine", engine)
    with pytest.raises(RuntimeError, match="missing not.installed.v1"):
        task_module.compile_dispatch_preview_v2_job.run(str(job_id))
    with Session(engine) as session:
        job = session.get(Job, job_id)
        assert job is not None and job.status == JobStatus.failed.value
        assert "capability mismatch" in str(job.error).lower()
        assert list(session.scalars(select(WhatsAppDispatchPreview)).all()) == []


def test_repeated_member_invariant_is_one_structured_batch_issue() -> None:
    members = [
        ResolvedAudienceMember(
            id=uuid.uuid4(), audience_id=uuid.uuid4(), target_type="contact",
            target_key=f"aeo:{index}", route_scope_key="markaz",
            route_scope_value=str(uuid.uuid4()), route_scope_label=f"AEO {index}",
            enabled=True, source_id=uuid.uuid4(), officer_id=uuid.uuid4(),
            target_jid=f"92300000{index:03d}@s.whatsapp.net",
        )
        for index in range(25)
    ]
    context = SimpleNamespace(
        members=members,
        profile=SimpleNamespace(recipient_channel="individual"),
        planner_capability=None,
        report_type=SimpleNamespace(name="Unsupported dynamic report"),
    )
    result = build_dispatch_plan(context)
    assert result.dispatch_plan == []
    assert len(result.batch_issues) == 1
    assert result.batch_issues[0]["code"] == "profile_audience_capability_mismatch"
    assert result.batch_issues[0]["affected_count"] == 25
    assert result.batch_issues[0]["occurrence_count"] == 1


def test_issue_collector_aggregates_equal_findings_without_hiding_distinct_ones() -> None:
    collector = IssueCollector()
    for index in range(25):
        with collector.affecting(
            key=str(index), label=f"AEO {index}", entity_type="AEO recipient"
        ):
            collector.append(issue("missing_phone", "blocked", "No personal number is available."))
    with collector.affecting(key="different", label="AEO X", entity_type="AEO recipient"):
        collector.append(issue("missing_phone", "blocked", "The saved number is invalid."))
    assert len(collector) == 2
    assert collector[0]["occurrence_count"] == 25
    assert collector[0]["affected_count"] == 25
    assert len(collector[0]["affected_items"]) == 10
    assert collector[1]["occurrence_count"] == 1

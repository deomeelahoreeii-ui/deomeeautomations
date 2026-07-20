from __future__ import annotations

import asyncio
import hashlib
import uuid
from pathlib import Path

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import antidengue_automation.models  # noqa: F401
import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.models import Job, JobStatus, JobType
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppDelivery,
    WhatsAppDispatchApproval,
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
    WhatsAppSettings,
)
from whatsapp_gateway.previews.approval import approve_preview
from whatsapp_gateway.previews.schemas import PreviewApprovalInput
from whatsapp_gateway.previews.state import summarize_preview_state


def _engine():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _seed(session: Session, tmp_path: Path):
    account = WhatsAppAccount(
        name="Primary", worker_key=f"worker-{uuid.uuid4().hex}", enabled=True, connected=True
    )
    app = WhatsAppApplication(key="antidengue", name="AntiDengue")
    session.add_all([account, app]); session.flush()
    report = WhatsAppReportType(
        application_id=app.id, key="consolidated_action_digest",
        name="Consolidated Action Digest",
    )
    audience = WhatsAppAudience(application_id=app.id, key="all", name="All")
    session.add_all([report, audience]); session.flush()
    profile = WhatsAppDispatchProfile(
        application_id=app.id, key="digest", name="Digest",
        report_type_id=report.id, audience_id=audience.id, account_id=account.id,
        recipient_channel="group", delivery_mode="groups",
    )
    settings = WhatsAppSettings(default_account_id=account.id, live_delivery_enabled=True)
    source_job = Job(
        type=JobType.antidengue_report.value, title="Dry run",
        status=JobStatus.succeeded.value, parameters={"dry_run": True},
    )
    session.add_all([profile, settings, source_job]); session.flush()
    preview = WhatsAppDispatchPreview(
        preview_key=f"AD-{uuid.uuid4()}", application_id=app.id,
        source_job_id=source_job.id, dispatch_profile_id=profile.id,
        status="ready", profile_version=profile.version,
        application_name=app.name, report_type_name=report.name,
        audience_name=audience.name, profile_name=profile.name,
        account_name=account.name, ready_count=1, blocked_count=1,
        delivery_count=2, content_sha256="preview-hash",
        configuration_snapshot={"profile": {"id": str(profile.id), "version": profile.version}},
    )
    session.add(preview); session.flush()
    file_path = tmp_path / "digest.xlsx"
    file_path.write_bytes(b"exact frozen workbook")
    digest = hashlib.sha256(file_path.read_bytes()).hexdigest()
    artifact = WhatsAppDispatchPreviewArtifact(
        preview_id=preview.id, report_type_id=report.id, role="delivery",
        name=file_path.name, path_snapshot=str(file_path), size_bytes=file_path.stat().st_size,
        checksum_sha256=digest, status="ready",
    )
    session.add(artifact); session.flush()
    ready = WhatsAppDispatchPreviewDelivery(
        preview_id=preview.id, sequence=1, target_type="group",
        target_name="Clean group", target_jid="120000000001@g.us",
        message="Clean message", attachment_ids=[str(artifact.id)],
        routing_snapshot={"report_type_id": str(report.id)}, issues=[], status="ready",
        idempotency_key="clean",
    )
    blocked = WhatsAppDispatchPreviewDelivery(
        preview_id=preview.id, sequence=2, target_type="group",
        target_name="Blocked group", target_jid="120000000002@g.us",
        message="Blocked message", attachment_ids=[str(artifact.id)],
        routing_snapshot={"report_type_id": str(report.id)},
        issues=[{"code": "route_invalid", "severity": "blocked", "message": "Invalid route"}],
        status="blocked", idempotency_key="blocked",
    )
    session.add_all([ready, blocked]); session.commit()
    return preview.id, ready.id, blocked.id


def test_state_summary_keeps_delivery_counts_separate_from_issue_counts(tmp_path: Path) -> None:
    with Session(_engine()) as session:
        preview_id, _, _ = _seed(session, tmp_path)
        preview = session.get(WhatsAppDispatchPreview, preview_id)
        deliveries = session.exec(select(WhatsAppDispatchPreviewDelivery)).all()
        artifacts = session.exec(select(WhatsAppDispatchPreviewArtifact)).all()
        summary = summarize_preview_state(deliveries, preview.issues, artifacts)
        assert summary.ready_delivery_count == 1
        assert summary.blocked_delivery_count == 1
        assert summary.blocked_issue_count == 1
        assert summary.status == "ready"
        assert summary.partial_approval_available is True


def test_partial_approval_queues_only_eligible_deliveries_and_audits_exclusions(
    tmp_path: Path, monkeypatch,
) -> None:
    engine = _engine()
    with Session(engine) as session:
        preview_id, ready_id, blocked_id = _seed(session, tmp_path)

        async def healthy(_account):
            return {"ready": True}

        monkeypatch.setattr("whatsapp_gateway.api.worker_health", healthy)
        monkeypatch.setattr(
            "whatsapp_gateway.previews.approval.preview_is_stale",
            lambda *_args, **_kwargs: False,
        )
        monkeypatch.setattr(
            "whatsapp_gateway.previews.approval.publish_pending_tasks",
            lambda *_args, **_kwargs: {"published": 0},
        )
        monkeypatch.setattr(
            "whatsapp_gateway.previews.approval.stage_task",
            lambda *_args, **_kwargs: None,
        )

        job = asyncio.run(approve_preview(
            preview_id,
            PreviewApprovalInput(
                acknowledge_warnings=True,
                acknowledge_exclusions=True,
                approved_by="test-operator",
            ),
            session,
        ))
        assert job.id is not None

        approval = session.scalar(select(WhatsAppDispatchApproval))
        assert approval is not None
        assert approval.partial is True
        assert approval.delivery_count == 1
        assert approval.excluded_count == 1
        assert approval.approved_delivery_ids == [str(ready_id)]
        assert approval.excluded_deliveries[0]["id"] == str(blocked_id)
        assert approval.excluded_deliveries[0]["issues"][0]["code"] == "route_invalid"
        assert approval.approved_content_sha256

        queued = session.exec(select(WhatsAppDelivery)).all()
        assert len(queued) == 1
        assert queued[0].preview_delivery_id == ready_id


def test_partial_approval_requires_explicit_exclusion_acknowledgement(
    tmp_path: Path, monkeypatch,
) -> None:
    from fastapi import HTTPException

    with Session(_engine()) as session:
        preview_id, _, _ = _seed(session, tmp_path)
        monkeypatch.setattr(
            "whatsapp_gateway.previews.approval.preview_is_stale",
            lambda *_args, **_kwargs: False,
        )
        try:
            asyncio.run(approve_preview(
                preview_id,
                PreviewApprovalInput(
                    acknowledge_warnings=True,
                    acknowledge_exclusions=False,
                    approved_by="test-operator",
                ),
                session,
            ))
        except HTTPException as exc:
            assert exc.status_code == 422
            assert "excluded" in str(exc.detail)
        else:
            raise AssertionError("Partial approval must require explicit exclusion acknowledgement")


def test_stale_reason_identifies_template_body_change() -> None:
    from whatsapp_gateway.models import WhatsAppTemplate
    from whatsapp_gateway.previews.maintenance import preview_stale_reasons

    with Session(_engine()) as session:
        account = WhatsAppAccount(name="Primary", worker_key=f"worker-{uuid.uuid4().hex}")
        app = WhatsAppApplication(key="antidengue-stale", name="AntiDengue")
        session.add_all([account, app]); session.flush()
        report = WhatsAppReportType(application_id=app.id, key="digest", name="Digest")
        audience = WhatsAppAudience(application_id=app.id, key="empty", name="Empty")
        session.add_all([report, audience]); session.flush()
        template = WhatsAppTemplate(
            application_id=app.id, report_type_id=report.id,
            key=f"template-{uuid.uuid4().hex}", name="Digest template",
            body="{{report_body}}",
        )
        session.add(template); session.flush()
        profile = WhatsAppDispatchProfile(
            application_id=app.id, key="stale-profile", name="Stale profile",
            report_type_id=report.id, audience_id=audience.id, account_id=account.id,
            template_id=template.id, recipient_channel="group", delivery_mode="groups",
        )
        source_job = Job(
            type=JobType.antidengue_report.value, title="Dry run",
            status=JobStatus.succeeded.value, parameters={"dry_run": True},
        )
        session.add_all([profile, source_job]); session.flush()
        snapshot = {
            "profile": {"id": str(profile.id), "name": profile.name, "version": profile.version},
            "audience": {
                "id": str(audience.id), "target_keys": [], "target_routes": [],
            },
            "account": {"id": str(account.id), "worker_key": account.worker_key},
            "template": {"id": str(template.id), "body": template.body},
        }
        preview = WhatsAppDispatchPreview(
            preview_key=f"AD-{uuid.uuid4()}", application_id=app.id,
            source_job_id=source_job.id, dispatch_profile_id=profile.id,
            status="ready", profile_version=profile.version,
            application_name=app.name, report_type_name=report.name,
            audience_name=audience.name, profile_name=profile.name,
            account_name=account.name, configuration_snapshot=snapshot,
        )
        session.add(preview); session.commit()

        assert preview_stale_reasons(session, preview) == []
        template.body = "Changed {{report_body}}"
        session.add(template); session.commit()

        reasons = preview_stale_reasons(session, preview)
        assert [item["code"] for item in reasons] == ["template_body_changed"]


def test_batch_blocker_still_blocks_the_entire_preview(tmp_path: Path) -> None:
    with Session(_engine()) as session:
        preview_id, _, _ = _seed(session, tmp_path)
        preview = session.get(WhatsAppDispatchPreview, preview_id)
        deliveries = session.exec(select(WhatsAppDispatchPreviewDelivery)).all()
        artifacts = session.exec(select(WhatsAppDispatchPreviewArtifact)).all()
        summary = summarize_preview_state(
            deliveries,
            [{"code": "conflicting_profile_accounts", "severity": "blocked"}],
            artifacts,
        )
        assert summary.batch_blocked_count == 1
        assert summary.status == "blocked"
        assert summary.partial_approval_available is False


def test_blocked_attachment_used_by_eligible_delivery_is_a_batch_blocker(tmp_path: Path) -> None:
    with Session(_engine()) as session:
        preview_id, _, _ = _seed(session, tmp_path)
        preview = session.get(WhatsAppDispatchPreview, preview_id)
        deliveries = session.exec(select(WhatsAppDispatchPreviewDelivery)).all()
        artifact = session.scalar(select(WhatsAppDispatchPreviewArtifact))
        assert artifact is not None
        artifact.status = "blocked"
        artifact.issues = [{"code": "missing_attachment", "severity": "blocked"}]
        session.add(artifact); session.flush()

        summary = summarize_preview_state(deliveries, preview.issues, [artifact])
        assert summary.artifact_blocked_count == 1
        assert summary.status == "blocked"
        assert summary.partial_approval_available is False


def test_partial_approval_migration_is_the_single_linear_head() -> None:
    import ast

    versions = Path(__file__).resolve().parents[1] / "alembic" / "versions"
    revisions: dict[str, str | None] = {}
    for migration in versions.glob("*.py"):
        values: dict[str, object] = {}
        tree = ast.parse(migration.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.Assign):
                pairs = [(target, node.value) for target in node.targets]
            elif isinstance(node, ast.AnnAssign):
                pairs = [(node.target, node.value)]
            else:
                continue
            for target, value in pairs:
                if isinstance(target, ast.Name) and target.id in {"revision", "down_revision"}:
                    values[target.id] = ast.literal_eval(value)
        revision = values.get("revision")
        if isinstance(revision, str):
            down_revision = values.get("down_revision")
            revisions[revision] = down_revision if isinstance(down_revision, str) else None

    referenced = {value for value in revisions.values() if value}
    assert sorted(set(revisions) - referenced) == ["b1f4a7c9d203"]
    assert revisions["b1f4a7c9d203"] == "a1c3e5f7b902"
    assert revisions["a1c3e5f7b902"] == "a8d9e0f1b2c3"

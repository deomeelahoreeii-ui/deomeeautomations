from __future__ import annotations

import uuid

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
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
)
from whatsapp_gateway.previews.compiler.orchestrator import _merge_profile_previews


def memory_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def seed_profiles(session: Session) -> tuple[WhatsAppDispatchProfile, WhatsAppDispatchProfile]:
    account = WhatsAppAccount(name="Primary", worker_key=f"worker-{uuid.uuid4().hex}")
    app = WhatsAppApplication(key="antidengue", name="AntiDengue")
    session.add(account); session.add(app); session.flush()
    report = WhatsAppReportType(application_id=app.id, key="school_activity", name="School activity")
    audience = WhatsAppAudience(application_id=app.id, key="all", name="All routes")
    session.add(report); session.add(audience); session.flush()
    profiles = tuple(
        WhatsAppDispatchProfile(
            application_id=app.id,
            key=f"route-{index}",
            name=f"Route {index}",
            report_type_id=report.id,
            audience_id=audience.id,
            account_id=account.id,
            recipient_channel="group",
            delivery_mode="groups",
        )
        for index in (1, 2)
    )
    session.add_all(profiles); session.commit()
    return profiles  # type: ignore[return-value]


def make_preview(session: Session, job: Job, profile: WhatsAppDispatchProfile) -> WhatsAppDispatchPreview:
    preview = WhatsAppDispatchPreview(
        preview_key=f"AD-{uuid.uuid4()}",
        application_id=profile.application_id,
        source_job_id=job.id,
        dispatch_profile_id=profile.id,
        status="ready",
        profile_version=profile.version,
        application_name="AntiDengue",
        report_type_name="School activity",
        audience_name="All routes",
        profile_name=profile.name,
        account_name="Primary",
        configuration_snapshot={"profile": {"id": str(profile.id), "version": profile.version}},
    )
    session.add(preview); session.flush()
    return preview


def add_delivery(
    session: Session,
    preview: WhatsAppDispatchPreview,
    report_type_id: uuid.UUID,
    *,
    message: str,
    checksum: str,
    sequence: int,
) -> None:
    artifact = WhatsAppDispatchPreviewArtifact(
        preview_id=preview.id,
        report_type_id=report_type_id,
        name=f"{checksum}.pdf",
        path_snapshot=f"/tmp/{checksum}.pdf",
        checksum_sha256=checksum,
    )
    session.add(artifact); session.flush()
    session.add(WhatsAppDispatchPreviewDelivery(
        preview_id=preview.id,
        sequence=sequence,
        target_type="group",
        target_name="School group",
        target_jid="120000000000@g.us",
        message=message,
        attachment_ids=[str(artifact.id)],
        status="ready",
        idempotency_key=f"{preview.id}:{sequence}",
    ))


def test_multi_profile_merge_deduplicates_exact_routes_and_blocks_conflicts() -> None:
    engine = memory_engine()
    with Session(engine) as session:
        first, second = seed_profiles(session)
        job = Job(
            type=JobType.antidengue_report.value,
            title="Dry run",
            status=JobStatus.succeeded.value,
            parameters={"dry_run": True},
        )
        session.add(job); session.flush()
        one = make_preview(session, job, first)
        two = make_preview(session, job, second)
        add_delivery(session, one, first.report_type_id, message="Same", checksum="aaa", sequence=1)
        add_delivery(session, two, second.report_type_id, message="Same", checksum="aaa", sequence=1)
        add_delivery(session, two, second.report_type_id, message="Different", checksum="bbb", sequence=2)
        session.commit()

        merged = _merge_profile_previews(session, [one, two])
        deliveries = session.exec(select(WhatsAppDispatchPreviewDelivery).where(
            WhatsAppDispatchPreviewDelivery.preview_id == merged.id
        )).all()
        assert len(deliveries) == 2
        assert all(item.status == "blocked" for item in deliveries)
        assert all(any(issue["code"] == "conflicting_profile_payloads" for issue in item.issues) for item in deliveries)
        assert merged.blocked_count == 2
        assert merged.configuration_snapshot["profile_count"] == 2

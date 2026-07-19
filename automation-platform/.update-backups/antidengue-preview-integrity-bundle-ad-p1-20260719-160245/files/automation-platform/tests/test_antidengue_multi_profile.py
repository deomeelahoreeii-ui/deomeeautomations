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
        routing_snapshot={"report_type_id": str(report_type_id)},
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


def test_multi_profile_merge_keeps_one_copy_of_source_quality_issue() -> None:
    engine = memory_engine()
    with Session(engine) as session:
        first, second = seed_profiles(session)
        job = Job(
            type=JobType.antidengue_report.value,
            title="Dry run",
            status=JobStatus.succeeded.value,
            parameters={"dry_run": True},
        )
        session.add(job)
        session.flush()
        one = make_preview(session, job, first)
        two = make_preview(session, job, second)
        source_issue = {
            "code": "high_dormancy_rate",
            "severity": "warning",
            "message": "Dormant rate is unusually high.",
        }
        one.issues = [source_issue]
        two.issues = [source_issue]
        session.add(one)
        session.add(two)
        session.commit()

        merged = _merge_profile_previews(session, [one, two])

        assert merged.issues == [source_issue]
        assert merged.warning_count == 1


def test_multi_profile_merge_allows_distinct_reports_for_same_target() -> None:
    engine = memory_engine()
    with Session(engine) as session:
        dormant, hotspot = seed_profiles(session)
        second_report = WhatsAppReportType(
            application_id=hotspot.application_id,
            key="hotspot_distance_activity",
            name="Hotspot distance review",
        )
        session.add(second_report)
        session.flush()
        hotspot.report_type_id = second_report.id
        session.add(hotspot)
        job = Job(
            type=JobType.antidengue_report.value,
            title="Dry run",
            status=JobStatus.succeeded.value,
            parameters={"dry_run": True},
        )
        session.add(job)
        session.flush()
        one = make_preview(session, job, dormant)
        two = make_preview(session, job, hotspot)
        add_delivery(session, one, dormant.report_type_id, message="Dormant", checksum="aaa", sequence=1)
        add_delivery(session, two, hotspot.report_type_id, message="Hotspot", checksum="bbb", sequence=1)
        session.commit()

        merged = _merge_profile_previews(session, [one, two])
        deliveries = session.exec(select(WhatsAppDispatchPreviewDelivery).where(
            WhatsAppDispatchPreviewDelivery.preview_id == merged.id
        )).all()

        assert len(deliveries) == 2
        assert all(item.status == "ready" for item in deliveries)
        assert merged.ready_count == 2
        assert merged.blocked_count == 0


def test_multi_profile_merge_replaces_empty_report_warning_when_acknowledgement_is_pending() -> None:
    engine = memory_engine()
    with Session(engine) as session:
        first, second = seed_profiles(session)
        job = Job(
            type=JobType.antidengue_report.value,
            title="Dry run",
            status=JobStatus.succeeded.value,
            parameters={"dry_run": True},
        )
        session.add(job)
        session.flush()
        wing_preview = make_preview(session, job, first)
        acknowledgement_preview = make_preview(session, job, second)
        wing_preview.issues = [{
            "code": "nothing_to_dispatch",
            "severity": "warning",
            "message": "No dormant MEE schools were found.",
        }]
        for sequence, (name, status) in enumerate(
            (("SHALIMAR", "ready"), ("MODEL TOWN", "skipped"),
             ("RAIWIND", "skipped"), ("CITY", "skipped")),
            start=1,
        ):
            session.add(WhatsAppDispatchPreviewDelivery(
                preview_id=acknowledgement_preview.id,
                sequence=sequence,
                target_type="group",
                target_name=f"{name} heads",
                target_jid=f"12000000000{sequence}@g.us",
                message=f"No dormant schools were found in {name}.",
                attachment_ids=[],
                routing_snapshot={"delivery_kind": "zero_result_acknowledgement"},
                issues=[{
                    "code": (
                        "zero_result_acknowledgement_planned"
                        if status == "ready"
                        else "acknowledgement_already_sent_today"
                    ),
                    "severity": "info",
                    "message": name,
                }],
                status=status,
                idempotency_key=f"{acknowledgement_preview.id}:{sequence}",
            ))
        session.add(wing_preview)
        session.commit()

        merged = _merge_profile_previews(session, [wing_preview, acknowledgement_preview])

        assert merged.ready_count == 1
        assert merged.skipped_count == 3
        assert merged.warning_count == 0
        assert not any(issue["code"] == "nothing_to_dispatch" for issue in merged.issues)
        summary = next(
            issue
            for issue in merged.issues
            if issue["code"] == "zero_result_acknowledgements_resolved"
        )
        assert summary["severity"] == "info"
        assert summary["ready_count"] == 1
        assert summary["already_sent_count"] == 3


def test_multi_profile_merge_downgrades_empty_category_when_another_report_is_ready() -> None:
    engine = memory_engine()
    with Session(engine) as session:
        first, second = seed_profiles(session)
        job = Job(
            type=JobType.antidengue_report.value,
            title="Dry run",
            status=JobStatus.succeeded.value,
            parameters={"dry_run": True},
        )
        session.add(job)
        session.flush()
        dormant_preview = make_preview(session, job, first)
        hotspot_preview = make_preview(session, job, second)
        dormant_preview.issues = [{
            "code": "nothing_to_dispatch",
            "severity": "warning",
            "message": "No dormant MEE schools were found.",
        }]
        session.add(WhatsAppDispatchPreviewDelivery(
            preview_id=hotspot_preview.id,
            sequence=1,
            target_type="group",
            target_name="MEE heads",
            target_jid="120000000001@g.us",
            message="Activities requiring review.",
            attachment_ids=[],
            routing_snapshot={"delivery_kind": "hotspot_distance_review"},
            issues=[],
            status="ready",
            idempotency_key=f"{hotspot_preview.id}:1",
        ))
        session.add(dormant_preview)
        session.commit()

        merged = _merge_profile_previews(session, [dormant_preview, hotspot_preview])

        assert merged.ready_count == 1
        assert merged.warning_count == 0
        assert not any(issue["code"] == "nothing_to_dispatch" for issue in merged.issues)
        summary = next(
            issue
            for issue in merged.issues
            if issue["code"] == "empty_report_category_resolved"
        )
        assert summary["severity"] == "info"
        assert summary["ready_delivery_count"] == 1

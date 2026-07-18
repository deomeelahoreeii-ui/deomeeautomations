from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.models import Job, JobStatus, JobType
from master_data.models import Department, District, Tehsil, Wing
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppDailyMessageClaim,
    WhatsAppDirectoryGroup,
    WhatsAppDispatchApproval,
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchProfile,
    WhatsAppRecipientScope,
    WhatsAppReportType,
    WhatsAppTemplate,
)
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.zero_result_acknowledgements import (
    build_zero_result_plan,
)
from whatsapp_gateway.previews.approval import _claim_daily_message


def _engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _context(session: Session, *, finished_at: datetime) -> tuple[CompileContext, WhatsAppDirectoryGroup, Tehsil]:
    district = District(name=f"Lahore {uuid.uuid4().hex}")
    department = Department(name=f"School Education {uuid.uuid4().hex}")
    session.add_all([district, department])
    session.flush()
    wing = Wing(
        district_id=district.id,
        department_id=department.id,
        name="DEO MEE",
    )
    tehsil = Tehsil(district_id=district.id, name="MODEL TOWN")
    account = WhatsAppAccount(name="Primary", worker_key=f"worker-{uuid.uuid4().hex}")
    application = WhatsAppApplication(key="antidengue", name="AntiDengue")
    session.add_all([wing, tehsil, account, application])
    session.flush()
    report = WhatsAppReportType(
        application_id=application.id,
        key="tehsil_dormant_summary",
        name="Tehsil Dormant Summary",
        artifact_kind="message",
    )
    scope = WhatsAppRecipientScope(
        application_id=application.id,
        channel="group",
        key="tehsil",
        name="Tehsil group",
    )
    audience = WhatsAppAudience(application_id=application.id, key="tehsils", name="Tehsils")
    session.add_all([report, scope, audience])
    session.flush()
    profile = WhatsAppDispatchProfile(
        application_id=application.id,
        key="tehsil_ack",
        name="Tehsil acknowledgements",
        report_type_id=report.id,
        audience_id=audience.id,
        account_id=account.id,
        recipient_scope_id=scope.id,
        recipient_channel="group",
        wing_id=wing.id,
        delivery_mode="groups",
    )
    group = WhatsAppDirectoryGroup(
        account_id=account.id,
        jid="120363319976862432@g.us",
        name="Model Town Heads",
    )
    job = Job(
        type=JobType.antidengue_report.value,
        title="Dry run",
        status=JobStatus.succeeded.value,
        parameters={"dry_run": True},
        result={"summary": {"quality_gate": {"passed": True}}},
        finished_at=finished_at,
    )
    session.add_all([profile, group, job])
    for index, body in enumerate(
        (
            "Variant one for {{tehsil}} / {{wing}}",
            "Variant two for {{tehsil}} / {{wing}}",
            "Variant three for {{tehsil}} / {{wing}}",
        ),
        start=1,
    ):
        session.add(
            WhatsAppTemplate(
                application_id=application.id,
                report_type_id=report.id,
                recipient_scope_id=scope.id,
                recipient_channel="group",
                key=f"ack_{index}",
                name=f"Acknowledgement {index}",
                category="zero_result_acknowledgement",
                body=body,
            )
        )
    session.commit()
    return (
        CompileContext(
            session=session,
            source_job=job,
            profile=profile,
            application=application,
            report_type=report,
            audience=audience,
            account=account,
            template=None,
            wing=wing,
            recipient_scope=scope,
            members=[],
            audience_group_ids={group.id},
            audience_contact_ids=set(),
            summary={"quality_gate": {"passed": True}},
            whatsapp_summary={},
            source_dispatch_plan=[],
            source_plans=[],
        ),
        group,
        tehsil,
    )


def test_zero_result_pool_rotates_daily_and_claim_suppresses_same_day() -> None:
    engine = _engine()
    with Session(engine) as session:
        start = datetime(2026, 7, 18, 6, 0, tzinfo=UTC)
        ctx, group, tehsil = _context(session, finished_at=start)
        first = build_zero_result_plan(
            ctx,
            member_id=uuid.uuid4(),
            directory_group=group,
            tehsil=tehsil,
        )
        assert first is not None and first["status"] == "planned"
        assert first["payload"]["attachment_paths"] == []
        assert "MODEL TOWN" in first["payload"]["text"]

        claim = first["daily_claim"]
        session.add(
            WhatsAppDailyMessageClaim(
                semantic_key=claim["semantic_key"],
                business_date=datetime.fromisoformat(claim["business_date"]).date(),
                purpose=claim["purpose"],
                account_id=ctx.account.id,
                application_id=ctx.application.id,
                report_type_id=ctx.report_type.id,
                template_id=uuid.UUID(claim["template_id"]),
                preview_id=uuid.uuid4(),
                preview_delivery_id=uuid.uuid4(),
                approval_id=uuid.uuid4(),
                target_jid=group.jid,
                scope_key=claim["scope_key"],
                scope_value=claim["scope_value"],
                scope_label=claim["scope_label"],
                template_key=claim["template_key"],
            )
        )
        session.commit()
        repeated = build_zero_result_plan(
            ctx,
            member_id=uuid.uuid4(),
            directory_group=group,
            tehsil=tehsil,
        )
        assert repeated is not None and repeated["status"] == "skipped"
        assert repeated["skip_issue_code"] == "acknowledgement_already_sent_today"

        ctx.source_job.finished_at = start + timedelta(days=1)
        next_day = build_zero_result_plan(
            ctx,
            member_id=uuid.uuid4(),
            directory_group=group,
            tehsil=tehsil,
        )
        assert next_day is not None and next_day["status"] == "planned"
        assert next_day["daily_claim"]["semantic_key"] != claim["semantic_key"]
        assert next_day["payload"]["text"] != first["payload"]["text"]


def test_zero_result_requires_a_passing_quality_gate() -> None:
    engine = _engine()
    with Session(engine) as session:
        ctx, group, tehsil = _context(
            session,
            finished_at=datetime(2026, 7, 18, 6, 0, tzinfo=UTC),
        )
        ctx.summary = {"quality_gate": {"passed": False}}
        assert build_zero_result_plan(
            ctx,
            member_id=uuid.uuid4(),
            directory_group=group,
            tehsil=tehsil,
        ) is None
        assert session.exec(select(WhatsAppDailyMessageClaim)).all() == []


def test_daily_claim_is_atomic_across_competing_approvals() -> None:
    engine = _engine()
    with Session(engine) as session:
        ctx, group, tehsil = _context(
            session,
            finished_at=datetime(2026, 7, 18, 6, 0, tzinfo=UTC),
        )
        plan = build_zero_result_plan(
            ctx,
            member_id=uuid.uuid4(),
            directory_group=group,
            tehsil=tehsil,
        )
        assert plan is not None
        claim = plan["daily_claim"]

        candidates = []
        for index in range(2):
            send_job = Job(
                type=JobType.whatsapp_dispatch_send.value,
                title=f"Send {index}",
                status=JobStatus.queued.value,
            )
            preview = WhatsAppDispatchPreview(
                preview_key=f"claim-preview-{uuid.uuid4()}",
                application_id=ctx.application.id,
                source_job_id=ctx.source_job.id,
                dispatch_profile_id=ctx.profile.id,
                status="ready",
                profile_version=1,
                application_name=ctx.application.name,
                report_type_name=ctx.report_type.name,
                audience_name=ctx.audience.name,
                profile_name=ctx.profile.name,
                account_name=ctx.account.name,
                ready_count=1,
                delivery_count=1,
                content_sha256=str(index) * 64,
            )
            session.add_all([send_job, preview])
            session.flush()
            frozen = WhatsAppDispatchPreviewDelivery(
                preview_id=preview.id,
                sequence=1,
                target_type="group",
                target_name=group.name,
                target_jid=group.jid,
                route_kind="tehsil",
                route_scope=tehsil.name,
                message=plan["payload"]["text"],
                routing_snapshot={
                    "report_type_id": str(ctx.report_type.id),
                    "daily_claim": claim,
                },
                status="ready",
                idempotency_key=uuid.uuid4().hex,
            )
            approval = WhatsAppDispatchApproval(
                preview_id=preview.id,
                job_id=send_job.id,
                preview_content_sha256=preview.content_sha256,
            )
            session.add_all([frozen, approval])
            session.flush()
            candidates.append((preview, frozen, approval))

        first = _claim_daily_message(
            session,
            preview=candidates[0][0],
            frozen=candidates[0][1],
            approval=candidates[0][2],
            account=ctx.account,
        )
        second = _claim_daily_message(
            session,
            preview=candidates[1][0],
            frozen=candidates[1][1],
            approval=candidates[1][2],
            account=ctx.account,
        )

        assert first is True
        assert second is False
        assert len(session.exec(select(WhatsAppDailyMessageClaim)).all()) == 1

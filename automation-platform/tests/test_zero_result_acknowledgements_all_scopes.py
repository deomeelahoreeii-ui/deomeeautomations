from __future__ import annotations

import importlib.util
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from sqlmodel import Session

from master_data.models import Officer
from whatsapp_gateway.configuration.dynamic_audiences import ResolvedAudienceMember
from whatsapp_gateway.models import (
    WhatsAppAudienceMember,
    WhatsAppDailyMessageClaim,
    WhatsAppRecipientScope,
    WhatsAppReportType,
)
from whatsapp_gateway.previews.compiler.consolidated_plan import _plan as plan_digest
from whatsapp_gateway.previews.compiler.dynamic_plan_members import plan_dynamic_contact_member
from whatsapp_gateway.previews.compiler.plan_members import plan_group_member
from whatsapp_gateway.previews.compiler.zero_result_acknowledgements import build_zero_result_plan

from test_zero_result_acknowledgements import _context, _engine


def _switch_report(ctx, *, report_key: str, channel: str, scope_key: str) -> None:
    report = WhatsAppReportType(
        application_id=ctx.application.id,
        key=report_key,
        name=report_key.replace("_", " ").title(),
        artifact_kind="message",
    )
    scope = ctx.recipient_scope
    if scope is None or scope.channel != channel or scope.key != scope_key:
        scope = WhatsAppRecipientScope(
            application_id=ctx.application.id,
            channel=channel,
            key=scope_key,
            name=f"{scope_key.upper()} scope",
        )
        ctx.session.add(scope)
    ctx.session.add(report)
    ctx.session.flush()
    ctx.report_type = report
    ctx.recipient_scope = scope
    ctx.profile.report_type_id = report.id
    ctx.profile.recipient_scope_id = scope.id
    ctx.profile.recipient_channel = channel


def _claim_from_plan(ctx, plan, *, semantic_key: str | None = None) -> WhatsAppDailyMessageClaim:
    raw = plan["daily_claim"]
    return WhatsAppDailyMessageClaim(
        semantic_key=semantic_key or raw["semantic_key"],
        business_date=datetime.fromisoformat(raw["business_date"]).date(),
        purpose=raw["purpose"],
        account_id=ctx.account.id,
        application_id=ctx.application.id,
        report_type_id=ctx.report_type.id,
        template_id=uuid.UUID(raw["template_id"]) if raw.get("template_id") else None,
        preview_id=uuid.uuid4(),
        preview_delivery_id=uuid.uuid4(),
        approval_id=uuid.uuid4(),
        target_jid=plan["target"],
        scope_key=raw["scope_key"],
        scope_value=raw["scope_value"],
        scope_label=raw["scope_label"],
        template_key=raw["template_key"],
    )


def test_markaz_zero_result_is_a_ready_message_without_warning_or_attachment() -> None:
    engine = _engine()
    with Session(engine) as session:
        ctx, _group, _tehsil = _context(
            session, finished_at=datetime(2026, 7, 20, 8, 0, tzinfo=UTC)
        )
        _switch_report(ctx, report_key="markaz_dormant_summary", channel="individual", scope_key="aeo")
        plan = build_zero_result_plan(
            ctx,
            member_id=uuid.uuid4(),
            target_type="contact",
            target_jid="923001234567@s.whatsapp.net",
            recipient_name="AEO Example",
            scope_key="markaz",
            scope_value="officer:example",
            scope_label="BAGHBANPURA - MALE",
        )
        assert plan is not None
        assert plan["status"] == "planned"
        assert plan["payload"]["attachment_paths"] == []
        assert "zero dormant" in plan["payload"]["text"].lower() or "zero dormancy" in plan["payload"]["text"].lower()
        assert all(item["severity"] != "warning" for item in plan["native_issues"])


def test_wing_zero_result_is_a_once_daily_group_message() -> None:
    engine = _engine()
    with Session(engine) as session:
        ctx, group, _tehsil = _context(
            session, finished_at=datetime(2026, 7, 20, 8, 0, tzinfo=UTC)
        )
        _switch_report(ctx, report_key="wing_summary", channel="group", scope_key="wing")
        plan = build_zero_result_plan(
            ctx,
            member_id=uuid.uuid4(),
            target_type="group",
            target_jid=group.jid,
            recipient_name=group.name,
            scope_key="wing",
            scope_value=str(ctx.wing.id),
            scope_label=ctx.wing.name,
        )
        assert plan is not None and plan["status"] == "planned"
        assert plan["daily_claim"]["scope_key"] == "wing"
        assert plan["message_only"] is True
        assert plan["payload"]["type"] == "group"


def test_same_scope_is_suppressed_across_dormant_and_digest_and_legacy_keys() -> None:
    engine = _engine()
    with Session(engine) as session:
        ctx, group, tehsil = _context(
            session, finished_at=datetime(2026, 7, 20, 8, 0, tzinfo=UTC)
        )
        first = build_zero_result_plan(
            ctx,
            member_id=uuid.uuid4(),
            target_type="group",
            target_jid=group.jid,
            recipient_name=group.name,
            scope_key="tehsil",
            scope_value=str(tehsil.id),
            scope_label=tehsil.name,
        )
        assert first is not None
        session.add(_claim_from_plan(ctx, first, semantic_key="f" * 64))
        session.commit()

        _switch_report(ctx, report_key="consolidated_action_digest", channel="group", scope_key="tehsil")
        repeated = build_zero_result_plan(
            ctx,
            member_id=uuid.uuid4(),
            target_type="group",
            target_jid=group.jid,
            recipient_name=group.name,
            scope_key="tehsil",
            scope_value=str(tehsil.id),
            scope_label=tehsil.name,
        )
        assert repeated is not None
        assert repeated["status"] == "skipped"
        assert repeated["skip_issue_code"] == "acknowledgement_already_sent_today"


def test_digest_fallback_explicitly_reports_zero_dormancy_distance_and_timing() -> None:
    engine = _engine()
    with Session(engine) as session:
        ctx, group, tehsil = _context(
            session, finished_at=datetime(2026, 7, 20, 8, 0, tzinfo=UTC)
        )
        _switch_report(ctx, report_key="consolidated_action_digest", channel="group", scope_key="tehsil")
        plan = build_zero_result_plan(
            ctx,
            member_id=uuid.uuid4(),
            target_type="group",
            target_jid=group.jid,
            recipient_name=group.name,
            scope_key="tehsil",
            scope_value=str(tehsil.id),
            scope_label=tehsil.name,
        )
        text = plan["payload"]["text"].lower()
        assert "zero dormant" in text or "zero dormancy" in text
        assert "distance" in text
        assert "timing" in text


def test_migration_covers_every_live_zero_result_scope_with_unique_keys() -> None:
    path = Path(__file__).parents[1] / "alembic/versions/c3d8e1f4a702_zero_result_acknowledgements_all_scopes.py"
    spec = importlib.util.spec_from_file_location("zero_ack_migration", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    targets = set(module.TARGETS)
    assert ("markaz_dormant_summary", "individual", "aeo", "markaz") in targets
    assert ("wing_summary", "group", "wing", "wing") in targets
    assert ("consolidated_action_digest", "individual", "aeo", "markaz") in targets
    assert ("consolidated_action_digest", "group", "tehsil", "tehsil") in targets
    assert ("consolidated_action_digest", "group", "wing", "wing") in targets
    rows = list(module._rows())
    assert len(rows) == len(targets) * 3
    assert len({row["key"] for row in rows}) == len(rows)
    assert len({row["id"] for row in rows}) == len(rows)


def test_dynamic_markaz_planner_replaces_empty_warning_with_acknowledgement(monkeypatch) -> None:
    engine = _engine()
    with Session(engine) as session:
        ctx, _group, tehsil = _context(
            session, finished_at=datetime(2026, 7, 20, 8, 0, tzinfo=UTC)
        )
        _switch_report(ctx, report_key="markaz_dormant_summary", channel="individual", scope_key="aeo")
        ctx.planner_capability = SimpleNamespace(planner_key="dynamic_dormant")
        officer = Officer(
            role="aeo",
            name="AEO Example",
            mobile="03001234567",
            normalized_mobile="923001234567",
            district_id=ctx.wing.district_id,
            department_id=ctx.wing.department_id,
            wing_id=ctx.wing.id,
            tehsil_id=tehsil.id,
        )
        session.add(officer)
        session.flush()
        member = ResolvedAudienceMember(
            id=uuid.uuid4(),
            audience_id=ctx.audience.id,
            target_type="contact",
            target_key="master:aeo",
            route_scope_key="markaz",
            route_scope_value="markaz-one",
            route_scope_label="BAGHBANPURA - MALE",
            enabled=True,
            source_id=uuid.uuid4(),
            officer_id=officer.id,
            target_jid="923001234567@s.whatsapp.net",
            scope_ids=[uuid.uuid4()],
        )
        monkeypatch.setattr(
            "whatsapp_gateway.previews.compiler.dynamic_plan_members.render_aeo_markaz_dormant_report",
            lambda *args, **kwargs: SimpleNamespace(
                schools=[],
                issues=[{"code": "nothing_to_dispatch", "severity": "warning", "message": "empty"}],
            ),
        )
        batch_issues = []
        plan, handled = plan_dynamic_contact_member(ctx, member, batch_issues)
        assert handled is True
        assert plan is not None and plan["status"] == "planned"
        assert plan["payload"]["attachment_paths"] == []
        assert not batch_issues
        assert all(item["code"] != "nothing_to_dispatch" for item in plan["native_issues"])


def test_wing_planner_replaces_empty_warning_with_acknowledgement(monkeypatch) -> None:
    engine = _engine()
    with Session(engine) as session:
        ctx, group, _tehsil = _context(
            session, finished_at=datetime(2026, 7, 20, 8, 0, tzinfo=UTC)
        )
        _switch_report(ctx, report_key="wing_summary", channel="group", scope_key="wing")
        ctx.planner_capability = SimpleNamespace(planner_key="group_dormant")
        member = WhatsAppAudienceMember(
            audience_id=ctx.audience.id,
            target_type="group",
            target_key=group.jid,
            directory_group_id=group.id,
            route_scope_key="wing",
            route_scope_value=str(ctx.wing.id),
            route_scope_label=ctx.wing.name,
        )
        session.add(member)
        session.flush()
        monkeypatch.setattr(
            "whatsapp_gateway.previews.compiler.plan_members.render_wing_dormant_report",
            lambda *args, **kwargs: SimpleNamespace(
                schools=[],
                issues=[{"code": "nothing_to_dispatch", "severity": "warning", "message": "empty"}],
            ),
        )
        batch_issues = []
        plan, handled = plan_group_member(ctx, member, batch_issues)
        assert handled is True
        assert plan is not None and plan["status"] == "planned"
        assert not batch_issues
        assert all(item["severity"] != "warning" for item in plan["native_issues"])


def test_consolidated_all_clear_is_message_only_and_once_daily(monkeypatch) -> None:
    engine = _engine()
    with Session(engine) as session:
        ctx, group, tehsil = _context(
            session, finished_at=datetime(2026, 7, 20, 8, 0, tzinfo=UTC)
        )
        _switch_report(ctx, report_key="consolidated_action_digest", channel="group", scope_key="tehsil")
        monkeypatch.setattr(
            "whatsapp_gateway.previews.compiler.consolidated_plan.render_consolidated_action_digest",
            lambda *args, **kwargs: SimpleNamespace(
                schools=[],
                message="Legacy all-clear message",
                attachment_paths=[Path("should-not-send.xlsx")],
                issues=[{"code": "nothing_to_dispatch", "severity": "warning", "message": "empty"}],
                presentation_metadata={"mode": "summary"},
            ),
        )
        plan, issues = plan_digest(
            ctx,
            member_id=uuid.uuid4(),
            target_type="group",
            target=group.jid,
            recipient_name=group.name,
            scope_key="tehsil",
            scope_value=str(tehsil.id),
            scope_values=[str(tehsil.id)],
            scope_label=tehsil.name,
        )
        assert plan is not None and plan["status"] == "planned"
        assert plan["payload"]["attachment_paths"] == []
        assert plan["daily_claim"]["purpose"] == "zero_result_acknowledgement"
        assert all(item["code"] != "nothing_to_dispatch" for item in issues)

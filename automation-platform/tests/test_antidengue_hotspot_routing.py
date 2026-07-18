from __future__ import annotations

import uuid
from pathlib import Path

from openpyxl import Workbook
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import antidengue_automation.models  # noqa: F401
import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.models import Artifact, Job, JobStatus, JobType
from master_data.models import Wing
from antidengue_automation.hotspot_routing import (
    configure_hotspot_routes,
    expand_hotspot_profile_ids,
)
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
    WhatsAppTemplate,
)
from whatsapp_gateway.previews.compiler.messages import _render_message
from whatsapp_gateway.rendering.antidengue.hotspot_report import render_hotspot_distance_report


def _engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _review_workbook(path: Path, wing_id: uuid.UUID, tehsil_id: uuid.UUID) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Review Required"
    sheet.append([
        "School EMIS", "School Name", "Wing ID", "Tehsil ID", "Markaz ID",
        "Tehsil", "Markaz", "Distance Difference (In meters)",
        "Activity(Lat,Long)", "ID", "Submitted by",
    ])
    sheet.append([
        "35210001", "School A", str(wing_id), str(tehsil_id), "",
        "CITY", "MARKAZ ONE", 75.5, "31.5204, 74.3587",
        "activity-1", "35210001.lahore.sed",
    ])
    sheet.append([
        "35210001", "School A", str(wing_id), str(tehsil_id), "",
        "CITY", "MARKAZ ONE", 90.25, "31.5210, 74.3590",
        "activity-2", "35210001.lahore.sed",
    ])
    sheet.append([
        "35210002", "School B", str(uuid.uuid4()), str(tehsil_id), "",
        "CITY", "MARKAZ TWO", 500.0, "31.5000, 74.3000",
        "activity-3", "35210002.lahore.sed",
    ])
    workbook.save(path)
    workbook.close()


def test_hotspot_renderer_scopes_classified_rows_to_authorized_wing(tmp_path: Path) -> None:
    engine = _engine()
    wing = Wing(
        id=uuid.uuid4(), district_id=uuid.uuid4(), department_id=uuid.uuid4(),
        name="MEE", code="MEE",
    )
    tehsil_id = uuid.uuid4()
    report_path = tmp_path / "Hotspot Distance Review - test.xlsx"
    _review_workbook(report_path, wing.id, tehsil_id)

    with Session(engine) as session:
        job = Job(
            type=JobType.antidengue_report.value,
            title="Dry run",
            status=JobStatus.succeeded.value,
            result={"summary": {"portal_acquisition": {"window": {"dateto": "2026-07-18T08:30"}}}},
        )
        session.add(job)
        session.flush()
        session.add(Artifact(
            job_id=job.id, module_key="antidengue", kind="report",
            name=report_path.name, path=str(report_path), size_bytes=report_path.stat().st_size,
        ))
        session.commit()

        rendered = render_hotspot_distance_report(
            session,
            source_job=job,
            wing=wing,
            recipient_name="MEE Heads",
            scope_key="wing",
            scope_value=str(wing.id),
            scope_label="MEE",
            presentation_policy={"attachment_mode": "none"},
        )

        assert [row.emis for row in rendered.rows] == ["35210001", "35210001"]
        assert "matched activity records" not in rendered.message
        assert "Maximum distance" not in rendered.message
        assert "*1. School A*" in rendered.message
        assert "*EMIS:* `35210001`" in rendered.message
        assert "*Tehsil:* CITY" in rendered.message
        assert "*Markaz:* MARKAZ ONE" in rendered.message
        assert "*Matched activities:*" not in rendered.message
        assert rendered.message.count("*Distance Difference (In meters):*") == 2
        assert "*Distance Difference (In meters):* 90.25" in rendered.message
        assert "*Distance Difference (In meters):* 75.50" in rendered.message
        assert "https://www.google.com/maps?q=31.5210000,74.3590000" in rendered.message
        assert "https://www.google.com/maps?q=31.5204000,74.3587000" in rendered.message
        assert rendered.context["school_activity_details"] in rendered.message
        assert rendered.context["attachment_note"] == ""
        assert "attached Excel" not in rendered.message
        assert rendered.attachment_paths == []

        template = WhatsAppTemplate(
            key="hotspot-custom-test",
            name="Custom hotspot message",
            body="CUSTOM HEADER\n{{school_activity_details}}\n{{disclaimer}}",
        )
        customized, issues = _render_message(template, rendered.message, rendered.context)
        assert issues == []
        assert customized.startswith("CUSTOM HEADER")
        assert "*Distance Difference (In meters):* 90.25" in customized
        assert "rule-based review candidates" in customized


def test_enabled_hotspot_routes_are_global_execution_selection() -> None:
    engine = _engine()
    with Session(engine) as session:
        account = WhatsAppAccount(name="Primary", worker_key="routing-test")
        application = WhatsAppApplication(key="antidengue", name="AntiDengue")
        session.add(account)
        session.add(application)
        session.flush()
        dormant_type = WhatsAppReportType(
            application_id=application.id, key="tehsil_dormant_summary", name="Dormant"
        )
        hotspot_type = WhatsAppReportType(
            application_id=application.id,
            key="hotspot_distance_activity",
            name="Hotspot distance",
        )
        audience = WhatsAppAudience(
            application_id=application.id, key="routing-test", name="Routing test"
        )
        session.add_all([dormant_type, hotspot_type, audience])
        session.flush()
        sources = [
            WhatsAppDispatchProfile(
                application_id=application.id,
                key=f"source-{index}",
                name=f"Source {index}",
                report_type_id=dormant_type.id,
                audience_id=audience.id,
                account_id=account.id,
                recipient_channel="group",
                delivery_mode="groups",
            )
            for index in (1, 2)
        ]
        session.add_all(sources)
        session.commit()

        configured = configure_hotspot_routes(session, [item.id for item in sources])
        assert configured["enabled_count"] == 2
        hotspot_by_source = {
            item["source_profile_id"]: uuid.UUID(item["hotspot_profile_id"])
            for item in configured["items"]
        }

        # Running only source 1 still includes both routes enabled on the
        # Activity Rules page.
        expanded = expand_hotspot_profile_ids(session, [sources[0].id])
        assert set(expanded) == {
            sources[0].id,
            hotspot_by_source[str(sources[0].id)],
            hotspot_by_source[str(sources[1].id)],
        }

        configured = configure_hotspot_routes(session, [sources[1].id])
        assert configured["enabled_count"] == 1
        expanded = expand_hotspot_profile_ids(session, [sources[0].id])
        assert set(expanded) == {
            sources[0].id,
            hotspot_by_source[str(sources[1].id)],
        }

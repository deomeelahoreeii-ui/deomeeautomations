from __future__ import annotations

import uuid
import shutil
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
from openpyxl import Workbook
from sqlmodel import Session, select

from automation_api.main import app
from automation_core.database import engine
from automation_core.models import Artifact, Job, JobLog, JobStatus, JobType, SourceFile, SourceFileRun
from master_data.models import School, Wing
from whatsapp_gateway.api import ensure_defaults
from whatsapp_gateway.models import (
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppAudienceMember,
    WhatsAppDirectoryGroup,
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchProfile,
    WhatsAppGroup,
    WhatsAppReportType,
)
from whatsapp_gateway.preview_service import (
    _canonical_contact_target,
    _plan_matches_audience_route,
    _plan_report_type,
    _plan_route_label,
    _retarget_group_plan,
    _same_route_value,
)
from whatsapp_gateway.antidengue_renderer import (
    ScopedDormantSchool,
    _build_message,
    _build_wing_message,
    normalize_presentation_policy,
)
from whatsapp_gateway.tasks import compile_dispatch_preview_job


def test_health_endpoint() -> None:
    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_audience_route_matching_uses_hierarchy_scope() -> None:
    district_plan = {
        "channel": "group",
        "payload": {"type": "group", "dispatch_route": {"route_kind": "district"}},
    }
    tehsil_plan = {
        "channel": "group",
        "payload": {
            "type": "group",
            "dispatch_route": {"route_kind": "tehsil", "tehsil": "MODEL TOWN"},
        },
    }
    assert _plan_matches_audience_route(
        district_plan,
        scope_key="wing",
        scope_label="DEO MEE",
        report_type_key="wing_summary",
    )
    assert _plan_matches_audience_route(
        tehsil_plan,
        scope_key="tehsil",
        scope_label="Model Town",
        report_type_key="tehsil_dormant_summary",
    )
    assert not _plan_matches_audience_route(
        tehsil_plan,
        scope_key="tehsil",
        scope_label="Cantt",
        report_type_key="tehsil_dormant_summary",
    )


def test_native_tehsil_dormant_renderer_matches_approved_message_shape() -> None:
    source_job = SimpleNamespace(
        finished_at=datetime(2026, 7, 13, 6, 30, tzinfo=timezone.utc),
        started_at=None,
        created_at=datetime(2026, 7, 13, 6, 0, tzinfo=timezone.utc),
    )
    wing = SimpleNamespace(code="DEO MEE", name="DEO MEE")
    tehsil = SimpleNamespace(name="RAIWIND")
    schools = [
        ScopedDormantSchool("35220207", "GES SHADAB COLONY CHUNG LAHORE", "RAIWIND", "CHUNG - MALE"),
        ScopedDormantSchool("35220216", "GPS FATAH SINGH WALA", "RAIWIND", "CHUNG - MALE"),
        ScopedDormantSchool("35220105", "GES WATNA", "RAIWIND", "PAJI - MALE"),
        ScopedDormantSchool("35220180", "GPS JIA", "RAIWIND", "PAJI - MALE"),
        ScopedDormantSchool("35220235", "GPS MAQADDAM SINGH WALA", "RAIWIND", "SUNDER-MALE"),
    ]

    message, context = _build_message(
        source_job=source_job,
        recipient_name="Tehsil Raiwind Heads",
        wing=wing,
        tehsil=tehsil,
        schools=schools,
        deadline="12:30 PM",
    )

    assert message == """*Anti-Dengue Dormant Users - 13-Jul-2026 - 11:30 AM*
*Group:* Tehsil Raiwind Heads
*Tehsil:* RAIWIND
*Wing:* MEE
*Total dormant:* 5 schools

*TEHSIL SUMMARY*
1. RAIWIND: 5 schools

*DETAILS BY MARKAZ*

*RAIWIND - 5 schools*

*CHUNG - MALE - 2 schools*
1. 35220207 - GES SHADAB COLONY CHUNG LAHORE
2. 35220216 - GPS FATAH SINGH WALA

*PAJI - MALE - 2 schools*
1. 35220105 - GES WATNA
2. 35220180 - GPS JIA

*SUNDER-MALE - 1 school*
1. 35220235 - GPS MAQADDAM SINGH WALA

Please ensure activities are submitted before 12:30 PM today."""
    assert context["report_body"] == message
    assert context["renderer_key"] == "antidengue.tehsil_dormant.v1"


def test_native_wing_renderer_uses_selected_recipient_and_wing() -> None:
    source_job = SimpleNamespace(
        finished_at=datetime(2026, 7, 13, 17, 22, tzinfo=timezone.utc),
        started_at=None,
        created_at=datetime(2026, 7, 13, 17, 0, tzinfo=timezone.utc),
    )
    wing = SimpleNamespace(code="DEO SE", name="DEO SE")
    schools = [
        ScopedDormantSchool("1001", "Secondary School A", "CANTT", "UNMAPPED MARKAZ"),
        ScopedDormantSchool("1002", "Secondary School B", "CITY", "UNMAPPED MARKAZ"),
    ]

    message, context = _build_wing_message(
        source_job=source_job,
        recipient_name="Digital Studio",
        wing=wing,
        schools=schools,
    )

    assert "*Anti-Dengue Dormancy Summary*" in message
    assert "*Secondary Wing — 13-Jul-2026, 10:22 PM*" in message
    assert "*Total dormant:* 2 schools across 2 tehsils" in message
    assert "*TEHSIL BREAKDOWN*" in message
    assert "1. CANTT: 1 school" in message
    assert "2. CITY: 1 school" in message
    assert "MARKAZ" not in message
    assert "The attached workbook contains the detailed school list." in message
    assert "Digital Studio" not in message
    assert "M-EE" not in message
    assert context["renderer_key"] == "antidengue.wing_dormant.v1"


def test_native_wing_renderer_supports_wee_without_markaz_noise() -> None:
    source_job = SimpleNamespace(
        finished_at=datetime(2026, 7, 13, 17, 22, tzinfo=timezone.utc),
        started_at=None,
        created_at=datetime(2026, 7, 13, 17, 0, tzinfo=timezone.utc),
    )
    message, _ = _build_wing_message(
        source_job=source_job,
        recipient_name="WEE District Group",
        wing=SimpleNamespace(code="DEO WEE", name="DEO WEE"),
        schools=[ScopedDormantSchool("2001", "Elementary School", "CITY", "UNMAPPED MARKAZ")],
    )

    assert "*WEE Wing — 13-Jul-2026, 10:22 PM*" in message
    assert "*Total dormant:* 1 school across 1 tehsil" in message
    assert "MARKAZ" not in message
    assert "WEE District Group" not in message


def test_native_report_presentation_policy_supports_all_delivery_formats() -> None:
    assert normalize_presentation_policy(
        {
            "message_style": "summary",
            "attachment_mode": "image_excel",
            "image_content": "details",
        },
        report_key="tehsil_dormant_summary",
    ) == {
        "message_style": "summary",
        "attachment_mode": "image_excel",
        "image_content": "details",
    }
    assert normalize_presentation_policy(None, report_key="tehsil_dormant_summary")[
        "attachment_mode"
    ] == "none"
    assert normalize_presentation_policy(None, report_key="wing_summary")[
        "attachment_mode"
    ] == "excel"


def test_openapi_contains_filter_job_endpoints() -> None:
    with TestClient(app) as client:
        schema = client.get("/openapi.json").json()

    paths = schema["paths"]
    assert "/api/v1/crm/overview" in paths
    assert "/api/v1/crm/sheets" in paths
    assert "/api/v1/crm/sheets/uploads" in paths
    assert "/api/v1/crm/sheets/{source_file_id}/process" in paths
    assert "/api/v1/crm/sheets/{source_file_id}/convert-to-pdfs" in paths
    assert "/api/v1/crm/sheets/{source_file_id}/hard" in paths
    assert "/api/v1/crm/pdf-batches" in paths
    assert "/api/v1/crm/pdf-batches/uploads" in paths
    assert "/api/v1/crm/pdf-batches/{source_file_id}/process" in paths
    assert "/api/v1/crm/pdf-batches/{source_file_id}/download" in paths
    assert "/api/v1/crm/pdf-batches/{source_file_id}/hard" in paths
    assert "/api/v1/crm/filters/sheets/jobs" in paths
    assert "/api/v1/crm/filters/pdfs/jobs" in paths
    assert "/api/v1/antidengue/runs" in paths
    assert "/api/v1/antidengue/overview" in paths
    assert "/api/v1/antidengue/manual-reports/{source_file_id}/hard" in paths
    assert "/api/v1/jobs" in paths
    assert "/api/v1/master-data/schools/{school_id}/hard" in paths
    assert "/api/v1/master-data/officers/{role}/{officer_id}/hard" in paths
    assert "/api/v1/whatsapp/previews/{preview_id}" in paths
    assert "/api/v1/whatsapp/audiences/{audience_id}/hard" in paths
    assert "/api/v1/whatsapp/dispatch-profiles/{profile_id}/hard" in paths
    assert "/api/v1/whatsapp/reporting-routes/setup" in paths
    assert "/api/v1/whatsapp/inbound/status" in paths
    assert "/api/v1/whatsapp/inbound/exports/preview" in paths
    assert "/api/v1/whatsapp/inbound/exports" in paths
    assert "/api/v1/whatsapp/inbound/attachments/{attachment_id}/content" in paths
    assert "/api/v1/whatsapp/inbound/storage/status" in paths
    assert "/api/v1/whatsapp/inbound/batches" in paths
    assert "/api/v1/whatsapp/inbound/batches/{batch_id}" in paths


def test_jobs_endpoint_starts_empty() -> None:
    with TestClient(app) as client:
        response = client.get("/api/v1/jobs")

    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_antidengue_overview_finds_existing_project() -> None:
    with TestClient(app) as client:
        response = client.get("/api/v1/antidengue/overview")

    assert response.status_code == 200
    assert response.json()["project_available"] is True
    assert set(response.json()["counts"]) == {
        "raw_files",
        "output_files",
        "archived_files",
        "unmapped_reports",
    }


def test_master_data_postgres_endpoints_are_available() -> None:
    with TestClient(app) as client:
        dashboard = client.get("/api/v1/master-data/dashboard")
        schools = client.get("/api/v1/master-data/schools?page=1&page_size=10")
        jurisdictions = client.get("/api/v1/master-data/jurisdictions")

    assert dashboard.status_code == 200
    assert schools.status_code == 200
    assert len(schools.json()["items"]) <= 10
    assert jurisdictions.status_code == 200
    assert set(jurisdictions.json()) == {"aeo", "ddeo"}


def test_whatsapp_control_plane_endpoints_are_available() -> None:
    with TestClient(app) as client:
        overview = client.get("/api/v1/whatsapp/overview")
        recipients = client.get("/api/v1/whatsapp/recipients?page=1&page_size=10")
        deliveries = client.get("/api/v1/whatsapp/deliveries?page=1&page_size=10")
        directory = client.get("/api/v1/whatsapp/directory/summary")
        detected_groups = client.get(
            "/api/v1/whatsapp/directory/groups?page=1&page_size=10"
        )
        identities = client.get(
            "/api/v1/whatsapp/directory/contacts?page=1&page_size=10"
        )
        configuration = client.get("/api/v1/whatsapp/configuration/options")
        report_types = client.get("/api/v1/whatsapp/report-types")
        audiences = client.get("/api/v1/whatsapp/audiences?page=1&page_size=10")
        profiles = client.get(
            "/api/v1/whatsapp/dispatch-profiles?page=1&page_size=10"
        )
        previews = client.get("/api/v1/whatsapp/previews?page=1&page_size=10")
        preview_options = client.get("/api/v1/whatsapp/previews/options")

    assert overview.status_code == 200
    assert set(overview.json()["counts"]) == {
        "groups",
        "queued",
        "delivered_24h",
        "failed_24h",
        "recipients",
    }
    assert recipients.status_code == 200
    assert len(recipients.json()["items"]) <= 10
    assert deliveries.status_code == 200
    assert directory.status_code == 200
    assert set(directory.json()) == {
        "groups",
        "configured_groups",
        "contacts",
        "lid_mappings",
        "last_synced_at",
    }
    assert detected_groups.status_code == 200
    assert len(detected_groups.json()["items"]) <= 10
    assert identities.status_code == 200
    assert len(identities.json()["items"]) <= 10
    assert configuration.status_code == 200
    assert {item["key"] for item in configuration.json()["applications"]} == {
        "antidengue",
        "crm",
        "pmdu",
    }
    assert report_types.status_code == 200
    assert len(report_types.json()) >= 6
    assert audiences.status_code == 200
    assert len(audiences.json()["items"]) <= 10
    assert profiles.status_code == 200
    assert len(profiles.json()["items"]) <= 10
    assert previews.status_code == 200
    assert len(previews.json()["items"]) <= 10
    assert preview_options.status_code == 200
    assert set(preview_options.json()) == {"runs", "profiles"}


def test_antidengue_dispatch_preview_freezes_exact_payload(tmp_path) -> None:
    suffix = uuid.uuid4().hex[:10]
    target = f"120363{suffix}@g.us"
    attachment_path = tmp_path / "MEE dormant schools.xlsx"
    created: dict[str, object] = {}

    with Session(engine) as session:
        account, _ = ensure_defaults(session)
        application = session.exec(
            select(WhatsAppApplication).where(WhatsAppApplication.key == "antidengue")
        ).one()
        report_type = session.exec(
            select(WhatsAppReportType).where(
                WhatsAppReportType.application_id == application.id,
                WhatsAppReportType.key == "school_activity",
            )
        ).first()
        school = session.exec(select(School).where(School.active.is_(True))).first()
        wing = session.get(Wing, school.wing_id) if school else None
        assert report_type is not None and wing is not None and school is not None

        workbook = Workbook()
        sheet = workbook.active
        sheet.append(["School Name", "School EMIS"])
        sheet.append([school.name, school.emis])
        workbook.save(attachment_path)

        directory_group = WhatsAppDirectoryGroup(
            account_id=account.id,
            jid=target,
            name=f"Preview test {suffix}",
        )
        session.add(directory_group)
        session.flush()
        configured_group = WhatsAppGroup(
            account_id=account.id,
            directory_group_id=directory_group.id,
            wing_id=wing.id,
            name=directory_group.name,
            jid=target,
        )
        audience = WhatsAppAudience(
            application_id=application.id,
            key=f"preview_test_{suffix}",
            name=f"Preview test {suffix}",
        )
        session.add_all([configured_group, audience])
        session.flush()
        member = WhatsAppAudienceMember(
            audience_id=audience.id,
            target_type="group",
            target_key=f"group:{directory_group.id}",
            directory_group_id=directory_group.id,
        )
        profile = WhatsAppDispatchProfile(
            application_id=application.id,
            key=f"preview_test_{suffix}",
            name=f"Preview test {suffix}",
            report_type_id=report_type.id,
            audience_id=audience.id,
            account_id=account.id,
            wing_id=wing.id,
            delivery_mode="groups",
        )
        job = Job(
            type=JobType.antidengue_report.value,
            title="AntiDengue report (dry run)",
            status=JobStatus.succeeded.value,
            parameters={"dry_run": True, "login_mode": "auto"},
            result={
                "artifact_count": 1,
                "summary": {
                    "quality_gate": {"warnings": []},
                    "whatsapp": {
                        "dispatch_plan": [
                            {
                                "job_id": suffix,
                                "channel": "group",
                                "target": target,
                                "recipient_name": directory_group.name,
                                "route_kind": "tehsil",
                                "payload": {
                                    "type": "group",
                                    "target": target,
                                    "recipient_name": directory_group.name,
                                    "text": "Exact frozen AntiDengue message",
                                    "excel_path": str(attachment_path),
                                    "dispatch_route": {
                                        "route_kind": "tehsil",
                                        "row_count": 3,
                                    },
                                },
                            },
                            {
                                "job_id": f"{suffix}-officer",
                                "status": "skipped",
                                "cause": "Officer delivery is disabled for this dry run.",
                                "channel": "individual",
                                "target": "923001234567",
                                "recipient_name": "Preview officer",
                                "payload": {
                                    "type": "contact",
                                    "target": "923001234567",
                                    "recipient_name": "Preview officer",
                                    "text": "This route is intentionally not sent.",
                                    "excel_path": str(attachment_path),
                                },
                            }
                        ]
                    },
                },
            },
        )
        session.add_all([member, profile, job])
        session.flush()
        artifact = Artifact(
            job_id=job.id,
            kind="xlsx",
            name=attachment_path.name,
            path=str(attachment_path),
            size_bytes=attachment_path.stat().st_size,
        )
        session.add(artifact)
        session.commit()
        created = {
            "job": (Job, job.id),
            "artifact": (Artifact, artifact.id),
            "profile": (WhatsAppDispatchProfile, profile.id),
            "member": (WhatsAppAudienceMember, member.id),
            "audience": (WhatsAppAudience, audience.id),
            "configured_group": (WhatsAppGroup, configured_group.id),
            "directory_group": (WhatsAppDirectoryGroup, directory_group.id),
        }

    try:
        queued_task = MagicMock(id=f"preview-task-{suffix}")

        def run_queued_preview(*, args: list[str], queue: str) -> MagicMock:
            assert queue == "antidengue"
            compile_dispatch_preview_job.run(args[0])
            return queued_task

        with (
            patch(
                "whatsapp_gateway.preview_api.compile_dispatch_preview_job.apply_async",
                side_effect=run_queued_preview,
            ),
            TestClient(app) as client,
        ):
            response = client.post(
                "/api/v1/whatsapp/previews",
                json={
                    "source_job_id": str(created["job"][1]),
                    "dispatch_profile_id": str(created["profile"][1]),
                },
            )
            assert response.status_code == 202, response.text
            compile_job = response.json()
            created["compile_job"] = (Job, uuid.UUID(compile_job["id"]))
            preview_id = compile_job["result"]["preview_id"]
            preview = client.get(f"/api/v1/whatsapp/previews/{preview_id}").json()
            deliveries = client.get(
                f"/api/v1/whatsapp/previews/{preview['id']}/deliveries?page=1&page_size=10"
            )
            files = client.get(
                f"/api/v1/whatsapp/previews/{preview['id']}/artifacts?page=1&page_size=10"
            )

        assert preview["delivery_count"] == 1
        assert preview["blocked_count"] == 0
        assert preview["skipped_count"] == 0
        assert deliveries.status_code == 200
        assert deliveries.json()["items"][0]["message"] == "Exact frozen AntiDengue message"
        assert deliveries.json()["items"][0]["target_jid"] == target
        assert files.status_code == 200
        assert files.json()["items"][0]["checksum_sha256"]

        frozen_artifact = files.json()["items"][0]
        attachment_path.write_bytes(b"source file changed after preview")
        frozen_download = client.get(
            f"/api/v1/whatsapp/previews/{preview['id']}/artifacts/{frozen_artifact['id']}/download"
        )
        assert frozen_download.status_code == 200
        assert frozen_download.content != attachment_path.read_bytes()
    finally:
        with Session(engine) as session:
            previews = session.exec(
                select(WhatsAppDispatchPreview).where(
                    WhatsAppDispatchPreview.source_job_id == created["job"][1]
                )
            ).all()
            for preview in previews:
                for delivery in session.exec(
                    select(WhatsAppDispatchPreviewDelivery).where(
                        WhatsAppDispatchPreviewDelivery.preview_id == preview.id
                    )
                ):
                    session.delete(delivery)
                for snapshot in session.exec(
                    select(WhatsAppDispatchPreviewArtifact).where(
                        WhatsAppDispatchPreviewArtifact.preview_id == preview.id
                    )
                ):
                    session.delete(snapshot)
                session.flush()
                session.delete(preview)
            session.commit()
            for key in (
                "profile",
                "member",
                "configured_group",
                "audience",
                "directory_group",
                "artifact",
                "job",
            ):
                model, item_id = created[key]
                item = session.get(model, item_id)
                if item is not None:
                    session.delete(item)
            compile_job_id = created.get("compile_job", (None, None))[1]
            if compile_job_id:
                for log in session.exec(select(JobLog).where(JobLog.job_id == compile_job_id)):
                    session.delete(log)
                compile_job = session.get(Job, compile_job_id)
                if compile_job is not None:
                    session.delete(compile_job)
            session.commit()


def test_contact_targets_are_normalized_to_phone_jids() -> None:
    assert _canonical_contact_target("+92 300 1234567") == "923001234567@s.whatsapp.net"
    assert (
        _canonical_contact_target("923001234567@s.whatsapp.net")
        == "923001234567@s.whatsapp.net"
    )


def test_dispatch_plans_are_classified_by_recipient_and_scope() -> None:
    assert _plan_report_type({"channel": "individual", "target": "923001234567"}) == "officer_summary"
    assert _plan_report_type({"channel": "group", "target": "1@g.us", "route_kind": "district"}) == "wing_summary"
    assert _plan_report_type({"channel": "group", "target": "1@g.us", "route_kind": "tehsil"}) == "school_activity"


def test_audience_routes_retarget_source_data_without_legacy_destination() -> None:
    source = {
        "target": "legacy@g.us",
        "channel": "group",
        "payload": {
            "target": "legacy@g.us",
            "type": "group",
            "excel_path": "/tmp/legacy.xlsx",
            "dispatch_route": {
                "route_kind": "tehsil",
                "tehsil": "MODEL TOWN",
                "row_count": 82,
            },
        },
    }

    assert _plan_route_label(source, "tehsil") == "MODEL TOWN"
    assert _same_route_value("Model Town", "MODEL-TOWN")
    resolved = _retarget_group_plan(
        source,
        target="testing@g.us",
        recipient_name="Testing Group",
        scope_key="tehsil",
        report_is_message_only=True,
    )
    assert resolved["payload"]["target"] == "testing@g.us"
    assert resolved["payload"]["recipient_name"] == "Testing Group"
    assert resolved["payload"]["dispatch_route"]["recipient_scope"] == "tehsil"
    assert "excel_path" not in resolved["payload"]
    assert source["payload"]["target"] == "legacy@g.us"


def test_antidengue_dry_run_is_queued() -> None:
    queued_task = MagicMock(id="test-celery-task")
    with (
        patch("antidengue_automation.api.get_active_job", return_value=None),
        patch(
            "antidengue_automation.api.run_antidengue_job.apply_async",
            return_value=queued_task,
        ),
        TestClient(app) as client,
    ):
        response = client.post(
            "/api/v1/antidengue/runs",
            json={"dry_run": True, "login_mode": "auto"},
        )

    assert response.status_code == 202
    assert response.json()["type"] == "antidengue.report"
    assert response.json()["status"] == "queued"
    assert response.json()["task_id"] == "test-celery-task"

    job_id = uuid.UUID(response.json()["id"])
    with Session(engine) as session:
        for log in session.exec(select(JobLog).where(JobLog.job_id == job_id)):
            session.delete(log)
        for artifact in session.exec(select(Artifact).where(Artifact.job_id == job_id)):
            session.delete(artifact)
        job = session.get(Job, job_id)
        if job is not None:
            session.delete(job)
        session.commit()


def test_manual_antidengue_report_is_validated_deduplicated_and_queued() -> None:
    suffix = str(uuid.uuid4().int)[:10]
    # The portal serves CSV content with an .xls filename; validation must sniff
    # the content instead of trusting the extension.
    payload = (
        "Username,Simple Activities,Total Activities\n"
        f"{suffix}01.lahore.sed(Manual test school),0,0\n"
    ).encode()
    queued_task = MagicMock(id=f"manual-task-{suffix}")
    source_id: uuid.UUID | None = None
    job_id: uuid.UUID | None = None
    stored_parent = None

    try:
        with (
            patch("antidengue_automation.api.get_active_job", return_value=None),
            patch(
                "antidengue_automation.api.run_antidengue_job.apply_async",
                return_value=queued_task,
            ),
            TestClient(app) as client,
        ):
            upload = client.post(
                "/api/v1/antidengue/manual-reports/uploads",
                files={"file": (f"portal-{suffix}.xls", payload, "application/vnd.ms-excel")},
            )
            assert upload.status_code == 201, upload.text
            source = upload.json()
            source_id = uuid.UUID(source["id"])
            assert source["validation_status"] == "valid"
            assert source["detected_metadata"]["row_count"] == 1
            assert source["detected_metadata"]["school_count"] == 1

            duplicate = client.post(
                "/api/v1/antidengue/manual-reports/uploads",
                files={"file": (f"duplicate-{suffix}.xls", payload, "application/vnd.ms-excel")},
            )
            assert duplicate.status_code == 409
            assert duplicate.json()["detail"]["source_file_id"] == str(source_id)

            process = client.post(f"/api/v1/antidengue/manual-reports/{source_id}/process")
            assert process.status_code == 202, process.text
            job_id = uuid.UUID(process.json()["id"])
            assert process.json()["parameters"]["dry_run"] is True
            assert process.json()["parameters"]["input_source"] == "manual_upload"
            assert process.json()["task_id"] == queued_task.id

            listing = client.get("/api/v1/antidengue/manual-reports?page=1&page_size=10")
            assert listing.status_code == 200
            listed = next(item for item in listing.json()["items"] if item["id"] == str(source_id))
            assert listed["latest_job"]["id"] == str(job_id)
    finally:
        with Session(engine) as session:
            source = session.get(SourceFile, source_id) if source_id else None
            if source:
                stored_parent = Path(source.stored_path).parent
            if job_id:
                for link in session.exec(select(SourceFileRun).where(SourceFileRun.job_id == job_id)):
                    session.delete(link)
                for log in session.exec(select(JobLog).where(JobLog.job_id == job_id)):
                    session.delete(log)
                job = session.get(Job, job_id)
                if job:
                    session.delete(job)
            if source:
                session.delete(source)
            session.commit()
        if stored_parent:
            shutil.rmtree(stored_parent, ignore_errors=True)

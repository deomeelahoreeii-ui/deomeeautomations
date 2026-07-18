from __future__ import annotations

import uuid
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from antidengue_automation.activity_rule_api import router
from antidengue_automation.activity_rule_schemas import ActivityRuleInput
from antidengue_automation.activity_rules import preview_rule
from automation_core.database import get_session
from automation_core.models import Artifact, Job, JobStatus, JobType


def _engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _report(path: Path) -> None:
    rows = (
        (1, 50.0, "06:00AM"),
        (2, 50.01, "07:00AM"),
        (3, 100.0, "11:00PM"),
        (4, 10.0, "02:00AM"),
    )
    body = "".join(
        f"<tr><td>{activity_id}</td><td>Lahore</td><td>City</td><td>School {activity_id}</td>"
        f"<td>{distance}</td><td>user-{activity_id}</td>"
        f"<td>on 07/18/2026 at {submitted}</td></tr>"
        for activity_id, distance, submitted in rows
    )
    path.write_text(
        "<table><thead><tr><th>ID</th><th>District</th><th>Town</th>"
        "<th>Hotspot Name</th><th>Distance Difference (In meters)</th>"
        "<th>Submitted by</th><th>Activity Date/Time</th></tr></thead>"
        f"<tbody>{body}</tbody></table>",
        encoding="utf-8",
    )


def _seed_report(session: Session, tmp_path: Path) -> None:
    path = tmp_path / "hotspot_distance_report_20260718_0830.xls"
    _report(path)
    job = Job(
        type=JobType.antidengue_report.value,
        title="Hotspot report",
        status=JobStatus.succeeded.value,
    )
    session.add(job)
    session.flush()
    session.add(Artifact(
        job_id=job.id,
        module_key="antidengue",
        kind="raw",
        name=path.name,
        path=str(path),
        size_bytes=path.stat().st_size,
    ))
    session.commit()


def test_distance_operator_and_time_boundaries_are_exact(tmp_path: Path) -> None:
    engine = _engine()
    with Session(engine) as session:
        _seed_report(session, tmp_path)
        greater_or_equal = preview_rule(session, ActivityRuleInput(
            name="Morning distance",
            distance_operator="gte",
            distance_threshold_meters=50,
            time_enabled=True,
            time_start="00:00",
            time_end="07:00",
        ))
        strictly_greater = preview_rule(session, ActivityRuleInput(
            name="Morning distance",
            distance_operator="gt",
            distance_threshold_meters=50,
            time_enabled=True,
            time_start="00:00",
            time_end="07:00",
        ))

        assert greater_or_equal["matching_activities"] == 1
        assert greater_or_equal["sample"][0]["activity_id"] == "1"
        assert strictly_greater["matching_activities"] == 0


def test_overnight_and_outside_time_ranges(tmp_path: Path) -> None:
    engine = _engine()
    with Session(engine) as session:
        _seed_report(session, tmp_path)
        overnight = preview_rule(session, ActivityRuleInput(
            name="Overnight",
            distance_enabled=False,
            time_enabled=True,
            time_operator="between",
            time_start="22:00",
            time_end="05:00",
        ))
        outside = preview_rule(session, ActivityRuleInput(
            name="Outside morning",
            distance_enabled=False,
            time_enabled=True,
            time_operator="outside",
            time_start="00:00",
            time_end="07:00",
        ))

        assert overnight["matching_activities"] == 2
        assert {item["activity_id"] for item in overnight["sample"]} == {"3", "4"}
        assert outside["matching_activities"] == 2
        assert {item["activity_id"] for item in outside["sample"]} == {"2", "3"}


def test_rule_publish_is_immutable_and_new_version_is_created() -> None:
    engine = _engine()
    app = FastAPI()
    app.include_router(router)

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    client = TestClient(app)
    payload = {
        "name": "Distance review",
        "distance_enabled": True,
        "distance_operator": "gte",
        "distance_threshold_meters": 50,
        "time_enabled": False,
    }

    created = client.post("/api/v1/antidengue/activity-rules", json=payload)
    assert created.status_code == 201
    draft = created.json()
    published = client.post(
        f"/api/v1/antidengue/activity-rules/{draft['id']}/publish"
    )
    assert published.status_code == 200
    assert published.json()["enabled"] is True
    assert client.patch(
        f"/api/v1/antidengue/activity-rules/{draft['id']}",
        json={"distance_threshold_meters": 100},
    ).status_code == 409

    version = client.post(
        f"/api/v1/antidengue/activity-rules/{draft['id']}/versions"
    )
    assert version.status_code == 201
    assert version.json()["version"] == 2
    assert version.json()["status"] == "draft"
    assert version.json()["supersedes_id"] == draft["id"]

from __future__ import annotations

import uuid
from types import SimpleNamespace

from automation_core.models import Job, JobStatus, JobType
from whatsapp_gateway.previews import creation
from whatsapp_gateway.previews.schemas import BulkPreviewInput


class _ScalarResult:
    def __init__(self, values):
        self._values = values

    def all(self):
        return list(self._values)


class _Session:
    def __init__(self, source_job: Job, profiles):
        self.source_job = source_job
        self.profiles = list(profiles)
        self.commits = 0

    def get(self, model, key):
        if model is Job and key == self.source_job.id:
            return self.source_job
        return None

    def scalars(self, _statement):
        return _ScalarResult(self.profiles)

    def commit(self):
        self.commits += 1


def test_manual_bulk_compilation_uses_one_atomic_multi_profile_job(monkeypatch) -> None:
    source_job = Job(
        id=uuid.uuid4(),
        type=JobType.antidengue_report.value,
        title="Successful source run",
        status=JobStatus.succeeded.value,
    )
    application_id = uuid.uuid4()
    first_id = uuid.uuid4()
    second_id = uuid.uuid4()
    # Deliberately reversed to prove the request order is preserved in the
    # immutable compilation contract.
    profiles = [
        SimpleNamespace(id=second_id, application_id=application_id, enabled=True, name="Second"),
        SimpleNamespace(id=first_id, application_id=application_id, enabled=True, name="First"),
    ]
    session = _Session(source_job, profiles)
    calls: dict[str, object] = {"stage": [], "logs": []}
    queued_job_id = uuid.uuid4()

    def required_contract(_session, ordered_profiles):
        calls["contract_profiles"] = [profile.id for profile in ordered_profiles]
        return {"protocol": 3, "capabilities": {"test": 1}}

    def add_job(_session, *, job_type, title, parameters):
        calls["parameters"] = parameters
        return Job(
            id=queued_job_id,
            type=job_type,
            title=title,
            status=JobStatus.queued.value,
            parameters=parameters,
        )

    def stage_task(_session, **kwargs):
        calls["stage"].append(kwargs)

    monkeypatch.setattr(creation, "required_compiler_contract", required_contract)
    monkeypatch.setattr(creation, "require_compatible_workers", lambda *_args: None)
    monkeypatch.setattr(
        creation,
        "resolve_deadline_policy",
        lambda _session: SimpleNamespace(model_dump=lambda: {"timezone": "Asia/Karachi"}),
    )
    monkeypatch.setattr(creation, "add_job", add_job)
    monkeypatch.setattr(
        creation,
        "add_job_log",
        lambda _session, job_id, message: calls["logs"].append((job_id, message)),
    )
    monkeypatch.setattr(creation, "stage_task", stage_task)
    monkeypatch.setattr(creation, "publish_pending_tasks", lambda *_args, **_kwargs: {"published": 1})
    monkeypatch.setattr(
        creation,
        "get_job",
        lambda _session, job_id: Job(
            id=job_id,
            type=JobType.whatsapp_dispatch_preview.value,
            title="Compile AntiDengue dispatch preview (2 profiles)",
            status=JobStatus.queued.value,
            parameters=dict(calls["parameters"]),
        ),
    )

    result = creation.create_previews_bulk(
        BulkPreviewInput(
            source_job_id=source_job.id,
            dispatch_profile_ids=[first_id, second_id],
        ),
        session,
    )

    assert result["count"] == 1
    assert result["profile_count"] == 2
    assert result["combined"] is True
    assert len(result["jobs"]) == 1
    assert calls["contract_profiles"] == [first_id, second_id]
    assert calls["parameters"]["dispatch_profile_id"] == str(first_id)
    assert calls["parameters"]["dispatch_profile_ids"] == [str(first_id), str(second_id)]
    assert len(calls["stage"]) == 1
    assert calls["stage"][0]["task_name"] == creation.PREVIEW_COMPILER_TASK
    assert calls["stage"][0]["queue"] == creation.PREVIEW_COMPILER_QUEUE
    assert calls["stage"][0]["args"] == [str(queued_job_id)]
    assert session.commits == 1

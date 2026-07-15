from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from automation_core.models import Job, JobStatus, JobType, SourceFile, SourceFileRun
from crm_filters.api import _source_file_dict


class _Result:
    def __init__(self, *, first_value=None, all_value=None) -> None:
        self._first_value = first_value
        self._all_value = all_value or []

    def first(self):
        return self._first_value

    def all(self):
        return self._all_value


def test_source_payload_exposes_workflow_specific_latest_jobs() -> None:
    source = SourceFile(
        module_key="crm",
        source_kind="crm_sheet_upload",
        original_name="crm.xlsx",
        stored_path="/tmp/crm.xlsx",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        extension=".xlsx",
        size_bytes=100,
        sha256="a" * 64,
        validation_status="valid",
        schema_version="crm_sheet_v1",
        detected_metadata={"row_count": 2},
        validation_errors=[],
        validation_warnings=[],
    )
    filter_job = Job(
        id=uuid.uuid4(),
        type=JobType.crm_sheet_filter.value,
        title="CRM sheet filter",
        status=JobStatus.succeeded.value,
    )
    conversion_job = Job(
        id=uuid.uuid4(),
        type=JobType.crm_sheet_to_pdf.value,
        title="CRM sheet rows to PDFs",
        status=JobStatus.running.value,
    )
    filter_link = SourceFileRun(source_file_id=source.id, job_id=filter_job.id)
    conversion_link = SourceFileRun(source_file_id=source.id, job_id=conversion_job.id)

    session = MagicMock()
    session.execute.side_effect = [
        _Result(first_value=(conversion_link, conversion_job)),
        _Result(first_value=(filter_link, filter_job)),
        _Result(first_value=(conversion_link, conversion_job)),
        _Result(all_value=[(conversion_link, conversion_job), (filter_link, filter_job)]),
    ]

    payload = _source_file_dict(session, source, include_runs=True)

    assert payload["latest_job"]["id"] == str(conversion_job.id)
    assert payload["latest_filter_job"]["type"] == JobType.crm_sheet_filter.value
    assert payload["latest_pdf_conversion_job"]["type"] == JobType.crm_sheet_to_pdf.value
    assert [run["type"] for run in payload["processing_runs"]] == [
        JobType.crm_sheet_to_pdf.value,
        JobType.crm_sheet_filter.value,
    ]

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session

from automation_core.database import get_session
from automation_core.job_service import append_log, create_job, get_job, set_task_id
from automation_core.models import JobPublic, JobType
from crm_filters.schemas import PdfFilterJobRequest, SheetFilterJobRequest
from crm_filters.tasks import run_pdf_filter_job, run_sheet_filter_job

router = APIRouter(prefix="/api/v1/crm/filters", tags=["crm-filters"])


@router.post(
    "/sheets/jobs",
    response_model=JobPublic,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_sheet_filter_job(
    request: SheetFilterJobRequest,
    session: Session = Depends(get_session),
) -> JobPublic:
    parameters = request.model_dump(exclude_none=True)
    job = create_job(
        session,
        job_type=JobType.crm_sheet_filter.value,
        title="CRM sheet duplicate filter",
        parameters=parameters,
    )
    try:
        task = run_sheet_filter_job.delay(str(job.id))
    except Exception as exc:
        append_log(session, job.id, f"Queue unavailable: {exc}", level="error")
        raise HTTPException(status_code=503, detail="Queue unavailable") from exc
    set_task_id(session, job.id, task.id)
    append_log(session, job.id, "Queued CRM sheet filter job.")
    queued = get_job(session, job.id)
    assert queued is not None
    return queued


@router.post(
    "/pdfs/jobs",
    response_model=JobPublic,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_pdf_filter_job(
    request: PdfFilterJobRequest,
    session: Session = Depends(get_session),
) -> JobPublic:
    parameters = request.model_dump(exclude_none=True)
    job = create_job(
        session,
        job_type=JobType.crm_pdf_filter.value,
        title="CRM PDF duplicate filter",
        parameters=parameters,
    )
    try:
        task = run_pdf_filter_job.delay(str(job.id))
    except Exception as exc:
        append_log(session, job.id, f"Queue unavailable: {exc}", level="error")
        raise HTTPException(status_code=503, detail="Queue unavailable") from exc
    set_task_id(session, job.id, task.id)
    append_log(session, job.id, "Queued CRM PDF filter job.")
    queued = get_job(session, job.id)
    assert queued is not None
    return queued


from __future__ import annotations

import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlmodel import Session

from automation_core.database import get_session
from automation_core.job_service import (
    get_job,
    list_artifacts,
    list_jobs,
    list_logs,
)
from automation_core.models import Artifact, ArtifactPublic, JobLogPublic, JobPublic

router = APIRouter(prefix="/api/v1", tags=["jobs"])


@router.get("/jobs", response_model=list[JobPublic])
def read_jobs(
    limit: int = 50,
    job_type: str | None = None,
    session: Session = Depends(get_session),
) -> list[JobPublic]:
    return list_jobs(session, limit=min(max(limit, 1), 200), job_type=job_type)


@router.get("/jobs/{job_id}", response_model=JobPublic)
def read_job(
    job_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> JobPublic:
    job = get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/jobs/{job_id}/logs", response_model=list[JobLogPublic])
def read_job_logs(
    job_id: uuid.UUID,
    limit: int = 500,
    session: Session = Depends(get_session),
) -> list[JobLogPublic]:
    if get_job(session, job_id) is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return list_logs(session, job_id, limit=limit)


@router.get("/jobs/{job_id}/artifacts", response_model=list[ArtifactPublic])
def read_job_artifacts(
    job_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> list[ArtifactPublic]:
    if get_job(session, job_id) is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return list_artifacts(session, job_id)


@router.get("/artifacts/{artifact_id}/download")
def download_artifact(
    artifact_id: int,
    session: Session = Depends(get_session),
) -> FileResponse:
    artifact = session.get(Artifact, artifact_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found")
    path = Path(artifact.path)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Artifact file is missing")
    return FileResponse(path, filename=path.name)

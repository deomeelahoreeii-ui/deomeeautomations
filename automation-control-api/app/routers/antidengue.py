from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.adapters.antidengue import (
    AntiDengueRunRequest,
    antidengue_command,
    latest_summary,
    latest_summary_brief,
    portal_login_status,
)
from app.auth import require_api_token
from app.config import settings
from app.runners.process_runner import ProcessRegistry
from app.runners.service_supervisor import ServiceSupervisor


router = APIRouter(prefix="/antidengue", tags=["antidengue"])


def registry() -> ProcessRegistry:
    from app.main import process_registry

    return process_registry


def services() -> ServiceSupervisor:
    from app.main import service_supervisor

    return service_supervisor


@router.get("/status")
def antidengue_status() -> dict[str, object]:
    runner = registry()
    active = runner.active("antidengue")
    latest_job = runner.latest("antidengue")
    return {
        "project": "antidengue",
        "active_job": asdict(active) if active else None,
        "latest_job": asdict(latest_job) if latest_job else None,
        "latest_run": latest_summary_brief(),
        "portal_login": portal_login_status(),
        "services": services().status(),
    }


@router.post("/run", dependencies=[Depends(require_api_token)])
def run_antidengue(request: AntiDengueRunRequest) -> dict[str, object]:
    try:
        command, env = antidengue_command(request)
        dependency_health = services().ensure_antidengue_dependencies(
            require_whatsapp=not request.dry_run
        )
        record = registry().start(
            project="antidengue",
            title="Run AntiDengue",
            command=command,
            cwd=settings.antidengue_dir,
            env=env,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except RuntimeError as exc:
        message = str(exc)
        if "NATS" in message or "WhatsApp worker" in message:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=message) from exc
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=message) from exc

    return {
        "started": True,
        "job": asdict(record),
        "login_mode": request.login_mode,
        "dry_run": request.dry_run,
        "services": dependency_health,
    }


@router.post("/stop", dependencies=[Depends(require_api_token)])
def stop_antidengue() -> dict[str, object]:
    try:
        record = registry().stop("antidengue")
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return {"stopped": True, "job": asdict(record)}


@router.get("/logs")
def antidengue_logs(lines: int = Query(default=200, ge=1, le=2000)) -> dict[str, object]:
    return registry().tail("antidengue", lines=lines)


@router.get("/latest-run")
def antidengue_latest_run() -> dict[str, object]:
    return latest_summary()

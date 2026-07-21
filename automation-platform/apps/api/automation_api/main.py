from __future__ import annotations

from contextlib import asynccontextmanager
import time
import uuid

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from starlette.middleware.trustedhost import TrustedHostMiddleware

from antidengue_automation.api import router as antidengue_router
from antidengue_automation.activity_rule_api import router as antidengue_activity_rule_router
from antidengue_automation.simple_activity_rule_api import router as antidengue_simple_activity_rule_router
from antidengue_automation.routing_profile_api import router as antidengue_routing_profile_router
from automation_core.api import router as jobs_router
from automation_core.config import get_settings
from automation_core.database import create_db_and_tables, engine
from automation_core.database_identity import database_identity
from automation_core.logging_config import configure_service_logging
from automation_core.storage_api import router as storage_router
from crm_filters.api import router as crm_filters_router
from crm_domain.api import router as crm_domain_router
from crm_domain.reply_api import router as crm_reply_router
from crm_domain.bulk_operations_api import router as crm_bulk_operations_router
from crm_domain.ai_pipeline_api import router as crm_ai_pipeline_router
from crm_domain.knowledge_api import router as crm_knowledge_router
from crm_domain.reply_workspace_api import router as crm_reply_workspace_router
from crm_domain.official_letters_api import router as crm_official_letters_router
from crm_domain.dispatch_api import router as crm_dispatch_router
from crm_domain.taxonomy_api import router as crm_taxonomy_router
from crm_integrations.frappe_helpdesk.api import router as crm_helpdesk_router
from master_data.api import router as master_data_router
from automation_api.notification_api import router as notification_router
from whatsapp_gateway.api import router as whatsapp_router
from whatsapp_gateway.preview_api import router as whatsapp_preview_router
from whatsapp_gateway.inbound_api import router as whatsapp_inbound_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    identity = database_identity()
    logger.info(
        "service.started",
        extra={
            "context": {
                "service": "api",
                "database_fingerprint": identity["fingerprint"],
            }
        },
    )
    yield


settings = get_settings()
logger = configure_service_logging(level=settings.log_level, log_format=settings.log_format)

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=[
        "Content-Type",
        "X-Request-ID",
        "X-WhatsApp-Worker-Token",
        "X-WhatsApp-Worker-Id",
    ],
)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.allowed_hosts)


@app.middleware("http")
async def request_observability(request: Request, call_next):  # type: ignore[no-untyped-def]
    request_id = request.headers.get("x-request-id", "").strip()[:128] or uuid.uuid4().hex
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "http.request.failed",
            extra={
                "context": {
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 2),
                }
            },
        )
        raise
    response.headers["x-request-id"] = request_id
    response.headers.setdefault("x-content-type-options", "nosniff")
    response.headers.setdefault("x-frame-options", "DENY")
    response.headers.setdefault("referrer-policy", "no-referrer")
    logger.info(
        "http.request.completed",
        extra={
            "context": {
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            }
        },
    )
    return response

app.include_router(jobs_router)
app.include_router(storage_router)
app.include_router(notification_router)
app.include_router(antidengue_router)
app.include_router(antidengue_activity_rule_router)
app.include_router(antidengue_simple_activity_rule_router)
app.include_router(antidengue_routing_profile_router)
app.include_router(crm_filters_router)
app.include_router(crm_domain_router)
app.include_router(crm_reply_router)
app.include_router(crm_bulk_operations_router)
app.include_router(crm_ai_pipeline_router)
app.include_router(crm_knowledge_router)
app.include_router(crm_taxonomy_router)
app.include_router(crm_reply_workspace_router)
app.include_router(crm_official_letters_router)
app.include_router(crm_dispatch_router)
app.include_router(crm_helpdesk_router)
app.include_router(master_data_router)
app.include_router(whatsapp_router)
app.include_router(whatsapp_preview_router)
app.include_router(whatsapp_inbound_router)


def _readiness_payload() -> dict[str, str]:
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as exc:
        logger.warning(
            "service.readiness.failed",
            extra={"context": {"component": "database", "error_type": type(exc).__name__}},
        )
        raise HTTPException(
            status_code=503,
            detail={"status": "not_ready", "database": "unavailable"},
        ) from exc

    identity = database_identity()
    return {
        "status": "ok",
        "database_fingerprint": identity["fingerprint"],
        "database": "reachable",
    }


@app.get("/livez", include_in_schema=False)
def liveness() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz", include_in_schema=False)
def readiness() -> dict[str, str]:
    return _readiness_payload()


@app.get("/health", include_in_schema=False)
def health() -> dict[str, str]:
    """Backward-compatible readiness endpoint for local scripts and Compose."""

    return _readiness_payload()

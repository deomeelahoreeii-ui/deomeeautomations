from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from antidengue_automation.api import router as antidengue_router
from antidengue_automation.activity_rule_api import router as antidengue_activity_rule_router
from antidengue_automation.simple_activity_rule_api import router as antidengue_simple_activity_rule_router
from antidengue_automation.routing_profile_api import router as antidengue_routing_profile_router
from automation_core.api import router as jobs_router
from automation_core.config import get_settings
from automation_core.database import create_db_and_tables
from automation_core.database_identity import database_identity
from automation_core.storage_api import router as storage_router
from crm_filters.api import router as crm_filters_router
from crm_domain.api import router as crm_domain_router
from crm_domain.reply_api import router as crm_reply_router
from master_data.api import router as master_data_router
from automation_api.notification_api import router as notification_router
from whatsapp_gateway.api import router as whatsapp_router
from whatsapp_gateway.preview_api import router as whatsapp_preview_router
from whatsapp_gateway.inbound_api import router as whatsapp_inbound_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    identity = database_identity()
    print(
        f"Automation API database={identity['fingerprint']} ({identity['display']})",
        flush=True,
    )
    yield


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
app.include_router(master_data_router)
app.include_router(whatsapp_router)
app.include_router(whatsapp_preview_router)
app.include_router(whatsapp_inbound_router)


@app.get("/health")
def health() -> dict[str, str]:
    identity = database_identity()
    return {
        "status": "ok",
        "database_fingerprint": identity["fingerprint"],
        "database": identity["display"],
    }

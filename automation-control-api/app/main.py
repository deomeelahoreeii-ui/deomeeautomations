from __future__ import annotations

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import antidengue, dashboard, health, portal_login
from app.runners.process_runner import ProcessRegistry
from app.runners.service_supervisor import ServiceSupervisor


process_registry = ProcessRegistry(settings.data_dir)
service_supervisor = ServiceSupervisor(settings.data_dir)


app = FastAPI(
    title="Deomee Automation Control API",
    version="0.1.0",
    description="Local control plane for Deomee automation projects.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1",
        "http://localhost",
        "https://127.0.0.1",
        "https://localhost",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type", "X-API-Token"],
)

app.include_router(health.router)
app.include_router(antidengue.router)
app.include_router(portal_login.router)
app.include_router(dashboard.router)


def run() -> None:
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=False,
    )


if __name__ == "__main__":
    run()

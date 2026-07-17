from __future__ import annotations

from celery import Celery
from celery.signals import worker_ready

from automation_core.config import get_settings

settings = get_settings()

transport_options: dict[str, str] = {}
if settings.celery_broker_url.startswith("filesystem://"):
    broker_folder = settings.celery_filesystem_folder.resolve()
    processed_folder = broker_folder / "processed"
    broker_folder.mkdir(parents=True, exist_ok=True)
    processed_folder.mkdir(parents=True, exist_ok=True)
    transport_options = {
        "data_folder_in": str(broker_folder),
        "data_folder_out": str(broker_folder),
        "processed_folder": str(processed_folder),
    }

celery_app = Celery(
    "automation_platform",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        "antidengue_automation.tasks",
        "crm_filters.tasks",
        "whatsapp_gateway.tasks",
        "whatsapp_gateway.inbound_tasks",
        "whatsapp_gateway.inbound.processing_tasks",
    ],
)

celery_app.conf.update(
    accept_content=["json"],
    broker_transport_options=transport_options,
    control_queue_durable=False,
    control_queue_exclusive=True,
    enable_utc=True,
    result_expires=60 * 60 * 24,
    task_ignore_result=settings.celery_result_backend is None,
    task_routes={
        "antidengue_automation.*": {"queue": "antidengue"},
        "whatsapp_gateway.compile_dispatch_preview": {"queue": "antidengue"},
        "whatsapp_gateway.send_approved_preview": {"queue": "antidengue"},
        "whatsapp_gateway.build_inbound_export": {"queue": "whatsapp"},
        "whatsapp_gateway.process_inbound_batch": {"queue": "whatsapp"},
        "crm_filters.*": {"queue": "crm"},
    },
    task_serializer="json",
    task_track_started=True,
    timezone="Asia/Karachi",
)


@worker_ready.connect
def _announce_worker_database(sender=None, **_kwargs):  # type: ignore[no-untyped-def]
    from automation_core.database_identity import database_identity

    identity = database_identity()
    print(
        f"Automation Celery worker database={identity['fingerprint']} ({identity['display']})",
        flush=True,
    )

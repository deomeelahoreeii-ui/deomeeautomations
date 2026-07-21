from __future__ import annotations

from celery import Celery
from celery.signals import heartbeat_sent, worker_ready

from automation_core.config import get_settings
from automation_core.logging_config import configure_service_logging

settings = get_settings()
logger = configure_service_logging(level=settings.log_level, log_format=settings.log_format)

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
        "whatsapp_gateway.inbound.publication_tasks",
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
        "whatsapp_gateway.compile_dispatch_preview.v2": {"queue": "antidengue-preview-v2"},
        "whatsapp_gateway.send_approved_preview": {"queue": "antidengue"},
        "whatsapp_gateway.build_inbound_export": {"queue": "whatsapp"},
        "whatsapp_gateway.process_inbound_batch": {"queue": "whatsapp"},
        "crm_filters.*": {"queue": "crm"},
        "crm_domain.*": {"queue": "crm"},
    },
    task_serializer="json",
    task_track_started=True,
    timezone="Asia/Karachi",
)


_registered_worker_name = ""


def _consumed_queues(sender) -> list[str]:  # type: ignore[no-untyped-def]
    try:
        consumer = getattr(sender, "consumer", sender)
        queues = consumer.task_consumer.queues
        if isinstance(queues, dict):
            return sorted(str(name) for name in queues)
        return sorted({str(getattr(queue, "name", queue)) for queue in queues})
    except (AttributeError, TypeError):
        return []


@worker_ready.connect
def _announce_worker_database(sender=None, **_kwargs):  # type: ignore[no-untyped-def]
    from automation_core.database_identity import database_identity

    global _registered_worker_name
    identity = database_identity()
    _registered_worker_name = str(getattr(sender, "hostname", "") or "automation-worker")
    logger.info(
        "worker.started",
        extra={
            "context": {
                "service": "celery-worker",
                "worker_name": _registered_worker_name,
                "database_fingerprint": identity["fingerprint"],
                "queues": _consumed_queues(sender),
            }
        },
    )
    try:
        from sqlmodel import Session

        from automation_core.database import engine
        from automation_core.worker_runtime import register_worker_runtime
        from whatsapp_gateway.previews.compiler.capabilities import (
            PREVIEW_COMPILER_PROTOCOL,
            compiler_build_id,
            compiler_capabilities,
            compiler_fingerprint,
        )

        with Session(engine) as session:
            register_worker_runtime(
                session,
                worker_name=_registered_worker_name,
                queues=_consumed_queues(sender),
                protocols={"antidengue_preview": PREVIEW_COMPILER_PROTOCOL},
                capabilities=compiler_capabilities(),
                capability_fingerprint=compiler_fingerprint(),
                build_id=compiler_build_id(),
                database_fingerprint=identity["fingerprint"],
            )
    except Exception as exc:
        logger.exception(
            "worker.capability_registration.failed",
            extra={"context": {"error_type": type(exc).__name__}},
        )


@heartbeat_sent.connect
def _refresh_worker_runtime(**_kwargs):  # type: ignore[no-untyped-def]
    if not _registered_worker_name:
        return
    try:
        from sqlmodel import Session

        from automation_core.database import engine
        from automation_core.worker_runtime import touch_worker_runtime

        with Session(engine) as session:
            touch_worker_runtime(session, _registered_worker_name)
    except Exception as exc:
        logger.exception(
            "worker.capability_heartbeat.failed",
            extra={"context": {"error_type": type(exc).__name__}},
        )

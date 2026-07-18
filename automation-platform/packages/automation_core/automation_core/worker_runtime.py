from __future__ import annotations

from datetime import timedelta
from typing import Any, Iterable

from sqlmodel import Session, select

from automation_core.models import AutomationWorkerRuntime
from automation_core.time import utcnow

WORKER_RUNTIME_TTL_SECONDS = 90


class WorkerCapabilityUnavailable(RuntimeError):
    def __init__(self, *, required: dict[str, Any], workers: list[dict[str, Any]]) -> None:
        self.required = required
        self.workers = workers
        capabilities = ", ".join(
            f"{key}.v{value}" for key, value in dict(required.get("capabilities") or {}).items()
        ) or "preview compiler protocol"
        super().__init__(
            f"No compatible preview compiler is available for {capabilities}. Restart or deploy the AntiDengue preview worker."
        )

    def detail(self) -> dict[str, Any]:
        return {
            "code": "preview_worker_capability_unavailable",
            "message": str(self),
            "required": self.required,
            "live_workers": self.workers,
        }


def register_worker_runtime(
    session: Session, *, worker_name: str, queues: Iterable[str],
    protocols: dict[str, int], capabilities: dict[str, int],
    capability_fingerprint: str, build_id: str, database_fingerprint: str,
) -> AutomationWorkerRuntime:
    item = session.scalar(select(AutomationWorkerRuntime).where(
        AutomationWorkerRuntime.worker_name == worker_name
    ))
    now = utcnow()
    if item is None:
        item = AutomationWorkerRuntime(worker_name=worker_name, started_at=now)
    else:
        item.started_at = now
    item.queues = sorted(set(queues))
    item.protocols = dict(protocols)
    item.capabilities = dict(capabilities)
    item.capability_fingerprint = capability_fingerprint
    item.build_id = build_id
    item.database_fingerprint = database_fingerprint
    item.last_seen_at = now
    session.add(item)
    session.commit()
    session.refresh(item)
    return item


def touch_worker_runtime(session: Session, worker_name: str) -> bool:
    item = session.scalar(select(AutomationWorkerRuntime).where(
        AutomationWorkerRuntime.worker_name == worker_name
    ))
    if item is None:
        return False
    item.last_seen_at = utcnow()
    session.add(item)
    session.commit()
    return True


def live_queue_consumers(
    session: Session, queue: str, *, ttl_seconds: int = WORKER_RUNTIME_TTL_SECONDS
) -> list[AutomationWorkerRuntime]:
    cutoff = utcnow() - timedelta(seconds=ttl_seconds)
    candidates = list(session.scalars(select(AutomationWorkerRuntime).where(
        AutomationWorkerRuntime.last_seen_at >= cutoff
    )).all())
    return [item for item in candidates if queue in set(item.queues or [])]


def _worker_summary(item: AutomationWorkerRuntime, problems: list[str]) -> dict[str, Any]:
    return {
        "worker_name": item.worker_name,
        "build_id": item.build_id,
        "last_seen_at": item.last_seen_at.isoformat(),
        "capability_fingerprint": item.capability_fingerprint,
        "problems": problems,
    }


def require_compatible_workers(
    session: Session, required: dict[str, Any]
) -> list[AutomationWorkerRuntime]:
    from whatsapp_gateway.previews.compiler.capabilities import contract_mismatches

    queue = str(required.get("queue") or "")
    workers = live_queue_consumers(session, queue)
    summaries: list[dict[str, Any]] = []
    incompatible = False
    for worker in workers:
        runtime = {
            "protocol": (worker.protocols or {}).get("antidengue_preview", 0),
            "capabilities": worker.capabilities or {},
        }
        problems = contract_mismatches(required, runtime)
        summaries.append(_worker_summary(worker, problems))
        incompatible = incompatible or bool(problems)
    if not workers or incompatible:
        raise WorkerCapabilityUnavailable(required=required, workers=summaries)
    return workers


__all__ = [
    "WORKER_RUNTIME_TTL_SECONDS", "WorkerCapabilityUnavailable",
    "live_queue_consumers", "register_worker_runtime", "require_compatible_workers",
    "touch_worker_runtime",
]

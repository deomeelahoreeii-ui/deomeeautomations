from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Iterable

from sqlmodel import Session

from whatsapp_gateway.configuration.dynamic_audiences import active_audience_sources
from whatsapp_gateway.models import (
    WhatsAppDispatchProfile,
    WhatsAppRecipientScope,
    WhatsAppReportType,
)

PREVIEW_COMPILER_PROTOCOL = 3
PREVIEW_COMPILER_TASK = "whatsapp_gateway.compile_dispatch_preview.v2"
PREVIEW_COMPILER_QUEUE = "antidengue-preview-v2"


@dataclass(frozen=True, slots=True)
class PlannerCapability:
    report_key: str
    audience_kind: str
    channel: str
    scope_key: str
    planner_key: str
    capability_id: str
    revision: int = 1
    delivery_granularities: tuple[str, ...] = ("recipient",)


class UnsupportedPlannerCapability(ValueError):
    def __init__(
        self, *, profile_name: str, report_key: str, audience_kind: str,
        channel: str, scope_key: str, delivery_granularity: str = "recipient",
    ) -> None:
        self.detail = {
            "code": "unsupported_preview_planner_capability",
            "profile": profile_name,
            "report_key": report_key,
            "audience_kind": audience_kind,
            "recipient_channel": channel,
            "scope_key": scope_key,
            "delivery_granularity": delivery_granularity,
            "message": (
                f"{profile_name} requires an unregistered preview planner for "
                f"{report_key}/{audience_kind}/{channel}/{scope_key or 'no-scope'}/"
                f"{delivery_granularity}."
            ),
        }
        super().__init__(self.detail["message"])


def _spec(
    report: str, audience: str, channel: str, scope: str, planner: str, capability: str,
    *, revision: int = 1, delivery_granularities: tuple[str, ...] = ("recipient",),
) -> PlannerCapability:
    return PlannerCapability(
        report, audience, channel, scope, planner, capability,
        revision, delivery_granularities,
    )


PLANNER_CAPABILITIES: tuple[PlannerCapability, ...] = (
    _spec("markaz_dormant_summary", "dynamic", "individual", "aeo", "dynamic_dormant", "antidengue.dormant.dynamic-aeo-markaz", revision=2, delivery_granularities=("recipient", "scope")),
    _spec("hotspot_distance_activity", "dynamic", "individual", "aeo", "dynamic_activity", "antidengue.hotspot.dynamic-aeo-markaz", revision=2, delivery_granularities=("recipient", "scope")),
    _spec("simple_activity_timing", "dynamic", "individual", "aeo", "dynamic_activity", "antidengue.timing.dynamic-aeo-markaz", revision=2, delivery_granularities=("recipient", "scope")),
    _spec("consolidated_action_digest", "dynamic", "individual", "aeo", "dynamic_digest", "antidengue.digest.dynamic-aeo-markaz", revision=2, delivery_granularities=("recipient", "scope")),
    _spec("wing_summary", "static", "group", "district", "wing_dormant", "antidengue.dormant.group-district"),
    _spec("wing_summary", "static", "group", "wing", "wing_dormant", "antidengue.dormant.group-wing"),
    _spec("tehsil_dormant_summary", "static", "group", "tehsil", "tehsil_dormant", "antidengue.dormant.group-tehsil"),
)

for _report, _planner, _prefix in (
    ("hotspot_distance_activity", "group_activity", "antidengue.hotspot"),
    ("simple_activity_timing", "group_activity", "antidengue.timing"),
    ("consolidated_action_digest", "group_digest", "antidengue.digest"),
):
    PLANNER_CAPABILITIES += tuple(
        _spec(_report, "static", "group", scope, _planner, f"{_prefix}.group-{scope}")
        for scope in ("district", "wing", "tehsil", "markaz")
    )


def resolve_planner_capability(
    *, report_key: str, audience_kind: str, channel: str, scope_key: str,
    delivery_granularity: str = "recipient",
) -> PlannerCapability | None:
    return next((
        item for item in PLANNER_CAPABILITIES
        if item.report_key == report_key
        and item.audience_kind == audience_kind
        and item.channel == channel
        and item.scope_key == scope_key
        and delivery_granularity in item.delivery_granularities
    ), None)


def _manifest() -> dict[str, int]:
    capabilities = {item.capability_id: item.revision for item in PLANNER_CAPABILITIES}
    capabilities["antidengue.preview.legacy-static"] = 1
    return dict(sorted(capabilities.items()))


def compiler_capabilities() -> dict[str, int]:
    return _manifest()


def compiler_fingerprint() -> str:
    value = {"protocol": PREVIEW_COMPILER_PROTOCOL, "capabilities": _manifest()}
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def compiler_build_id() -> str:
    return os.getenv("AUTOMATION_BUILD_ID", "").strip() or f"dev-{compiler_fingerprint()[:12]}"


def local_compiler_runtime(*, worker_name: str = "") -> dict[str, Any]:
    return {
        "worker_name": worker_name,
        "protocol": PREVIEW_COMPILER_PROTOCOL,
        "task": PREVIEW_COMPILER_TASK,
        "queue": PREVIEW_COMPILER_QUEUE,
        "build_id": compiler_build_id(),
        "capability_fingerprint": compiler_fingerprint(),
        "capabilities": compiler_capabilities(),
    }


def profile_capability(
    session: Session, profile: WhatsAppDispatchProfile
) -> PlannerCapability | None:
    report = session.get(WhatsAppReportType, profile.report_type_id)
    scope = session.get(WhatsAppRecipientScope, profile.recipient_scope_id) if profile.recipient_scope_id else None
    if report is None:
        raise ValueError(f"Routing profile {profile.name} has no report type.")
    audience_kind = "dynamic" if active_audience_sources(session, profile.audience_id) else "static"
    scope_key = scope.key if scope else ""
    return resolve_planner_capability(
        report_key=report.key,
        audience_kind=audience_kind,
        channel=profile.recipient_channel,
        scope_key=scope_key,
        delivery_granularity=profile.delivery_granularity,
    )


def required_compiler_contract(
    session: Session, profiles: Iterable[WhatsAppDispatchProfile]
) -> dict[str, Any]:
    requirements: dict[str, int] = {}
    routes: list[dict[str, str]] = []
    for profile in profiles:
        report = session.get(WhatsAppReportType, profile.report_type_id)
        scope = session.get(WhatsAppRecipientScope, profile.recipient_scope_id) if profile.recipient_scope_id else None
        audience_kind = "dynamic" if active_audience_sources(session, profile.audience_id) else "static"
        capability = profile_capability(session, profile)
        if capability is None:
            if audience_kind == "dynamic":
                raise UnsupportedPlannerCapability(
                    profile_name=profile.name,
                    report_key=report.key if report else "missing-report",
                    audience_kind=audience_kind,
                    channel=profile.recipient_channel,
                    scope_key=scope.key if scope else "",
                    delivery_granularity=profile.delivery_granularity,
                )
            requirements["antidengue.preview.legacy-static"] = 1
            routes.append({"profile": profile.name, "capability": "antidengue.preview.legacy-static"})
            continue
        requirements[capability.capability_id] = capability.revision
        routes.append({
            "profile": profile.name, "capability": capability.capability_id,
            "delivery_granularity": profile.delivery_granularity,
        })
    return {
        "protocol": PREVIEW_COMPILER_PROTOCOL,
        "task": PREVIEW_COMPILER_TASK,
        "queue": PREVIEW_COMPILER_QUEUE,
        "requested_by_build": compiler_build_id(),
        "requested_capability_fingerprint": compiler_fingerprint(),
        "capabilities": dict(sorted(requirements.items())),
        "routes": routes,
    }


def contract_mismatches(
    required: dict[str, Any], runtime: dict[str, Any]
) -> list[str]:
    problems: list[str] = []
    if runtime.get("task") and runtime.get("task") != required.get("task"):
        problems.append(f"task contract {runtime.get('task')} does not match {required.get('task')}")
    if runtime.get("queue") and runtime.get("queue") != required.get("queue"):
        problems.append(f"queue contract {runtime.get('queue')} does not match {required.get('queue')}")
    if int(runtime.get("protocol") or 0) < int(required.get("protocol") or 0):
        problems.append(
            f"compiler protocol {runtime.get('protocol') or 0} is older than required {required.get('protocol')}"
        )
    available = dict(runtime.get("capabilities") or {})
    for capability, revision in dict(required.get("capabilities") or {}).items():
        if int(available.get(capability) or 0) < int(revision):
            problems.append(f"missing {capability}.v{revision}")
    return problems


__all__ = [
    "PLANNER_CAPABILITIES", "PREVIEW_COMPILER_PROTOCOL", "PREVIEW_COMPILER_QUEUE",
    "PREVIEW_COMPILER_TASK", "compiler_build_id", "compiler_capabilities",
    "compiler_fingerprint", "contract_mismatches", "local_compiler_runtime",
    "UnsupportedPlannerCapability", "profile_capability", "required_compiler_contract",
    "resolve_planner_capability",
]

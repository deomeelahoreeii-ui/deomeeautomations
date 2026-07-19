from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlmodel import Session

from whatsapp_gateway.models import (
    WhatsAppAccount, WhatsAppAudience, WhatsAppAudienceMember,
    WhatsAppDispatchPreview, WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchProfile, WhatsAppTemplate,
)
from whatsapp_gateway.configuration.dynamic_audiences import (
    active_audience_sources, dynamic_audience_fingerprint, resolve_dynamic_audience,
)
from whatsapp_gateway.previews.artifact_storage import sha256_file

def _stale_reason(code: str, message: str, **details: Any) -> dict[str, Any]:
    return {"code": code, "message": message, **details}


def _snapshot_stale_reasons(
    session: Session, snapshot: dict[str, Any]
) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    profile_snapshot = snapshot.get("profile") or {}
    profile_id = profile_snapshot.get("id")
    profile = session.get(WhatsAppDispatchProfile, uuid.UUID(profile_id)) if profile_id else None
    profile_label = str(profile_snapshot.get("name") or profile_snapshot.get("key") or profile_id or "profile")
    if profile is None:
        return [_stale_reason("profile_missing", f"Routing profile {profile_label} no longer exists.", profile_id=profile_id)]
    if not profile.enabled:
        reasons.append(_stale_reason("profile_disabled", f"Routing profile {profile_label} is disabled.", profile_id=profile_id))
    if profile.version != profile_snapshot.get("version"):
        reasons.append(_stale_reason(
            "profile_version_changed",
            f"Routing profile {profile_label} changed after compilation.",
            profile_id=profile_id,
            frozen_version=profile_snapshot.get("version"),
            current_version=profile.version,
        ))

    audience_snapshot = snapshot.get("audience") or {}
    audience_id = audience_snapshot.get("id")
    audience = session.get(WhatsAppAudience, uuid.UUID(audience_id)) if audience_id else None
    if audience is None:
        reasons.append(_stale_reason("audience_missing", f"Audience for {profile_label} no longer exists.", profile_id=profile_id))
    elif not audience.enabled:
        reasons.append(_stale_reason("audience_disabled", f"Audience {audience.name} is disabled.", profile_id=profile_id))
    else:
        manual_members = list(session.scalars(
            select(WhatsAppAudienceMember).where(
                WhatsAppAudienceMember.audience_id == audience.id,
                WhatsAppAudienceMember.enabled.is_(True),
            )
        ).all())
        dynamic_members = resolve_dynamic_audience(
            session, audience_id=audience.id, granularity=profile.delivery_granularity,
        )
        current_targets = sorted(member.target_key for member in [*manual_members, *dynamic_members])
        if current_targets != sorted(audience_snapshot.get("target_keys") or []):
            reasons.append(_stale_reason(
                "audience_targets_changed",
                f"Audience membership for {profile_label} changed after compilation.",
                profile_id=profile_id,
            ))
        current_routes = sorted(
            f"{member.target_key}:{member.route_scope_key}:{member.route_scope_value}"
            for member in [*manual_members, *dynamic_members]
        )
        if current_routes != sorted(audience_snapshot.get("target_routes") or []):
            reasons.append(_stale_reason(
                "audience_routes_changed",
                f"Audience route bindings for {profile_label} changed after compilation.",
                profile_id=profile_id,
            ))
        frozen_dynamic_fingerprint = audience_snapshot.get("dynamic_fingerprint")
        if active_audience_sources(session, audience.id) or frozen_dynamic_fingerprint is not None:
            current_fingerprint = dynamic_audience_fingerprint(
                session, audience.id, granularity=profile.delivery_granularity,
            )
            if current_fingerprint != frozen_dynamic_fingerprint:
                reasons.append(_stale_reason(
                    "dynamic_audience_changed",
                    f"Master Data jurisdiction membership for {profile_label} changed after compilation.",
                    profile_id=profile_id,
                ))

    account_snapshot = snapshot.get("account") or {}
    account_id = account_snapshot.get("id")
    account = session.get(WhatsAppAccount, uuid.UUID(account_id)) if account_id else None
    if account is None:
        reasons.append(_stale_reason("account_missing", f"WhatsApp account for {profile_label} no longer exists.", profile_id=profile_id))
    elif not account.enabled:
        reasons.append(_stale_reason("account_disabled", f"WhatsApp account {account.name} is disabled.", profile_id=profile_id))
    elif account.worker_key != account_snapshot.get("worker_key"):
        reasons.append(_stale_reason("account_worker_changed", f"WhatsApp worker assignment for {profile_label} changed.", profile_id=profile_id))

    template_snapshot = snapshot.get("template") or {}
    template_id = template_snapshot.get("id")
    if template_id:
        template = session.get(WhatsAppTemplate, uuid.UUID(template_id))
        if template is None:
            reasons.append(_stale_reason("template_missing", f"Message template for {profile_label} no longer exists.", profile_id=profile_id))
        elif not template.enabled:
            reasons.append(_stale_reason("template_disabled", f"Message template {template.name} is disabled.", profile_id=profile_id))
        elif template.body != template_snapshot.get("body"):
            reasons.append(_stale_reason("template_body_changed", f"Message template {template.name} changed after compilation.", profile_id=profile_id))
    elif profile.template_id is not None:
        reasons.append(_stale_reason("template_binding_changed", f"A template was assigned to {profile_label} after compilation.", profile_id=profile_id))
    return reasons


def preview_stale_reasons(
    session: Session, preview: WhatsAppDispatchPreview, *, check_files: bool = False
) -> list[dict[str, Any]]:
    snapshot = dict(preview.configuration_snapshot or {})
    snapshots = list(snapshot.get("profile_snapshots") or [])
    if not snapshots:
        snapshots = [snapshot]
    reasons: list[dict[str, Any]] = []
    for item in snapshots:
        reasons.extend(_snapshot_stale_reasons(session, dict(item or {})))

    # Older merged previews only retained compact secondary profile snapshots.
    if not snapshot.get("profile_snapshots"):
        checked = {str((item.get("profile") or {}).get("id") or "") for item in snapshots}
        for profile_snapshot in snapshot.get("profiles") or []:
            profile_id = str(profile_snapshot.get("id") or "")
            if not profile_id or profile_id in checked:
                continue
            selected = session.get(WhatsAppDispatchProfile, uuid.UUID(profile_id))
            if selected is None:
                reasons.append(_stale_reason("profile_missing", "A routing profile in the merged preview no longer exists.", profile_id=profile_id))
            elif not selected.enabled:
                reasons.append(_stale_reason("profile_disabled", f"Routing profile {selected.name} is disabled.", profile_id=profile_id))
            elif selected.version != profile_snapshot.get("version"):
                reasons.append(_stale_reason("profile_version_changed", f"Routing profile {selected.name} changed after compilation.", profile_id=profile_id))

    if check_files:
        artifacts = session.scalars(
            select(WhatsAppDispatchPreviewArtifact).where(
                WhatsAppDispatchPreviewArtifact.preview_id == preview.id,
                WhatsAppDispatchPreviewArtifact.role == "delivery",
            )
        ).all()
        for artifact in artifacts:
            path = Path(artifact.path_snapshot)
            if not path.is_file():
                reasons.append(_stale_reason("attachment_missing", f"Frozen attachment {artifact.name} is missing.", artifact_id=str(artifact.id)))
            elif path.stat().st_size != artifact.size_bytes:
                reasons.append(_stale_reason("attachment_size_changed", f"Frozen attachment {artifact.name} changed size.", artifact_id=str(artifact.id)))
            elif artifact.checksum_sha256 and sha256_file(path) != artifact.checksum_sha256:
                reasons.append(_stale_reason("attachment_checksum_changed", f"Frozen attachment {artifact.name} changed after compilation.", artifact_id=str(artifact.id)))

    unique: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for reason in reasons:
        identity = (str(reason.get("code")), str(reason.get("profile_id") or ""), str(reason.get("artifact_id") or ""))
        if identity not in seen:
            unique.append(reason)
            seen.add(identity)
    return unique


def preview_is_stale(session: Session, preview: WhatsAppDispatchPreview, *, check_files: bool = False) -> bool:
    return bool(preview_stale_reasons(session, preview, check_files=check_files))

__all__ = ["preview_is_stale", "preview_stale_reasons"]

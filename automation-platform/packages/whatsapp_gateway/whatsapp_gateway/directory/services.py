from __future__ import annotations

from whatsapp_gateway.api_imports import *

from whatsapp_gateway.api_schemas import *
from whatsapp_gateway.gateway.services import gateway_datetime
from whatsapp_gateway.directory.master_contacts import ensure_master_contact

def directory_group_dict(
    item: WhatsAppDirectoryGroup,
    configured: WhatsAppGroup | None = None,
    wing: Wing | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "jid": item.jid,
        "name": item.name,
        "description": item.description,
        "owner_jid": item.owner_jid,
        "participant_count": item.participant_count,
        "available": item.available,
        "discovered_at": item.discovered_at,
        "last_seen_at": item.last_seen_at,
        "last_synced_at": item.last_synced_at,
        "configured_id": str(configured.id) if configured else None,
        "wing_id": str(configured.wing_id) if configured else None,
        "wing_name": wing.name if wing else None,
        "purpose": configured.purpose if configured else None,
        "dispatch_enabled": configured.enabled if configured else False,
    }

def upsert_directory_contact(
    session: Session,
    account: WhatsAppAccount,
    source: dict[str, Any],
    synced_at: datetime,
) -> WhatsAppDirectoryContact | None:
    phone_jid = str(source.get("phoneJid") or source.get("phone_jid") or "").strip() or None
    primary_lid = str(
        source.get("primaryLidJid") or source.get("primary_lid_jid") or ""
    ).strip() or None
    canonical_key = phone_jid or primary_lid
    if not canonical_key:
        return None
    item = session.scalar(
        select(WhatsAppDirectoryContact).where(
            WhatsAppDirectoryContact.account_id == account.id,
            WhatsAppDirectoryContact.canonical_key == canonical_key,
        )
    )
    if item is None and primary_lid:
        alias = session.scalar(
            select(WhatsAppIdentityAlias).where(
                WhatsAppIdentityAlias.account_id == account.id,
                WhatsAppIdentityAlias.lid_jid == primary_lid,
            )
        )
        item = session.get(WhatsAppDirectoryContact, alias.contact_id) if alias else None
    if item is None:
        item = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key=canonical_key,
            first_seen_at=gateway_datetime(source.get("firstSeenAt"), synced_at),
        )
    token = source.get("token") or {}
    item.canonical_key = phone_jid or item.canonical_key
    item.phone_jid = phone_jid or item.phone_jid
    item.primary_lid_jid = primary_lid or item.primary_lid_jid
    observed_name = str(source.get("displayName") or source.get("name") or "").strip()
    item.display_name = observed_name or item.display_name
    item.source = str(source.get("source") or item.source or "gateway")
    item.confidence = float(source.get("confidence") or item.confidence or 0)
    item.active = bool(source.get("active", True))
    item.last_seen_at = gateway_datetime(source.get("lastSeenAt"), synced_at)
    item.last_synced_at = synced_at
    if token:
        item.token_status = str(token.get("status") or "unknown")
        item.token_checked_at = gateway_datetime(token.get("checkedAt"), synced_at)
        item.token_issued_at = (
            gateway_datetime(token.get("issuedAt")) if token.get("issuedAt") else None
        )
    session.add(item)
    session.flush()
    ensure_master_contact(
        session,
        item,
        observed_name=observed_name,
        name_source="whatsapp_profile",
        observed_at=synced_at,
    )
    aliases = source.get("aliases") or []
    if primary_lid and not any(
        (alias.get("lidJid") or alias.get("lid_jid")) == primary_lid
        for alias in aliases
        if isinstance(alias, dict)
    ):
        aliases = [*aliases, {"lidJid": primary_lid, "source": item.source}]
    for alias_source in aliases:
        if not isinstance(alias_source, dict):
            continue
        lid_jid = str(alias_source.get("lidJid") or alias_source.get("lid_jid") or "").strip()
        if not lid_jid:
            continue
        alias = session.scalar(
            select(WhatsAppIdentityAlias).where(
                WhatsAppIdentityAlias.account_id == account.id,
                WhatsAppIdentityAlias.lid_jid == lid_jid,
            )
        )
        if alias is None:
            alias = WhatsAppIdentityAlias(
                account_id=account.id,
                contact_id=item.id,
                lid_jid=lid_jid,
                first_seen_at=gateway_datetime(alias_source.get("firstSeenAt"), synced_at),
            )
        alias.contact_id = item.id
        alias.source = str(alias_source.get("source") or item.source)
        alias.confidence = float(alias_source.get("confidence") or item.confidence)
        alias.last_seen_at = gateway_datetime(alias_source.get("lastSeenAt"), synced_at)
        session.add(alias)
    return item

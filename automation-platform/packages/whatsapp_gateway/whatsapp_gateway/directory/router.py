from __future__ import annotations

from fastapi import APIRouter

from whatsapp_gateway.api_common import *
from master_data.models import MasterContact
from whatsapp_gateway.directory.master_contacts import resolved_contact_dict
from whatsapp_gateway.directory.master_contact_routes import router as _master_contact_router

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp"])
router.include_router(_master_contact_router)
@router.post("/directory/sync")
async def sync_directory(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    config = get_settings()
    groups_result = await gateway_request(
        config.whatsapp_group_directory_subject,
        {"refresh": True},
        timeout=30,
    )
    identities_result = await gateway_request(
        config.whatsapp_identity_directory_subject,
        {"action": "list"},
        timeout=30,
    )
    synced_at = gateway_datetime(
        identities_result.get("checkedAt") or groups_result.get("checkedAt")
    )
    for item in session.scalars(
        select(WhatsAppDirectoryGroup).where(
            WhatsAppDirectoryGroup.account_id == account.id
        )
    ):
        item.available = False
        session.add(item)
    for source in groups_result.get("groups") or []:
        jid = str(source.get("jid") or "").strip()
        if not jid.endswith("@g.us"):
            continue
        item = session.scalar(
            select(WhatsAppDirectoryGroup).where(
                WhatsAppDirectoryGroup.account_id == account.id,
                WhatsAppDirectoryGroup.jid == jid,
            )
        )
        if item is None:
            item = WhatsAppDirectoryGroup(account_id=account.id, jid=jid)
        item.name = str(source.get("name") or item.name or jid)
        item.description = str(source.get("description") or "")
        item.owner_jid = str(source.get("ownerJid") or "") or None
        item.participant_count = int(source.get("participantCount") or 0)
        item.available = True
        item.last_seen_at = synced_at
        item.last_synced_at = synced_at
        session.add(item)
    for item in session.scalars(
        select(WhatsAppDirectoryContact).where(
            WhatsAppDirectoryContact.account_id == account.id
        )
    ):
        item.active = False
        session.add(item)
    contact_count = 0
    for source in identities_result.get("contacts") or []:
        if upsert_directory_contact(session, account, source, synced_at):
            contact_count += 1
    activity(
        session,
        account,
        "directory_synchronized",
        "Synchronized WhatsApp groups and identities",
        details={
            "groups": len(groups_result.get("groups") or []),
            "contacts": contact_count,
        },
    )
    session.commit()
    return {
        "groups": len(groups_result.get("groups") or []),
        "contacts": contact_count,
        "synced_at": synced_at,
    }

@router.get("/directory/summary")
def directory_summary(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    count = lambda model, *filters: session.scalar(
        select(func.count()).select_from(model).where(*filters)
    ) or 0
    last_group_sync = session.scalar(
        select(func.max(WhatsAppDirectoryGroup.last_synced_at)).where(
            WhatsAppDirectoryGroup.account_id == account.id
        )
    )
    last_contact_sync = session.scalar(
        select(func.max(WhatsAppDirectoryContact.last_synced_at)).where(
            WhatsAppDirectoryContact.account_id == account.id
        )
    )
    return {
        "groups": count(
            WhatsAppDirectoryGroup,
            WhatsAppDirectoryGroup.account_id == account.id,
            WhatsAppDirectoryGroup.available.is_(True),
        ),
        "configured_groups": count(
            WhatsAppGroup,
            WhatsAppGroup.account_id == account.id,
            WhatsAppGroup.enabled.is_(True),
        ),
        "contacts": count(
            WhatsAppDirectoryContact,
            WhatsAppDirectoryContact.account_id == account.id,
            WhatsAppDirectoryContact.active.is_(True),
        ),
        "lid_mappings": count(
            WhatsAppIdentityAlias,
            WhatsAppIdentityAlias.account_id == account.id,
        ),
        "last_synced_at": max(
            (value for value in [last_group_sync, last_contact_sync] if value),
            default=None,
        ),
    }

@router.get("/directory/groups")
def directory_groups(
    search: str = "",
    availability: Literal["all", "available", "unavailable"] = "all",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    filters: list[Any] = [WhatsAppDirectoryGroup.account_id == account.id]
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                WhatsAppDirectoryGroup.name.ilike(pattern),
                WhatsAppDirectoryGroup.jid.ilike(pattern),
            )
        )
    if availability != "all":
        filters.append(
            WhatsAppDirectoryGroup.available.is_(availability == "available")
        )
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDirectoryGroup).where(*filters)
    ) or 0
    records = session.execute(
        select(WhatsAppDirectoryGroup, WhatsAppGroup, Wing)
        .outerjoin(
            WhatsAppGroup,
            WhatsAppGroup.directory_group_id == WhatsAppDirectoryGroup.id,
        )
        .outerjoin(Wing, Wing.id == WhatsAppGroup.wing_id)
        .where(*filters)
        .order_by(WhatsAppDirectoryGroup.name, WhatsAppDirectoryGroup.jid)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [
            directory_group_dict(item, configured, wing)
            for item, configured, wing in records
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }

@router.get("/directory/groups/{group_id}")
def directory_group(group_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppDirectoryGroup, group_id)
    if item is None or item.account_id != account.id:
        raise HTTPException(status_code=404, detail="Detected WhatsApp group not found")
    configured = session.scalar(
        select(WhatsAppGroup).where(WhatsAppGroup.directory_group_id == item.id)
    )
    wing = session.get(Wing, configured.wing_id) if configured else None
    return directory_group_dict(item, configured, wing)

@router.post("/directory/groups/{group_id}/members/sync")
async def sync_group_members(
    group_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    group = session.get(WhatsAppDirectoryGroup, group_id)
    if group is None or group.account_id != account.id:
        raise HTTPException(status_code=404, detail="Detected WhatsApp group not found")
    result = await gateway_request(
        get_settings().whatsapp_group_members_subject,
        {"groupJid": group.jid},
        timeout=30,
    )
    synced_at = gateway_datetime(result.get("checkedAt"))
    for item in session.scalars(
        select(WhatsAppGroupMember).where(
            WhatsAppGroupMember.directory_group_id == group.id
        )
    ):
        item.active = False
        session.add(item)
    for source in result.get("members") or []:
        member_jid = str(source.get("id") or "").strip() or None
        phone_jid = str(source.get("phoneNumber") or "").strip() or None
        lid_jid = str(source.get("lid") or "").strip() or None
        if member_jid and member_jid.endswith("@s.whatsapp.net"):
            phone_jid = phone_jid or member_jid
        if member_jid and member_jid.endswith("@lid"):
            lid_jid = lid_jid or member_jid
        member_key = phone_jid or lid_jid or member_jid
        if not member_key:
            continue
        contact = upsert_directory_contact(
            session,
            account,
            {
                "phoneJid": phone_jid,
                "primaryLidJid": lid_jid,
                "displayName": source.get("name") or "",
                "source": "group_metadata",
                "confidence": 0.9 if phone_jid and lid_jid else 0.6,
                "lastSeenAt": synced_at.isoformat(),
                "aliases": [{"lidJid": lid_jid, "source": "group_metadata"}]
                if lid_jid
                else [],
            },
            synced_at,
        )
        member = session.scalar(
            select(WhatsAppGroupMember).where(
                WhatsAppGroupMember.directory_group_id == group.id,
                WhatsAppGroupMember.member_key == member_key,
            )
        )
        if member is None:
            member = WhatsAppGroupMember(
                directory_group_id=group.id,
                member_key=member_key,
            )
        member.contact_id = contact.id if contact else None
        member.member_jid = member_jid
        member.phone_jid = phone_jid
        member.lid_jid = lid_jid
        member.display_name = str(source.get("name") or member.display_name)
        member.admin_role = str(source.get("admin") or "") or None
        member.active = True
        member.last_seen_at = synced_at
        session.add(member)
    group.name = str(result.get("groupName") or group.name)
    group.participant_count = int(result.get("size") or 0)
    group.last_synced_at = synced_at
    group.last_seen_at = synced_at
    session.add(group)
    activity(
        session,
        account,
        "group_members_synchronized",
        f"Synchronized members for {group.name}",
        details={"group_id": str(group.id), "members": len(result.get("members") or [])},
    )
    session.commit()
    return {"members": len(result.get("members") or []), "synced_at": synced_at}

@router.get("/directory/groups/{group_id}/members")
def directory_group_members(
    group_id: uuid.UUID,
    search: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    group = session.get(WhatsAppDirectoryGroup, group_id)
    if group is None or group.account_id != account.id:
        raise HTTPException(status_code=404, detail="Detected WhatsApp group not found")
    filters: list[Any] = [
        WhatsAppGroupMember.directory_group_id == group.id,
        WhatsAppGroupMember.active.is_(True),
    ]
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                WhatsAppGroupMember.display_name.ilike(pattern),
                WhatsAppGroupMember.phone_jid.ilike(pattern),
                WhatsAppGroupMember.lid_jid.ilike(pattern),
                WhatsAppGroupMember.member_jid.ilike(pattern),
            )
        )
    total = session.scalar(
        select(func.count()).select_from(WhatsAppGroupMember).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppGroupMember)
        .where(*filters)
        .order_by(WhatsAppGroupMember.display_name, WhatsAppGroupMember.member_key)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [
            {
                "id": str(item.id),
                "contact_id": str(item.contact_id) if item.contact_id else None,
                "display_name": item.display_name,
                "member_jid": item.member_jid,
                "phone_jid": item.phone_jid,
                "lid_jid": item.lid_jid,
                "admin_role": item.admin_role,
                "last_seen_at": item.last_seen_at,
            }
            for item in records
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }

@router.get("/directory/contacts")
def directory_contacts(
    search: str = "",
    token_status: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    filters: list[Any] = [
        WhatsAppDirectoryContact.account_id == account.id,
        WhatsAppDirectoryContact.active.is_(True),
    ]
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                MasterContact.name.ilike(pattern),
                WhatsAppDirectoryContact.display_name.ilike(pattern),
                WhatsAppDirectoryContact.phone_jid.ilike(pattern),
                WhatsAppDirectoryContact.primary_lid_jid.ilike(pattern),
            )
        )
    if token_status:
        filters.append(WhatsAppDirectoryContact.token_status == token_status)
    total = session.scalar(
        select(func.count())
        .select_from(WhatsAppDirectoryContact)
        .outerjoin(MasterContact, MasterContact.id == WhatsAppDirectoryContact.master_contact_id)
        .where(*filters)
    ) or 0
    records = session.execute(
        select(WhatsAppDirectoryContact, MasterContact)
        .outerjoin(MasterContact, MasterContact.id == WhatsAppDirectoryContact.master_contact_id)
        .where(*filters)
        .order_by(
            func.nullif(MasterContact.name, "").asc().nulls_last(),
            WhatsAppDirectoryContact.display_name,
            WhatsAppDirectoryContact.phone_jid,
        )
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    items = []
    for item, master_contact in records:
        aliases = session.scalars(
            select(WhatsAppIdentityAlias)
            .where(WhatsAppIdentityAlias.contact_id == item.id)
            .order_by(WhatsAppIdentityAlias.confidence.desc())
        ).all()
        group_count = session.scalar(
            select(func.count())
            .select_from(WhatsAppGroupMember)
            .where(
                WhatsAppGroupMember.contact_id == item.id,
                WhatsAppGroupMember.active.is_(True),
            )
        ) or 0
        link_count = session.scalar(
            select(func.count())
            .select_from(WhatsAppContactLink)
            .where(
                WhatsAppContactLink.directory_contact_id == item.id,
                WhatsAppContactLink.active.is_(True),
                WhatsAppContactLink.status == "verified",
            )
        ) or 0
        items.append(
            {
                "id": str(item.id),
                **resolved_contact_dict(item, master_contact),
                "master_contact_id": str(master_contact.id) if master_contact else None,
                "phone_jid": item.phone_jid,
                "primary_lid_jid": item.primary_lid_jid,
                "aliases": [alias.lid_jid for alias in aliases],
                "alias_count": len(aliases),
                "group_count": group_count,
                "link_count": link_count,
                "source": item.source,
                "confidence": item.confidence,
                "token_status": item.token_status,
                "token_checked_at": item.token_checked_at,
                "token_issued_at": item.token_issued_at,
                "last_seen_at": item.last_seen_at,
            }
        )
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.post("/directory/contacts/{contact_id}/token/refresh")
async def refresh_contact_token(
    contact_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or contact.account_id != account.id:
        raise HTTPException(status_code=404, detail="WhatsApp contact not found")
    if not contact.primary_lid_jid:
        raise HTTPException(status_code=422, detail="This contact has no mapped LID")
    result = await gateway_request(
        get_settings().whatsapp_identity_directory_subject,
        {"action": "refresh_token", "lidJid": contact.primary_lid_jid},
        timeout=30,
    )
    token = result.get("token") or {}
    contact.token_status = str(token.get("status") or "unknown")
    contact.token_checked_at = gateway_datetime(token.get("checkedAt"))
    contact.token_issued_at = (
        gateway_datetime(token.get("issuedAt")) if token.get("issuedAt") else None
    )
    contact.last_synced_at = utcnow()
    session.add(contact)
    activity(
        session,
        account,
        "identity_token_refreshed",
        f"Refreshed privacy token status for {contact.display_name or contact.phone_jid}",
        details={"contact_id": str(contact.id), "status": contact.token_status},
    )
    session.commit()
    return {
        "id": str(contact.id),
        "token_status": contact.token_status,
        "token_checked_at": contact.token_checked_at,
        "token_issued_at": contact.token_issued_at,
    }

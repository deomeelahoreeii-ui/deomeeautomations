from __future__ import annotations

from fastapi import APIRouter

from whatsapp_gateway.api_common import *

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp"])

from whatsapp_gateway.directory.router import directory_group

@router.get("/groups")
def groups(session: Session = Depends(get_session)) -> list[dict[str, Any]]:
    ensure_defaults(session)
    records = session.execute(
        select(WhatsAppGroup, Wing)
        .join(Wing, Wing.id == WhatsAppGroup.wing_id)
        .order_by(Wing.name, WhatsAppGroup.name)
    ).all()
    return [
        group_dict(item, wing.name, session.get(WhatsAppDirectoryGroup, item.directory_group_id))
        for item, wing in records
    ]

@router.post("/groups", status_code=status.HTTP_201_CREATED)
def save_group(data: GroupInput, session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    wing = session.get(Wing, data.wing_id)
    if wing is None or not wing.active:
        raise HTTPException(status_code=422, detail="Select a valid wing")
    directory_group = (
        session.get(WhatsAppDirectoryGroup, data.directory_group_id)
        if data.directory_group_id
        else None
    )
    if data.directory_group_id and (
        directory_group is None
        or directory_group.account_id != account.id
        or not directory_group.available
    ):
        raise HTTPException(status_code=422, detail="Select an available detected group")
    resolved_name = directory_group.name if directory_group else data.name.strip()
    resolved_jid = directory_group.jid if directory_group else data.jid.strip()
    if not resolved_jid.endswith("@g.us"):
        raise HTTPException(status_code=422, detail="Group JID must end with @g.us")
    duplicate = session.scalar(
        select(WhatsAppGroup).where(
            WhatsAppGroup.account_id == account.id,
            WhatsAppGroup.jid == resolved_jid,
            WhatsAppGroup.id != data.id if data.id else True,
        )
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="This WhatsApp group already exists")
    item = session.get(WhatsAppGroup, data.id) if data.id else None
    if data.id and item is None:
        raise HTTPException(status_code=404, detail="WhatsApp group not found")
    if item is None:
        item = WhatsAppGroup(
            account_id=account.id,
            wing_id=data.wing_id,
            name=resolved_name,
            jid=resolved_jid,
        )
    item.directory_group_id = directory_group.id if directory_group else None
    item.wing_id = data.wing_id
    item.name = resolved_name
    item.jid = resolved_jid
    item.purpose = data.purpose
    item.notes = data.notes
    item.enabled = data.enabled
    item.verified_at = utcnow() if directory_group else item.verified_at
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "group_saved", f"Saved WhatsApp group {item.name}")
    session.commit()
    return group_dict(item, wing.name, directory_group)

@router.delete("/groups/{group_id}")
def archive_group(group_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppGroup, group_id)
    if item is None:
        raise HTTPException(status_code=404, detail="WhatsApp group not found")
    item.enabled = False
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "group_archived", f"Archived WhatsApp group {item.name}")
    session.commit()
    wing = session.get(Wing, item.wing_id)
    directory_group = (
        session.get(WhatsAppDirectoryGroup, item.directory_group_id)
        if item.directory_group_id
        else None
    )
    return group_dict(item, wing.name if wing else "", directory_group)

@router.delete("/groups/{group_id}/hard")
def hard_delete_group(group_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppGroup, group_id)
    if item is None:
        raise HTTPException(status_code=404, detail="WhatsApp group not found")
    name = item.name
    session.delete(item)
    activity(session, account, "group_deleted", f"Permanently deleted WhatsApp group {name}")
    session.commit()
    return {"id": str(group_id), "deleted": True}

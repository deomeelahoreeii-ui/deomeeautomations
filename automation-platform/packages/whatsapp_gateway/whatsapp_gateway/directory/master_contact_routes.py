from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlmodel import Session

from automation_core.database import get_session
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.directory.master_contacts import (
    resolved_contact_dict,
    set_directory_master_name,
)
from whatsapp_gateway.gateway.activity import activity
from whatsapp_gateway.models import WhatsAppDirectoryContact


class MasterContactNameInput(BaseModel):
    name: str = Field(min_length=1, max_length=200)


router = APIRouter()


@router.put("/directory/contacts/{contact_id}/master-contact")
def update_directory_master_contact(
    contact_id: uuid.UUID,
    data: MasterContactNameInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or contact.account_id != account.id or not contact.active:
        raise HTTPException(status_code=404, detail="Active WhatsApp contact not found")
    try:
        master = set_directory_master_name(session, contact, data.name)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    activity(
        session,
        account,
        "master_contact_named",
        f"Set verified master contact name to {master.name}",
        details={
            "directory_contact_id": str(contact.id),
            "master_contact_id": str(master.id),
        },
    )
    session.commit()
    session.refresh(master)
    return resolved_contact_dict(contact, master)

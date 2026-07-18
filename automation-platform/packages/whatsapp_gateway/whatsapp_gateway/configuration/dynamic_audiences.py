from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

from sqlalchemy import or_
from sqlmodel import Session, select

from master_data.models import Markaz, Officer, OfficerJurisdiction, School, Tehsil, Wing
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
)
from whatsapp_gateway.persistence.audience_sources import WhatsAppAudienceSource

ResolutionGranularity = Literal["source", "recipient", "scope"]


@dataclass(slots=True)
class ResolvedAudienceMember:
    """Runtime-compatible audience member derived from authoritative Master Data."""

    id: uuid.UUID
    audience_id: uuid.UUID
    target_type: str
    target_key: str
    route_scope_key: str
    route_scope_value: str
    route_scope_label: str
    enabled: bool
    source_id: uuid.UUID
    officer_id: uuid.UUID
    target_jid: str
    jurisdiction_ids: list[uuid.UUID] = field(default_factory=list)
    scope_ids: list[uuid.UUID] = field(default_factory=list)
    scope_labels: list[str] = field(default_factory=list)
    school_ids: list[uuid.UUID] = field(default_factory=list)
    school_emis: list[str] = field(default_factory=list)
    source_fingerprint: str = ""
    directory_contact_id: uuid.UUID | None = None
    directory_group_id: uuid.UUID | None = None


def _phone_jid(normalized_mobile: str) -> str:
    digits = re.sub(r"\D", "", normalized_mobile or "")
    return f"{digits}@s.whatsapp.net" if 10 <= len(digits) <= 15 else ""


def _digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def active_audience_sources(
    session: Session, audience_id: uuid.UUID
) -> list[WhatsAppAudienceSource]:
    return list(
        session.scalars(
            select(WhatsAppAudienceSource)
            .where(
                WhatsAppAudienceSource.audience_id == audience_id,
                WhatsAppAudienceSource.enabled.is_(True),
            )
            .order_by(WhatsAppAudienceSource.created_at, WhatsAppAudienceSource.id)
        ).all()
    )


def resolve_dynamic_audience(
    session: Session,
    *,
    audience_id: uuid.UUID,
    account: WhatsAppAccount | None = None,
    granularity: ResolutionGranularity = "source",
) -> list[ResolvedAudienceMember]:
    resolved: list[ResolvedAudienceMember] = []
    for source in active_audience_sources(session, audience_id):
        resolved.extend(resolve_audience_source(
            session, source=source, account=account, granularity=granularity,
        ))
    return resolved


def resolve_audience_source(
    session: Session,
    *,
    source: WhatsAppAudienceSource,
    account: WhatsAppAccount | None = None,
    granularity: ResolutionGranularity = "source",
) -> list[ResolvedAudienceMember]:
    resolved: list[ResolvedAudienceMember] = []
    if source.source_type != "master_data_jurisdictions":
        raise ValueError(f"Unsupported audience source: {source.source_type}")
    expected_scope = "markaz" if source.recipient_role == "aeo" else "tehsil"
    if source.route_scope_key != expected_scope:
        raise ValueError(f"{source.recipient_role.upper()} jurisdiction audiences require {expected_scope} scope")
    wing = session.get(Wing, source.wing_id)
    if wing is None or not wing.active:
        return []

    records = session.execute(
        select(OfficerJurisdiction, Officer)
        .join(Officer, Officer.id == OfficerJurisdiction.officer_id)
        .where(
            OfficerJurisdiction.active.is_(True),
            OfficerJurisdiction.role == source.recipient_role,
            OfficerJurisdiction.wing_id == source.wing_id,
            Officer.active.is_(True),
            Officer.role == source.recipient_role,
            Officer.wing_id == source.wing_id,
        )
        .order_by(Officer.name, OfficerJurisdiction.tehsil_id, OfficerJurisdiction.markaz_id)
    ).all()

    aggregate_by_recipient = (
        source.aggregate_by_recipient
        if granularity == "source"
        else granularity == "recipient"
    )
    buckets: dict[str, list[tuple[OfficerJurisdiction, Officer]]] = {}
    for jurisdiction, officer in records:
        if source.route_scope_key == "markaz" and jurisdiction.markaz_id is None:
            continue
        scope_id = (
            jurisdiction.markaz_id
            if source.route_scope_key == "markaz"
            else jurisdiction.tehsil_id
        )
        key = (
            f"recipient:{officer.id}"
            if aggregate_by_recipient
            else f"scope:{officer.id}:{source.route_scope_key}:{scope_id}"
        )
        buckets.setdefault(key, []).append((jurisdiction, officer))

    scope_model = Markaz if source.route_scope_key == "markaz" else Tehsil
    jurisdiction_scope_ids = {
        jurisdiction.markaz_id if source.route_scope_key == "markaz" else jurisdiction.tehsil_id
        for assignments in buckets.values()
        for jurisdiction, _ in assignments
    } - {None}
    scopes_by_id = {
        item.id: item
        for item in session.scalars(
            select(scope_model).where(
                scope_model.id.in_(jurisdiction_scope_ids),
                scope_model.active.is_(True),
            )
        ).all()
        if source.route_scope_key != "markaz" or item.wing_id == source.wing_id
    }
    school_scope_column = School.markaz_id if source.route_scope_key == "markaz" else School.tehsil_id
    schools_by_scope: dict[uuid.UUID, list[School]] = {}
    if scopes_by_id:
        for school in session.scalars(
            select(School).where(
                School.active.is_(True),
                School.wing_id == source.wing_id,
                school_scope_column.in_(scopes_by_id),
            ).order_by(School.name, School.emis)
        ).all():
            scope_id = school.markaz_id if source.route_scope_key == "markaz" else school.tehsil_id
            schools_by_scope.setdefault(scope_id, []).append(school)

    target_jids = {_phone_jid(assignments[0][1].normalized_mobile) for assignments in buckets.values()}
    target_jids.discard("")
    directory_by_jid: dict[str, WhatsAppDirectoryContact] = {}
    if account is not None and target_jids:
        contacts = session.scalars(
            select(WhatsAppDirectoryContact).where(
                WhatsAppDirectoryContact.account_id == account.id,
                or_(
                    WhatsAppDirectoryContact.phone_jid.in_(target_jids),
                    WhatsAppDirectoryContact.canonical_key.in_(target_jids),
                ),
            )
        ).all()
        for contact in contacts:
            if contact.phone_jid in target_jids:
                directory_by_jid.setdefault(contact.phone_jid, contact)
            if contact.canonical_key in target_jids:
                directory_by_jid.setdefault(contact.canonical_key, contact)

    for bucket_key, assignments in buckets.items():
        officer = assignments[0][1]
        scope_ids = [
            assignment.markaz_id if source.route_scope_key == "markaz" else assignment.tehsil_id
            for assignment, _ in assignments
        ]
        scope_ids = sorted({value for value in scope_ids if value is not None}, key=str)
        scopes = [scopes_by_id[value] for value in scope_ids if value in scopes_by_id]
        active_scope_ids = {item.id for item in scopes}
        scope_ids = sorted(active_scope_ids, key=str)
        assignments = [
            pair for pair in assignments
            if (pair[0].markaz_id if source.route_scope_key == "markaz" else pair[0].tehsil_id)
            in active_scope_ids
        ]
        if not assignments:
            continue

        schools_by_id = {
            school.id: school
            for scope_id in active_scope_ids
            for school in schools_by_scope.get(scope_id, [])
        }
        schools = sorted(
            schools_by_id.values(), key=lambda item: (item.name.casefold(), item.emis)
        )
        target_jid = _phone_jid(officer.normalized_mobile)
        directory_contact = directory_by_jid.get(target_jid)
        scope_labels = [item.name for item in sorted(scopes, key=lambda item: item.name.casefold())]
        jurisdiction_ids = sorted({item.id for item, _ in assignments}, key=str)
        payload = {
            "source_id": source.id,
            "officer_id": officer.id,
            "officer_mobile": officer.normalized_mobile,
            "jurisdiction_ids": jurisdiction_ids,
            "scope_ids": scope_ids,
            "scope_labels": scope_labels,
            "school_ids": sorted((school.id for school in schools), key=str),
            "school_emis": sorted(school.emis for school in schools),
        }
        resolved.append(
            ResolvedAudienceMember(
                id=uuid.uuid5(source.id, bucket_key),
                audience_id=source.audience_id,
                target_type="contact",
                target_key=f"master_data:{source.recipient_role}:{bucket_key}",
                route_scope_key=source.route_scope_key,
                route_scope_value=",".join(str(value) for value in scope_ids),
                route_scope_label=", ".join(scope_labels),
                enabled=True,
                source_id=source.id,
                officer_id=officer.id,
                target_jid=target_jid,
                jurisdiction_ids=jurisdiction_ids,
                scope_ids=scope_ids,
                scope_labels=scope_labels,
                school_ids=sorted((school.id for school in schools), key=str),
                school_emis=sorted(school.emis for school in schools),
                source_fingerprint=_digest(payload),
                directory_contact_id=directory_contact.id if directory_contact else None,
            )
        )
    return sorted(resolved, key=lambda item: (item.route_scope_label.casefold(), item.target_key))


def dynamic_audience_fingerprint(
    session: Session, audience_id: uuid.UUID, *,
    granularity: ResolutionGranularity = "source",
) -> str:
    sources = active_audience_sources(session, audience_id)
    members = resolve_dynamic_audience(
        session, audience_id=audience_id, granularity=granularity,
    )
    return _digest(
        {
            "resolution_granularity": granularity,
            "sources": [
                {
                    "id": item.id,
                    "type": item.source_type,
                    "role": item.recipient_role,
                    "wing_id": item.wing_id,
                    "scope": item.route_scope_key,
                    "aggregate": item.aggregate_by_recipient,
                }
                for item in sources
            ],
            "members": [
                {"target_key": item.target_key, "fingerprint": item.source_fingerprint}
                for item in members
            ],
        }
    )

from __future__ import annotations

import uuid

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from sqlmodel import Session, col, func, select

from antidengue_automation.activity_rule_schemas import (
    ActivityRuleInput,
    ActivityRulePreviewInput,
    ActivityRuleUpdate,
)
from antidengue_automation.activity_rules import (
    preview_rule,
    rule_dict,
    rule_input_from_model,
    validate_rule_values,
)
from antidengue_automation.models import AntiDengueActivityRule
from automation_core.database import get_session
from automation_core.time import utcnow
from antidengue_automation.hotspot_routing import (
    configure_hotspot_routes,
    hotspot_routing_status,
    link_hotspot_routes,
)

router = APIRouter(prefix="/api/v1/antidengue/activity-rules", tags=["antidengue-activity-rules"])


@router.get("/routing")
def read_hotspot_routing(session: Session = Depends(get_session)) -> dict:
    return hotspot_routing_status(session)


@router.post("/routing/link")
def reuse_dormant_routes_for_hotspot(
    source_profile_ids: list[uuid.UUID] = Body(default=[]),
    session: Session = Depends(get_session),
) -> dict:
    try:
        return link_hotspot_routes(session, source_profile_ids)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.put("/routing")
def update_hotspot_routing(
    source_profile_ids: list[uuid.UUID] = Body(default=[]),
    session: Session = Depends(get_session),
) -> dict:
    try:
        return configure_hotspot_routes(session, source_profile_ids)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _get_rule(session: Session, rule_id: uuid.UUID) -> AntiDengueActivityRule:
    item = session.get(AntiDengueActivityRule, rule_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Activity rule not found")
    return item


@router.get("")
def list_activity_rules(
    include_archived: bool = Query(False),
    session: Session = Depends(get_session),
) -> dict:
    statement = select(AntiDengueActivityRule)
    if not include_archived:
        statement = statement.where(AntiDengueActivityRule.status != "archived")
    items = session.scalars(
        statement.order_by(col(AntiDengueActivityRule.created_at).desc())
    ).all()
    return {"items": [rule_dict(item) for item in items], "total": len(items)}


@router.post("", status_code=status.HTTP_201_CREATED)
def create_activity_rule(
    data: ActivityRuleInput,
    session: Session = Depends(get_session),
) -> dict:
    try:
        values = validate_rule_values(data.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    item = AntiDengueActivityRule(**values, status="draft", enabled=False)
    session.add(item)
    session.commit()
    session.refresh(item)
    return rule_dict(item)


@router.patch("/{rule_id}")
def update_activity_rule(
    rule_id: uuid.UUID,
    data: ActivityRuleUpdate,
    session: Session = Depends(get_session),
) -> dict:
    item = _get_rule(session, rule_id)
    if item.status != "draft":
        raise HTTPException(status_code=409, detail="Published rules are immutable; create a new version")
    values = rule_input_from_model(item).model_dump()
    values.update(data.model_dump(exclude_unset=True))
    try:
        values = validate_rule_values(values)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    for key, value in values.items():
        if key != "created_by":
            setattr(item, key, value)
    item.updated_at = utcnow()
    session.add(item)
    session.commit()
    session.refresh(item)
    return rule_dict(item)


@router.post("/{rule_id}/versions", status_code=status.HTTP_201_CREATED)
def create_activity_rule_version(
    rule_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    source = _get_rule(session, rule_id)
    existing_draft = session.scalar(
        select(AntiDengueActivityRule).where(
            AntiDengueActivityRule.rule_key == source.rule_key,
            AntiDengueActivityRule.status == "draft",
        )
    )
    if existing_draft is not None:
        return rule_dict(existing_draft)
    latest_version = session.scalar(
        select(func.max(AntiDengueActivityRule.version)).where(
            AntiDengueActivityRule.rule_key == source.rule_key
        )
    ) or source.version
    values = rule_input_from_model(source).model_dump()
    item = AntiDengueActivityRule(
        **values,
        rule_key=source.rule_key,
        version=int(latest_version) + 1,
        status="draft",
        enabled=False,
        supersedes_id=source.id,
    )
    session.add(item)
    session.commit()
    session.refresh(item)
    return rule_dict(item)


@router.post("/{rule_id}/publish")
def publish_activity_rule(
    rule_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    item = _get_rule(session, rule_id)
    if item.status != "draft":
        raise HTTPException(status_code=409, detail="Only draft rules can be published")
    for previous in session.scalars(
        select(AntiDengueActivityRule).where(
            AntiDengueActivityRule.rule_key == item.rule_key,
            AntiDengueActivityRule.status == "published",
        )
    ).all():
        previous.status = "archived"
        previous.enabled = False
        previous.updated_at = utcnow()
        session.add(previous)
    item.status = "published"
    item.enabled = True
    item.published_at = utcnow()
    item.updated_at = utcnow()
    session.add(item)
    session.commit()
    session.refresh(item)
    return rule_dict(item)


@router.post("/{rule_id}/enabled")
def set_activity_rule_enabled(
    rule_id: uuid.UUID,
    enabled: bool,
    session: Session = Depends(get_session),
) -> dict:
    item = _get_rule(session, rule_id)
    if item.status != "published":
        raise HTTPException(status_code=409, detail="Only published rules can be enabled")
    item.enabled = enabled
    item.updated_at = utcnow()
    session.add(item)
    session.commit()
    session.refresh(item)
    return rule_dict(item)


@router.delete("/{rule_id}")
def archive_activity_rule(
    rule_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    item = _get_rule(session, rule_id)
    item.status = "archived"
    item.enabled = False
    item.updated_at = utcnow()
    session.add(item)
    session.commit()
    return {"archived": True, "id": str(item.id)}


@router.post("/previews/evaluate")
def preview_activity_rule(
    data: ActivityRulePreviewInput,
    session: Session = Depends(get_session),
) -> dict:
    try:
        rule_input = ActivityRuleInput(**data.model_dump(exclude={"sample_size"}))
        return preview_rule(session, rule_input, sample_size=data.sample_size)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


__all__ = ["router"]

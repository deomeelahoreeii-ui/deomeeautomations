from __future__ import annotations

import uuid
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlmodel import Session, col, func, select

from antidengue_automation.models import AntiDengueSimpleActivityRule
from antidengue_automation.simple_activity_rules import SimpleActivityRuleInput, preview_rule, rule_dict
from automation_core.database import get_session
from automation_core.time import utcnow
from antidengue_automation.simple_activity_routing import (
    configure_simple_activity_routes, link_simple_activity_routes, simple_activity_routing_status,
)

router = APIRouter(prefix="/api/v1/antidengue/simple-activity-rules", tags=["antidengue-simple-activity-rules"])


@router.get("/routing")
def routing(session: Session = Depends(get_session)) -> dict:
    return simple_activity_routing_status(session)


@router.post("/routing/link")
def link_routing(source_profile_ids: list[uuid.UUID] = [], session: Session = Depends(get_session)) -> dict:
    try: return link_simple_activity_routes(session, source_profile_ids)
    except ValueError as exc: raise HTTPException(422, str(exc)) from exc


@router.put("/routing")
def configure_routing(source_profile_ids: list[uuid.UUID] = [], session: Session = Depends(get_session)) -> dict:
    try: return configure_simple_activity_routes(session, source_profile_ids)
    except ValueError as exc: raise HTTPException(422, str(exc)) from exc


class PreviewInput(SimpleActivityRuleInput):
    sample_size: int = Field(default=20, ge=1, le=100)


class RuleUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=180)
    operator: str | None = Field(default=None, pattern="^(lt|lte)$")
    minimum_seconds: int | None = Field(default=None, ge=1, le=86400)


def _get(session: Session, rule_id: uuid.UUID) -> AntiDengueSimpleActivityRule:
    item = session.get(AntiDengueSimpleActivityRule, rule_id)
    if item is None:
        raise HTTPException(404, "Simple Activity rule not found")
    return item


@router.get("")
def list_rules(include_archived: bool = Query(False), session: Session = Depends(get_session)) -> dict:
    statement = select(AntiDengueSimpleActivityRule)
    if not include_archived:
        statement = statement.where(AntiDengueSimpleActivityRule.status != "archived")
    items = session.scalars(statement.order_by(col(AntiDengueSimpleActivityRule.created_at).desc())).all()
    return {"items": [rule_dict(item) for item in items], "total": len(items)}


@router.post("", status_code=status.HTTP_201_CREATED)
def create_rule(data: SimpleActivityRuleInput, session: Session = Depends(get_session)) -> dict:
    item = AntiDengueSimpleActivityRule(**data.model_dump(), status="draft", enabled=False)
    session.add(item); session.commit(); session.refresh(item)
    return rule_dict(item)


@router.patch("/{rule_id}")
def update_rule(rule_id: uuid.UUID, data: RuleUpdate, session: Session = Depends(get_session)) -> dict:
    item = _get(session, rule_id)
    if item.status != "draft":
        raise HTTPException(409, "Published rules are immutable; create a new version")
    for key, value in data.model_dump(exclude_unset=True).items():
        setattr(item, key, value)
    item.updated_at = utcnow(); session.add(item); session.commit(); session.refresh(item)
    return rule_dict(item)


@router.post("/{rule_id}/versions", status_code=status.HTTP_201_CREATED)
def create_version(rule_id: uuid.UUID, session: Session = Depends(get_session)) -> dict:
    source = _get(session, rule_id)
    draft = session.scalar(select(AntiDengueSimpleActivityRule).where(AntiDengueSimpleActivityRule.rule_key == source.rule_key, AntiDengueSimpleActivityRule.status == "draft"))
    if draft: return rule_dict(draft)
    latest = session.scalar(select(func.max(AntiDengueSimpleActivityRule.version)).where(AntiDengueSimpleActivityRule.rule_key == source.rule_key)) or source.version
    item = AntiDengueSimpleActivityRule(name=source.name, operator=source.operator, minimum_seconds=source.minimum_seconds, timezone=source.timezone, created_by=source.created_by, rule_key=source.rule_key, version=int(latest)+1, supersedes_id=source.id)
    session.add(item); session.commit(); session.refresh(item)
    return rule_dict(item)


@router.post("/{rule_id}/publish")
def publish(rule_id: uuid.UUID, session: Session = Depends(get_session)) -> dict:
    item = _get(session, rule_id)
    if item.status != "draft": raise HTTPException(409, "Only draft rules can be published")
    for previous in session.scalars(select(AntiDengueSimpleActivityRule).where(AntiDengueSimpleActivityRule.rule_key == item.rule_key, AntiDengueSimpleActivityRule.status == "published")).all():
        previous.status = "archived"; previous.enabled = False; previous.updated_at = utcnow(); session.add(previous)
    item.status = "published"; item.enabled = True; item.published_at = utcnow(); item.updated_at = utcnow()
    session.add(item); session.commit(); session.refresh(item)
    return rule_dict(item)


@router.post("/{rule_id}/enabled")
def enable(rule_id: uuid.UUID, enabled: bool, session: Session = Depends(get_session)) -> dict:
    item = _get(session, rule_id)
    if item.status != "published": raise HTTPException(409, "Only published rules can be enabled")
    item.enabled = enabled; item.updated_at = utcnow(); session.add(item); session.commit(); session.refresh(item)
    return rule_dict(item)


@router.delete("/{rule_id}")
def archive(rule_id: uuid.UUID, session: Session = Depends(get_session)) -> dict:
    item = _get(session, rule_id); item.status = "archived"; item.enabled = False; item.updated_at = utcnow()
    session.add(item); session.commit(); return {"archived": True, "id": str(item.id)}


@router.post("/previews/evaluate")
def evaluate(data: PreviewInput, session: Session = Depends(get_session)) -> dict:
    try:
        values = data.model_dump(); sample_size = values.pop("sample_size")
        return preview_rule(session, SimpleActivityRuleInput(**values), sample_size)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


__all__ = ["router"]

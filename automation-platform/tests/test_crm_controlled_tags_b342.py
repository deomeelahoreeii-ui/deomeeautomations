from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.config import Settings
from crm_domain.ai_pipeline import AiPipelineError, CrmAiPipelineService
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintCategory,
    CrmComplaintClassification,
    CrmComplaintTag,
    CrmComplaintTagGroup,
    CrmComplaintTagLink,
    CrmTaxonomySuggestion,
)


ROOT = Path(__file__).resolve().parents[1]
TAG_PAGE = ROOT / "apps/web/src/pages/crm/taxonomy/tags/index.astro"
REVIEW_PAGE = ROOT / "apps/web/src/pages/crm/replies/bulk/classification/index.astro"
TAXONOMY_PAGE = ROOT / "apps/web/src/pages/crm/taxonomy/index.astro"
CSS = ROOT / "apps/web/src/styles/crm.css"
BULK_PAGE = ROOT / "apps/web/src/pages/crm/replies/bulk/index.astro"
MODELS = ROOT / "packages/crm_domain/crm_domain/models.py"
API = ROOT / "packages/crm_domain/crm_domain/ai_pipeline_api.py"
SERVICE = ROOT / "packages/crm_domain/crm_domain/ai_pipeline.py"
MIGRATION = ROOT / "alembic/versions/f5c9e1a3b620_crm_controlled_tag_management.py"


def engine():
    db = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db)
    return db


def settings(tmp_path: Path) -> Settings:
    return Settings(
        database_url="sqlite://",
        artifact_root=tmp_path / "artifacts",
        deomee_root=tmp_path,
    )


def route_paths(routes) -> set[str]:
    paths: set[str] = set()
    pending = list(routes)
    visited: set[int] = set()
    while pending:
        route = pending.pop()
        if id(route) in visited:
            continue
        visited.add(id(route))
        path = getattr(route, "path", None)
        if isinstance(path, str):
            paths.add(path)
        effective = getattr(route, "effective_route_contexts", None)
        if callable(effective):
            for context in effective():
                value = getattr(context, "path", None)
                if isinstance(value, str):
                    paths.add(value)
        nested = getattr(route, "routes", None)
        if nested:
            pending.extend(list(nested))
    return paths


def test_tag_crud_alias_search_merge_and_group_management(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmAiPipelineService(session, settings(tmp_path))
        service.ensure_defaults()
        groups = service.tag_groups()["items"]
        assert {row["slug"] for row in groups} >= {"institution", "issue", "evidence", "routing", "handling", "outcome"}

        new_group = service.create_tag_group(
            name="Programme",
            slug="programme",
            description="Government or school programme context",
            display_order=70,
            actor="test",
        )
        assert new_group["slug"] == "programme"
        updated_group = service.update_tag_group(
            uuid.UUID(new_group["id"]),
            name="Programmes",
            description="Updated group description",
            display_order=75,
            active=True,
            actor="test",
        )
        assert updated_group["name"] == "Programmes"
        assert isinstance(updated_group["created_at"], str)
        assert isinstance(updated_group["updated_at"], str)
        group_audit = session.exec(
            select(ComplaintAuditEvent).where(
                ComplaintAuditEvent.entity_type == "complaint_tag_group",
                ComplaintAuditEvent.entity_id == new_group["id"],
                ComplaintAuditEvent.event_type == "tag_group_updated",
            )
        ).one()
        assert isinstance(group_audit.before_json["created_at"], str)
        assert isinstance(group_audit.after_json["updated_at"], str)

        created = service.create_tag(
            display_name="Building Certificate Fraud",
            slug="building-certificate-fraud",
            group_name="issue",
            description="Advertising or issuing a false building certificate",
            aliases=["fake building certificate", "bogus building approval"],
            active=True,
            ai_available=True,
            actor="test",
        )
        assert created["name"] == "building-certificate-fraud"
        assert isinstance(created["created_at"], str)
        assert isinstance(created["updated_at"], str)
        assert "fake building certificate" in created["aliases"]
        assert service.tags(search="bogus building")["items"][0]["id"] == created["id"]

        with pytest.raises(AiPipelineError, match="same name or alias"):
            service.create_tag(
                display_name="Fake Building Certificate",
                slug=None,
                group_name="issue",
                description="Duplicate alias",
                aliases=[],
                active=True,
                ai_available=True,
                actor="test",
            )

        case = ComplaintCase(complaint_number="104-9900001", state="published", remarks="Test")
        session.add(case)
        session.flush()
        session.add(
            CrmComplaintTagLink(
                complaint_case_id=case.id,
                tag_id=uuid.UUID(created["id"]),
                source="manual",
                created_by="test",
            )
        )
        session.commit()
        target = session.exec(
            select(CrmComplaintTag).where(CrmComplaintTag.name == "advance-fee")
        ).one()
        merged = service.merge_tag(
            uuid.UUID(created["id"]),
            target_tag_id=target.id,
            actor="test",
        )
        assert merged["links_moved"] == 1
        link = session.exec(
            select(CrmComplaintTagLink).where(CrmComplaintTagLink.complaint_case_id == case.id)
        ).one()
        assert link.tag_id == target.id
        source = session.get(CrmComplaintTag, uuid.UUID(created["id"]))
        assert source is not None and source.merged_into_id == target.id and source.active is False
        assert "Building Certificate Fraud" in target.aliases_json


def test_tag_suggestion_can_be_promoted_to_controlled_tag(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmAiPipelineService(session, settings(tmp_path))
        service.ensure_defaults()
        category = ComplaintCategory(name="Administrative", normalized_name="administrative", active=True)
        case = ComplaintCase(complaint_number="104-9900002", state="published", remarks="Complaint text")
        session.add_all([category, case])
        session.flush()
        classification = CrmComplaintClassification(
            complaint_case_id=case.id,
            suggested_category_id=category.id,
            suggested_category_name=category.name,
            status="review_required",
            confidence=0.7,
        )
        session.add(classification)
        session.commit()
        session.refresh(classification)

        suggestion = service.suggest_tag(
            classification.id,
            display_name="Group Insurance Delay",
            group_name="issue",
            reason="No existing tag describes a delayed group insurance claim.",
            actor="reviewer",
        )
        stored = session.get(CrmTaxonomySuggestion, uuid.UUID(suggestion["id"]))
        assert stored is not None and stored.proposed_group_name == "issue"
        promoted = service.create_tag_from_suggestion(
            stored.id,
            display_name=None,
            slug=None,
            group_name=None,
            description="Delayed group insurance claim",
            aliases=["insurance claim delay"],
            active=True,
            ai_available=True,
            actor="taxonomy-admin",
        )
        assert promoted["status"] == "approved"
        assert promoted["tag"]["name"] == "group-insurance-delay"
        session.refresh(stored)
        assert stored.resolved_tag_id == uuid.UUID(promoted["tag"]["id"])


def test_b342_ui_replaces_multi_select_and_exposes_manual_tag_management() -> None:
    tag_page = TAG_PAGE.read_text(encoding="utf-8")
    review = REVIEW_PAGE.read_text(encoding="utf-8")
    taxonomy = TAXONOMY_PAGE.read_text(encoding="utf-8")
    css = CSS.read_text(encoding="utf-8")
    bulk = BULK_PAGE.read_text(encoding="utf-8")
    for token in (
        "Controlled Complaint Tags",
        "controlled-tags-table",
        "+ New tag",
        "Manage groups",
        "Aliases",
        "Merge tag",
        "/api/v1/crm/ai-pipeline/tags",
        "/api/v1/crm/ai-pipeline/tag-groups",
    ):
        assert token in tag_page
    for token in (
        "selected-tag-chips",
        "tag-picker-search",
        "tag-picker-group",
        "tag-picker-scope",
        "Create controlled tag",
        "Suggest a new tag",
        "classification-tag-option",
        "classification-review/${current.id}/tag-suggestions",
    ):
        assert token in review
    assert 'href="/crm/taxonomy/tags/"' in taxonomy
    assert 'id="copy-classification-prompt"' in bulk
    assert "classification-results.csv" in bulk
    assert "multiple size=" not in review
    for token in (
        ".classification-tag-picker",
        ".selected-tag-chip",
        ".classification-tag-group",
        ".tag-groups-layout",
        ".tag-toolbar-actions",
    ):
        assert token in css


def test_b342_models_api_migration_and_runtime_routes() -> None:
    models = MODELS.read_text(encoding="utf-8")
    api = API.read_text(encoding="utf-8")
    service = SERVICE.read_text(encoding="utf-8")
    migration = MIGRATION.read_text(encoding="utf-8")
    assert "class CrmComplaintTagGroup" in models
    for token in ("display_name", "aliases_json", "ai_available", "merged_into_id"):
        assert token in models
    for token in (
        '"/tag-statistics"',
        '"/tag-groups"',
        '"/tags"',
        '"/tags/{tag_id}/merge"',
        '"/classification-review/{classification_id}/tag-suggestions"',
        '"/taxonomy-suggestions/{suggestion_id}/create-tag"',
    ):
        assert token in api
    for token in ("create_tag_group", "create_tag", "update_tag", "merge_tag", "suggest_tag", "create_tag_from_suggestion"):
        assert f"def {token}" in service
    assert 'revision: str = "f5c9e1a3b620"' in migration
    assert 'down_revision: Union[str, None] = "f3b7d9e1a408"' in migration
    assert '"crm_complaint_tag_groups"' in migration
    assert 'op.drop_table("crm_complaint_tag_groups")' in migration

    paths = route_paths(app.routes)
    assert "/api/v1/crm/ai-pipeline/tags" in paths
    assert "/api/v1/crm/ai-pipeline/tag-groups" in paths
    assert "/api/v1/crm/ai-pipeline/tags/{tag_id}/merge" in paths
    assert "/api/v1/crm/ai-pipeline/classification-review/{classification_id}/tag-suggestions" in paths

from __future__ import annotations

import uuid
import zipfile
from pathlib import Path

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from automation_api.main import app
from crm_domain.ai_pipeline import CrmAiPipelineService
from crm_domain.bulk_operations import CrmBulkOperationService
from crm_domain.models import (
    ComplaintCase,
    ComplaintCategory,
    ComplaintReply,
    ComplaintReplyRevision,
    ComplaintSubcategory,
    CrmComplaintClassification,
    CrmPromptProfileVersion,
    CrmTaxonomySuggestion,
)


ROOT = Path(__file__).resolve().parents[1]
BULK_PAGE = ROOT / "apps/web/src/pages/crm/replies/bulk/index.astro"
REVIEW_PAGE = ROOT / "apps/web/src/pages/crm/replies/bulk/classification/index.astro"
PROMPT_PAGE = ROOT / "apps/web/src/pages/crm/replies/bulk/prompt-profiles/index.astro"
MAIN = ROOT / "apps/api/automation_api/main.py"
MODELS = ROOT / "packages/crm_domain/crm_domain/models.py"
PIPELINE = ROOT / "packages/crm_domain/crm_domain/ai_pipeline.py"
MIGRATION = ROOT / "alembic/versions/f3b7d9e1a408_crm_ai_classification_context_pipeline.py"
CRM_CSS = ROOT / "apps/web/src/styles/crm.css"


def _runtime_route_paths(routes) -> set[str]:
    """Collect paths across flat and nested FastAPI route registries.

    FastAPI 0.139 may retain included routers as lightweight wrapper objects in
    ``app.routes``.  Older versions flatten those routes immediately.  Runtime
    contract tests must support both representations instead of assuming every
    registry entry has a direct ``path`` attribute.
    """

    paths: set[str] = set()
    pending = list(routes)
    visited: set[int] = set()
    while pending:
        route = pending.pop()
        identity = id(route)
        if identity in visited:
            continue
        visited.add(identity)
        path = getattr(route, "path", None)
        if isinstance(path, str):
            paths.add(path)
        effective_contexts = getattr(route, "effective_route_contexts", None)
        if callable(effective_contexts):
            for context in effective_contexts():
                effective_path = getattr(context, "path", None)
                if isinstance(effective_path, str):
                    paths.add(effective_path)
        nested = getattr(route, "routes", None)
        if nested:
            pending.extend(list(nested))
    return paths


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


def seed(session: Session):
    fees = ComplaintCategory(
        name="Fees",
        normalized_name="fees",
        description="School fee complaints",
        reply_guidance="Keep the reply evidence-based and concise.",
        policy_notes="Regular monthly fee may be received; advance lump-sum recovery is not permissible.",
        active=True,
    )
    admin = ComplaintCategory(
        name="Administrative",
        normalized_name="administrative",
        active=True,
    )
    session.add_all([fees, admin])
    session.flush()
    vacation = ComplaintSubcategory(
        category_id=fees.id,
        name="Summer Vacation Fee",
        normalized_name="summer vacation fee",
        reply_guidance="Distinguish regular monthly fee from advance fee.",
        policy_notes="Ask for documentary evidence when an advance demand is alleged.",
        active=True,
    )
    timing = ComplaintSubcategory(
        category_id=admin.id,
        name="School Timing",
        normalized_name="school timing",
        active=True,
    )
    session.add_all([vacation, timing])
    session.flush()
    first = ComplaintCase(
        complaint_number="104-9100001",
        state="published",
        remarks="Private school is demanding three months summer vacation fee in advance without a voucher.",
    )
    second = ComplaintCase(
        complaint_number="104-9100002",
        state="published",
        remarks="School has demanded an admission charge that does not fit the listed fee options.",
    )
    historical = ComplaintCase(
        complaint_number="104-9000001",
        state="published",
        remarks="Private school demanded June and July tuition fee together before summer vacation.",
        category="Fees",
        sub_category="Summer Vacation Fee",
        category_id=fees.id,
        sub_category_id=vacation.id,
        frappe_ai_eligible=True,
    )
    session.add_all([first, second, historical])
    session.flush()
    historical_reply = ComplaintReply(
        complaint_case_id=historical.id,
        reply_text="Historical imported reply.",
        source_filename="historical.csv",
        source_row=2,
    )
    session.add(historical_reply)
    revision = ComplaintReplyRevision(
        complaint_case_id=historical.id,
        reply_text=(
            "Respected Worthy Chief Executive Officer (DEA),\n\n"
            "The advance fee allegation has been reviewed on the basis of available record.\n\n"
            "Submitted for kind perusal and further necessary action, please."
        ),
        content_hash="b34-historical-approved-reply",
        source_reference="HD-EXAMPLE",
        approval_status="Approved",
        is_current=True,
    )
    session.add(revision)
    session.commit()
    for row in (fees, admin, vacation, timing, first, second, historical, revision):
        session.refresh(row)
    return fees, admin, vacation, timing, first, second, historical, revision


def test_classification_then_contextual_reply_package_preserves_lineage(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        fees, _, vacation, _, first, second, _, revision = seed(session)
        service = CrmAiPipelineService(session, settings(tmp_path))

        export = service.create_classification_export_batch(scope="unclassified")
        assert export["operation_type"] == "classification_export"
        assert export["total_items"] == 2
        artifact = next(item for item in export["artifacts"] if item["kind"] == "classification_package")
        _, path = service.bulk.artifact_path(uuid.UUID(artifact["id"]))
        with zipfile.ZipFile(path) as archive:
            assert {
                "01-complaints-to-classify.csv",
                "02-existing-taxonomy.csv",
                "03-existing-tags.csv",
                "04-classification-instructions.txt",
                "05-classification-output-template.csv",
                "06-completed-classification-sample.csv",
                "batch-summary.json",
            } <= set(archive.namelist())
            assert "Prefer an existing active category" in archive.read(
                "04-classification-instructions.txt"
            ).decode()

        csv_content = (
            "Complaint Number,Category,Subcategory,Tags,Confidence,Needs New Taxonomy,Proposed Name,Reason\n"
            '104-9100001,Fees,Summer Vacation Fee,"private-school;advance-fee;no-evidence",0.96,No,,Advance vacation fee complaint\n'
            '104-9100002,Fees,Admission Fee,"private-school",0.72,Yes,Admission Fee,No existing subcategory fits\n'
        ).encode()
        validation = service.validate_classification_import(
            content=csv_content,
            filename="classified.csv",
            parent_batch_id=uuid.UUID(export["id"]),
        )
        assert validation["operation_type"] == "classification_import"
        assert validation["status"] == "ready"
        assert validation["valid_items"] == 2
        committed = service.commit_classification_import(uuid.UUID(validation["id"]))
        assert committed["commit_summary"] == {
            "auto_accepted": 1,
            "review_required": 1,
        }
        session.refresh(first)
        assert first.category_id == fees.id
        assert first.sub_category_id == vacation.id
        rows = session.exec(select(CrmComplaintClassification)).all()
        assert {row.status for row in rows} == {"auto_accepted", "review_required"}
        suggestion = session.exec(select(CrmTaxonomySuggestion)).one()
        assert suggestion.proposal_type == "subcategory"
        assert suggestion.proposed_name == "Admission Fee"

        review = service.review_queue()
        assert review["total"] == 1
        pending = review["items"][0]
        service.resolve_classification(
            uuid.UUID(pending["id"]),
            category_id=fees.id,
            subcategory_id=vacation.id,
            tag_ids=[],
            decision="approve",
            actor="test-reviewer",
        )
        session.refresh(second)
        assert second.category_id == fees.id
        assert second.sub_category_id == vacation.id
        session.refresh(suggestion)
        assert suggestion.status == "merged"
        assert service.taxonomy_suggestions(status="pending")["total"] == 0

        context = service.create_reply_context_export(
            scope="classification_batch",
            parent_batch_id=uuid.UUID(validation["id"]),
            examples_limit=3,
            redact_personal_data=True,
        )
        assert context["operation_type"] == "reply_context_export"
        assert context["total_items"] == 2
        context_artifact = next(
            item for item in context["artifacts"] if item["kind"] == "reply_context_package"
        )
        _, context_path = service.bulk.artifact_path(uuid.UUID(context_artifact["id"]))
        with zipfile.ZipFile(context_path) as archive:
            assert {
                "01-new-complaints.csv",
                "02-reply-template.csv",
                "03-active-reply-sop.txt",
                "04-taxonomy-context.csv",
                "05-approved-historical-examples.csv",
                "06-chatgpt-reply-instructions.txt",
                "07-example-completed-replies.csv",
                "batch-manifest.csv",
                "batch-summary.json",
            } <= set(archive.namelist())
            sop = archive.read("03-active-reply-sop.txt").decode()
            assert "CM COMPLAINT REPLY MASTER PROMPT + SOP" in sop
            assert "Respected Worthy Chief Executive Officer (DEA)," in sop
            examples = archive.read("05-approved-historical-examples.csv").decode("utf-8-sig")
            assert "104-9000001" in examples
            assert revision.reply_text in examples

        prompt_version = session.exec(select(CrmPromptProfileVersion)).one()
        assert prompt_version.version_label == "2026-06-20"
        assert prompt_version.is_active is True

        bulk = CrmBulkOperationService(session, settings(tmp_path))
        reply_import = bulk.validate_import_batch(
            content=(
                "Complaint Number,Reply\n"
                '104-9100001,"Respected Worthy Chief Executive Officer (DEA), reply one."\n'
                '104-9100002,"Respected Worthy Chief Executive Officer (DEA), reply two."\n'
            ).encode(),
            filename="replies.csv",
            parent_batch_id=uuid.UUID(context["id"]),
        )
        assert reply_import["parent"]["id"] == context["id"]


def test_b34_ui_exposes_five_stage_human_controlled_workflow() -> None:
    bulk = BULK_PAGE.read_text(encoding="utf-8")
    review = REVIEW_PAGE.read_text(encoding="utf-8")
    prompt = PROMPT_PAGE.read_text(encoding="utf-8")
    css = CRM_CSS.read_text(encoding="utf-8")
    for token in (
        "Classify new complaints",
        "Accept confident matches, review exceptions",
        "Prepare a category-aware ChatGPT reply package",
        "Validate before changing replies",
        "Generate letters from a controlled source",
        "classification-exports",
        "classification-imports/validate",
        "reply-context-exports",
        "redact-personal-data",
        "active-prompt-version",
    ):
        assert token in bulk
    for token in (
        "classification-review-table",
        "Approve classification",
        "Reject suggestion",
        "taxonomy-options",
        "classification-review/${current.id}/resolve",
        "taxonomy-suggestions",
    ):
        assert token in review
    for token in (
        "CM Complaint Reply Master Prompt + SOP",
        "Create a new immutable version",
        "prompt-profiles",
        "prompt-versions/${id}/activate",
    ):
        assert token in prompt
    assert ".ai-pipeline-steps" in css
    assert ".classification-review-dialog" in css
    assert ".prompt-profile-grid" in css


def test_b34_router_models_and_reversible_migration_are_registered() -> None:
    main = MAIN.read_text(encoding="utf-8")
    models = MODELS.read_text(encoding="utf-8")
    pipeline = PIPELINE.read_text(encoding="utf-8")
    migration = MIGRATION.read_text(encoding="utf-8")
    assert "app.include_router(crm_ai_pipeline_router)" in main
    for model in (
        "CrmPromptProfile",
        "CrmPromptProfileVersion",
        "CrmComplaintTag",
        "CrmComplaintTagLink",
        "CrmComplaintClassification",
        "CrmTaxonomySuggestion",
        "CrmReplyContextExample",
    ):
        assert f"class {model}" in models
    assert "DEFAULT_REPLY_SOP_CONTENT" in pipeline
    assert "classification_export" in pipeline
    assert "reply_context_export" in pipeline
    assert 'revision: str = "f3b7d9e1a408"' in migration
    assert 'down_revision: Union[str, None] = "f1a5c7e9b306"' in migration
    for table in (
        "crm_prompt_profiles",
        "crm_prompt_profile_versions",
        "crm_complaint_tags",
        "crm_complaint_tag_links",
        "crm_complaint_classifications",
        "crm_taxonomy_suggestions",
        "crm_reply_context_examples",
    ):
        assert f'"{table}"' in migration
        assert f'op.drop_table("{table}")' in migration
    assert "ck_crm_bulk_operation_batches_type" in migration
    assert "reply_context_export" in migration


def test_b34_runtime_router_exposes_pipeline_and_suggestion_endpoints() -> None:
    paths = _runtime_route_paths(app.routes)
    assert "/api/v1/crm/ai-pipeline/statistics" in paths
    assert "/api/v1/crm/ai-pipeline/classification-exports" in paths
    assert "/api/v1/crm/ai-pipeline/classification-imports/validate" in paths
    assert "/api/v1/crm/ai-pipeline/classification-review" in paths
    assert "/api/v1/crm/ai-pipeline/taxonomy-suggestions" in paths
    assert "/api/v1/crm/ai-pipeline/reply-context-exports" in paths
    assert "/api/v1/crm/ai-pipeline/prompt-profiles" in paths

from __future__ import annotations

import io
import uuid
from datetime import date
from pathlib import Path
from zipfile import ZipFile

from PIL import Image as PillowImage
from reportlab.pdfgen import canvas
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintReplyRevision,
)
from crm_domain.official_letters import (
    OfficialLetterService,
    _logical_paragraphs,
)
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatchItem,
    WhatsAppInboundProcessingItem,
)

ROOT = Path(__file__).resolve().parents[1]
QUEUE = ROOT / "apps/web/src/pages/crm/replies/index.astro"
EDITOR = ROOT / "apps/web/src/pages/crm/replies/[id].astro"
PREPARE = ROOT / "apps/web/src/pages/crm/replies/[id]/official-letter.astro"
CSS = ROOT / "apps/web/src/styles/crm.css"
API = ROOT / "packages/crm_domain/crm_domain/official_letters_api.py"


def engine():
    db = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(db)
    return db


def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        database_url="sqlite://",
        artifact_root=tmp_path / "artifacts",
        deomee_root=tmp_path,
        object_storage_enabled=False,
    )


def source_pdf(path: Path, label: str) -> None:
    pdf = canvas.Canvas(str(path))
    pdf.drawString(72, 760, label)
    pdf.showPage()
    pdf.save()


def add_source_document(
    session: Session,
    case: ComplaintCase,
    source: Path,
    *,
    role: str,
    mime_type: str,
) -> None:
    message_id = uuid.uuid4()
    attachment = WhatsAppInboundAttachment(
        message_id=message_id,
        media_kind="document" if mime_type == "application/pdf" else "image",
        message_key=f"message-{uuid.uuid4()}",
        original_filename=source.name,
        mime_type=mime_type,
        stored_path=str(source),
        download_status="downloaded",
    )
    session.add(attachment)
    session.flush()
    batch_item = WhatsAppInboundBatchItem(
        batch_id=uuid.uuid4(),
        attachment_id=attachment.id,
        message_id=message_id,
        original_filename=source.name,
        mime_type=mime_type,
        status="stored",
    )
    session.add(batch_item)
    session.flush()
    processing = WhatsAppInboundProcessingItem(
        run_id=uuid.uuid4(),
        batch_item_id=batch_item.id,
        attachment_id=attachment.id,
        status="approved",
        review_status="approved",
        primary_category="crm_complaint" if role == "main_complaint" else "supporting_document",
    )
    session.add(processing)
    session.flush()
    document = ComplaintDocument(
        complaint_case_id=case.id,
        source_processing_item_id=processing.id,
        source_attachment_id=attachment.id,
        original_filename=source.name,
        mime_type=mime_type,
        role=role,
        source_sha256=f"{role}-{source.stat().st_size}",
        review_state="accepted",
    )
    session.add(document)
    session.flush()
    session.add(
        ComplaintDocumentCaseLink(
            complaint_document_id=document.id,
            complaint_case_id=case.id,
            role=role,
            review_state="accepted",
            confidence=1.0,
            source_locator=f"test:{document.id}",
        )
    )


def seed(session: Session) -> tuple[ComplaintCase, ComplaintReplyRevision]:
    case = ComplaintCase(
        complaint_number="104-6747651",
        state="published",
        remarks="Original complaint details for the complete official record.",
        category="Fee",
        sub_category="Summer Vacations",
    )
    session.add(case)
    session.flush()
    revision = ComplaintReplyRevision(
        complaint_case_id=case.id,
        reply_text=(
            "Respected Worthy Chief Executive Officer (DEA),\n\n\n"
            "Reference to the complaint received through CM Complaint Portal.\n\n"
            "The matter stands addressed on the available record.\n\n\n"
            "Submitted for kind perusal and further necessary action, please."
        ),
        content_hash="c" * 64,
        approval_status="Approved",
        source_reference="HD-0029",
    )
    session.add(revision)
    session.commit()
    session.refresh(case)
    session.refresh(revision)
    return case, revision


def test_logical_reply_paragraphs_remove_extra_empty_spacing() -> None:
    assert _logical_paragraphs("First line\nwrapped line\n\n\nSecond paragraph\n") == [
        "First line wrapped line",
        "Second paragraph",
    ]


def test_odt_signature_has_safe_transparent_margin_and_compact_reply(tmp_path: Path) -> None:
    with Session(engine()) as session:
        case, revision = seed(session)
        service = OfficialLetterService(session, settings(tmp_path))
        letter = service.generate(
            case.id,
            letter_number="1511/PMDU/CRM",
            letter_date=date(2026, 7, 21),
            recipient_name="The Chief Executive Officer (DEA),",
            recipient_location="Lahore",
            subject_prefix="COMPLAINT NO.",
            cc_entries="Office Record.",
            template_id=None,
            signature_profile_id=None,
            reply_revision_id=revision.id,
            actor="test",
            finalize=True,
        )
        odt = next(item for item in letter["artifacts"] if item["kind"] == "odt")
        odt_path = service.artifact_path(uuid.UUID(odt["id"]))[1]
        template = session.get(type(service._resolve_template(None)), uuid.UUID(letter["template_id"]))
        assert template is not None
        with ZipFile(odt_path) as archive:
            content = archive.read("content.xml").decode("utf-8")
            signature = archive.read(template.signature_target_path)
        assert 'fo:margin-bottom="0.035in"' in content
        assert 'fo:line-height="110%"' in content
        assert "{{approved_reply}}" not in content
        with PillowImage.open(io.BytesIO(signature)) as image:
            image.load()
            rgba = image.convert("RGBA")
            assert rgba.getpixel((0, 0))[3] == 0
            assert rgba.getpixel((rgba.width - 1, rgba.height - 1))[3] == 0


def test_complete_pdf_orders_letter_then_complaint_then_attachments(tmp_path: Path) -> None:
    with Session(engine()) as session:
        case, revision = seed(session)
        main = tmp_path / "main-complaint.pdf"
        source_pdf(main, "MAIN COMPLAINT")
        attachment = tmp_path / "attachment.png"
        PillowImage.new("RGB", (600, 300), "white").save(attachment)
        add_source_document(session, case, main, role="main_complaint", mime_type="application/pdf")
        add_source_document(session, case, attachment, role="attachment", mime_type="image/png")
        session.commit()

        service = OfficialLetterService(session, settings(tmp_path))
        prepared = service.prepare_case(case.id)
        assert [item["role"] for item in prepared["source_documents"]] == [
            "main_complaint",
            "attachment",
        ]
        letter = service.generate(
            case.id,
            letter_number="1511/PMDU/CRM",
            letter_date=date(2026, 7, 21),
            recipient_name="The Chief Executive Officer (DEA),",
            recipient_location="Lahore",
            subject_prefix="COMPLAINT NO.",
            cc_entries="Office Record.",
            template_id=None,
            signature_profile_id=None,
            reply_revision_id=revision.id,
            actor="test",
            finalize=True,
        )
        composed = service.compose_complete_pdf(uuid.UUID(letter["id"]), actor="test")
        complete = next(item for item in composed["artifacts"] if item["kind"] == "complete_pdf")
        _, path = service.artifact_path(uuid.UUID(complete["id"]))
        assert path.read_bytes().startswith(b"%PDF")
        assert composed["complete_pdf"]["page_count"] >= 3
        assert composed["complete_pdf"]["order"] == [
            "official_letter",
            "main_complaint",
            "attachment",
        ]
        assert composed["complete_pdf"]["skipped_documents"] == []


def test_b352_ui_and_api_contracts() -> None:
    queue = QUEUE.read_text()
    editor = EDITOR.read_text()
    prepare = PREPARE.read_text()
    css = CSS.read_text()
    api = API.read_text()
    assert 'header: "Complaint text"' not in queue
    assert 'button.addEventListener("click", () => void openComplaintPreview(item))' in queue
    assert 'editor.href = editorUrl(item)' in queue
    assert "Build Complete PDF" in prepare
    assert "Download Complete PDF" in prepare
    assert "/complete-pdf" in prepare
    assert '@router.post("/letters/{letter_id}/complete-pdf")' in api
    assert 'record.kind not in {"pdf", "complete_pdf"}' in api
    assert 'id="editor-more-actions"' in editor
    assert "closeMoreActions" in editor
    assert ".editor-more-actions .crm-action-popover" in css
    assert "bottom: calc(100% + 7px)" in css

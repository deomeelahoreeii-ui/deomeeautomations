from __future__ import annotations

import tempfile
import uuid
from datetime import UTC, timedelta
from pathlib import Path

from sqlmodel import Session, select

from automation_core.celery_app import celery_app
from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.object_storage import S3ObjectStorage
from automation_core.time import utcnow
from crm_domain.fields import paperless_custom_field_values
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    PaperlessPublication,
)
from whatsapp_gateway.inbound.processing_support import (
    materialize_source,
    paperless_client,
)
from whatsapp_gateway.models import WhatsAppInboundProcessingItem


def _case_values(case: ComplaintCase) -> dict[str, object]:
    return {
        "complaint_number": case.complaint_number,
        "revision": case.version,
        "source": "CRM Portal",
        "direction": "Incoming",
        "status": "Pending",
        "complainant_name": case.complainant_name,
        "complainant_mobile": case.complainant_mobile,
        "complainant_cnic": case.complainant_cnic,
        "complainant_address": case.complainant_address,
        "district": case.district,
        "tehsil": case.tehsil,
        "department": case.department,
        "category": case.category,
        "sub_category": case.sub_category,
        "remarks": case.remarks,
    }


def _paperless_value(values: dict[str, str], *names: str) -> str | None:
    folded = {name.casefold(): value.strip() for name, value in values.items() if value.strip()}
    return next((folded[name.casefold()] for name in names if name.casefold() in folded), None)


def _validate_crm_pending_contract(metadata) -> None:
    """Fail before upload unless the live CRM Pending view contract is available."""

    required_options = {
        "Source": ["CRM Portal"],
        "Status": ["Pending"],
        "Document Role": ["Main Complaint", "Complaint Details"],
        "Direction": ["Incoming"],
    }
    missing: list[str] = []
    for field_name in (
        "Complaint Number",
        "Revision",
        "Remarks",
        "Parent Case",
        *required_options,
    ):
        field_id = metadata.field_ids_by_name.get(field_name.casefold())
        if field_id is None:
            missing.append(f"custom field {field_name!r}")
    for field_name, option_labels in required_options.items():
        field_id = metadata.field_ids_by_name.get(field_name.casefold())
        if field_id is None:
            continue
        labels = metadata.option_labels_by_field_id.get(str(field_id), {})
        available = {label.casefold() for label in labels.values()}
        for option_label in option_labels:
            if option_label.casefold() not in available:
                missing.append(f"{field_name!r} option {option_label!r}")
    if metadata.complaint_type_id is None:
        missing.append("document type 'Complaint'")
    if metadata.attachment_type_id is None:
        missing.append("document type 'Attachment'")
    if metadata.correspondent_id is None:
        missing.append("correspondent 'CEO, (DEA), Lahore'")
    if missing:
        raise RuntimeError(
            "Paperless is missing the CRM Pending publication contract: "
            + ", ".join(missing)
        )


@celery_app.task(name="crm_domain.publish_complaint_case", soft_time_limit=900, time_limit=960)
def publish_complaint_case(case_id: str) -> dict[str, object]:
    case_uuid = uuid.UUID(case_id)
    settings = get_settings()
    storage = S3ObjectStorage(settings)
    client = paperless_client(settings)
    metadata = client.connect()
    _validate_crm_pending_contract(metadata)
    with Session(engine) as session:
        case = session.get(ComplaintCase, case_uuid)
        if case is None:
            raise ValueError("Complaint case was not found")
        if case.state not in {"fresh", "publishing", "published"}:
            raise ValueError("Complaint case must be approved as fresh before publishing")
        publications = list(
            session.exec(
                select(PaperlessPublication)
                .where(PaperlessPublication.complaint_case_id == case.id)
                .order_by(PaperlessPublication.created_at)
            ).all()
        )
        case.state = "publishing"
        case.updated_at = utcnow()
        session.add(case)
        session.commit()

    main_document_id: int | None = None
    completed = 0
    with tempfile.TemporaryDirectory(prefix=f"crm-publish-{case_id}-") as temp_name:
        for publication_id in [item.id for item in publications]:
            with Session(engine) as session:
                publication = session.get(PaperlessPublication, publication_id)
                case = session.get(ComplaintCase, case_uuid)
                document = (
                    session.get(ComplaintDocument, publication.complaint_document_id)
                    if publication
                    else None
                )
                if publication is None or case is None or document is None:
                    continue
                publication_role = str(
                    publication.intended_fields_json.get("role") or document.role
                )
                if publication.state == "completed" and publication.paperless_document_id:
                    if publication_role == "main_complaint":
                        main_document_id = publication.paperless_document_id
                    completed += 1
                    continue
                publication_updated_at = publication.updated_at
                if publication_updated_at.tzinfo is None:
                    publication_updated_at = publication_updated_at.replace(tzinfo=UTC)
                if (
                    publication.state == "uploading"
                    and publication_updated_at > utcnow() - timedelta(minutes=20)
                ):
                    raise RuntimeError(
                        f"Publication {publication.id} is already owned by another active worker"
                    )
                if publication_role != "main_complaint" and main_document_id is None:
                    raise RuntimeError("The main complaint must be published before its attachments")
                item = session.get(WhatsAppInboundProcessingItem, document.source_processing_item_id)
                if item is None:
                    raise RuntimeError("The source processing item is unavailable")
                suffix = Path(document.original_filename or "evidence.bin").suffix
                destination = Path(temp_name) / f"{document.id}{suffix}"
                publication.state = "uploading"
                publication.attempts += 1
                publication.updated_at = utcnow()
                session.add(publication)
                session.commit()
                try:
                    source = materialize_source(
                        session=session,
                        item=item,
                        destination=destination,
                        settings=settings,
                        storage=storage,
                    )
                    is_main = publication_role == "main_complaint"
                    role_label = "Main Complaint" if is_main else "Complaint Details"
                    fields = paperless_custom_field_values(
                        _case_values(case),
                        document_role=role_label,
                        parent_document_id=main_document_id,
                    )
                    document_type_id = (
                        metadata.complaint_type_id if is_main else metadata.attachment_type_id
                    )
                    if document_type_id is None:
                        raise RuntimeError(f"Paperless document type for {role_label} is unavailable")
                    title = (
                        f"CRM - {case.complaint_number} - Main Complaint - v{case.version}"
                        if is_main
                        else f"CRM - {case.complaint_number} - Complaint Details - {document.original_filename or document.id}"
                    )
                    paperless_id = publication.paperless_document_id
                    if paperless_id is None:
                        task_id, paperless_id = client.post_document(
                            source,
                            title=title,
                            document_type_id=document_type_id,
                            correspondent_id=metadata.correspondent_id,
                        )
                        publication.paperless_task_id = task_id
                        publication.paperless_document_id = paperless_id
                        publication.state = "uploaded"
                        publication.updated_at = utcnow()
                        session.add(publication)
                        session.commit()
                    client.update_document_metadata(
                        paperless_id,
                        title=title,
                        document_type_id=document_type_id,
                        correspondent_id=metadata.correspondent_id,
                        custom_fields=fields,
                    )
                    publication.state = "completed"
                    publication.last_error = None
                    document.paperless_document_id = paperless_id
                    if is_main:
                        main_document_id = paperless_id
                        case.canonical_paperless_document_id = paperless_id
                    publication.updated_at = utcnow()
                    document.updated_at = utcnow()
                    session.add(publication)
                    session.add(document)
                    session.add(case)
                    session.commit()
                    completed += 1
                except Exception as exc:
                    publication.state = "failed"
                    publication.last_error = str(exc)
                    publication.updated_at = utcnow()
                    case.state = "fresh"
                    case.updated_at = utcnow()
                    session.add(publication)
                    session.add(case)
                    session.commit()
                    raise

    with Session(engine) as session:
        case = session.get(ComplaintCase, case_uuid)
        assert case is not None
        case.state = "published"
        case.updated_at = utcnow()
        session.add(case)
        session.commit()
    return {"case_id": case_id, "status": "published", "documents": completed}


@celery_app.task(name="crm_domain.backfill_paperless_cases", soft_time_limit=1800, time_limit=1860)
def backfill_paperless_cases(limit: int | None = None) -> dict[str, int]:
    client = paperless_client(get_settings())
    client.connect()
    candidates = client.fetch_complaint_index(limit=limit)
    created = 0
    updated = 0
    with Session(engine) as session:
        for candidate in candidates:
            if not candidate.complaint_number:
                continue
            case = session.exec(
                select(ComplaintCase).where(
                    ComplaintCase.source_system == "crm_portal",
                    ComplaintCase.complaint_number == candidate.complaint_number,
                )
            ).first()
            if case is None:
                case = ComplaintCase(
                    source_system="crm_portal",
                    complaint_number=candidate.complaint_number,
                    state="existing",
                    canonical_paperless_document_id=candidate.document_id,
                    remarks=candidate.remarks_text or None,
                    complainant_name=_paperless_value(
                        candidate.fields, "Complainant Name", "Person Name", "Applicant Name"
                    ),
                    complainant_mobile=_paperless_value(
                        candidate.fields, "Complainant Mobile Number", "Mobile No", "Mobile Number"
                    ),
                    complainant_cnic=_paperless_value(
                        candidate.fields, "Complainant CNIC", "CNIC No", "CNIC"
                    ),
                    complainant_address=_paperless_value(
                        candidate.fields, "Complainant Address", "Person Address", "Applicant Address"
                    ),
                    district=_paperless_value(candidate.fields, "District", "Complaint District"),
                    tehsil=_paperless_value(candidate.fields, "Tehsil"),
                    department=_paperless_value(candidate.fields, "Department"),
                    category=_paperless_value(candidate.fields, "Complaint Category", "Category"),
                    sub_category=_paperless_value(
                        candidate.fields, "Complaint Sub-Category", "Sub Category"
                    ),
                )
                session.add(case)
                created += 1
            else:
                case.state = "existing"
                case.canonical_paperless_document_id = candidate.document_id
                for attribute, names in {
                    "complainant_name": ("Complainant Name", "Person Name", "Applicant Name"),
                    "complainant_mobile": ("Complainant Mobile Number", "Mobile No", "Mobile Number"),
                    "complainant_cnic": ("Complainant CNIC", "CNIC No", "CNIC"),
                    "complainant_address": ("Complainant Address", "Person Address", "Applicant Address"),
                    "district": ("District", "Complaint District"),
                    "tehsil": ("Tehsil",),
                    "department": ("Department",),
                    "category": ("Complaint Category", "Category"),
                    "sub_category": ("Complaint Sub-Category", "Sub Category"),
                }.items():
                    if not getattr(case, attribute):
                        setattr(case, attribute, _paperless_value(candidate.fields, *names))
                if not case.remarks and candidate.remarks_text:
                    case.remarks = candidate.remarks_text
                case.updated_at = utcnow()
                session.add(case)
                updated += 1
        session.commit()
    return {"created": created, "updated": updated}

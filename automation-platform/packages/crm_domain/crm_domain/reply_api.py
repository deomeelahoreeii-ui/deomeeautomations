from __future__ import annotations

import csv
import io
import uuid

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from sqlalchemy import func, or_
from sqlmodel import Session, select

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from crm_domain.bulk_operations import (
    BulkOperationError,
    BulkOperationValidationError,
    CrmBulkOperationService,
)
from crm_domain.models import ComplaintCase, ComplaintReply


router = APIRouter(prefix="/api/v1/crm/replies", tags=["crm-replies"])
MAX_REPLY_FILE_BYTES = 5 * 1024 * 1024


def _published_statement():
    return (
        select(ComplaintCase, ComplaintReply)
        .join(
            ComplaintReply,
            ComplaintReply.complaint_case_id == ComplaintCase.id,
            isouter=True,
        )
        .where(ComplaintCase.state == "published")
    )


def _published_rows(session: Session) -> list[tuple[ComplaintCase, ComplaintReply | None]]:
    return list(session.exec(_published_statement().order_by(ComplaintCase.complaint_number)).all())


@router.get("/statistics")
def reply_statistics(session: Session = Depends(get_session)) -> dict[str, int]:
    rows = _published_rows(session)
    replied = [reply for _case, reply in rows if reply is not None]
    return {
        "published_cases": len(rows),
        "awaiting_reply": len(rows) - len(replied),
        "replies_imported": len(replied),
        "letters_generated": sum(reply.generated_at is not None for reply in replied),
    }


@router.get("")
def list_replies(
    view: str = Query(default="published", pattern="^(published|awaiting|imported|generated)$"),
    search: str = Query(default="", max_length=300),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
) -> dict[str, object]:
    statement = _published_statement()
    count_statement = (
        select(func.count())
        .select_from(ComplaintCase)
        .join(ComplaintReply, ComplaintReply.complaint_case_id == ComplaintCase.id, isouter=True)
        .where(ComplaintCase.state == "published")
    )
    filters = []
    if view == "awaiting":
        filters.append(ComplaintReply.id.is_(None))
    elif view == "imported":
        filters.append(ComplaintReply.id.is_not(None))
    elif view == "generated":
        filters.append(ComplaintReply.generated_at.is_not(None))
    if search.strip():
        like = f"%{search.strip()}%"
        filters.append(
            or_(
                ComplaintCase.complaint_number.ilike(like),
                ComplaintCase.remarks.ilike(like),
                ComplaintReply.reply_text.ilike(like),
            )
        )
    statement = statement.where(*filters)
    count_statement = count_statement.where(*filters)
    total = session.scalar(count_statement) or 0
    rows = session.exec(
        statement.order_by(ComplaintCase.complaint_number)
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()
    return {
        "items": [
            {
                "case_id": str(case.id),
                "complaint_number": case.complaint_number,
                "complaint_remarks": case.remarks,
                "paperless_document_id": case.canonical_paperless_document_id,
                "reply": reply.reply_text if reply else None,
                "reply_version": reply.version if reply else None,
                "source_filename": reply.source_filename if reply else None,
                "imported_at": reply.imported_at if reply else None,
                "generated_at": reply.generated_at if reply else None,
            }
            for case, reply in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "view": view,
    }


@router.get("/complaints.csv")
def export_complaints_csv(
    scope: str = Query(default="awaiting", pattern="^(awaiting|all)$"),
    session: Session = Depends(get_session),
) -> Response:
    """Compatibility CSV endpoint. New UI creates durable export batches."""
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["Complaint Number", "Complaint Remarks"])
    count = 0
    for case, reply in _published_rows(session):
        if scope == "awaiting" and reply is not None:
            continue
        writer.writerow([case.complaint_number or "", case.remarks or ""])
        count += 1
    filename = f"crm-published-complaints-{scope}-{count}.csv"
    return Response(
        content="\ufeff" + output.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/imports")
async def import_replies(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, object]:
    """Compatibility atomic import backed by the durable batch service."""
    filename = file.filename or "replies.csv"
    if not filename.casefold().endswith(".csv"):
        raise HTTPException(status_code=422, detail="Upload a UTF-8 CSV reply file.")
    content = await file.read(MAX_REPLY_FILE_BYTES + 1)
    await file.close()
    if len(content) > MAX_REPLY_FILE_BYTES:
        raise HTTPException(status_code=413, detail="The reply CSV exceeds 5 MB.")
    service = CrmBulkOperationService(session, settings)
    try:
        validation = service.validate_import_batch(content=content, filename=filename)
        if validation["status"] != "ready" or validation["failed_items"]:
            items = service.list_items(uuid.UUID(validation["id"]), page=1, page_size=500)["items"]
            errors = [
                item["error_message"]
                for item in items
                if item.get("error_message")
            ]
            if not errors and validation.get("error_summary"):
                errors = [validation["error_summary"]]
            raise HTTPException(
                status_code=422,
                detail={"message": "Reply CSV validation failed", "errors": errors},
            )
        committed = service.commit_import_batch(uuid.UUID(validation["id"]), allow_partial=False)
        summary = committed["commit_summary"]
        return {
            "batch_id": committed["id"],
            "batch_number": committed["batch_number"],
            "rows": validation["valid_items"],
            "imported": summary["imported"],
            "updated": summary["updated"],
            "unchanged": summary["unchanged"],
            "duplicate_rows": validation["duplicate_items"],
            "letters_ready": summary["letters_ready"],
        }
    except HTTPException:
        raise
    except BulkOperationValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except BulkOperationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/letter-packages")
def download_reply_letters(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> Response:
    """Compatibility download backed by a durable formal-letter batch."""
    service = CrmBulkOperationService(session, settings)
    try:
        batch = service.create_letter_batch(scope="all_imported")
        artifact = next(
            (item for item in batch["artifacts"] if item["kind"] == "letter_package"),
            None,
        )
        if artifact is None:
            raise BulkOperationError("The formal-letter package was not created")
        _record, path = service.artifact_path(uuid.UUID(artifact["id"]))
        return Response(
            content=path.read_bytes(),
            media_type="application/zip",
            headers={
                "Content-Disposition": f'attachment; filename="{artifact["name"]}"',
                "X-CRM-Batch-ID": batch["id"],
                "X-CRM-Batch-Number": batch["batch_number"],
            },
        )
    except BulkOperationValidationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except BulkOperationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

from __future__ import annotations

import csv
import io
import re
import zipfile

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from sqlmodel import Session, select

from automation_core.database import get_session
from automation_core.time import utcnow
from crm_domain.identifiers import normalize_complaint_number
from crm_domain.models import ComplaintCase, ComplaintReply
from crm_domain.reply_documents import build_deo_report_odt


router = APIRouter(prefix="/api/v1/crm/replies", tags=["crm-replies"])
MAX_REPLY_FILE_BYTES = 5 * 1024 * 1024


def _published_rows(session: Session) -> list[tuple[ComplaintCase, ComplaintReply | None]]:
    return list(
        session.exec(
            select(ComplaintCase, ComplaintReply)
            .join(
                ComplaintReply,
                ComplaintReply.complaint_case_id == ComplaintCase.id,
                isouter=True,
            )
            .where(ComplaintCase.state == "published")
            .order_by(ComplaintCase.complaint_number)
        ).all()
    )


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
def list_replies(session: Session = Depends(get_session)) -> dict[str, object]:
    rows = _published_rows(session)
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
        "total": len(rows),
    }


@router.get("/complaints.csv")
def export_complaints_csv(
    scope: str = Query(default="awaiting", pattern="^(awaiting|all)$"),
    session: Session = Depends(get_session),
) -> Response:
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


def _header_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.casefold())


@router.post("/imports")
async def import_replies(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
) -> dict[str, object]:
    filename = file.filename or "replies.csv"
    if not filename.casefold().endswith(".csv"):
        raise HTTPException(status_code=422, detail="Upload a UTF-8 CSV reply file.")
    content = await file.read(MAX_REPLY_FILE_BYTES + 1)
    await file.close()
    if len(content) > MAX_REPLY_FILE_BYTES:
        raise HTTPException(status_code=413, detail="The reply CSV exceeds 5 MB.")
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=422, detail="The reply CSV must use UTF-8 encoding.") from exc
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    if len(headers) != 2:
        raise HTTPException(
            status_code=422,
            detail="Reply CSV must contain exactly two columns: Complaint Number and Reply.",
        )
    by_key = {_header_key(header): header for header in headers}
    number_header = next(
        (by_key[key] for key in ("complaintnumber", "complaintno", "number") if key in by_key),
        None,
    )
    reply_header = next(
        (by_key[key] for key in ("reply", "complaintreply", "remarks", "deoreply") if key in by_key),
        None,
    )
    if number_header is None or reply_header is None or number_header == reply_header:
        raise HTTPException(
            status_code=422,
            detail="Use headers 'Complaint Number' and 'Reply'.",
        )

    parsed: dict[str, tuple[str, int]] = {}
    errors: list[str] = []
    duplicates = 0
    for row_number, row in enumerate(reader, start=2):
        number = normalize_complaint_number(str(row.get(number_header) or ""))
        reply = str(row.get(reply_header) or "").replace("\r\n", "\n").replace("\r", "\n").strip()
        if not number and not reply:
            continue
        if not number:
            errors.append(f"Row {row_number}: invalid complaint number")
            continue
        if not reply:
            errors.append(f"Row {row_number}: reply is empty for {number}")
            continue
        if number in parsed:
            duplicates += 1
        parsed[number] = (reply, row_number)
    if not parsed and not errors:
        errors.append("The reply CSV contains no reply rows")

    cases = {
        case.complaint_number: case
        for case in session.exec(
            select(ComplaintCase).where(ComplaintCase.complaint_number.in_(list(parsed)))
        ).all()
    }
    for number in parsed:
        case = cases.get(number)
        if case is None:
            errors.append(f"{number}: complaint case was not found")
        elif case.state != "published":
            errors.append(f"{number}: complaint is not published to Paperless yet")
    if errors:
        raise HTTPException(
            status_code=422,
            detail={"message": "Reply CSV validation failed", "errors": errors},
        )

    imported = 0
    updated = 0
    now = utcnow()
    for number, (reply_text, row_number) in parsed.items():
        case = cases[number]
        existing = session.exec(
            select(ComplaintReply).where(ComplaintReply.complaint_case_id == case.id)
        ).first()
        if existing is None:
            existing = ComplaintReply(
                complaint_case_id=case.id,
                reply_text=reply_text,
                source_filename=filename,
                source_row=row_number,
            )
            imported += 1
        else:
            existing.reply_text = reply_text
            existing.source_filename = filename
            existing.source_row = row_number
            existing.version += 1
            existing.imported_at = now
            existing.generated_at = None
            existing.updated_at = now
            updated += 1
        session.add(existing)
    session.commit()
    return {
        "rows": len(parsed),
        "imported": imported,
        "updated": updated,
        "duplicate_rows": duplicates,
        "letters_ready": len(parsed),
    }


@router.post("/letter-packages")
def download_reply_letters(session: Session = Depends(get_session)) -> Response:
    rows = [(case, reply) for case, reply in _published_rows(session) if reply is not None]
    if not rows:
        raise HTTPException(status_code=409, detail="Import at least one reply before generating letters.")
    output = io.BytesIO()
    manifest = io.StringIO(newline="")
    manifest_writer = csv.writer(manifest)
    manifest_writer.writerow(["Complaint Number", "Paperless Document ID", "Reply Version", "ODT File"])
    generated_at = utcnow()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for case, reply in rows:
            assert reply is not None
            number = case.complaint_number or str(case.id)
            relative = f"{number}/{number} - DEO Report.odt"
            archive.writestr(relative, build_deo_report_odt(case, reply.reply_text))
            manifest_writer.writerow(
                [number, case.canonical_paperless_document_id or "", reply.version, relative]
            )
            reply.generated_at = generated_at
            reply.updated_at = generated_at
            session.add(reply)
        archive.writestr("manifest.csv", "\ufeff" + manifest.getvalue())
        archive.writestr(
            "README.txt",
            "Native CRM reply package. Each complaint folder contains its DEO Report ODT.\n",
        )
    session.commit()
    return Response(
        content=output.getvalue(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="crm-deo-reply-letters-{len(rows)}.zip"'
        },
    )

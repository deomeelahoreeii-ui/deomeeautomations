from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import uuid
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import func, or_
from sqlmodel import Session, select

from automation_core.config import Settings
from automation_core.time import utcnow
from crm_domain.identifiers import normalize_complaint_number
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintReply,
    CrmBulkOperationArtifact,
    CrmBulkOperationBatch,
    CrmBulkOperationItem,
)
from crm_domain.reply_documents import build_deo_report_odt


CHATGPT_INSTRUCTIONS = """Use the attached complaint CSV to prepare citizen-facing replies.

Return one UTF-8 CSV file containing exactly these two columns:
Complaint Number
Reply

Requirements:
- Keep every complaint number unchanged.
- Provide exactly one row for every complaint in the export.
- Do not add explanations, markdown, numbering, code fences, or extra columns.
- Preserve commas and line breaks using valid CSV quoting.
- Do not leave a Reply cell blank.
"""

EXAMPLE_COMPLAINT_NUMBER = "104-1234567"
EXAMPLE_COMPLAINT_TEXT = "A sample complaint requesting examination of an administrative matter."
EXAMPLE_REPLY_TEXT = (
    "The matter has been examined at the appropriate level. The institution concerned has "
    "been sensitized to comply with the applicable instructions, and necessary action will be "
    "taken if any violation is established."
)


class BulkOperationError(RuntimeError):
    pass


class BulkOperationNotFound(BulkOperationError):
    pass


class BulkOperationValidationError(BulkOperationError):
    pass


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _reply_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _csv_bytes(headers: list[str], rows: Iterable[Iterable[Any]]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(headers)
    writer.writerows(rows)
    return ("\ufeff" + output.getvalue()).encode("utf-8")


def _safe_name(value: str, fallback: str) -> str:
    name = Path(value or fallback).name.strip() or fallback
    return re.sub(r"[^A-Za-z0-9._ -]+", "-", name)[:255]


def _operation_prefix(operation_type: str) -> str:
    return {
        "classification_export": "CLS",
        "classification_import": "CLR",
        "reply_context_export": "CTX",
        "reply_export": "EXP",
        "reply_import": "IMP",
        "formal_letters": "LET",
    }[operation_type]


def _batch_number(operation_type: str) -> str:
    return (
        f"CRM-{_operation_prefix(operation_type)}-"
        f"{utcnow().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6].upper()}"
    )


def reference_file(kind: str) -> tuple[str, str, bytes]:
    if kind == "reply-template":
        return (
            "crm-reply-template.csv",
            "text/csv; charset=utf-8",
            _csv_bytes(["Complaint Number", "Reply"], [[EXAMPLE_COMPLAINT_NUMBER, ""]]),
        )
    if kind == "example-completed":
        return (
            "crm-example-completed-replies.csv",
            "text/csv; charset=utf-8",
            _csv_bytes(
                ["Complaint Number", "Reply"],
                [[EXAMPLE_COMPLAINT_NUMBER, EXAMPLE_REPLY_TEXT]],
            ),
        )
    if kind == "chatgpt-instructions":
        return (
            "crm-chatgpt-reply-instructions.txt",
            "text/plain; charset=utf-8",
            CHATGPT_INSTRUCTIONS.encode("utf-8"),
        )
    raise BulkOperationNotFound("Reference file was not found")


class CrmBulkOperationService:
    def __init__(self, session: Session, settings: Settings):
        self.session = session
        self.settings = settings

    @property
    def root(self) -> Path:
        root = self.settings.artifact_root.expanduser().resolve() / "crm-bulk-operations"
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _batch_dir(self, batch: CrmBulkOperationBatch) -> Path:
        directory = self.root / batch.batch_number
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _create_batch(
        self,
        operation_type: str,
        *,
        actor: str,
        parent_batch_id: uuid.UUID | None = None,
        source_filename: str | None = None,
        source_sha256: str | None = None,
        scope: dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        status: str = "draft",
    ) -> CrmBulkOperationBatch:
        batch = CrmBulkOperationBatch(
            batch_number=_batch_number(operation_type),
            operation_type=operation_type,
            status=status,
            parent_batch_id=parent_batch_id,
            source_filename=source_filename,
            source_sha256=source_sha256,
            scope_json=scope or {},
            settings_json=settings or {},
            created_by=actor.strip() or "web-operator",
        )
        self.session.add(batch)
        self.session.flush()
        return batch

    def _write_artifact(
        self,
        batch: CrmBulkOperationBatch,
        *,
        kind: str,
        name: str,
        content_type: str,
        content: bytes,
    ) -> CrmBulkOperationArtifact:
        safe = _safe_name(name, f"{kind}.bin")
        path = self._batch_dir(batch) / safe
        path.write_bytes(content)
        artifact = CrmBulkOperationArtifact(
            batch_id=batch.id,
            kind=kind,
            name=safe,
            path=str(path),
            content_type=content_type,
            size_bytes=len(content),
            sha256=_sha256_bytes(content),
        )
        self.session.add(artifact)
        self.session.flush()
        return artifact

    @staticmethod
    def _batch_payload(batch: CrmBulkOperationBatch) -> dict[str, Any]:
        processed = batch.successful_items + batch.failed_items + batch.skipped_items
        return {
            "id": str(batch.id),
            "batch_number": batch.batch_number,
            "operation_type": batch.operation_type,
            "status": batch.status,
            "parent_batch_id": str(batch.parent_batch_id) if batch.parent_batch_id else None,
            "source_filename": batch.source_filename,
            "source_sha256": batch.source_sha256,
            "scope": batch.scope_json,
            "settings": batch.settings_json,
            "total_items": batch.total_items,
            "valid_items": batch.valid_items,
            "successful_items": batch.successful_items,
            "failed_items": batch.failed_items,
            "skipped_items": batch.skipped_items,
            "duplicate_items": batch.duplicate_items,
            "processed_items": processed,
            "progress_percent": round((processed / batch.total_items) * 100) if batch.total_items else 0,
            "created_by": batch.created_by,
            "error_summary": batch.error_summary,
            "created_at": batch.created_at,
            "updated_at": batch.updated_at,
            "started_at": batch.started_at,
            "completed_at": batch.completed_at,
        }

    @staticmethod
    def _artifact_payload(artifact: CrmBulkOperationArtifact) -> dict[str, Any]:
        return {
            "id": str(artifact.id),
            "batch_id": str(artifact.batch_id),
            "kind": artifact.kind,
            "name": artifact.name,
            "content_type": artifact.content_type,
            "size_bytes": artifact.size_bytes,
            "sha256": artifact.sha256,
            "created_at": artifact.created_at,
            "download_url": f"/api/v1/crm/bulk-operations/artifacts/{artifact.id}/download",
        }

    def _require_batch(self, batch_id: uuid.UUID) -> CrmBulkOperationBatch:
        batch = self.session.get(CrmBulkOperationBatch, batch_id)
        if batch is None:
            raise BulkOperationNotFound("Bulk-operation batch was not found")
        return batch

    def statistics(self) -> dict[str, Any]:
        published = self.session.scalar(
            select(func.count()).select_from(ComplaintCase).where(ComplaintCase.state == "published")
        ) or 0
        imported = self.session.scalar(
            select(func.count())
            .select_from(ComplaintCase)
            .join(ComplaintReply, ComplaintReply.complaint_case_id == ComplaintCase.id)
            .where(ComplaintCase.state == "published")
        ) or 0
        generated = self.session.scalar(
            select(func.count())
            .select_from(ComplaintCase)
            .join(ComplaintReply, ComplaintReply.complaint_case_id == ComplaintCase.id)
            .where(
                ComplaintCase.state == "published",
                ComplaintReply.generated_at.is_not(None),
            )
        ) or 0
        batch_counts = dict(
            self.session.exec(
                select(CrmBulkOperationBatch.operation_type, func.count())
                .group_by(CrmBulkOperationBatch.operation_type)
            ).all()
        )
        return {
            "published_cases": published,
            "awaiting_reply": max(0, published - imported),
            "replies_imported": imported,
            "letters_generated": generated,
            "classification_export_batches": int(batch_counts.get("classification_export", 0)),
            "classification_import_batches": int(batch_counts.get("classification_import", 0)),
            "reply_context_batches": int(batch_counts.get("reply_context_export", 0)),
            "export_batches": int(batch_counts.get("reply_export", 0)) + int(batch_counts.get("reply_context_export", 0)),
            "import_batches": int(batch_counts.get("reply_import", 0)),
            "letter_batches": int(batch_counts.get("formal_letters", 0)),
        }

    def list_batches(
        self,
        *,
        operation_type: str = "",
        status: str = "",
        search: str = "",
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        filters: list[Any] = []
        if operation_type:
            filters.append(CrmBulkOperationBatch.operation_type == operation_type)
        if status:
            filters.append(CrmBulkOperationBatch.status == status)
        term = search.strip()
        if term:
            like = f"%{term}%"
            filters.append(
                or_(
                    CrmBulkOperationBatch.batch_number.ilike(like),
                    CrmBulkOperationBatch.source_filename.ilike(like),
                    CrmBulkOperationBatch.created_by.ilike(like),
                )
            )
        total = self.session.scalar(
            select(func.count()).select_from(CrmBulkOperationBatch).where(*filters)
        ) or 0
        rows = self.session.exec(
            select(CrmBulkOperationBatch)
            .where(*filters)
            .order_by(CrmBulkOperationBatch.created_at.desc(), CrmBulkOperationBatch.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all()
        return {
            "items": [self._batch_payload(row) for row in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    def batch_detail(self, batch_id: uuid.UUID) -> dict[str, Any]:
        batch = self._require_batch(batch_id)
        payload = self._batch_payload(batch)
        parent = self.session.get(CrmBulkOperationBatch, batch.parent_batch_id) if batch.parent_batch_id else None
        children = self.session.exec(
            select(CrmBulkOperationBatch)
            .where(CrmBulkOperationBatch.parent_batch_id == batch.id)
            .order_by(CrmBulkOperationBatch.created_at)
        ).all()
        artifacts = self.session.exec(
            select(CrmBulkOperationArtifact)
            .where(CrmBulkOperationArtifact.batch_id == batch.id)
            .order_by(CrmBulkOperationArtifact.created_at)
        ).all()
        payload.update(
            {
                "parent": self._batch_payload(parent) if parent else None,
                "children": [self._batch_payload(child) for child in children],
                "artifacts": [self._artifact_payload(item) for item in artifacts],
            }
        )
        return payload

    def list_items(
        self,
        batch_id: uuid.UUID,
        *,
        status: str = "",
        search: str = "",
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        self._require_batch(batch_id)
        filters: list[Any] = [CrmBulkOperationItem.batch_id == batch_id]
        if status:
            filters.append(CrmBulkOperationItem.status == status)
        if search.strip():
            filters.append(CrmBulkOperationItem.complaint_number_snapshot.ilike(f"%{search.strip()}%"))
        total = self.session.scalar(
            select(func.count()).select_from(CrmBulkOperationItem).where(*filters)
        ) or 0
        rows = self.session.exec(
            select(CrmBulkOperationItem, ComplaintCase, ComplaintReply)
            .join(ComplaintCase, ComplaintCase.id == CrmBulkOperationItem.complaint_case_id, isouter=True)
            .join(ComplaintReply, ComplaintReply.complaint_case_id == ComplaintCase.id, isouter=True)
            .where(*filters)
            .order_by(CrmBulkOperationItem.source_row, CrmBulkOperationItem.created_at)
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all()
        items: list[dict[str, Any]] = []
        for item, case, reply in rows:
            details = dict(item.details_json or {})
            details.pop("reply_text", None)
            items.append(
                {
                    "id": str(item.id),
                    "batch_id": str(item.batch_id),
                    "case_id": str(item.complaint_case_id) if item.complaint_case_id else None,
                    "complaint_number": item.complaint_number_snapshot,
                    "complaint_preview": (case.remarks or "")[:500] if case else "",
                    "paperless_document_id": case.canonical_paperless_document_id if case else None,
                    "source_row": item.source_row,
                    "status": item.status,
                    "error_code": item.error_code,
                    "error_message": item.error_message,
                    "reply_version_before": item.reply_version_before,
                    "reply_version_after": item.reply_version_after,
                    "current_reply_version": reply.version if reply else None,
                    "reply_preview": (reply.reply_text or "")[:500] if reply else "",
                    "reply_workspace_status": reply.workspace_status if reply else None,
                    "reply_sync_status": reply.sync_status if reply else None,
                    "reply_source_kind": reply.source_kind if reply else None,
                    "reply_content_hash": item.reply_content_hash,
                    "output_filename": item.output_filename,
                    "output_sha256": item.output_sha256,
                    "details": details,
                    "created_at": item.created_at,
                    "processed_at": item.processed_at,
                }
            )
        return {"items": items, "total": total, "page": page, "page_size": page_size}

    def artifact_path(self, artifact_id: uuid.UUID) -> tuple[CrmBulkOperationArtifact, Path]:
        artifact = self.session.get(CrmBulkOperationArtifact, artifact_id)
        if artifact is None:
            raise BulkOperationNotFound("Batch artifact was not found")
        path = Path(artifact.path).expanduser().resolve(strict=False)
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise BulkOperationError("Artifact path is outside the CRM bulk-operation store") from exc
        if not path.is_file():
            raise BulkOperationNotFound("Batch artifact file is unavailable")
        if artifact.sha256 and _sha256_bytes(path.read_bytes()) != artifact.sha256:
            raise BulkOperationError("Batch artifact checksum verification failed")
        return artifact, path

    def create_export_batch(
        self,
        *,
        scope: str,
        case_ids: list[uuid.UUID] | None = None,
        actor: str = "web-operator",
    ) -> dict[str, Any]:
        if scope not in {"awaiting", "all", "selected"}:
            raise BulkOperationValidationError("Export scope must be awaiting, all or selected")
        statement = (
            select(ComplaintCase, ComplaintReply)
            .join(ComplaintReply, ComplaintReply.complaint_case_id == ComplaintCase.id, isouter=True)
            .where(ComplaintCase.state == "published")
            .order_by(ComplaintCase.complaint_number)
        )
        if scope == "awaiting":
            statement = statement.where(ComplaintReply.id.is_(None))
        elif scope == "selected":
            unique_ids = list(dict.fromkeys(case_ids or []))
            if not unique_ids:
                raise BulkOperationValidationError("Select at least one complaint for the export batch")
            statement = statement.where(ComplaintCase.id.in_(unique_ids))
        rows = list(self.session.exec(statement).all())
        if not rows:
            raise BulkOperationValidationError("No published complaints match the export scope")

        now = utcnow()
        batch = self._create_batch(
            "reply_export",
            actor=actor,
            status="processing",
            scope={"scope": scope, "case_ids": [str(value) for value in case_ids or []]},
            settings={"format": "chatgpt_csv_v1"},
        )
        batch.started_at = now
        complaint_rows: list[list[str]] = []
        template_rows: list[list[str]] = []
        manifest_rows: list[list[Any]] = []
        for case, reply in rows:
            number = case.complaint_number or str(case.id)
            complaint_rows.append([number, case.remarks or ""])
            template_rows.append([number, ""])
            manifest_rows.append([number, str(case.id), case.canonical_paperless_document_id or "", bool(reply)])
            self.session.add(
                CrmBulkOperationItem(
                    batch_id=batch.id,
                    complaint_case_id=case.id,
                    complaint_number_snapshot=number,
                    status="exported",
                    reply_version_before=reply.version if reply else None,
                    details_json={"had_reply_at_export": bool(reply)},
                    processed_at=now,
                )
            )

        complaints = _csv_bytes(["Complaint Number", "Complaint Remarks"], complaint_rows)
        template = _csv_bytes(["Complaint Number", "Reply"], template_rows)
        example = _csv_bytes(
            ["Complaint Number", "Reply"],
            [[EXAMPLE_COMPLAINT_NUMBER, EXAMPLE_REPLY_TEXT]],
        )
        manifest = _csv_bytes(
            ["Complaint Number", "Case ID", "Paperless Document ID", "Had Reply At Export"],
            manifest_rows,
        )
        metadata = json.dumps(
            {
                "batch_number": batch.batch_number,
                "batch_id": str(batch.id),
                "scope": scope,
                "complaints": len(rows),
                "created_at": now.isoformat(),
                "required_reply_columns": ["Complaint Number", "Reply"],
            },
            indent=2,
        ).encode("utf-8")
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("01-complaints-for-chatgpt.csv", complaints)
            archive.writestr("02-reply-template.csv", template)
            archive.writestr("03-chatgpt-instructions.txt", CHATGPT_INSTRUCTIONS)
            archive.writestr("04-example-completed-replies.csv", example)
            archive.writestr("batch-manifest.csv", manifest)
            archive.writestr("batch-summary.json", metadata)
        self._write_artifact(
            batch,
            kind="export_package",
            name=f"{batch.batch_number}.zip",
            content_type="application/zip",
            content=output.getvalue(),
        )
        batch.total_items = len(rows)
        batch.valid_items = len(rows)
        batch.successful_items = len(rows)
        batch.status = "completed"
        batch.completed_at = now
        batch.updated_at = now
        self.session.add(batch)
        self.session.commit()
        return self.batch_detail(batch.id)

    def validate_import_batch(
        self,
        *,
        content: bytes,
        filename: str,
        parent_batch_id: uuid.UUID | None = None,
        actor: str = "web-operator",
    ) -> dict[str, Any]:
        safe_filename = _safe_name(filename, "replies.csv")
        parent: CrmBulkOperationBatch | None = None
        expected: dict[str, CrmBulkOperationItem] = {}
        if parent_batch_id:
            parent = self._require_batch(parent_batch_id)
            if parent.operation_type not in {"reply_export", "reply_context_export"} or parent.status != "completed":
                raise BulkOperationValidationError("The related reply export batch must be completed")
            expected = {
                item.complaint_number_snapshot: item
                for item in self.session.exec(
                    select(CrmBulkOperationItem).where(
                        CrmBulkOperationItem.batch_id == parent.id,
                        CrmBulkOperationItem.status == "exported",
                    )
                ).all()
            }

        now = utcnow()
        batch = self._create_batch(
            "reply_import",
            actor=actor,
            parent_batch_id=parent_batch_id,
            source_filename=safe_filename,
            source_sha256=_sha256_bytes(content),
            status="validating",
            scope={"parent_export_batch_id": str(parent_batch_id) if parent_batch_id else None},
            settings={"strict_default": True, "format": "chatgpt_csv_v1"},
        )
        batch.started_at = now
        self._write_artifact(
            batch,
            kind="source_csv",
            name=safe_filename,
            content_type="text/csv; charset=utf-8",
            content=content,
        )

        fatal: str | None = None
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = ""
            fatal = "The reply CSV must use UTF-8 encoding."
        reader: csv.DictReader[str] | None = None
        number_header = reply_header = None
        if not fatal:
            reader = csv.DictReader(io.StringIO(text))
            headers = reader.fieldnames or []
            normalized = {re.sub(r"[^a-z0-9]+", "", value.casefold()): value for value in headers}
            if len(headers) != 2 or set(normalized) != {"complaintnumber", "reply"}:
                fatal = "Reply CSV must contain exactly two columns: Complaint Number and Reply."
            else:
                number_header = normalized["complaintnumber"]
                reply_header = normalized["reply"]

        seen: set[str] = set()
        received: set[str] = set()
        counts: Counter[str] = Counter()
        invalid_rows: list[list[Any]] = []
        if fatal:
            batch.status = "failed"
            batch.error_summary = fatal
        else:
            assert reader is not None and number_header and reply_header
            for row_number, row in enumerate(reader, start=2):
                raw_number = str(row.get(number_header) or "").strip()
                reply_text = str(row.get(reply_header) or "").replace("\r\n", "\n").replace("\r", "\n").strip()
                if not raw_number and not reply_text:
                    continue
                number = normalize_complaint_number(raw_number)
                status = "valid"
                error_code = error_message = None
                case: ComplaintCase | None = None
                existing: ComplaintReply | None = None
                action = "import"
                if not number:
                    status, error_code, error_message = "invalid", "invalid_complaint_number", "Invalid complaint number"
                elif number in seen:
                    status, error_code, error_message = "invalid", "duplicate_complaint_number", f"Duplicate complaint number {number}"
                    batch.duplicate_items += 1
                elif not reply_text:
                    status, error_code, error_message = "invalid", "empty_reply", f"Reply is empty for {number}"
                else:
                    seen.add(number)
                    received.add(number)
                    case = self.session.exec(
                        select(ComplaintCase).where(ComplaintCase.complaint_number == number)
                    ).first()
                    if case is None:
                        status, error_code, error_message = "invalid", "complaint_not_found", f"Complaint {number} was not found"
                    elif case.state != "published":
                        status, error_code, error_message = "invalid", "complaint_not_published", f"Complaint {number} is not published"
                    elif parent and number not in expected:
                        status, error_code, error_message = "invalid", "not_in_export_batch", f"Complaint {number} was not in {parent.batch_number}"
                    else:
                        existing = self.session.exec(
                            select(ComplaintReply).where(ComplaintReply.complaint_case_id == case.id)
                        ).first()
                        if existing and existing.reply_text.strip() == reply_text:
                            action = "unchanged"
                        elif existing:
                            action = "update"
                        if (
                            case.frappe_reply_approval_status in {"Approved", "Issued"}
                            and existing
                            and existing.reply_text.strip() != reply_text
                        ):
                            status = "invalid"
                            error_code = "approved_reply_conflict"
                            error_message = (
                                f"Complaint {number} has an approved or issued reply; revise it in the Reply Editor"
                            )
                item = CrmBulkOperationItem(
                    batch_id=batch.id,
                    complaint_case_id=case.id if case else None,
                    complaint_number_snapshot=number or raw_number,
                    source_row=row_number,
                    status=status,
                    error_code=error_code,
                    error_message=error_message,
                    reply_version_before=existing.version if existing else None,
                    reply_content_hash=_reply_hash(reply_text) if reply_text else None,
                    details_json={"reply_text": reply_text, "action": action, "raw_number": raw_number},
                )
                self.session.add(item)
                counts[status] += 1
                if error_message:
                    invalid_rows.append([row_number, number or raw_number, error_code, error_message])

            if parent:
                for number, export_item in expected.items():
                    if number in received:
                        continue
                    missing = CrmBulkOperationItem(
                        batch_id=batch.id,
                        complaint_case_id=export_item.complaint_case_id,
                        complaint_number_snapshot=number,
                        status="missing",
                        error_code="missing_from_import",
                        error_message=f"Complaint {number} from {parent.batch_number} is missing from the imported CSV",
                        details_json={"parent_export_item_id": str(export_item.id)},
                    )
                    self.session.add(missing)
                    counts["missing"] += 1
                    invalid_rows.append(["", number, "missing_from_import", missing.error_message])

            total = sum(counts.values())
            batch.total_items = total
            batch.valid_items = counts["valid"]
            batch.failed_items = counts["invalid"] + counts["missing"]
            if total == 0:
                batch.status = "failed"
                batch.error_summary = "The reply CSV contains no reply rows."
            elif batch.valid_items:
                batch.status = "ready"
                if batch.failed_items:
                    batch.error_summary = f"{batch.failed_items} row(s) require correction before a strict import."
            else:
                batch.status = "failed"
                batch.error_summary = "No valid reply rows were found."

        if fatal:
            batch.total_items = 0
            batch.valid_items = 0
            batch.failed_items = 1
            invalid_rows.append(["", "", "invalid_file", fatal])
        if invalid_rows:
            self._write_artifact(
                batch,
                kind="validation_errors",
                name=f"{batch.batch_number}-validation-errors.csv",
                content_type="text/csv; charset=utf-8",
                content=_csv_bytes(
                    ["Source Row", "Complaint Number", "Error Code", "Error"],
                    invalid_rows,
                ),
            )
        batch.updated_at = utcnow()
        self.session.add(batch)
        self.session.commit()
        return self.batch_detail(batch.id)

    def commit_import_batch(
        self,
        batch_id: uuid.UUID,
        *,
        allow_partial: bool = False,
        actor: str = "web-operator",
    ) -> dict[str, Any]:
        batch = self._require_batch(batch_id)
        if batch.operation_type != "reply_import":
            raise BulkOperationValidationError("Only reply-import batches can be committed")
        if batch.status != "ready":
            raise BulkOperationValidationError("The import batch is not ready to commit")
        if batch.failed_items and not allow_partial:
            raise BulkOperationValidationError(
                "Strict import is enabled. Correct every invalid or missing row, or explicitly import valid rows only."
            )
        now = utcnow()
        batch.status = "processing"
        batch.started_at = batch.started_at or now
        batch.updated_at = now
        self.session.add(batch)
        self.session.commit()

        imported = updated = unchanged = 0
        valid_items = self.session.exec(
            select(CrmBulkOperationItem)
            .where(
                CrmBulkOperationItem.batch_id == batch.id,
                CrmBulkOperationItem.status == "valid",
            )
            .order_by(CrmBulkOperationItem.source_row)
        ).all()
        manifest_rows: list[list[Any]] = []
        try:
            for item in valid_items:
                case = self.session.get(ComplaintCase, item.complaint_case_id)
                if case is None:
                    item.status = "failed"
                    item.error_code = "complaint_not_found_at_commit"
                    item.error_message = "Complaint disappeared before import commit"
                    item.processed_at = now
                    batch.failed_items += 1
                    self.session.add(item)
                    continue
                reply_text = str((item.details_json or {}).get("reply_text") or "").strip()
                existing = self.session.exec(
                    select(ComplaintReply).where(ComplaintReply.complaint_case_id == case.id)
                ).first()
                before_version = existing.version if existing else None
                if existing and existing.reply_text.strip() == reply_text:
                    item.status = "unchanged"
                    item.reply_version_before = existing.version
                    item.reply_version_after = existing.version
                    unchanged += 1
                elif existing:
                    existing.reply_text = reply_text
                    existing.source_filename = batch.source_filename or "replies.csv"
                    existing.source_row = item.source_row or 0
                    existing.version += 1
                    existing.imported_at = now
                    existing.generated_at = None
                    existing.updated_at = now
                    item.status = "updated"
                    item.reply_version_before = before_version
                    item.reply_version_after = existing.version
                    updated += 1
                else:
                    existing = ComplaintReply(
                        complaint_case_id=case.id,
                        reply_text=reply_text,
                        source_filename=batch.source_filename or "replies.csv",
                        source_row=item.source_row or 0,
                        imported_at=now,
                        updated_at=now,
                    )
                    self.session.add(existing)
                    self.session.flush()
                    item.status = "imported"
                    item.reply_version_after = existing.version
                    imported += 1
                existing.source_kind = "bulk_import"
                existing.workspace_status = "Imported Draft"
                existing.sync_status = "not_synced"
                existing.sync_error = None
                existing.source_batch_id = batch.id
                existing.source_item_id = item.id
                existing.last_synced_at = None
                self.session.add(existing)
                item.processed_at = now
                self.session.add(item)
                self.session.add(
                    ComplaintAuditEvent(
                        complaint_case_id=case.id,
                        entity_type="complaint_reply",
                        entity_id=str(existing.id),
                        event_type=f"bulk_reply_{item.status}",
                        actor=actor,
                        before_json={"version": before_version},
                        after_json={
                            "version": item.reply_version_after,
                            "batch_id": str(batch.id),
                            "workspace_status": existing.workspace_status,
                            "sync_status": existing.sync_status,
                        },
                        details_json={
                            "batch_number": batch.batch_number,
                            "source_row": item.source_row,
                            "source_filename": batch.source_filename,
                            "source_item_id": str(item.id),
                        },
                    )
                )
                manifest_rows.append(
                    [
                        item.source_row or "",
                        item.complaint_number_snapshot,
                        item.status,
                        before_version or "",
                        item.reply_version_after or "",
                        item.reply_content_hash or "",
                    ]
                )
            batch.successful_items = imported + updated
            batch.skipped_items = unchanged
            batch.status = "completed_with_errors" if batch.failed_items else "completed"
            batch.completed_at = now
            batch.updated_at = now
            batch.settings_json = {
                **(batch.settings_json or {}),
                "allow_partial": allow_partial,
                "committed_by": actor,
            }
            self._write_artifact(
                batch,
                kind="import_manifest",
                name=f"{batch.batch_number}-import-manifest.csv",
                content_type="text/csv; charset=utf-8",
                content=_csv_bytes(
                    ["Source Row", "Complaint Number", "Result", "Version Before", "Version After", "Reply SHA-256"],
                    manifest_rows,
                ),
            )
            self.session.add(batch)
            self.session.commit()
        except Exception as exc:
            self.session.rollback()
            failed_batch = self._require_batch(batch_id)
            failed_batch.status = "failed"
            failed_batch.error_summary = str(exc)[:4000]
            failed_batch.updated_at = utcnow()
            failed_batch.completed_at = utcnow()
            self.session.add(failed_batch)
            self.session.commit()
            raise BulkOperationError(f"Reply import failed: {exc}") from exc
        result = self.batch_detail(batch.id)
        result["commit_summary"] = {
            "imported": imported,
            "updated": updated,
            "unchanged": unchanged,
            "letters_ready": imported + updated + unchanged,
        }
        return result

    def create_letter_batch(
        self,
        *,
        parent_batch_id: uuid.UUID | None = None,
        scope: str = "ready",
        case_ids: list[uuid.UUID] | None = None,
        actor: str = "web-operator",
    ) -> dict[str, Any]:
        if scope not in {"ready", "all_imported", "selected", "import_batch"}:
            raise BulkOperationValidationError("Letter scope is invalid")
        parent: CrmBulkOperationBatch | None = None
        selected_ids: list[uuid.UUID] = []
        if parent_batch_id:
            parent = self._require_batch(parent_batch_id)
            if parent.operation_type not in {"reply_import", "formal_letters"}:
                raise BulkOperationValidationError("Letters must originate from an import or retry batch")
            if parent.status not in {"completed", "completed_with_errors"}:
                raise BulkOperationValidationError("The source batch must be completed")
            selected_ids = [
                item.complaint_case_id
                for item in self.session.exec(
                    select(CrmBulkOperationItem).where(
                        CrmBulkOperationItem.batch_id == parent.id,
                        CrmBulkOperationItem.complaint_case_id.is_not(None),
                        CrmBulkOperationItem.status.in_(("imported", "updated", "unchanged", "failed")),
                    )
                ).all()
                if item.complaint_case_id is not None
            ]
            scope = "import_batch" if parent.operation_type == "reply_import" else "selected"
        elif scope == "selected":
            selected_ids = list(dict.fromkeys(case_ids or []))
            if not selected_ids:
                raise BulkOperationValidationError("Select at least one complaint for the letter batch")

        statement = (
            select(ComplaintCase, ComplaintReply)
            .join(ComplaintReply, ComplaintReply.complaint_case_id == ComplaintCase.id)
            .where(ComplaintCase.state == "published")
            .order_by(ComplaintCase.complaint_number)
        )
        if selected_ids:
            statement = statement.where(ComplaintCase.id.in_(selected_ids))
        elif scope == "ready":
            statement = statement.where(ComplaintReply.generated_at.is_(None))
        rows = list(self.session.exec(statement).all())
        if not rows:
            raise BulkOperationValidationError("No imported replies match the formal-letter scope")

        now = utcnow()
        batch = self._create_batch(
            "formal_letters",
            actor=actor,
            parent_batch_id=parent_batch_id,
            status="processing",
            scope={"scope": scope, "case_ids": [str(value) for value in selected_ids]},
            settings={"document_format": "odt", "template": "deo_report_v1"},
        )
        batch.started_at = now
        batch.total_items = len(rows)
        output = io.BytesIO()
        manifest = io.StringIO(newline="")
        writer = csv.writer(manifest)
        writer.writerow(
            ["Complaint Number", "Case ID", "Paperless Document ID", "Reply Version", "Reply SHA-256", "ODT File", "ODT SHA-256"]
        )
        success = failed = 0
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for case, reply in rows:
                number = case.complaint_number or str(case.id)
                relative = f"{number}/{number} - DEO Report.odt"
                item = CrmBulkOperationItem(
                    batch_id=batch.id,
                    complaint_case_id=case.id,
                    complaint_number_snapshot=number,
                    status="pending",
                    reply_version_before=reply.version,
                    reply_version_after=reply.version,
                    reply_content_hash=_reply_hash(reply.reply_text),
                    output_filename=relative,
                    details_json={"template": "deo_report_v1"},
                )
                try:
                    document = build_deo_report_odt(case, reply.reply_text)
                    document_hash = _sha256_bytes(document)
                    archive.writestr(relative, document)
                    item.status = "generated"
                    item.output_sha256 = document_hash
                    item.processed_at = now
                    reply.generated_at = now
                    reply.updated_at = now
                    self.session.add(reply)
                    writer.writerow(
                        [
                            number,
                            str(case.id),
                            case.canonical_paperless_document_id or "",
                            reply.version,
                            item.reply_content_hash,
                            relative,
                            document_hash,
                        ]
                    )
                    success += 1
                except Exception as exc:
                    item.status = "failed"
                    item.error_code = "letter_generation_failed"
                    item.error_message = str(exc)[:4000]
                    item.processed_at = now
                    failed += 1
                self.session.add(item)
            archive.writestr("manifest.csv", "\ufeff" + manifest.getvalue())
            archive.writestr(
                "batch-summary.json",
                json.dumps(
                    {
                        "batch_number": batch.batch_number,
                        "batch_id": str(batch.id),
                        "source_batch_id": str(parent_batch_id) if parent_batch_id else None,
                        "generated": success,
                        "failed": failed,
                        "created_at": now.isoformat(),
                    },
                    indent=2,
                ),
            )
            archive.writestr(
                "README.txt",
                "CRM formal-letter batch. Each complaint folder contains the exact ODT generated from the reply version recorded in manifest.csv.\n",
            )
        if success:
            self._write_artifact(
                batch,
                kind="letter_package",
                name=f"{batch.batch_number}.zip",
                content_type="application/zip",
                content=output.getvalue(),
            )
        batch.valid_items = len(rows)
        batch.successful_items = success
        batch.failed_items = failed
        batch.status = "completed_with_errors" if failed and success else "failed" if failed else "completed"
        batch.error_summary = f"{failed} letter(s) failed to generate." if failed else None
        batch.completed_at = now
        batch.updated_at = now
        self.session.add(batch)
        self.session.commit()
        return self.batch_detail(batch.id)

    def retry_batch(self, batch_id: uuid.UUID, *, actor: str = "web-operator") -> dict[str, Any]:
        batch = self._require_batch(batch_id)
        if batch.operation_type != "formal_letters":
            raise BulkOperationValidationError("Only failed formal-letter items can be retried")
        failed_ids = [
            item.complaint_case_id
            for item in self.session.exec(
                select(CrmBulkOperationItem).where(
                    CrmBulkOperationItem.batch_id == batch.id,
                    CrmBulkOperationItem.status == "failed",
                    CrmBulkOperationItem.complaint_case_id.is_not(None),
                )
            ).all()
            if item.complaint_case_id is not None
        ]
        if not failed_ids:
            raise BulkOperationValidationError("This batch has no failed letter items")
        return self.create_letter_batch(
            parent_batch_id=batch.id,
            scope="selected",
            case_ids=failed_ids,
            actor=actor,
        )

    def cancel_batch(self, batch_id: uuid.UUID, *, actor: str = "web-operator") -> dict[str, Any]:
        batch = self._require_batch(batch_id)
        if batch.status not in {"draft", "validating", "ready"}:
            raise BulkOperationValidationError("Only a draft, validating or ready batch can be cancelled")
        batch.status = "cancelled"
        batch.completed_at = utcnow()
        batch.updated_at = batch.completed_at
        batch.settings_json = {**(batch.settings_json or {}), "cancelled_by": actor}
        self.session.add(batch)
        self.session.commit()
        return self.batch_detail(batch.id)

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
from typing import Any, Iterable

from sqlalchemy import Text, cast, func, or_
from sqlmodel import Session, select

from automation_core.config import Settings
from automation_core.time import utcnow
from crm_domain.bulk_operations import (
    BulkOperationNotFound,
    BulkOperationValidationError,
    CrmBulkOperationService,
    _csv_bytes,
    _safe_name,
    _sha256_bytes,
)
from crm_domain.case_scopes import reply_case_eligibility_clause
from crm_domain.default_reply_sop import (
    DEFAULT_REPLY_SOP_CONTENT,
    DEFAULT_REPLY_SOP_NAME,
    DEFAULT_REPLY_SOP_SLUG,
    DEFAULT_REPLY_SOP_VERSION,
)
from crm_domain.identifiers import normalize_complaint_number
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintCategory,
    ComplaintReply,
    ComplaintReplyRevision,
    ComplaintSubcategory,
    CrmBulkOperationBatch,
    CrmBulkOperationItem,
    CrmComplaintClassification,
    CrmComplaintTag,
    CrmComplaintTagGroup,
    CrmComplaintTagLink,
    CrmPromptProfile,
    CrmPromptProfileVersion,
    CrmReplyContextExample,
    CrmTaxonomySuggestion,
)
from crm_domain.taxonomy import clean_name, normalized_name


CLASSIFICATION_HEADERS = [
    "Complaint Number",
    "Category",
    "Subcategory",
    "Tags",
    "Confidence",
    "Needs New Taxonomy",
    "Proposed Name",
    "Reason",
]

CLASSIFICATION_INSTRUCTIONS = """Classify every complaint in the attached CSV.

Return one UTF-8 CSV file containing exactly these columns:
Complaint Number,Category,Subcategory,Tags,Confidence,Needs New Taxonomy,Proposed Name,Reason

Rules:
- Keep every complaint number unchanged and return exactly one row per complaint.
- Prefer an existing active category and subcategory from 02-existing-taxonomy.csv.
- Use controlled tags from 03-existing-tags.csv for extra dimensions.
- Separate multiple tags with semicolons.
- Suggest a new subcategory only when no existing subcategory reasonably fits.
- Suggest a new top-level category only in exceptional cases.
- Never create taxonomy records yourself. Set Needs New Taxonomy to Yes and put one concise proposed name in Proposed Name.
- Confidence must be a number between 0 and 1.
- Reason must be a short operational explanation, not a citizen-facing reply.
- Do not add markdown, code fences, explanations, or extra columns.
"""

REPLY_CONTEXT_INSTRUCTIONS = """Prepare official replies for every complaint in 01-new-complaints.csv.

The active office SOP is included in 03-active-reply-sop.txt. Taxonomy and policy context is in
04-taxonomy-context.csv. Relevant approved historical examples are in
05-approved-historical-examples.csv.

Return one UTF-8 CSV containing exactly these two columns:
Complaint Number,Reply

Keep every complaint number unchanged. Produce one complete reply per complaint. Do not add
markdown, code fences, explanations, numbering, or extra columns. Current SOP and policy notes
always take precedence over historical examples. Never claim an inspection, visit, strict action,
refund, satisfaction, or other fact unless it is present in the complaint context.
"""

DEFAULT_TAG_GROUPS: tuple[tuple[str, str, str, int], ...] = (
    ("institution", "Institution", "Institution or provider type", 10),
    ("issue", "Issue", "Primary or secondary complaint issue", 20),
    ("evidence", "Evidence", "Evidence supplied or missing", 30),
    ("routing", "Routing", "Jurisdiction and referral destination", 40),
    ("handling", "Handling", "Administrative handling and review context", 50),
    ("outcome", "Outcome", "Resolution and disposal outcome", 60),
)


DEFAULT_TAGS: tuple[tuple[str, str, str], ...] = (
    ("private-school", "institution", "Privately managed school"),
    ("government-school", "institution", "Government school"),
    ("pef", "institution", "Punjab Education Foundation institution"),
    ("academy", "institution", "Academy or tuition centre"),
    ("madrasa", "institution", "Madrasa or religious institution"),
    ("college", "institution", "College or HED matter"),
    ("advance-fee", "issue", "Advance or lump-sum fee demand"),
    ("monthly-fee", "issue", "Regular monthly fee issue"),
    ("fee-increase", "issue", "Increase in fee"),
    ("summer-camp", "issue", "Summer camp or vacation activity"),
    ("certificates-withheld", "issue", "Certificate, result or examination document withheld"),
    ("heatwave", "issue", "Heatwave, ventilation, drinking water or cooling"),
    ("compulsory-attendance", "issue", "Compulsory attendance allegation"),
    ("no-evidence", "evidence", "No supporting documentary evidence supplied"),
    ("fee-voucher-attached", "evidence", "Fee voucher or receipt attached"),
    ("whatsapp-proof", "evidence", "WhatsApp or message evidence attached"),
    ("school-report-attached", "evidence", "School or institute report attached"),
    ("photographs-attached", "evidence", "Photographs attached"),
    ("outside-lahore", "routing", "Matter belongs outside Lahore"),
    ("deo-se", "routing", "High or secondary school referral"),
    ("hed", "routing", "Higher Education Department referral"),
    ("pef-referral", "routing", "Punjab Education Foundation referral"),
    ("pectaa", "routing", "Assessment or PECTAA referral"),
    ("cybercrime", "routing", "Cybercrime or online fraud referral"),
    ("confidentiality-requested", "handling", "Complainant requested confidentiality"),
    ("vague-particulars", "handling", "Complaint lacks required particulars"),
    ("field-report-available", "handling", "Field or institute report is available"),
    ("time-barred", "handling", "Complaint relates to a past or closed period"),
    ("repeat-complaint", "handling", "Repeated complaint or duplicate issue"),
)


class AiPipelineError(BulkOperationValidationError):
    pass


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _tag_slug(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value or "").casefold()).strip("-")
    return cleaned[:120]


def _tag_display_name(value: str) -> str:
    cleaned = clean_name(value)
    return cleaned or "Tag"


def _clean_aliases(values: Iterable[str], *, slug: str) -> list[str]:
    aliases: list[str] = []
    seen = {slug}
    for value in values:
        label = clean_name(value)
        key = _tag_slug(label)
        if not label or not key or key in seen:
            continue
        seen.add(key)
        aliases.append(label[:140])
    return aliases[:50]


def _split_tags(value: str) -> list[str]:
    parts = re.split(r"[;,|]", value or "")
    result: list[str] = []
    for part in parts:
        name = normalized_name(part).replace(" ", "-")
        name = re.sub(r"[^a-z0-9-]+", "-", name).strip("-")
        if name and name not in result:
            result.append(name)
    return result


def _yes(value: str) -> bool:
    return str(value or "").strip().casefold() in {"yes", "y", "true", "1", "new"}


def _confidence(value: str) -> float:
    try:
        result = float(str(value or "").strip())
    except ValueError as exc:
        raise AiPipelineError("Confidence must be a number between 0 and 1") from exc
    if result < 0 or result > 1:
        raise AiPipelineError("Confidence must be a number between 0 and 1")
    return result


def _redact(value: str) -> str:
    text = value or ""
    text = re.sub(r"\b\d{5}-?\d{7}-?\d\b", "[CNIC REDACTED]", text)
    text = re.sub(r"\b(?:\+92|0092|0)?3\d{9}\b", "[PHONE REDACTED]", text)
    text = re.sub(
        r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", "[EMAIL REDACTED]", text, flags=re.I
    )
    return text


def _token_score(left: str, right: str) -> float:
    words = lambda value: {w for w in re.findall(r"[a-z0-9]{3,}", (value or "").casefold())}
    a, b = words(left), words(right)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


class CrmAiPipelineService:
    def __init__(self, session: Session, settings: Settings):
        self.session = session
        self.settings = settings
        self.bulk = CrmBulkOperationService(session, settings)

    def ensure_defaults(self) -> CrmPromptProfileVersion:
        profile = self.session.exec(
            select(CrmPromptProfile).where(CrmPromptProfile.slug == DEFAULT_REPLY_SOP_SLUG)
        ).first()
        if profile is None:
            profile = CrmPromptProfile(
                slug=DEFAULT_REPLY_SOP_SLUG,
                name=DEFAULT_REPLY_SOP_NAME,
                description="Versioned office instructions used for complaint reply context packages.",
                active=True,
            )
            self.session.add(profile)
            self.session.flush()
        version = self.session.exec(
            select(CrmPromptProfileVersion).where(
                CrmPromptProfileVersion.profile_id == profile.id,
                CrmPromptProfileVersion.version_label == DEFAULT_REPLY_SOP_VERSION,
            )
        ).first()
        if version is None:
            existing_active = self.session.exec(
                select(CrmPromptProfileVersion).where(
                    CrmPromptProfileVersion.profile_id == profile.id,
                    CrmPromptProfileVersion.is_active == True,  # noqa: E712
                )
            ).first()
            version = CrmPromptProfileVersion(
                profile_id=profile.id,
                version_label=DEFAULT_REPLY_SOP_VERSION,
                content=DEFAULT_REPLY_SOP_CONTENT,
                content_sha256=_hash_text(DEFAULT_REPLY_SOP_CONTENT),
                structured_json={
                    "source": "uploaded CM complaint reply master prompt and SOP",
                    "required_opening": "Respected Worthy Chief Executive Officer (DEA),",
                    "required_closing": "Submitted for kind perusal and further necessary action, please.",
                    "output_columns": ["Complaint Number", "Reply"],
                },
                is_active=existing_active is None,
                effective_from=datetime(2026, 6, 20),
                created_by="bundle-b3.4",
            )
            self.session.add(version)
        for slug, name, description, order in DEFAULT_TAG_GROUPS:
            if (
                self.session.exec(
                    select(CrmComplaintTagGroup).where(CrmComplaintTagGroup.slug == slug)
                ).first()
                is None
            ):
                self.session.add(
                    CrmComplaintTagGroup(
                        slug=slug,
                        name=name,
                        description=description,
                        display_order=order,
                    )
                )
        for name, group, description in DEFAULT_TAGS:
            existing_tag = self.session.exec(
                select(CrmComplaintTag).where(CrmComplaintTag.normalized_name == name)
            ).first()
            if existing_tag is None:
                self.session.add(
                    CrmComplaintTag(
                        name=name,
                        display_name=name.replace("-", " ").title(),
                        normalized_name=name,
                        group_name=group,
                        description=description,
                        aliases_json=[],
                        ai_available=True,
                    )
                )
            elif not existing_tag.display_name:
                existing_tag.display_name = existing_tag.name.replace("-", " ").title()
                self.session.add(existing_tag)
        self.session.commit()
        self.session.refresh(version)
        return version

    def active_prompt_version(self) -> CrmPromptProfileVersion:
        self.ensure_defaults()
        version = self.session.exec(
            select(CrmPromptProfileVersion)
            .where(CrmPromptProfileVersion.is_active == True)  # noqa: E712
            .order_by(CrmPromptProfileVersion.created_at.desc())
        ).first()
        if version is None:
            raise AiPipelineError("No active reply SOP version is configured")
        return version

    def prompt_profiles(self) -> dict[str, Any]:
        active = self.active_prompt_version()
        profiles = list(
            self.session.exec(select(CrmPromptProfile).order_by(CrmPromptProfile.name)).all()
        )
        versions = list(
            self.session.exec(
                select(CrmPromptProfileVersion).order_by(
                    CrmPromptProfileVersion.profile_id,
                    CrmPromptProfileVersion.created_at.desc(),
                )
            ).all()
        )
        by_profile: dict[uuid.UUID, list[dict[str, Any]]] = {}
        for row in versions:
            by_profile.setdefault(row.profile_id, []).append(
                self._version_payload(row, include_content=False)
            )
        return {
            "active_version_id": str(active.id),
            "profiles": [
                {
                    "id": str(row.id),
                    "slug": row.slug,
                    "name": row.name,
                    "description": row.description or "",
                    "active": row.active,
                    "versions": by_profile.get(row.id, []),
                }
                for row in profiles
            ],
        }

    @staticmethod
    def _version_payload(row: CrmPromptProfileVersion, *, include_content: bool) -> dict[str, Any]:
        result = {
            "id": str(row.id),
            "profile_id": str(row.profile_id),
            "version_label": row.version_label,
            "content_sha256": row.content_sha256,
            "structured": row.structured_json,
            "is_active": row.is_active,
            "effective_from": row.effective_from,
            "created_by": row.created_by,
            "created_at": row.created_at,
        }
        if include_content:
            result["content"] = row.content
        return result

    def prompt_version(self, version_id: uuid.UUID) -> dict[str, Any]:
        self.ensure_defaults()
        row = self.session.get(CrmPromptProfileVersion, version_id)
        if row is None:
            raise BulkOperationNotFound("Prompt profile version was not found")
        return self._version_payload(row, include_content=True)

    def create_prompt_version(
        self,
        profile_id: uuid.UUID,
        *,
        version_label: str,
        content: str,
        activate: bool,
        actor: str,
    ) -> dict[str, Any]:
        profile = self.session.get(CrmPromptProfile, profile_id)
        if profile is None:
            raise BulkOperationNotFound("Prompt profile was not found")
        label = clean_name(version_label)
        body = str(content or "").strip()
        if not label or not body:
            raise AiPipelineError("Version label and SOP content are required")
        if self.session.exec(
            select(CrmPromptProfileVersion).where(
                CrmPromptProfileVersion.profile_id == profile.id,
                CrmPromptProfileVersion.version_label == label,
            )
        ).first():
            raise AiPipelineError("This prompt profile version already exists")
        if activate:
            for old in self.session.exec(
                select(CrmPromptProfileVersion).where(
                    CrmPromptProfileVersion.profile_id == profile.id,
                    CrmPromptProfileVersion.is_active == True,  # noqa: E712
                )
            ).all():
                old.is_active = False
                self.session.add(old)
        row = CrmPromptProfileVersion(
            profile_id=profile.id,
            version_label=label,
            content=body,
            content_sha256=_hash_text(body),
            is_active=activate,
            effective_from=utcnow() if activate else None,
            created_by=actor or "web-operator",
        )
        self.session.add(row)
        profile.updated_at = utcnow()
        self.session.add(profile)
        self.session.commit()
        self.session.refresh(row)
        return self._version_payload(row, include_content=True)

    def activate_prompt_version(self, version_id: uuid.UUID, *, actor: str) -> dict[str, Any]:
        row = self.session.get(CrmPromptProfileVersion, version_id)
        if row is None:
            raise BulkOperationNotFound("Prompt profile version was not found")
        for old in self.session.exec(
            select(CrmPromptProfileVersion).where(
                CrmPromptProfileVersion.profile_id == row.profile_id,
                CrmPromptProfileVersion.is_active == True,  # noqa: E712
            )
        ).all():
            old.is_active = False
            self.session.add(old)
        row.is_active = True
        row.effective_from = row.effective_from or utcnow()
        self.session.add(row)
        self.session.add(
            ComplaintAuditEvent(
                entity_type="prompt_profile_version",
                entity_id=str(row.id),
                event_type="prompt_profile_activated",
                actor=actor or "web-operator",
                details_json={"version_label": row.version_label},
            )
        )
        self.session.commit()
        return self._version_payload(row, include_content=True)

    def statistics(self) -> dict[str, Any]:
        active = self.active_prompt_version()
        eligible = (
            self.session.scalar(
                select(func.count())
                .select_from(ComplaintCase)
                .where(reply_case_eligibility_clause())
            )
            or 0
        )
        classified = (
            self.session.scalar(
                select(func.count())
                .select_from(ComplaintCase)
                .where(
                    reply_case_eligibility_clause(),
                    ComplaintCase.category_id.is_not(None),
                )
            )
            or 0
        )
        review = (
            self.session.scalar(
                select(func.count())
                .select_from(CrmComplaintClassification)
                .where(CrmComplaintClassification.status == "review_required")
            )
            or 0
        )
        replies = self.session.scalar(select(func.count()).select_from(ComplaintReply)) or 0
        suggestions = (
            self.session.scalar(
                select(func.count())
                .select_from(CrmTaxonomySuggestion)
                .where(CrmTaxonomySuggestion.status == "pending")
            )
            or 0
        )
        return {
            "published_cases": int(eligible),
            "reply_eligible_cases": int(eligible),
            "awaiting_classification": max(0, int(eligible) - int(classified)),
            "classified_cases": int(classified),
            "needs_review": int(review),
            "awaiting_reply": max(0, int(eligible) - int(replies)),
            "pending_taxonomy_suggestions": int(suggestions),
            "active_prompt_version": active.version_label,
            "active_prompt_version_id": str(active.id),
        }

    def _taxonomy_rows(self) -> tuple[list[ComplaintCategory], list[ComplaintSubcategory]]:
        categories = list(
            self.session.exec(
                select(ComplaintCategory)
                .where(ComplaintCategory.active == True)  # noqa: E712
                .order_by(ComplaintCategory.display_order, ComplaintCategory.name)
            ).all()
        )
        subcategories = list(
            self.session.exec(
                select(ComplaintSubcategory)
                .where(ComplaintSubcategory.active == True)  # noqa: E712
                .order_by(
                    ComplaintSubcategory.category_id,
                    ComplaintSubcategory.display_order,
                    ComplaintSubcategory.name,
                )
            ).all()
        )
        return categories, subcategories

    def create_classification_export_batch(
        self,
        *,
        scope: str = "unclassified",
        case_ids: list[uuid.UUID] | None = None,
        actor: str = "web-operator",
    ) -> dict[str, Any]:
        self.ensure_defaults()
        if scope not in {"unclassified", "all", "selected"}:
            raise AiPipelineError("Classification scope must be unclassified, all or selected")
        statement = select(ComplaintCase).where(reply_case_eligibility_clause())
        if scope == "unclassified":
            statement = statement.where(ComplaintCase.category_id.is_(None))
        elif scope == "selected":
            selected = list(dict.fromkeys(case_ids or []))
            if not selected:
                raise AiPipelineError("Select at least one complaint")
            statement = statement.where(ComplaintCase.id.in_(selected))
        cases = list(self.session.exec(statement.order_by(ComplaintCase.complaint_number)).all())
        if not cases:
            raise AiPipelineError("No complaints match the classification scope")

        categories, subcategories = self._taxonomy_rows()
        category_by_id = {row.id: row for row in categories}
        tags = list(
            self.session.exec(
                select(CrmComplaintTag)
                .where(
                    CrmComplaintTag.active == True,  # noqa: E712
                    CrmComplaintTag.ai_available == True,  # noqa: E712
                    CrmComplaintTag.merged_into_id.is_(None),
                )
                .order_by(
                    CrmComplaintTag.group_name, CrmComplaintTag.display_name, CrmComplaintTag.name
                )
            ).all()
        )
        now = utcnow()
        batch = self.bulk._create_batch(
            "classification_export",
            actor=actor,
            status="processing",
            scope={"scope": scope, "case_ids": [str(value) for value in case_ids or []]},
            settings={"format": "classification_csv_v1", "existing_taxonomy_preferred": True},
        )
        batch.started_at = now
        complaint_rows: list[list[str]] = []
        template_rows: list[list[str]] = []
        for case in cases:
            number = case.complaint_number or str(case.id)
            complaint_rows.append([number, case.remarks or ""])
            template_rows.append([number, "", "", "", "", "No", "", ""])
            self.session.add(
                CrmBulkOperationItem(
                    batch_id=batch.id,
                    complaint_case_id=case.id,
                    complaint_number_snapshot=number,
                    status="exported",
                    details_json={
                        "category_at_export": case.category,
                        "subcategory_at_export": case.sub_category,
                    },
                    processed_at=now,
                )
            )
        taxonomy_rows = [
            [
                category_by_id[sub.category_id].name,
                sub.name,
                category_by_id[sub.category_id].description or "",
                sub.description or "",
                category_by_id[sub.category_id].reply_guidance or "",
                category_by_id[sub.category_id].policy_notes or "",
                sub.reply_guidance or "",
                sub.policy_notes or "",
            ]
            for sub in subcategories
            if sub.category_id in category_by_id
        ]
        tag_rows = [
            [
                row.name,
                row.display_name or row.name.replace("-", " ").title(),
                row.group_name,
                row.description or "",
                "; ".join(row.aliases_json or []),
            ]
            for row in tags
        ]
        example = _csv_bytes(
            CLASSIFICATION_HEADERS,
            [
                [
                    "104-1234567",
                    "Fees",
                    "Summer Vacation Fee",
                    "private-school;advance-fee;no-evidence",
                    "0.96",
                    "No",
                    "",
                    "Advance vacation fee complaint",
                ]
            ],
        )
        summary = {
            "batch_id": str(batch.id),
            "batch_number": batch.batch_number,
            "created_at": now.isoformat(),
            "complaints": len(cases),
            "required_columns": CLASSIFICATION_HEADERS,
            "rule": "Prefer existing taxonomy and controlled tags; suggestions require human approval.",
        }
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "01-complaints-to-classify.csv",
                _csv_bytes(["Complaint Number", "Complaint Remarks"], complaint_rows),
            )
            archive.writestr(
                "02-existing-taxonomy.csv",
                _csv_bytes(
                    [
                        "Category",
                        "Subcategory",
                        "Category Description",
                        "Subcategory Description",
                        "Category Reply Guidance",
                        "Category Policy Notes",
                        "Subcategory Reply Guidance",
                        "Subcategory Policy Notes",
                    ],
                    taxonomy_rows,
                ),
            )
            archive.writestr(
                "03-existing-tags.csv",
                _csv_bytes(["Tag", "Display Name", "Group", "Description", "Aliases"], tag_rows),
            )
            archive.writestr("04-classification-instructions.txt", CLASSIFICATION_INSTRUCTIONS)
            archive.writestr(
                "05-classification-output-template.csv",
                _csv_bytes(CLASSIFICATION_HEADERS, template_rows),
            )
            archive.writestr("06-completed-classification-sample.csv", example)
            archive.writestr("batch-summary.json", json.dumps(summary, indent=2))
        self.bulk._write_artifact(
            batch,
            kind="classification_package",
            name=f"{batch.batch_number}.zip",
            content_type="application/zip",
            content=output.getvalue(),
        )
        batch.total_items = len(cases)
        batch.valid_items = len(cases)
        batch.successful_items = len(cases)
        batch.status = "completed"
        batch.completed_at = now
        batch.updated_at = now
        self.session.add(batch)
        self.session.commit()
        return self.bulk.batch_detail(batch.id)

    def validate_classification_import(
        self,
        *,
        content: bytes,
        filename: str,
        parent_batch_id: uuid.UUID,
        actor: str = "web-operator",
        auto_accept_threshold: float = 0.85,
    ) -> dict[str, Any]:
        parent = self.bulk._require_batch(parent_batch_id)
        if parent.operation_type != "classification_export" or parent.status != "completed":
            raise AiPipelineError("The related classification export batch must be completed")
        if auto_accept_threshold < 0 or auto_accept_threshold > 1:
            raise AiPipelineError("Auto-accept threshold must be between 0 and 1")
        expected = {
            item.complaint_number_snapshot: item
            for item in self.session.exec(
                select(CrmBulkOperationItem).where(
                    CrmBulkOperationItem.batch_id == parent.id,
                    CrmBulkOperationItem.status == "exported",
                )
            ).all()
        }
        safe_filename = _safe_name(filename, "classification.csv")
        now = utcnow()
        batch = self.bulk._create_batch(
            "classification_import",
            actor=actor,
            parent_batch_id=parent.id,
            source_filename=safe_filename,
            source_sha256=_sha256_bytes(content),
            status="validating",
            scope={"parent_classification_export_id": str(parent.id)},
            settings={
                "auto_accept_threshold": auto_accept_threshold,
                "format": "classification_csv_v1",
            },
        )
        batch.started_at = now
        self.bulk._write_artifact(
            batch,
            kind="source_csv",
            name=safe_filename,
            content_type="text/csv; charset=utf-8",
            content=content,
        )
        errors: list[list[Any]] = []
        seen: Counter[str] = Counter()
        valid = invalid = missing = duplicates = 0
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = ""
            errors.append(
                ["", "", "invalid_encoding", "The classification CSV must use UTF-8 encoding."]
            )
        if text:
            reader = csv.DictReader(io.StringIO(text))
            if list(reader.fieldnames or []) != CLASSIFICATION_HEADERS:
                errors.append(
                    [
                        "",
                        "",
                        "invalid_headers",
                        "The CSV columns must exactly match the classification template.",
                    ]
                )
            else:
                for source_row, row in enumerate(reader, start=2):
                    raw_number = str(row.get("Complaint Number") or "").strip()
                    number = normalize_complaint_number(raw_number)
                    if not number:
                        invalid += 1
                        errors.append(
                            [
                                source_row,
                                raw_number,
                                "missing_complaint_number",
                                "Complaint Number is required.",
                            ]
                        )
                        continue
                    seen[number] += 1
                    if seen[number] > 1:
                        duplicates += 1
                        invalid += 1
                        errors.append(
                            [
                                source_row,
                                number,
                                "duplicate_complaint_number",
                                "Complaint Number occurs more than once.",
                            ]
                        )
                        self.session.add(
                            CrmBulkOperationItem(
                                batch_id=batch.id,
                                complaint_number_snapshot=number,
                                source_row=source_row,
                                status="invalid",
                                error_code="duplicate_complaint_number",
                                error_message="Complaint Number occurs more than once.",
                            )
                        )
                        continue
                    export_item = expected.get(number)
                    if export_item is None:
                        invalid += 1
                        errors.append(
                            [
                                source_row,
                                number,
                                "outside_export_batch",
                                "Complaint was not included in the related classification export.",
                            ]
                        )
                        self.session.add(
                            CrmBulkOperationItem(
                                batch_id=batch.id,
                                complaint_number_snapshot=number,
                                source_row=source_row,
                                status="invalid",
                                error_code="outside_export_batch",
                                error_message="Complaint was not included in the related classification export.",
                            )
                        )
                        continue
                    try:
                        confidence = _confidence(str(row.get("Confidence") or ""))
                    except AiPipelineError as exc:
                        invalid += 1
                        errors.append([source_row, number, "invalid_confidence", str(exc)])
                        self.session.add(
                            CrmBulkOperationItem(
                                batch_id=batch.id,
                                complaint_case_id=export_item.complaint_case_id,
                                complaint_number_snapshot=number,
                                source_row=source_row,
                                status="invalid",
                                error_code="invalid_confidence",
                                error_message=str(exc),
                            )
                        )
                        continue
                    category = clean_name(str(row.get("Category") or ""))
                    subcategory = clean_name(str(row.get("Subcategory") or ""))
                    needs_new = _yes(str(row.get("Needs New Taxonomy") or ""))
                    proposed = clean_name(str(row.get("Proposed Name") or ""))
                    reason = clean_name(str(row.get("Reason") or ""))
                    tags = _split_tags(str(row.get("Tags") or ""))
                    if not category and not needs_new:
                        invalid += 1
                        message = (
                            "Choose an existing category or mark the row as needing new taxonomy."
                        )
                        errors.append([source_row, number, "missing_category", message])
                        self.session.add(
                            CrmBulkOperationItem(
                                batch_id=batch.id,
                                complaint_case_id=export_item.complaint_case_id,
                                complaint_number_snapshot=number,
                                source_row=source_row,
                                status="invalid",
                                error_code="missing_category",
                                error_message=message,
                            )
                        )
                        continue
                    if needs_new and not proposed:
                        invalid += 1
                        message = "Proposed Name is required when Needs New Taxonomy is Yes."
                        errors.append([source_row, number, "missing_proposed_name", message])
                        self.session.add(
                            CrmBulkOperationItem(
                                batch_id=batch.id,
                                complaint_case_id=export_item.complaint_case_id,
                                complaint_number_snapshot=number,
                                source_row=source_row,
                                status="invalid",
                                error_code="missing_proposed_name",
                                error_message=message,
                            )
                        )
                        continue
                    self.session.add(
                        CrmBulkOperationItem(
                            batch_id=batch.id,
                            complaint_case_id=export_item.complaint_case_id,
                            complaint_number_snapshot=number,
                            source_row=source_row,
                            status="valid",
                            details_json={
                                "category": category,
                                "subcategory": subcategory,
                                "tags": tags,
                                "confidence": confidence,
                                "needs_new_taxonomy": needs_new,
                                "proposed_name": proposed,
                                "reason": reason,
                            },
                        )
                    )
                    valid += 1
        for number, export_item in expected.items():
            if not seen[number]:
                missing += 1
                errors.append(
                    [
                        "",
                        number,
                        "missing_from_import",
                        "Complaint from the export batch is missing from the classification CSV.",
                    ]
                )
                self.session.add(
                    CrmBulkOperationItem(
                        batch_id=batch.id,
                        complaint_case_id=export_item.complaint_case_id,
                        complaint_number_snapshot=number,
                        status="missing",
                        error_code="missing_from_import",
                        error_message="Complaint from the export batch is missing from the classification CSV.",
                    )
                )
        batch.total_items = valid + invalid + missing
        batch.valid_items = valid
        batch.failed_items = invalid + missing
        batch.duplicate_items = duplicates
        batch.status = "ready" if valid else "failed"
        if errors:
            batch.error_summary = f"{invalid + missing} row(s) require correction or review."
            self.bulk._write_artifact(
                batch,
                kind="validation_errors",
                name=f"{batch.batch_number}-validation-errors.csv",
                content_type="text/csv; charset=utf-8",
                content=_csv_bytes(
                    ["Source Row", "Complaint Number", "Error Code", "Error"], errors
                ),
            )
        batch.updated_at = utcnow()
        self.session.add(batch)
        self.session.commit()
        return self.bulk.batch_detail(batch.id)

    def _category_lookup(
        self,
    ) -> tuple[dict[str, ComplaintCategory], dict[tuple[uuid.UUID, str], ComplaintSubcategory]]:
        categories, subs = self._taxonomy_rows()
        return (
            {row.normalized_name: row for row in categories},
            {(row.category_id, row.normalized_name): row for row in subs},
        )

    def _tag_lookup(self) -> dict[str, CrmComplaintTag]:
        lookup: dict[str, CrmComplaintTag] = {}
        rows = self.session.exec(
            select(CrmComplaintTag).where(
                CrmComplaintTag.active == True,  # noqa: E712
                CrmComplaintTag.ai_available == True,  # noqa: E712
                CrmComplaintTag.merged_into_id.is_(None),
            )
        ).all()
        for row in rows:
            for value in (
                row.normalized_name,
                row.name,
                row.display_name,
                *(row.aliases_json or []),
            ):
                key = _tag_slug(value)
                if key:
                    lookup[key] = row
        return lookup

    def commit_classification_import(
        self,
        batch_id: uuid.UUID,
        *,
        allow_partial: bool = False,
        actor: str = "web-operator",
    ) -> dict[str, Any]:
        self.ensure_defaults()
        batch = self.bulk._require_batch(batch_id)
        if batch.operation_type != "classification_import" or batch.status != "ready":
            raise AiPipelineError("The classification import batch is not ready to commit")
        if batch.failed_items and not allow_partial:
            raise AiPipelineError(
                "Strict classification import is enabled. Correct every invalid or missing row first."
            )
        threshold = float((batch.settings_json or {}).get("auto_accept_threshold", 0.85))
        categories, subs = self._category_lookup()
        tags = self._tag_lookup()
        now = utcnow()
        auto_accepted = review_required = 0
        items = list(
            self.session.exec(
                select(CrmBulkOperationItem)
                .where(
                    CrmBulkOperationItem.batch_id == batch.id,
                    CrmBulkOperationItem.status == "valid",
                )
                .order_by(CrmBulkOperationItem.source_row)
            ).all()
        )
        for item in items:
            case = self.session.get(ComplaintCase, item.complaint_case_id)
            if case is None:
                item.status = "failed"
                item.error_code = "complaint_not_found_at_commit"
                item.error_message = "Complaint was not found at commit time."
                batch.failed_items += 1
                self.session.add(item)
                continue
            details = item.details_json or {}
            category_name = clean_name(str(details.get("category") or ""))
            sub_name = clean_name(str(details.get("subcategory") or ""))
            category = categories.get(normalized_name(category_name))
            sub = (
                subs.get((category.id, normalized_name(sub_name)))
                if category and sub_name
                else None
            )
            tag_names = list(details.get("tags") or [])
            known_tags = [tags[name] for name in tag_names if name in tags]
            unknown_tags = [name for name in tag_names if name not in tags]
            needs_new = bool(details.get("needs_new_taxonomy"))
            confidence = float(details.get("confidence") or 0)
            proposals: list[tuple[str, str, uuid.UUID | None]] = []
            if category is None:
                proposals.append(
                    (
                        "category",
                        clean_name(str(details.get("proposed_name") or category_name)),
                        None,
                    )
                )
            elif sub_name and sub is None:
                proposals.append(
                    (
                        "subcategory",
                        clean_name(str(details.get("proposed_name") or sub_name)),
                        category.id,
                    )
                )
            for name in unknown_tags:
                proposals.append(("tag", name, None))
            if needs_new and not proposals:
                explicit = clean_name(str(details.get("proposed_name") or ""))
                if explicit:
                    proposals.append(
                        (
                            "subcategory" if category else "category",
                            explicit,
                            category.id if category else None,
                        )
                    )
            accept = bool(
                category
                and (sub or not sub_name)
                and not needs_new
                and not proposals
                and confidence >= threshold
            )
            for previous in self.session.exec(
                select(CrmComplaintClassification).where(
                    CrmComplaintClassification.complaint_case_id == case.id,
                    CrmComplaintClassification.status.in_(
                        ("pending", "auto_accepted", "review_required", "approved")
                    ),
                )
            ).all():
                previous.status = "superseded"
                previous.updated_at = now
                self.session.add(previous)
            classification = CrmComplaintClassification(
                complaint_case_id=case.id,
                batch_id=batch.id,
                suggested_category_id=category.id if category else None,
                suggested_subcategory_id=sub.id if sub else None,
                suggested_category_name=category_name or None,
                suggested_subcategory_name=sub_name or None,
                approved_category_id=category.id if accept and category else None,
                approved_subcategory_id=sub.id if accept and sub else None,
                confidence=confidence,
                reason=str(details.get("reason") or ""),
                tags_json=tag_names,
                status="auto_accepted" if accept else "review_required",
                decision_source="ai_csv",
                taxonomy_snapshot_json={
                    "category_version": category.version if category else None,
                    "subcategory_version": sub.version if sub else None,
                    "auto_accept_threshold": threshold,
                },
                decided_by=actor if accept else None,
                decided_at=now if accept else None,
            )
            self.session.add(classification)
            self.session.flush()
            if accept and category:
                case.category_id = category.id
                case.category = category.name
                case.sub_category_id = sub.id if sub else None
                case.sub_category = sub.name if sub else None
                case.classification_sync_status = "pending"
                case.updated_at = now
                self.session.add(case)
                for tag in known_tags:
                    if (
                        self.session.exec(
                            select(CrmComplaintTagLink).where(
                                CrmComplaintTagLink.complaint_case_id == case.id,
                                CrmComplaintTagLink.tag_id == tag.id,
                            )
                        ).first()
                        is None
                    ):
                        self.session.add(
                            CrmComplaintTagLink(
                                complaint_case_id=case.id,
                                tag_id=tag.id,
                                classification_id=classification.id,
                                source="ai_auto_accepted",
                                created_by=actor,
                            )
                        )
                item.status = "classified"
                auto_accepted += 1
            else:
                item.status = "review_required"
                review_required += 1
            for proposal_type, proposed_name, parent_id in proposals:
                if proposed_name:
                    self.session.add(
                        CrmTaxonomySuggestion(
                            classification_id=classification.id,
                            proposal_type=proposal_type,
                            parent_category_id=parent_id,
                            proposed_name=proposed_name,
                            normalized_name=_tag_slug(proposed_name)
                            if proposal_type == "tag"
                            else normalized_name(proposed_name),
                            proposed_group_name="issue" if proposal_type == "tag" else None,
                            reason=classification.reason,
                        )
                    )
            item.details_json = {
                **details,
                "classification_id": str(classification.id),
                "auto_accepted": accept,
            }
            item.processed_at = now
            self.session.add(item)
            self.session.add(
                ComplaintAuditEvent(
                    complaint_case_id=case.id,
                    entity_type="complaint_classification",
                    entity_id=str(classification.id),
                    event_type="classification_auto_accepted"
                    if accept
                    else "classification_review_required",
                    actor=actor,
                    after_json={
                        "category": category.name if category else category_name,
                        "subcategory": sub.name if sub else sub_name,
                        "confidence": confidence,
                        "tags": tag_names,
                    },
                    details_json={"batch_number": batch.batch_number},
                )
            )
        batch.successful_items = auto_accepted
        batch.skipped_items = review_required
        batch.status = "completed_with_errors" if batch.failed_items else "completed"
        batch.completed_at = now
        batch.updated_at = now
        batch.settings_json = {
            **(batch.settings_json or {}),
            "committed_by": actor,
            "allow_partial": allow_partial,
        }
        self.session.add(batch)
        self.session.commit()
        result = self.bulk.batch_detail(batch.id)
        result["commit_summary"] = {
            "auto_accepted": auto_accepted,
            "review_required": review_required,
        }
        return result

    @staticmethod
    def _classification_payload(
        classification: CrmComplaintClassification,
        case: ComplaintCase,
        category: ComplaintCategory | None,
        subcategory: ComplaintSubcategory | None,
    ) -> dict[str, Any]:
        return {
            "id": str(classification.id),
            "case_id": str(case.id),
            "complaint_number": case.complaint_number or str(case.id),
            "complaint_preview": (case.remarks or "").replace("\n", " ")[:360],
            "suggested_category_id": str(classification.suggested_category_id)
            if classification.suggested_category_id
            else None,
            "suggested_subcategory_id": str(classification.suggested_subcategory_id)
            if classification.suggested_subcategory_id
            else None,
            "suggested_category": category.name
            if category
            else classification.suggested_category_name or "",
            "suggested_subcategory": subcategory.name
            if subcategory
            else classification.suggested_subcategory_name or "",
            "confidence": classification.confidence,
            "reason": classification.reason or "",
            "tags": classification.tags_json,
            "status": classification.status,
            "created_at": classification.created_at,
        }

    def review_queue(
        self, *, search: str = "", page: int = 1, page_size: int = 25
    ) -> dict[str, Any]:
        filters: list[Any] = [CrmComplaintClassification.status == "review_required"]
        if search.strip():
            term = f"%{search.strip()}%"
            filters.append(
                or_(ComplaintCase.complaint_number.ilike(term), ComplaintCase.remarks.ilike(term))
            )
        count = (
            self.session.scalar(
                select(func.count())
                .select_from(CrmComplaintClassification)
                .join(
                    ComplaintCase, ComplaintCase.id == CrmComplaintClassification.complaint_case_id
                )
                .where(*filters)
            )
            or 0
        )
        rows = self.session.exec(
            select(CrmComplaintClassification, ComplaintCase)
            .join(ComplaintCase, ComplaintCase.id == CrmComplaintClassification.complaint_case_id)
            .where(*filters)
            .order_by(
                CrmComplaintClassification.confidence.asc(), CrmComplaintClassification.created_at
            )
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all()
        category_ids = {row.suggested_category_id for row, _ in rows if row.suggested_category_id}
        sub_ids = {row.suggested_subcategory_id for row, _ in rows if row.suggested_subcategory_id}
        category_map = (
            {
                row.id: row
                for row in self.session.exec(
                    select(ComplaintCategory).where(ComplaintCategory.id.in_(category_ids))
                ).all()
            }
            if category_ids
            else {}
        )
        sub_map = (
            {
                row.id: row
                for row in self.session.exec(
                    select(ComplaintSubcategory).where(ComplaintSubcategory.id.in_(sub_ids))
                ).all()
            }
            if sub_ids
            else {}
        )
        return {
            "items": [
                self._classification_payload(
                    row,
                    case,
                    category_map.get(row.suggested_category_id),
                    sub_map.get(row.suggested_subcategory_id),
                )
                for row, case in rows
            ],
            "total": int(count),
            "page": page,
            "page_size": page_size,
        }

    def resolve_classification(
        self,
        classification_id: uuid.UUID,
        *,
        category_id: uuid.UUID | None,
        subcategory_id: uuid.UUID | None,
        tag_ids: list[uuid.UUID],
        decision: str,
        actor: str,
    ) -> dict[str, Any]:
        row = self.session.get(CrmComplaintClassification, classification_id)
        if row is None:
            raise BulkOperationNotFound("Classification decision was not found")
        if row.status != "review_required":
            raise AiPipelineError("Only classifications awaiting review can be resolved")
        if decision not in {"approve", "reject"}:
            raise AiPipelineError("Decision must be approve or reject")
        case = self.session.get(ComplaintCase, row.complaint_case_id)
        if case is None:
            raise AiPipelineError("Complaint was not found")
        now = utcnow()
        batch_item = (
            self.session.exec(
                select(CrmBulkOperationItem).where(
                    CrmBulkOperationItem.batch_id == row.batch_id,
                    CrmBulkOperationItem.complaint_case_id == row.complaint_case_id,
                    CrmBulkOperationItem.status == "review_required",
                )
            ).first()
            if row.batch_id
            else None
        )
        suggestions = list(
            self.session.exec(
                select(CrmTaxonomySuggestion).where(
                    CrmTaxonomySuggestion.classification_id == row.id,
                    CrmTaxonomySuggestion.status == "pending",
                )
            ).all()
        )
        if decision == "reject":
            row.status = "rejected"
            row.decided_by = actor
            row.decided_at = now
            row.updated_at = now
            self.session.add(row)
            if batch_item:
                batch_item.status = "rejected"
                batch_item.processed_at = now
                self.session.add(batch_item)
            for suggestion in suggestions:
                suggestion.status = "rejected"
                suggestion.decided_by = actor
                suggestion.decided_at = now
                self.session.add(suggestion)
            self.session.commit()
            return {"id": str(row.id), "status": row.status}
        if category_id is None:
            raise AiPipelineError("Select an active category")
        category = self.session.get(ComplaintCategory, category_id)
        if category is None or not category.active:
            raise AiPipelineError("Active category was not found")
        subcategory = (
            self.session.get(ComplaintSubcategory, subcategory_id) if subcategory_id else None
        )
        if subcategory and (subcategory.category_id != category.id or not subcategory.active):
            raise AiPipelineError("Subcategory does not belong to the selected category")
        row.status = "approved"
        row.approved_category_id = category.id
        row.approved_subcategory_id = subcategory.id if subcategory else None
        row.decided_by = actor
        row.decided_at = now
        row.updated_at = now
        case.category_id = category.id
        case.category = category.name
        case.sub_category_id = subcategory.id if subcategory else None
        case.sub_category = subcategory.name if subcategory else None
        case.classification_sync_status = "pending"
        case.updated_at = now
        self.session.add(case)
        self.session.add(row)
        for tag_id in list(dict.fromkeys(tag_ids)):
            tag = self.session.get(CrmComplaintTag, tag_id)
            if tag is None or not tag.active:
                continue
            if (
                self.session.exec(
                    select(CrmComplaintTagLink).where(
                        CrmComplaintTagLink.complaint_case_id == case.id,
                        CrmComplaintTagLink.tag_id == tag.id,
                    )
                ).first()
                is None
            ):
                self.session.add(
                    CrmComplaintTagLink(
                        complaint_case_id=case.id,
                        tag_id=tag.id,
                        classification_id=row.id,
                        source="human_review",
                        created_by=actor,
                    )
                )
        if batch_item:
            batch_item.status = "classified"
            batch_item.processed_at = now
            batch_item.details_json = {**(batch_item.details_json or {}), "human_approved": True}
            self.session.add(batch_item)
        for suggestion in suggestions:
            if suggestion.proposal_type == "category":
                suggestion.status = "merged"
                suggestion.resolved_category_id = category.id
            elif suggestion.proposal_type == "subcategory" and subcategory:
                suggestion.status = "merged"
                suggestion.resolved_subcategory_id = subcategory.id
            else:
                continue
            suggestion.decided_by = actor
            suggestion.decided_at = now
            self.session.add(suggestion)
        self.session.add(
            ComplaintAuditEvent(
                complaint_case_id=case.id,
                entity_type="complaint_classification",
                entity_id=str(row.id),
                event_type="classification_approved",
                actor=actor,
                after_json={
                    "category": category.name,
                    "subcategory": subcategory.name if subcategory else None,
                },
            )
        )
        self.session.commit()
        return {"id": str(row.id), "status": row.status, "case_id": str(case.id)}

    def taxonomy_suggestions(
        self,
        *,
        status: str = "pending",
        search: str = "",
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        if status not in {"pending", "approved", "merged", "rejected", "deferred", "all"}:
            raise AiPipelineError("Taxonomy suggestion status is invalid")
        filters: list[Any] = []
        if status != "all":
            filters.append(CrmTaxonomySuggestion.status == status)
        if search.strip():
            term = f"%{search.strip()}%"
            filters.append(
                or_(
                    CrmTaxonomySuggestion.proposed_name.ilike(term),
                    CrmTaxonomySuggestion.reason.ilike(term),
                    ComplaintCase.complaint_number.ilike(term),
                )
            )
        base = (
            select(CrmTaxonomySuggestion, CrmComplaintClassification, ComplaintCase)
            .join(
                CrmComplaintClassification,
                CrmComplaintClassification.id == CrmTaxonomySuggestion.classification_id,
            )
            .join(ComplaintCase, ComplaintCase.id == CrmComplaintClassification.complaint_case_id)
            .where(*filters)
        )
        count = (
            self.session.scalar(
                select(func.count())
                .select_from(CrmTaxonomySuggestion)
                .join(
                    CrmComplaintClassification,
                    CrmComplaintClassification.id == CrmTaxonomySuggestion.classification_id,
                )
                .join(
                    ComplaintCase, ComplaintCase.id == CrmComplaintClassification.complaint_case_id
                )
                .where(*filters)
            )
            or 0
        )
        rows = self.session.exec(
            base.order_by(
                CrmTaxonomySuggestion.supporting_count.desc(),
                CrmTaxonomySuggestion.created_at.asc(),
            )
            .offset((page - 1) * page_size)
            .limit(page_size)
        ).all()
        return {
            "items": [
                {
                    "id": str(suggestion.id),
                    "classification_id": str(classification.id),
                    "complaint_number": case.complaint_number or str(case.id),
                    "proposal_type": suggestion.proposal_type,
                    "proposed_name": suggestion.proposed_name,
                    "reason": suggestion.reason or "",
                    "supporting_count": suggestion.supporting_count,
                    "status": suggestion.status,
                    "parent_category_id": str(suggestion.parent_category_id)
                    if suggestion.parent_category_id
                    else None,
                    "proposed_group_name": suggestion.proposed_group_name,
                    "created_at": suggestion.created_at,
                }
                for suggestion, classification, case in rows
            ],
            "total": int(count),
            "page": page,
            "page_size": page_size,
        }

    def resolve_taxonomy_suggestion(
        self,
        suggestion_id: uuid.UUID,
        *,
        status: str,
        actor: str,
        resolved_category_id: uuid.UUID | None = None,
        resolved_subcategory_id: uuid.UUID | None = None,
        resolved_tag_id: uuid.UUID | None = None,
    ) -> dict[str, Any]:
        if status not in {"merged", "rejected", "deferred"}:
            raise AiPipelineError("Suggestion decision must be merged, rejected or deferred")
        row = self.session.get(CrmTaxonomySuggestion, suggestion_id)
        if row is None:
            raise BulkOperationNotFound("Taxonomy suggestion was not found")
        if row.status != "pending":
            raise AiPipelineError("Only pending taxonomy suggestions can be resolved")
        if (
            resolved_category_id
            and self.session.get(ComplaintCategory, resolved_category_id) is None
        ):
            raise AiPipelineError("Resolved category was not found")
        if (
            resolved_subcategory_id
            and self.session.get(ComplaintSubcategory, resolved_subcategory_id) is None
        ):
            raise AiPipelineError("Resolved subcategory was not found")
        if resolved_tag_id and self.session.get(CrmComplaintTag, resolved_tag_id) is None:
            raise AiPipelineError("Resolved tag was not found")
        row.status = status
        row.resolved_category_id = resolved_category_id
        row.resolved_subcategory_id = resolved_subcategory_id
        row.resolved_tag_id = resolved_tag_id
        row.decided_by = actor or "web-operator"
        row.decided_at = utcnow()
        self.session.add(row)
        self.session.commit()
        return {"id": str(row.id), "status": row.status}

    @staticmethod
    def _tag_payload(row: CrmComplaintTag, *, usage_count: int = 0) -> dict[str, Any]:
        return {
            "id": str(row.id),
            "name": row.name,
            "display_name": row.display_name or row.name.replace("-", " ").title(),
            "group": row.group_name,
            "description": row.description or "",
            "aliases": list(row.aliases_json or []),
            "ai_available": row.ai_available,
            "active": row.active,
            "merged_into_id": str(row.merged_into_id) if row.merged_into_id else None,
            "version": row.version,
            "usage_count": int(usage_count),
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @staticmethod
    def _tag_group_payload(row: CrmComplaintTagGroup, *, tag_count: int = 0) -> dict[str, Any]:
        return {
            "id": str(row.id),
            "slug": row.slug,
            "name": row.name,
            "description": row.description or "",
            "display_order": row.display_order,
            "active": row.active,
            "tag_count": int(tag_count),
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    def _require_tag_group(self, slug: str, *, require_active: bool = True) -> CrmComplaintTagGroup:
        normalized = _tag_slug(slug)
        row = self.session.exec(
            select(CrmComplaintTagGroup).where(CrmComplaintTagGroup.slug == normalized)
        ).first()
        if row is None or (require_active and not row.active):
            raise AiPipelineError("Select an active controlled tag group")
        return row

    def _tag_identity_conflict(
        self,
        *,
        slug: str,
        aliases: list[str],
        exclude_id: uuid.UUID | None = None,
    ) -> CrmComplaintTag | None:
        candidate_keys = {slug, *(_tag_slug(value) for value in aliases)}
        candidate_keys.discard("")
        for row in self.session.exec(select(CrmComplaintTag)).all():
            if exclude_id and row.id == exclude_id:
                continue
            row_keys = {
                row.normalized_name,
                _tag_slug(row.name),
                _tag_slug(row.display_name),
                *(_tag_slug(value) for value in (row.aliases_json or [])),
            }
            row_keys.discard("")
            if candidate_keys & row_keys:
                return row
        return None

    def tag_statistics(self) -> dict[str, int]:
        self.ensure_defaults()
        active = (
            self.session.scalar(
                select(func.count())
                .select_from(CrmComplaintTag)
                .where(
                    CrmComplaintTag.active == True,  # noqa: E712
                    CrmComplaintTag.merged_into_id.is_(None),
                )
            )
            or 0
        )
        inactive = (
            self.session.scalar(
                select(func.count())
                .select_from(CrmComplaintTag)
                .where(
                    or_(
                        CrmComplaintTag.active == False, CrmComplaintTag.merged_into_id.is_not(None)
                    )  # noqa: E712
                )
            )
            or 0
        )
        groups = (
            self.session.scalar(
                select(func.count())
                .select_from(CrmComplaintTagGroup)
                .where(
                    CrmComplaintTagGroup.active == True  # noqa: E712
                )
            )
            or 0
        )
        links = self.session.scalar(select(func.count()).select_from(CrmComplaintTagLink)) or 0
        suggestions = (
            self.session.scalar(
                select(func.count())
                .select_from(CrmTaxonomySuggestion)
                .where(
                    CrmTaxonomySuggestion.proposal_type == "tag",
                    CrmTaxonomySuggestion.status == "pending",
                )
            )
            or 0
        )
        return {
            "active_tags": int(active),
            "inactive_tags": int(inactive),
            "active_groups": int(groups),
            "tag_links": int(links),
            "pending_suggestions": int(suggestions),
        }

    def tag_groups(self, *, include_inactive: bool = False) -> dict[str, Any]:
        self.ensure_defaults()
        statement = select(CrmComplaintTagGroup)
        if not include_inactive:
            statement = statement.where(CrmComplaintTagGroup.active == True)  # noqa: E712
        rows = list(
            self.session.exec(
                statement.order_by(CrmComplaintTagGroup.display_order, CrmComplaintTagGroup.name)
            ).all()
        )
        counts = {
            str(slug): int(count)
            for slug, count in self.session.exec(
                select(CrmComplaintTag.group_name, func.count()).group_by(
                    CrmComplaintTag.group_name
                )
            ).all()
        }
        return {
            "items": [
                self._tag_group_payload(row, tag_count=counts.get(row.slug, 0)) for row in rows
            ],
            "total": len(rows),
        }

    def create_tag_group(
        self,
        *,
        name: str,
        slug: str | None,
        description: str,
        display_order: int,
        actor: str,
    ) -> dict[str, Any]:
        group_name = _tag_display_name(name)[:120]
        group_slug = _tag_slug(slug or name)[:80]
        if not group_slug:
            raise AiPipelineError("Tag group name is required")
        if self.session.exec(
            select(CrmComplaintTagGroup).where(CrmComplaintTagGroup.slug == group_slug)
        ).first():
            raise AiPipelineError("A controlled tag group with this slug already exists")
        row = CrmComplaintTagGroup(
            slug=group_slug,
            name=group_name,
            description=clean_name(description) or None,
            display_order=max(0, min(int(display_order), 9999)),
            active=True,
        )
        self.session.add(row)
        self.session.add(
            ComplaintAuditEvent(
                entity_type="complaint_tag_group",
                entity_id=str(row.id),
                event_type="tag_group_created",
                actor=actor or "web-operator",
                after_json={"slug": group_slug, "name": group_name},
            )
        )
        self.session.commit()
        self.session.refresh(row)
        return self._tag_group_payload(row)

    def update_tag_group(
        self,
        group_id: uuid.UUID,
        *,
        name: str,
        description: str,
        display_order: int,
        active: bool,
        actor: str,
    ) -> dict[str, Any]:
        row = self.session.get(CrmComplaintTagGroup, group_id)
        if row is None:
            raise BulkOperationNotFound("Controlled tag group was not found")
        before = self._tag_group_payload(row)
        row.name = _tag_display_name(name)[:120]
        row.description = clean_name(description) or None
        row.display_order = max(0, min(int(display_order), 9999))
        row.active = bool(active)
        row.updated_at = utcnow()
        self.session.add(row)
        self.session.add(
            ComplaintAuditEvent(
                entity_type="complaint_tag_group",
                entity_id=str(row.id),
                event_type="tag_group_updated",
                actor=actor or "web-operator",
                before_json=before,
                after_json=self._tag_group_payload(row),
            )
        )
        self.session.commit()
        return self._tag_group_payload(row)

    def tags(
        self,
        *,
        search: str = "",
        group: str = "",
        status: str = "active",
        ai_available: bool | None = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        self.ensure_defaults()
        if status not in {"active", "inactive", "all"}:
            raise AiPipelineError("Controlled tag status is invalid")
        filters: list[Any] = []
        if status == "active":
            filters.extend(
                (CrmComplaintTag.active == True, CrmComplaintTag.merged_into_id.is_(None))
            )  # noqa: E712
        elif status == "inactive":
            filters.append(
                or_(CrmComplaintTag.active == False, CrmComplaintTag.merged_into_id.is_not(None))
            )  # noqa: E712
        if group.strip():
            filters.append(CrmComplaintTag.group_name == _tag_slug(group))
        if ai_available is not None:
            filters.append(CrmComplaintTag.ai_available == ai_available)
        if search.strip():
            term = f"%{search.strip()}%"
            filters.append(
                or_(
                    CrmComplaintTag.name.ilike(term),
                    CrmComplaintTag.display_name.ilike(term),
                    CrmComplaintTag.description.ilike(term),
                    CrmComplaintTag.group_name.ilike(term),
                    cast(CrmComplaintTag.aliases_json, Text).ilike(term),
                )
            )
        count = (
            self.session.scalar(select(func.count()).select_from(CrmComplaintTag).where(*filters))
            or 0
        )
        rows = list(
            self.session.exec(
                select(CrmComplaintTag)
                .where(*filters)
                .order_by(
                    CrmComplaintTag.group_name, CrmComplaintTag.display_name, CrmComplaintTag.name
                )
                .offset((page - 1) * page_size)
                .limit(page_size)
            ).all()
        )
        ids = [row.id for row in rows]
        usage = (
            {
                tag_id: int(total)
                for tag_id, total in self.session.exec(
                    select(CrmComplaintTagLink.tag_id, func.count())
                    .where(CrmComplaintTagLink.tag_id.in_(ids))
                    .group_by(CrmComplaintTagLink.tag_id)
                ).all()
            }
            if ids
            else {}
        )
        return {
            "items": [self._tag_payload(row, usage_count=usage.get(row.id, 0)) for row in rows],
            "total": int(count),
            "page": page,
            "page_size": page_size,
        }

    def create_tag(
        self,
        *,
        display_name: str,
        slug: str | None,
        group_name: str,
        description: str,
        aliases: list[str],
        active: bool,
        ai_available: bool,
        actor: str,
    ) -> dict[str, Any]:
        self.ensure_defaults()
        group = self._require_tag_group(group_name)
        label = _tag_display_name(display_name)[:140]
        tag_slug = _tag_slug(slug or display_name)
        if not tag_slug:
            raise AiPipelineError("Controlled tag name is required")
        cleaned_aliases = _clean_aliases(aliases, slug=tag_slug)
        conflict = self._tag_identity_conflict(slug=tag_slug, aliases=cleaned_aliases)
        if conflict:
            raise AiPipelineError(
                f"A controlled tag with the same name or alias already exists: "
                f"{conflict.display_name or conflict.name}"
            )
        row = CrmComplaintTag(
            name=tag_slug,
            display_name=label,
            normalized_name=tag_slug,
            group_name=group.slug,
            description=clean_name(description) or None,
            aliases_json=cleaned_aliases,
            active=bool(active),
            ai_available=bool(ai_available),
        )
        self.session.add(row)
        self.session.add(
            ComplaintAuditEvent(
                entity_type="complaint_tag",
                entity_id=str(row.id),
                event_type="tag_created",
                actor=actor or "web-operator",
                after_json={
                    "name": tag_slug,
                    "display_name": label,
                    "group": group.slug,
                    "aliases": cleaned_aliases,
                },
            )
        )
        self.session.commit()
        self.session.refresh(row)
        return self._tag_payload(row)

    def update_tag(
        self,
        tag_id: uuid.UUID,
        *,
        display_name: str,
        slug: str | None,
        group_name: str,
        description: str,
        aliases: list[str],
        active: bool,
        ai_available: bool,
        actor: str,
    ) -> dict[str, Any]:
        self.ensure_defaults()
        row = self.session.get(CrmComplaintTag, tag_id)
        if row is None:
            raise BulkOperationNotFound("Controlled tag was not found")
        if row.merged_into_id:
            raise AiPipelineError("Merged tags cannot be edited")
        group = self._require_tag_group(group_name, require_active=False)
        label = _tag_display_name(display_name)[:140]
        tag_slug = _tag_slug(slug or row.name or display_name)
        if not tag_slug:
            raise AiPipelineError("Controlled tag name is required")
        alias_values = list(aliases)
        if row.name != tag_slug:
            alias_values.append(row.name)
        cleaned_aliases = _clean_aliases(alias_values, slug=tag_slug)
        conflict = self._tag_identity_conflict(
            slug=tag_slug,
            aliases=cleaned_aliases,
            exclude_id=row.id,
        )
        if conflict:
            raise AiPipelineError(
                f"A controlled tag with the same name or alias already exists: "
                f"{conflict.display_name or conflict.name}"
            )
        before = self._tag_payload(row)
        row.name = tag_slug
        row.display_name = label
        row.normalized_name = tag_slug
        row.group_name = group.slug
        row.description = clean_name(description) or None
        row.aliases_json = cleaned_aliases
        row.active = bool(active)
        row.ai_available = bool(ai_available)
        row.version += 1
        row.updated_at = utcnow()
        self.session.add(row)
        self.session.add(
            ComplaintAuditEvent(
                entity_type="complaint_tag",
                entity_id=str(row.id),
                event_type="tag_updated",
                actor=actor or "web-operator",
                before_json=before,
                after_json=self._tag_payload(row),
            )
        )
        self.session.commit()
        return self._tag_payload(row)

    def deactivate_tag(self, tag_id: uuid.UUID, *, actor: str) -> dict[str, Any]:
        row = self.session.get(CrmComplaintTag, tag_id)
        if row is None:
            raise BulkOperationNotFound("Controlled tag was not found")
        if row.merged_into_id:
            raise AiPipelineError("Merged tags are already inactive")
        row.active = False
        row.ai_available = False
        row.version += 1
        row.updated_at = utcnow()
        self.session.add(row)
        self.session.add(
            ComplaintAuditEvent(
                entity_type="complaint_tag",
                entity_id=str(row.id),
                event_type="tag_deactivated",
                actor=actor or "web-operator",
                after_json={"name": row.name, "active": False},
            )
        )
        self.session.commit()
        return self._tag_payload(row)

    def merge_tag(
        self,
        tag_id: uuid.UUID,
        *,
        target_tag_id: uuid.UUID,
        actor: str,
    ) -> dict[str, Any]:
        source = self.session.get(CrmComplaintTag, tag_id)
        target = self.session.get(CrmComplaintTag, target_tag_id)
        if source is None or target is None:
            raise BulkOperationNotFound("Controlled tag was not found")
        if source.id == target.id:
            raise AiPipelineError("Choose another tag as the merge target")
        if source.merged_into_id:
            raise AiPipelineError("This tag has already been merged")
        if not target.active or target.merged_into_id:
            raise AiPipelineError("Merge into an active controlled tag")
        moved = 0
        removed_duplicates = 0
        for link in list(
            self.session.exec(
                select(CrmComplaintTagLink).where(CrmComplaintTagLink.tag_id == source.id)
            ).all()
        ):
            duplicate = self.session.exec(
                select(CrmComplaintTagLink).where(
                    CrmComplaintTagLink.complaint_case_id == link.complaint_case_id,
                    CrmComplaintTagLink.tag_id == target.id,
                )
            ).first()
            if duplicate:
                self.session.delete(link)
                removed_duplicates += 1
            else:
                link.tag_id = target.id
                self.session.add(link)
                moved += 1
        for suggestion in self.session.exec(
            select(CrmTaxonomySuggestion).where(CrmTaxonomySuggestion.resolved_tag_id == source.id)
        ).all():
            suggestion.resolved_tag_id = target.id
            self.session.add(suggestion)
        target.aliases_json = _clean_aliases(
            [
                *(target.aliases_json or []),
                source.display_name,
                source.name,
                *(source.aliases_json or []),
            ],
            slug=target.name,
        )
        target.version += 1
        target.updated_at = utcnow()
        source.active = False
        source.ai_available = False
        source.merged_into_id = target.id
        source.version += 1
        source.updated_at = utcnow()
        self.session.add(target)
        self.session.add(source)
        self.session.add(
            ComplaintAuditEvent(
                entity_type="complaint_tag",
                entity_id=str(source.id),
                event_type="tag_merged",
                actor=actor or "web-operator",
                after_json={
                    "source": source.name,
                    "target_id": str(target.id),
                    "target": target.name,
                    "links_moved": moved,
                    "duplicates_removed": removed_duplicates,
                },
            )
        )
        self.session.commit()
        return {
            "source": self._tag_payload(source),
            "target": self._tag_payload(target),
            "links_moved": moved,
            "duplicates_removed": removed_duplicates,
        }

    def suggest_tag(
        self,
        classification_id: uuid.UUID,
        *,
        display_name: str,
        group_name: str,
        reason: str,
        actor: str,
    ) -> dict[str, Any]:
        self.ensure_defaults()
        classification = self.session.get(CrmComplaintClassification, classification_id)
        if classification is None:
            raise BulkOperationNotFound("Classification decision was not found")
        if classification.status != "review_required":
            raise AiPipelineError(
                "New tags can only be suggested while classification is awaiting review"
            )
        group = self._require_tag_group(group_name)
        label = _tag_display_name(display_name)[:140]
        slug = _tag_slug(label)
        if not slug:
            raise AiPipelineError("Suggested tag name is required")
        existing = self._tag_identity_conflict(slug=slug, aliases=[])
        if existing:
            raise AiPipelineError(
                f"Use the existing controlled tag: {existing.display_name or existing.name}"
            )
        duplicate = self.session.exec(
            select(CrmTaxonomySuggestion).where(
                CrmTaxonomySuggestion.classification_id == classification.id,
                CrmTaxonomySuggestion.proposal_type == "tag",
                CrmTaxonomySuggestion.normalized_name == slug,
                CrmTaxonomySuggestion.status.in_(("pending", "deferred")),
            )
        ).first()
        if duplicate:
            return {
                "id": str(duplicate.id),
                "status": duplicate.status,
                "proposed_name": duplicate.proposed_name,
            }
        row = CrmTaxonomySuggestion(
            classification_id=classification.id,
            proposal_type="tag",
            proposed_name=label,
            normalized_name=slug,
            proposed_group_name=group.slug,
            reason=clean_name(reason) or "No existing controlled tag fits this complaint.",
            status="pending",
        )
        self.session.add(row)
        self.session.add(
            ComplaintAuditEvent(
                complaint_case_id=classification.complaint_case_id,
                entity_type="taxonomy_suggestion",
                entity_id=str(row.id),
                event_type="tag_suggested",
                actor=actor or "web-operator",
                after_json={"name": label, "group": group.slug},
            )
        )
        self.session.commit()
        self.session.refresh(row)
        return {
            "id": str(row.id),
            "status": row.status,
            "proposed_name": row.proposed_name,
            "proposed_group_name": row.proposed_group_name,
        }

    def create_tag_from_suggestion(
        self,
        suggestion_id: uuid.UUID,
        *,
        display_name: str | None,
        slug: str | None,
        group_name: str | None,
        description: str,
        aliases: list[str],
        active: bool,
        ai_available: bool,
        actor: str,
    ) -> dict[str, Any]:
        suggestion = self.session.get(CrmTaxonomySuggestion, suggestion_id)
        if suggestion is None:
            raise BulkOperationNotFound("Taxonomy suggestion was not found")
        if suggestion.proposal_type != "tag" or suggestion.status not in {"pending", "deferred"}:
            raise AiPipelineError("Only pending or deferred tag suggestions can create a tag")
        tag = self.create_tag(
            display_name=display_name or suggestion.proposed_name,
            slug=slug,
            group_name=group_name or suggestion.proposed_group_name or "issue",
            description=description or suggestion.reason or "",
            aliases=aliases,
            active=active,
            ai_available=ai_available,
            actor=actor,
        )
        suggestion.status = "approved"
        suggestion.resolved_tag_id = uuid.UUID(tag["id"])
        suggestion.decided_by = actor or "web-operator"
        suggestion.decided_at = utcnow()
        self.session.add(suggestion)
        self.session.commit()
        return {"suggestion_id": str(suggestion.id), "status": suggestion.status, "tag": tag}

    def taxonomy_options(self) -> dict[str, Any]:
        self.ensure_defaults()
        categories, subcategories = self._taxonomy_rows()
        tags = list(
            self.session.exec(
                select(CrmComplaintTag)
                .where(
                    CrmComplaintTag.active == True,  # noqa: E712
                    CrmComplaintTag.ai_available == True,  # noqa: E712
                    CrmComplaintTag.merged_into_id.is_(None),
                )
                .order_by(
                    CrmComplaintTag.group_name, CrmComplaintTag.display_name, CrmComplaintTag.name
                )
            ).all()
        )
        by_category: dict[uuid.UUID, list[dict[str, Any]]] = {}
        for row in subcategories:
            by_category.setdefault(row.category_id, []).append(
                {"id": str(row.id), "name": row.name}
            )
        return {
            "categories": [
                {"id": str(row.id), "name": row.name, "subcategories": by_category.get(row.id, [])}
                for row in categories
            ],
            "tags": [
                {
                    "id": str(row.id),
                    "name": row.name,
                    "display_name": row.display_name or row.name.replace("-", " ").title(),
                    "group": row.group_name,
                    "description": row.description or "",
                    "aliases": list(row.aliases_json or []),
                }
                for row in tags
            ],
        }

    def _case_tags(self, case_id: uuid.UUID) -> list[CrmComplaintTag]:
        return list(
            self.session.exec(
                select(CrmComplaintTag)
                .join(CrmComplaintTagLink, CrmComplaintTagLink.tag_id == CrmComplaintTag.id)
                .where(CrmComplaintTagLink.complaint_case_id == case_id)
                .order_by(CrmComplaintTag.group_name, CrmComplaintTag.name)
            ).all()
        )

    def _historical_examples(
        self,
        case: ComplaintCase,
        *,
        limit: int,
    ) -> list[tuple[ComplaintReplyRevision, ComplaintCase, float, str]]:
        statement = (
            select(ComplaintReplyRevision, ComplaintCase)
            .join(ComplaintCase, ComplaintCase.id == ComplaintReplyRevision.complaint_case_id)
            .where(
                ComplaintReplyRevision.is_current == True,  # noqa: E712
                ComplaintReplyRevision.approval_status.in_(("Approved", "Issued")),
                ComplaintCase.frappe_ai_eligible == True,  # noqa: E712
                ComplaintCase.id != case.id,
            )
        )
        candidates = list(
            self.session.exec(
                statement.order_by(ComplaintReplyRevision.captured_at.desc()).limit(250)
            ).all()
        )
        ranked: list[tuple[ComplaintReplyRevision, ComplaintCase, float, str]] = []
        for revision, example_case in candidates:
            basis = "semantic"
            bonus = 0.0
            if case.sub_category_id and example_case.sub_category_id == case.sub_category_id:
                bonus = 1.0
                basis = "same subcategory"
            elif case.category_id and example_case.category_id == case.category_id:
                bonus = 0.55
                basis = "same category"
            score = bonus + _token_score(case.remarks or "", example_case.remarks or "")
            if score > 0:
                ranked.append((revision, example_case, score, basis))
        ranked.sort(key=lambda item: (item[2], item[0].captured_at), reverse=True)
        return ranked[:limit]

    def create_reply_context_export(
        self,
        *,
        scope: str = "awaiting",
        parent_batch_id: uuid.UUID | None = None,
        case_ids: list[uuid.UUID] | None = None,
        examples_limit: int = 4,
        redact_personal_data: bool = True,
        actor: str = "web-operator",
    ) -> dict[str, Any]:
        if scope not in {"awaiting", "selected", "classification_batch"}:
            raise AiPipelineError("Reply context scope is invalid")
        if examples_limit < 0 or examples_limit > 10:
            raise AiPipelineError("Historical example limit must be between 0 and 10")
        statement = (
            select(ComplaintCase, ComplaintReply)
            .join(
                ComplaintReply, ComplaintReply.complaint_case_id == ComplaintCase.id, isouter=True
            )
            .where(
                reply_case_eligibility_clause(),
                ComplaintCase.category_id.is_not(None),
            )
        )
        if scope == "awaiting":
            statement = statement.where(ComplaintReply.id.is_(None))
        elif scope == "selected":
            selected = list(dict.fromkeys(case_ids or []))
            if not selected:
                raise AiPipelineError("Select at least one complaint")
            statement = statement.where(ComplaintCase.id.in_(selected))
        elif scope == "classification_batch":
            if parent_batch_id is None:
                raise AiPipelineError("Select a completed classification import batch")
            parent = self.bulk._require_batch(parent_batch_id)
            if parent.operation_type != "classification_import" or parent.status not in {
                "completed",
                "completed_with_errors",
            }:
                raise AiPipelineError("The source classification batch must be completed")
            selected = [
                item.complaint_case_id
                for item in self.session.exec(
                    select(CrmBulkOperationItem).where(
                        CrmBulkOperationItem.batch_id == parent.id,
                        CrmBulkOperationItem.status == "classified",
                        CrmBulkOperationItem.complaint_case_id.is_not(None),
                    )
                ).all()
                if item.complaint_case_id is not None
            ]
            if not selected:
                raise AiPipelineError("The classification batch has no classified complaints")
            statement = statement.where(ComplaintCase.id.in_(selected), ComplaintReply.id.is_(None))
        rows = list(self.session.exec(statement.order_by(ComplaintCase.complaint_number)).all())
        if not rows:
            raise AiPipelineError("No classified complaints match the reply context scope")
        version = self.active_prompt_version()
        now = utcnow()
        batch = self.bulk._create_batch(
            "reply_context_export",
            actor=actor,
            parent_batch_id=parent_batch_id,
            status="processing",
            scope={"scope": scope, "case_ids": [str(value) for value in case_ids or []]},
            settings={
                "format": "contextual_reply_csv_v1",
                "prompt_profile_version_id": str(version.id),
                "prompt_profile_version": version.version_label,
                "prompt_profile_sha256": version.content_sha256,
                "historical_examples_limit": examples_limit,
                "redact_personal_data": redact_personal_data,
            },
        )
        batch.started_at = now
        category_ids = {case.category_id for case, _ in rows if case.category_id}
        sub_ids = {case.sub_category_id for case, _ in rows if case.sub_category_id}
        categories = (
            {
                row.id: row
                for row in self.session.exec(
                    select(ComplaintCategory).where(ComplaintCategory.id.in_(category_ids))
                ).all()
            }
            if category_ids
            else {}
        )
        subs = (
            {
                row.id: row
                for row in self.session.exec(
                    select(ComplaintSubcategory).where(ComplaintSubcategory.id.in_(sub_ids))
                ).all()
            }
            if sub_ids
            else {}
        )
        complaint_rows: list[list[Any]] = []
        template_rows: list[list[Any]] = []
        context_rows: list[list[Any]] = []
        example_rows: list[list[Any]] = []
        manifest_rows: list[list[Any]] = []
        for case, reply in rows:
            number = case.complaint_number or str(case.id)
            category = categories.get(case.category_id)
            sub = subs.get(case.sub_category_id)
            case_tags = self._case_tags(case.id)
            complaint_text = (
                _redact(case.remarks or "") if redact_personal_data else case.remarks or ""
            )
            complaint_rows.append(
                [
                    number,
                    complaint_text,
                    category.name if category else "",
                    sub.name if sub else "",
                    ";".join(tag.name for tag in case_tags),
                ]
            )
            template_rows.append([number, ""])
            context_rows.append(
                [
                    number,
                    category.name if category else "",
                    sub.name if sub else "",
                    ";".join(tag.name for tag in case_tags),
                    category.reply_guidance if category else "",
                    category.policy_notes if category else "",
                    sub.reply_guidance if sub else "",
                    sub.policy_notes if sub else "",
                ]
            )
            examples = self._historical_examples(case, limit=examples_limit)
            for rank, (revision, example_case, score, basis) in enumerate(examples, start=1):
                example_rows.append(
                    [
                        number,
                        example_case.complaint_number or str(example_case.id),
                        basis,
                        f"{score:.4f}",
                        revision.approval_status,
                        _redact(example_case.remarks or "")
                        if redact_personal_data
                        else example_case.remarks or "",
                        _redact(revision.reply_text)
                        if redact_personal_data
                        else revision.reply_text,
                    ]
                )
                self.session.add(
                    CrmReplyContextExample(
                        batch_id=batch.id,
                        complaint_case_id=case.id,
                        example_case_id=example_case.id,
                        reply_revision_id=revision.id,
                        rank=rank,
                        score=score,
                        matched_by=basis,
                        snapshot_json={
                            "target_complaint_number": number,
                            "example_complaint_number": example_case.complaint_number,
                            "approval_status": revision.approval_status,
                            "reply_content_hash": revision.content_hash,
                            "prompt_profile_version_id": str(version.id),
                        },
                    )
                )
            manifest_rows.append(
                [
                    number,
                    str(case.id),
                    category.name if category else "",
                    sub.name if sub else "",
                    len(examples),
                    bool(reply),
                    redact_personal_data,
                ]
            )
            self.session.add(
                CrmBulkOperationItem(
                    batch_id=batch.id,
                    complaint_case_id=case.id,
                    complaint_number_snapshot=number,
                    status="exported",
                    reply_version_before=reply.version if reply else None,
                    details_json={
                        "category": category.name if category else "",
                        "subcategory": sub.name if sub else "",
                        "tags": [tag.name for tag in case_tags],
                        "historical_examples": len(examples),
                        "prompt_profile_version_id": str(version.id),
                        "prompt_profile_version": version.version_label,
                        "redacted": redact_personal_data,
                    },
                    processed_at=now,
                )
            )
        summary = {
            "batch_id": str(batch.id),
            "batch_number": batch.batch_number,
            "created_at": now.isoformat(),
            "complaints": len(rows),
            "prompt_profile": {
                "name": DEFAULT_REPLY_SOP_NAME,
                "version": version.version_label,
                "sha256": version.content_sha256,
            },
            "historical_examples": len(example_rows),
            "redact_personal_data": redact_personal_data,
            "required_reply_columns": ["Complaint Number", "Reply"],
        }
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "01-new-complaints.csv",
                _csv_bytes(
                    ["Complaint Number", "Complaint Remarks", "Category", "Subcategory", "Tags"],
                    complaint_rows,
                ),
            )
            archive.writestr(
                "02-reply-template.csv", _csv_bytes(["Complaint Number", "Reply"], template_rows)
            )
            archive.writestr("03-active-reply-sop.txt", version.content)
            archive.writestr(
                "04-taxonomy-context.csv",
                _csv_bytes(
                    [
                        "Complaint Number",
                        "Category",
                        "Subcategory",
                        "Tags",
                        "Category Reply Guidance",
                        "Category Policy Notes",
                        "Subcategory Reply Guidance",
                        "Subcategory Policy Notes",
                    ],
                    context_rows,
                ),
            )
            archive.writestr(
                "05-approved-historical-examples.csv",
                _csv_bytes(
                    [
                        "Target Complaint Number",
                        "Example Complaint Number",
                        "Match Basis",
                        "Score",
                        "Approval Status",
                        "Example Complaint",
                        "Approved Reply",
                    ],
                    example_rows,
                ),
            )
            archive.writestr("06-chatgpt-reply-instructions.txt", REPLY_CONTEXT_INSTRUCTIONS)
            archive.writestr(
                "07-example-completed-replies.csv",
                _csv_bytes(
                    ["Complaint Number", "Reply"],
                    [
                        [
                            "104-1234567",
                            "Respected Worthy Chief Executive Officer (DEA),\n\nThe matter has been reviewed on the basis of available record.\n\nSubmitted for kind perusal and further necessary action, please.",
                        ]
                    ],
                ),
            )
            archive.writestr(
                "batch-manifest.csv",
                _csv_bytes(
                    [
                        "Complaint Number",
                        "Case ID",
                        "Category",
                        "Subcategory",
                        "Historical Examples",
                        "Had Reply At Export",
                        "Personal Data Redacted",
                    ],
                    manifest_rows,
                ),
            )
            archive.writestr("batch-summary.json", json.dumps(summary, indent=2))
        self.bulk._write_artifact(
            batch,
            kind="reply_context_package",
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
        return self.bulk.batch_detail(batch.id)

    def reference_file(self, kind: str) -> tuple[str, str, bytes]:
        if kind == "classification-template":
            return (
                "crm-classification-template.csv",
                "text/csv; charset=utf-8",
                _csv_bytes(CLASSIFICATION_HEADERS, [["104-1234567", "", "", "", "", "No", "", ""]]),
            )
        if kind == "classification-sample":
            return (
                "crm-classification-sample.csv",
                "text/csv; charset=utf-8",
                _csv_bytes(
                    CLASSIFICATION_HEADERS,
                    [
                        [
                            "104-1234567",
                            "Fees",
                            "Summer Vacation Fee",
                            "private-school;advance-fee;no-evidence",
                            "0.96",
                            "No",
                            "",
                            "Advance vacation fee complaint",
                        ]
                    ],
                ),
            )
        if kind == "classification-instructions":
            return (
                "crm-classification-instructions.txt",
                "text/plain; charset=utf-8",
                CLASSIFICATION_INSTRUCTIONS.encode("utf-8"),
            )
        if kind == "active-reply-sop":
            version = self.active_prompt_version()
            return (
                f"cm-complaint-reply-sop-{version.version_label}.txt",
                "text/plain; charset=utf-8",
                version.content.encode("utf-8"),
            )
        if kind == "reply-context-instructions":
            return (
                "crm-contextual-reply-instructions.txt",
                "text/plain; charset=utf-8",
                REPLY_CONTEXT_INSTRUCTIONS.encode("utf-8"),
            )
        raise BulkOperationNotFound("AI pipeline reference file was not found")

from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
import subprocess
import tempfile
import uuid
from copy import deepcopy
from datetime import date, datetime
from html import escape
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZIP_STORED, BadZipFile, ZipFile

from lxml import etree
from PIL import Image as PillowImage
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import legal
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from sqlmodel import Session, func, select

from automation_core.config import Settings
from automation_core.time import utcnow
from crm_domain.models import (
    ComplaintCase,
    ComplaintReplyRevision,
    CrmOfficialLetter,
    CrmOfficialLetterArtifact,
    CrmOfficialLetterSettings,
    CrmOfficialLetterSignatureProfile,
    CrmOfficialLetterTemplate,
)

ODT_MIME = "application/vnd.oasis.opendocument.text"
REQUIRED_MARKERS = {
    "{{letter_number}}",
    "{{letter_date}}",
    "{{recipient_name}}",
    "{{recipient_location}}",
    "{{subject_prefix}}",
    "{{complaint_number}}",
    "{{complaint_details}}",
    "{{approved_reply}}",
    "{{signatory_designation}}",
    "{{signatory_location}}",
    "{{cc_entries}}",
}
TEXT_NS = "urn:oasis:names:tc:opendocument:xmlns:text:1.0"
DRAW_NS = "urn:oasis:names:tc:opendocument:xmlns:drawing:1.0"
XLINK_NS = "http://www.w3.org/1999/xlink"
NS = {"text": TEXT_NS, "draw": DRAW_NS, "xlink": XLINK_NS}


class OfficialLetterError(RuntimeError):
    pass


class OfficialLetterNotFound(OfficialLetterError):
    pass


class OfficialLetterValidationError(OfficialLetterError):
    pass


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _safe_filename(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in "-_. " else "-" for char in value)
    return " ".join(cleaned.split()).strip(" .") or "official-letter"


def _iso(value: datetime | date | None) -> str | None:
    return value.isoformat() if value else None


DATE_FORMATS = {
    "DD/MM/YYYY": "%d/%m/%Y",
    "DD-MM-YYYY": "%d-%m-%Y",
    "YYYY-MM-DD": "%Y-%m-%d",
}


def _format_letter_date(value: date, configured_format: str) -> str:
    pattern = DATE_FORMATS.get(configured_format)
    if pattern is None:
        raise OfficialLetterValidationError(
            "Unsupported official-letter date format. Choose DD/MM/YYYY, DD-MM-YYYY or YYYY-MM-DD"
        )
    return value.strftime(pattern)


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        temporary.write_bytes(content)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _pdf_page_count(content: bytes) -> int:
    # PDF page objects contain /Type /Page while the page-tree object is /Pages.
    # This works for LibreOffice and ReportLab output without another dependency.
    return max(1, len(re.findall(rb"/Type\s*/Page(?!s)\b", content)))


def _paragraph_text(node: etree._Element) -> str:
    return "".join(node.itertext()).strip()


def _set_paragraph_text(node: etree._Element, value: str) -> None:
    first_span = next((child for child in node if child.tag == f"{{{TEXT_NS}}}span"), None)
    span_attrib = dict(first_span.attrib) if first_span is not None else None
    for child in list(node):
        node.remove(child)
    node.text = None
    if span_attrib is not None:
        span = etree.SubElement(node, f"{{{TEXT_NS}}}span", attrib=span_attrib)
        span.text = value
    else:
        node.text = value


def _replace_block_marker(root: etree._Element, marker: str, value: str) -> bool:
    for paragraph in root.xpath("//text:p", namespaces=NS):
        if _paragraph_text(paragraph) != marker:
            continue
        parent = paragraph.getparent()
        if parent is None:
            return False
        index = parent.index(paragraph)
        lines = value.replace("\r\n", "\n").replace("\r", "\n").split("\n") or [""]
        parent.remove(paragraph)
        for offset, line in enumerate(lines):
            clone = deepcopy(paragraph)
            _set_paragraph_text(clone, line or " ")
            parent.insert(index + offset, clone)
        return True
    return False


def _replace_inline_marker(root: etree._Element, marker: str, value: str) -> int:
    """Replace a marker even when LibreOffice splits it across multiple spans."""
    replacements = 0
    for paragraph in root.xpath("//text:p | //text:h", namespaces=NS):
        visible = "".join(paragraph.itertext())
        if marker not in visible:
            continue
        _set_paragraph_text(paragraph, visible.replace(marker, value))
        replacements += visible.count(marker)
    return replacements


def _template_metadata(content: bytes) -> dict[str, Any]:
    try:
        with ZipFile(io.BytesIO(content), "r") as archive:
            names = set(archive.namelist())
            if "mimetype" not in names or "content.xml" not in names:
                raise OfficialLetterValidationError("The uploaded file is not a valid ODT template")
            if archive.read("mimetype").decode("utf-8", "replace").strip() != ODT_MIME:
                raise OfficialLetterValidationError("The uploaded file is not an OpenDocument Text template")
            xml = archive.read("content.xml")
            root = etree.fromstring(xml)
            visible = "\n".join(_paragraph_text(p) for p in root.xpath("//text:p", namespaces=NS))
            missing = sorted(marker for marker in REQUIRED_MARKERS if marker not in visible)
            if missing:
                raise OfficialLetterValidationError(
                    "Template is missing required placeholders: " + ", ".join(missing)
                )
            images = [
                image.get(f"{{{XLINK_NS}}}href")
                for image in root.xpath("//draw:image", namespaces=NS)
                if image.get(f"{{{XLINK_NS}}}href")
            ]
            signature_target = images[-1] if images else "Pictures/signature.png"
            return {
                "signature_target_path": signature_target,
                "image_paths": images,
                "sha256": _sha256(content),
                "size_bytes": len(content),
            }
    except BadZipFile as exc:
        raise OfficialLetterValidationError("The uploaded template is not a readable ODT archive") from exc
    except etree.XMLSyntaxError as exc:
        raise OfficialLetterValidationError("The template content.xml is invalid") from exc


def render_official_letter_odt(
    template: bytes,
    *,
    values: dict[str, str],
    signature_bytes: bytes,
    signature_target_path: str,
) -> bytes:
    _template_metadata(template)
    source = io.BytesIO(template)
    output = io.BytesIO()
    with ZipFile(source, "r") as source_archive:
        content = source_archive.read("content.xml")
        root = etree.fromstring(content)
        block_values = {
            "{{complaint_details}}": values["complaint_details"],
            "{{approved_reply}}": values["approved_reply"],
            "{{cc_entries}}": values["cc_entries"],
        }
        for marker, value in block_values.items():
            if not _replace_block_marker(root, marker, value):
                raise OfficialLetterValidationError(f"Template marker {marker} is not a standalone paragraph")
        for key, value in values.items():
            marker = "{{" + key + "}}"
            if marker in block_values:
                continue
            if not _replace_inline_marker(root, marker, value):
                raise OfficialLetterValidationError(f"Template marker {marker} could not be replaced")
        unresolved = sorted(set(text for text in REQUIRED_MARKERS if text in etree.tostring(root, encoding="unicode")))
        if unresolved:
            raise OfficialLetterValidationError("Unresolved template placeholders: " + ", ".join(unresolved))
        rendered_xml = etree.tostring(root, xml_declaration=True, encoding="UTF-8")
        with ZipFile(output, "w") as target_archive:
            # The ODF specification requires mimetype to be the first, uncompressed entry.
            target_archive.writestr("mimetype", source_archive.read("mimetype"), compress_type=ZIP_STORED)
            for item in source_archive.infolist():
                if item.filename == "mimetype":
                    continue
                if item.filename == "content.xml":
                    payload = rendered_xml
                elif item.filename == signature_target_path:
                    payload = signature_bytes
                else:
                    payload = source_archive.read(item.filename)
                target_archive.writestr(item.filename, payload, compress_type=ZIP_DEFLATED)
            if signature_target_path not in source_archive.namelist():
                target_archive.writestr(signature_target_path, signature_bytes, compress_type=ZIP_DEFLATED)
    return output.getvalue()


def _extract_template_logo(template: bytes) -> bytes | None:
    try:
        with ZipFile(io.BytesIO(template), "r") as archive:
            root = etree.fromstring(archive.read("content.xml"))
            images = [
                image.get(f"{{{XLINK_NS}}}href")
                for image in root.xpath("//draw:image", namespaces=NS)
                if image.get(f"{{{XLINK_NS}}}href")
            ]
            if images and images[0] in archive.namelist():
                return archive.read(images[0])
    except Exception:
        return None
    return None


def _render_fallback_pdf(
    *,
    template: bytes,
    values: dict[str, str],
    signature_bytes: bytes,
) -> bytes:
    output = io.BytesIO()
    document = SimpleDocTemplate(
        output,
        pagesize=legal,
        leftMargin=0.62 * inch,
        rightMargin=0.62 * inch,
        topMargin=0.55 * inch,
        bottomMargin=0.55 * inch,
        title=f"Official Letter {values['letter_number']}",
    )
    styles = getSampleStyleSheet()
    body = ParagraphStyle(
        "OfficialBody",
        parent=styles["BodyText"],
        fontName="Times-Roman",
        fontSize=10.5,
        leading=14,
        alignment=TA_JUSTIFY,
        spaceAfter=5,
    )
    bold = ParagraphStyle("OfficialBold", parent=body, fontName="Times-Bold", alignment=TA_LEFT)
    center_bold = ParagraphStyle(
        "OfficialCenterBold", parent=bold, alignment=TA_CENTER, fontSize=13, leading=15
    )
    small_center = ParagraphStyle(
        "OfficialSmallCenter", parent=center_bold, fontSize=11, leading=13
    )

    def paras(text: str, style: ParagraphStyle = body) -> list[Paragraph]:
        return [Paragraph(escape(line) if line else "&nbsp;", style) for line in text.splitlines()]

    logo_bytes = _extract_template_logo(template)
    logo = None
    if logo_bytes:
        logo = Image(io.BytesIO(logo_bytes), width=1.2 * inch, height=1.05 * inch)
    header_lines = [Paragraph("OFFICE OF THE", center_bold), Paragraph("DISTRICT EDUCATION OFFICER (M-EE)", center_bold), Paragraph("LAHORE", center_bold), Paragraph(f"No. <u>{escape(values['letter_number'])}</u>", small_center), Paragraph(f"<u>Dated {escape(values['letter_date'])}</u>", small_center)]
    header = Table([[logo or "", header_lines]], colWidths=[1.55 * inch, 5.55 * inch])
    header.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.4, colors.HexColor("#c8c8c8")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (0, 0), "CENTER"),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story: list[Any] = [header, Spacer(1, 0.2 * inch)]
    to_table = Table([
        [Paragraph("<b>To:</b>", bold), ""],
        ["", Paragraph(f"<b>{escape(values['recipient_name'])}<br/>{escape(values['recipient_location'])}</b>", bold)],
    ], colWidths=[0.8 * inch, 6.3 * inch])
    story.extend([to_table, Spacer(1, 0.16 * inch)])
    story.append(Paragraph(f"<u><b>Subject:</b></u>&nbsp;&nbsp;&nbsp;&nbsp;<u><b>{escape(values['subject_prefix'])} {escape(values['complaint_number'])}</b></u>", bold))
    story.append(Spacer(1, 0.12 * inch))
    details_flow = paras(values["complaint_details"])
    reply_flow = paras(values["approved_reply"])
    content_table = Table([
        [Paragraph("<b>Complaint Details</b>", center_bold), Paragraph("<b>Remarks</b>", center_bold)],
        [details_flow, reply_flow],
    ], colWidths=[3.55 * inch, 3.55 * inch])
    content_table.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.6, colors.black),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.extend([content_table, Spacer(1, 0.16 * inch)])
    signature = Image(io.BytesIO(signature_bytes), width=1.55 * inch, height=0.9 * inch)
    sign_table = Table([["", signature], ["", Paragraph(f"<b>{escape(values['signatory_designation'])}</b><br/><b>{escape(values['signatory_location'])}</b>", small_center)]], colWidths=[4.35 * inch, 2.75 * inch])
    sign_table.setStyle(TableStyle([("ALIGN", (1, 0), (1, -1), "CENTER"), ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
    story.extend([sign_table, Spacer(1, 0.1 * inch), Paragraph("<b>CC.</b>", bold)])
    for index, entry in enumerate([line.strip() for line in values["cc_entries"].splitlines() if line.strip()], start=1):
        story.append(Paragraph(f"<b>{index}.</b>&nbsp;&nbsp;{escape(entry)}", body))
    document.build(story)
    return output.getvalue()


def render_official_letter_pdf(
    odt: bytes,
    *,
    template: bytes,
    values: dict[str, str],
    signature_bytes: bytes,
) -> tuple[bytes, str]:
    executable = shutil.which("libreoffice") or shutil.which("soffice")
    if executable:
        with tempfile.TemporaryDirectory(prefix="crm-letter-") as temp:
            root = Path(temp)
            source = root / "letter.odt"
            source.write_bytes(odt)
            profile = root / "lo-profile"
            command = [
                executable,
                f"-env:UserInstallation={profile.as_uri()}",
                "--headless",
                "--convert-to",
                "pdf:writer_pdf_Export",
                "--outdir",
                str(root),
                str(source),
            ]
            try:
                completed = subprocess.run(
                    command,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=45,
                )
                converted = root / "letter.pdf"
                if completed.returncode == 0 and converted.exists() and converted.stat().st_size:
                    return converted.read_bytes(), "libreoffice"
            except (OSError, subprocess.TimeoutExpired):
                pass
    return _render_fallback_pdf(template=template, values=values, signature_bytes=signature_bytes), "reportlab-fallback"


class OfficialLetterService:
    def __init__(self, session: Session, settings: Settings):
        self.session = session
        self.settings = settings
        self.root = (settings.artifact_root.expanduser().resolve() / "crm-official-letters")
        self.templates_root = self.root / "templates"
        self.signatures_root = self.root / "signatures"
        self.letters_root = self.root / "letters"
        for path in (self.templates_root, self.signatures_root, self.letters_root):
            path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def shipped_template_path() -> Path:
        return Path(__file__).resolve().parents[3] / "data" / "crm" / "official-letter-sample" / "deo-crm-official-letter-v1.odt"

    @staticmethod
    def shipped_signature_path() -> Path:
        return Path(__file__).resolve().parents[3] / "data" / "crm" / "official-letter-sample" / "default-signature.png"

    def ensure_defaults(self) -> CrmOfficialLetterSettings:
        current = self.session.exec(
            select(CrmOfficialLetterSettings).where(CrmOfficialLetterSettings.singleton_key == "default")
        ).first()
        if current and current.default_template_id and current.default_signature_id:
            return current
        template = self.session.exec(
            select(CrmOfficialLetterTemplate).where(CrmOfficialLetterTemplate.is_default == True)  # noqa: E712
        ).first()
        if template is None:
            source = self.shipped_template_path()
            if not source.exists():
                raise OfficialLetterValidationError(f"Official letter template is missing: {source}")
            template = self._store_template(
                source.read_bytes(),
                name="DEO CRM Official Letter",
                description="Legal-size DEO (M-EE), Lahore template supplied by the office.",
                actor="system",
                make_default=True,
            )
        signature = self.session.exec(
            select(CrmOfficialLetterSignatureProfile).where(CrmOfficialLetterSignatureProfile.is_default == True)  # noqa: E712
        ).first()
        if signature is None:
            source = self.shipped_signature_path()
            if not source.exists():
                raise OfficialLetterValidationError(f"Default signature image is missing: {source}")
            signature = self._store_signature(
                source.read_bytes(),
                filename=source.name,
                name="DEO M-EE Lahore",
                officer_name=None,
                designation="DISTRICT EDUCATION OFFICER (M-EE)",
                office_location="LAHORE",
                actor="system",
                make_default=True,
            )
        if current is None:
            current = CrmOfficialLetterSettings(
                default_template_id=template.id,
                default_signature_id=signature.id,
            )
        else:
            current.default_template_id = current.default_template_id or template.id
            current.default_signature_id = current.default_signature_id or signature.id
            current.updated_at = utcnow()
        self.session.add(current)
        self.session.commit()
        self.session.refresh(current)
        return current

    def _store_template(
        self,
        content: bytes,
        *,
        name: str,
        description: str | None,
        actor: str,
        make_default: bool,
    ) -> CrmOfficialLetterTemplate:
        metadata = _template_metadata(content)
        maximum = self.session.scalar(
            select(func.max(CrmOfficialLetterTemplate.version)).where(
                CrmOfficialLetterTemplate.template_key == "deo-crm-official-letter"
            )
        ) or 0
        record = CrmOfficialLetterTemplate(
            name=name.strip() or "DEO CRM Official Letter",
            version=int(maximum) + 1,
            description=description,
            file_path="",
            sha256=metadata["sha256"],
            size_bytes=metadata["size_bytes"],
            signature_target_path=metadata["signature_target_path"],
            active=True,
            is_default=make_default,
            created_by=actor,
        )
        destination = self.templates_root / f"{record.id}.odt"
        _atomic_write(destination, content)
        record.file_path = str(destination)
        if make_default:
            for other in self.session.exec(select(CrmOfficialLetterTemplate).where(CrmOfficialLetterTemplate.is_default == True)).all():  # noqa: E712
                other.is_default = False
                self.session.add(other)
        self.session.add(record)
        self.session.flush()
        return record

    def _store_signature(
        self,
        content: bytes,
        *,
        filename: str,
        name: str,
        officer_name: str | None,
        designation: str,
        office_location: str,
        actor: str,
        make_default: bool,
    ) -> CrmOfficialLetterSignatureProfile:
        try:
            with PillowImage.open(io.BytesIO(content)) as image:
                image.load()
                fmt = (image.format or "").upper()
                if fmt not in {"PNG", "JPEG"}:
                    raise OfficialLetterValidationError("Signature image must be PNG or JPEG")
                normalized = io.BytesIO()
                image.convert("RGBA").save(normalized, format="PNG", optimize=True)
                content = normalized.getvalue()
        except OfficialLetterValidationError:
            raise
        except Exception as exc:
            raise OfficialLetterValidationError("The signature file is not a valid image") from exc
        suffix = ".png"
        record = CrmOfficialLetterSignatureProfile(
            name=name.strip() or Path(filename).stem,
            officer_name=(officer_name or "").strip() or None,
            designation=designation.strip() or "DISTRICT EDUCATION OFFICER (M-EE)",
            office_location=office_location.strip() or "LAHORE",
            image_path="",
            image_sha256=_sha256(content),
            image_size_bytes=len(content),
            active=True,
            is_default=make_default,
            created_by=actor,
        )
        destination = self.signatures_root / f"{record.id}{suffix}"
        _atomic_write(destination, content)
        record.image_path = str(destination)
        if make_default:
            for other in self.session.exec(select(CrmOfficialLetterSignatureProfile).where(CrmOfficialLetterSignatureProfile.is_default == True)).all():  # noqa: E712
                other.is_default = False
                self.session.add(other)
        self.session.add(record)
        self.session.flush()
        return record

    @staticmethod
    def _settings_payload(record: CrmOfficialLetterSettings) -> dict[str, Any]:
        return {
            "id": str(record.id),
            "office_title": record.office_title,
            "default_recipient_name": record.default_recipient_name,
            "default_recipient_location": record.default_recipient_location,
            "default_subject_prefix": record.default_subject_prefix,
            "default_cc_entries": record.default_cc_entries,
            "date_format": record.date_format,
            "numbering_prefix": record.numbering_prefix,
            "last_numeric_number": record.last_numeric_number,
            "suggested_letter_number": f"{record.last_numeric_number + 1}/{record.numbering_prefix}" if record.numbering_prefix else str(record.last_numeric_number + 1),
            "allow_manual_override": record.allow_manual_override,
            "require_unique_number": record.require_unique_number,
            "default_template_id": str(record.default_template_id) if record.default_template_id else None,
            "default_signature_id": str(record.default_signature_id) if record.default_signature_id else None,
            "updated_by": record.updated_by,
            "updated_at": _iso(record.updated_at),
        }

    @staticmethod
    def _template_payload(record: CrmOfficialLetterTemplate) -> dict[str, Any]:
        return {
            "id": str(record.id), "template_key": record.template_key, "name": record.name,
            "version": record.version, "description": record.description, "sha256": record.sha256,
            "size_bytes": record.size_bytes, "active": record.active, "is_default": record.is_default,
            "signature_target_path": record.signature_target_path, "created_by": record.created_by,
            "created_at": _iso(record.created_at),
            "download_url": f"/api/v1/crm/official-letters/templates/{record.id}/download",
        }

    @staticmethod
    def _signature_payload(record: CrmOfficialLetterSignatureProfile) -> dict[str, Any]:
        return {
            "id": str(record.id), "name": record.name, "officer_name": record.officer_name,
            "designation": record.designation, "office_location": record.office_location,
            "image_sha256": record.image_sha256, "image_size_bytes": record.image_size_bytes,
            "active": record.active, "is_default": record.is_default,
            "effective_from": _iso(record.effective_from), "effective_to": _iso(record.effective_to),
            "created_by": record.created_by, "created_at": _iso(record.created_at),
            "image_url": f"/api/v1/crm/official-letters/signatures/{record.id}/image",
        }

    def configuration(self) -> dict[str, Any]:
        settings = self.ensure_defaults()
        templates = self.session.exec(select(CrmOfficialLetterTemplate).order_by(CrmOfficialLetterTemplate.version.desc())).all()
        signatures = self.session.exec(select(CrmOfficialLetterSignatureProfile).order_by(CrmOfficialLetterSignatureProfile.is_default.desc(), CrmOfficialLetterSignatureProfile.name)).all()
        settings_payload = self._settings_payload(settings)
        settings_payload["suggested_letter_number"] = self.suggest_letter_number()
        return {
            "settings": settings_payload,
            "templates": [self._template_payload(item) for item in templates],
            "signatures": [self._signature_payload(item) for item in signatures],
        }

    def update_configuration(self, payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
        record = self.ensure_defaults()
        for field in (
            "office_title", "default_recipient_name", "default_recipient_location",
            "default_subject_prefix", "default_cc_entries", "date_format", "numbering_prefix",
        ):
            if field in payload:
                setattr(record, field, str(payload[field]).strip())
        if "last_numeric_number" in payload:
            record.last_numeric_number = max(0, int(payload["last_numeric_number"]))
        if "allow_manual_override" in payload:
            record.allow_manual_override = bool(payload["allow_manual_override"])
        if "date_format" in payload and str(payload["date_format"]).strip() not in DATE_FORMATS:
            raise OfficialLetterValidationError(
                "Unsupported date format. Choose DD/MM/YYYY, DD-MM-YYYY or YYYY-MM-DD"
            )
        if "require_unique_number" in payload and not bool(payload["require_unique_number"]):
            raise OfficialLetterValidationError(
                "Official letter numbers must remain unique for audit and diary integrity"
            )
        record.require_unique_number = True
        if payload.get("default_template_id"):
            template = self.session.get(CrmOfficialLetterTemplate, uuid.UUID(str(payload["default_template_id"])))
            if not template or not template.active:
                raise OfficialLetterValidationError("Default template was not found or is inactive")
            record.default_template_id = template.id
            for item in self.session.exec(select(CrmOfficialLetterTemplate).where(CrmOfficialLetterTemplate.is_default == True)).all():  # noqa: E712
                item.is_default = item.id == template.id
                self.session.add(item)
        if payload.get("default_signature_id"):
            signature = self.session.get(CrmOfficialLetterSignatureProfile, uuid.UUID(str(payload["default_signature_id"])))
            if not signature or not signature.active:
                raise OfficialLetterValidationError("Default signature profile was not found or is inactive")
            record.default_signature_id = signature.id
            for item in self.session.exec(select(CrmOfficialLetterSignatureProfile).where(CrmOfficialLetterSignatureProfile.is_default == True)).all():  # noqa: E712
                item.is_default = item.id == signature.id
                self.session.add(item)
        record.updated_by = actor
        record.updated_at = utcnow()
        self.session.add(record)
        self.session.commit()
        return self.configuration()

    def upload_template(self, content: bytes, *, filename: str, name: str, description: str | None, actor: str, make_default: bool) -> dict[str, Any]:
        if not filename.casefold().endswith((".odt", ".ott")):
            raise OfficialLetterValidationError("Upload an ODT/OTT official-letter template")
        if len(content) > 20 * 1024 * 1024:
            raise OfficialLetterValidationError("Official-letter template exceeds 20 MB")
        record = self._store_template(content, name=name, description=description, actor=actor, make_default=make_default)
        settings = self.ensure_defaults()
        if make_default:
            settings.default_template_id = record.id
            settings.updated_by = actor
            settings.updated_at = utcnow()
            self.session.add(settings)
        self.session.commit()
        return self._template_payload(record)

    def upload_signature(
        self, content: bytes, *, filename: str, name: str, officer_name: str | None,
        designation: str, office_location: str, effective_from: date | None,
        effective_to: date | None, actor: str, make_default: bool,
    ) -> dict[str, Any]:
        if len(content) > 5 * 1024 * 1024:
            raise OfficialLetterValidationError("Signature image exceeds 5 MB")
        if effective_from and effective_to and effective_from > effective_to:
            raise OfficialLetterValidationError("Signature effective-from date cannot be after effective-to date")
        record = self._store_signature(content, filename=filename, name=name, officer_name=officer_name, designation=designation, office_location=office_location, actor=actor, make_default=make_default)
        record.effective_from = effective_from
        record.effective_to = effective_to
        record.updated_at = utcnow()
        self.session.add(record)
        settings = self.ensure_defaults()
        if make_default:
            settings.default_signature_id = record.id
            settings.updated_by = actor
            settings.updated_at = utcnow()
            self.session.add(settings)
        self.session.commit()
        return self._signature_payload(record)

    def set_template_active(self, template_id: uuid.UUID, *, active: bool, actor: str) -> dict[str, Any]:
        record = self.session.get(CrmOfficialLetterTemplate, template_id)
        if record is None:
            raise OfficialLetterNotFound("Official-letter template was not found")
        settings = self.ensure_defaults()
        if not active and settings.default_template_id == record.id:
            raise OfficialLetterValidationError("Choose another default template before deactivating this one")
        record.active = active
        self.session.add(record)
        self.session.commit()
        return self._template_payload(record)

    def set_signature_active(self, signature_id: uuid.UUID, *, active: bool, actor: str) -> dict[str, Any]:
        record = self.session.get(CrmOfficialLetterSignatureProfile, signature_id)
        if record is None:
            raise OfficialLetterNotFound("Signature profile was not found")
        settings = self.ensure_defaults()
        if not active and settings.default_signature_id == record.id:
            raise OfficialLetterValidationError("Choose another default signature before deactivating this one")
        record.active = active
        record.updated_at = utcnow()
        self.session.add(record)
        self.session.commit()
        return self._signature_payload(record)

    def _revision_for_case(self, case_id: uuid.UUID, revision_id: uuid.UUID | None = None) -> ComplaintReplyRevision:
        if revision_id:
            revision = self.session.get(ComplaintReplyRevision, revision_id)
            if revision is None or revision.complaint_case_id != case_id:
                raise OfficialLetterValidationError("Approved reply revision does not belong to this complaint")
        else:
            revision = self.session.exec(
                select(ComplaintReplyRevision)
                .where(ComplaintReplyRevision.complaint_case_id == case_id)
                .order_by(ComplaintReplyRevision.is_current.desc(), ComplaintReplyRevision.captured_at.desc())
            ).first()
        if revision is None or revision.approval_status not in {"Approved", "Issued"}:
            raise OfficialLetterValidationError("Approve or issue the reply before creating an official letter")
        return revision

    def _resolve_template(self, template_id: uuid.UUID | None) -> CrmOfficialLetterTemplate:
        settings = self.ensure_defaults()
        record = self.session.get(CrmOfficialLetterTemplate, template_id or settings.default_template_id)
        if record is None or not record.active:
            raise OfficialLetterValidationError("An active official-letter template is required")
        return record

    def _resolve_signature(
        self, signature_id: uuid.UUID | None, *, effective_on: date | None = None
    ) -> CrmOfficialLetterSignatureProfile:
        settings = self.ensure_defaults()
        record = self.session.get(CrmOfficialLetterSignatureProfile, signature_id or settings.default_signature_id)
        if record is None or not record.active:
            raise OfficialLetterValidationError("An active signature profile is required")
        reference_date = effective_on or date.today()
        if record.effective_from and record.effective_from > reference_date:
            raise OfficialLetterValidationError("The selected signature profile is not effective on the letter date")
        if record.effective_to and record.effective_to < reference_date:
            raise OfficialLetterValidationError("The selected signature profile has expired for the letter date")
        return record

    def suggest_letter_number(self, *, start: int | None = None) -> str:
        settings = self.ensure_defaults()
        candidate = max(settings.last_numeric_number + 1, start or 0)
        prefix = settings.numbering_prefix.strip()
        for _ in range(100_000):
            number = f"{candidate}/{prefix}" if prefix else str(candidate)
            exists = self.session.exec(
                select(CrmOfficialLetter.id).where(CrmOfficialLetter.letter_number == number)
            ).first()
            if exists is None:
                return number
            candidate += 1
        raise OfficialLetterError("Unable to find an available official letter number")

    def statistics(self) -> dict[str, int]:
        approved_case_ids = set(
            self.session.exec(
                select(ComplaintReplyRevision.complaint_case_id).where(
                    ComplaintReplyRevision.is_current == True,  # noqa: E712
                    ComplaintReplyRevision.approval_status.in_(("Approved", "Issued")),
                )
            ).all()
        )
        finalized_case_ids = set(
            self.session.exec(
                select(CrmOfficialLetter.complaint_case_id).where(
                    CrmOfficialLetter.status == "finalized"
                )
            ).all()
        )
        counts = {"preview": 0, "finalized": 0, "superseded": 0, "failed": 0}
        for status, count in self.session.exec(
            select(CrmOfficialLetter.status, func.count()).group_by(CrmOfficialLetter.status)
        ).all():
            if status in counts:
                counts[status] = int(count)
        return {
            "awaiting_letter": len(approved_case_ids - finalized_case_ids),
            "previews": counts["preview"],
            "finalized": counts["finalized"],
            "superseded": counts["superseded"],
            "failures": counts["failed"],
        }

    def prepare_case(self, case_id: uuid.UUID) -> dict[str, Any]:
        settings = self.ensure_defaults()
        case = self.session.get(ComplaintCase, case_id)
        if case is None:
            raise OfficialLetterNotFound("Complaint case was not found")
        revision = self._revision_for_case(case_id)
        existing = self.session.exec(
            select(CrmOfficialLetter)
            .where(CrmOfficialLetter.complaint_case_id == case_id)
            .order_by(CrmOfficialLetter.revision.desc())
        ).all()
        config = self.configuration()
        return {
            "case": {
                "id": str(case.id), "complaint_number": case.complaint_number,
                "complaint_text": case.remarks or "", "category": case.category,
                "subcategory": case.sub_category,
            },
            "approved_revision": {
                "id": str(revision.id), "approval_status": revision.approval_status,
                "reply_text": revision.reply_text, "captured_at": _iso(revision.captured_at),
                "source_reference": revision.source_reference,
            },
            "defaults": {
                **config["settings"],
                "letter_date": date.today().isoformat(),
            },
            "templates": [item for item in config["templates"] if item["active"]],
            "signatures": [item for item in config["signatures"] if item["active"]],
            "letters": [self._letter_payload(item, include_artifacts=True) for item in existing],
        }

    def _assert_letter_number(self, number: str, *, exclude_id: uuid.UUID | None = None) -> None:
        settings = self.ensure_defaults()
        if not number.strip():
            raise OfficialLetterValidationError("Letter number is required")
        if not settings.allow_manual_override:
            expected = self._settings_payload(settings)["suggested_letter_number"]
            if number.strip() != expected:
                raise OfficialLetterValidationError(f"Letter number must be {expected}")
        if settings.require_unique_number:
            statement = select(CrmOfficialLetter).where(CrmOfficialLetter.letter_number == number.strip())
            if exclude_id:
                statement = statement.where(CrmOfficialLetter.id != exclude_id)
            if self.session.exec(statement).first():
                raise OfficialLetterValidationError("This official letter number is already in use")

    def _write_letter_artifact(self, letter: CrmOfficialLetter, *, kind: str, name: str, content_type: str, content: bytes) -> CrmOfficialLetterArtifact:
        directory = self.letters_root / str(letter.id)
        directory.mkdir(parents=True, exist_ok=True)
        destination = directory / name
        _atomic_write(destination, content)
        existing = self.session.exec(
            select(CrmOfficialLetterArtifact).where(
                CrmOfficialLetterArtifact.official_letter_id == letter.id,
                CrmOfficialLetterArtifact.kind == kind,
            )
        ).first()
        record = existing or CrmOfficialLetterArtifact(
            official_letter_id=letter.id, kind=kind, name=name, path=str(destination),
            content_type=content_type, size_bytes=len(content), sha256=_sha256(content),
        )
        record.name = name
        record.path = str(destination)
        record.content_type = content_type
        record.size_bytes = len(content)
        record.sha256 = _sha256(content)
        record.created_at = utcnow()
        self.session.add(record)
        return record

    def generate(
        self,
        case_id: uuid.UUID,
        *,
        letter_number: str,
        letter_date: date,
        recipient_name: str,
        recipient_location: str,
        subject_prefix: str,
        cc_entries: str,
        template_id: uuid.UUID | None,
        signature_profile_id: uuid.UUID | None,
        reply_revision_id: uuid.UUID | None,
        actor: str,
        finalize: bool,
        letter_id: uuid.UUID | None = None,
        supersedes_letter_id: uuid.UUID | None = None,
        commit: bool = True,
    ) -> dict[str, Any]:
        case = self.session.get(ComplaintCase, case_id)
        if case is None:
            raise OfficialLetterNotFound("Complaint case was not found")
        revision = self._revision_for_case(case_id, reply_revision_id)
        template = self._resolve_template(template_id)
        signature = self._resolve_signature(signature_profile_id, effective_on=letter_date)
        failed_retry = None
        if letter_id is None:
            failed_retry = self.session.exec(
                select(CrmOfficialLetter).where(
                    CrmOfficialLetter.complaint_case_id == case_id,
                    CrmOfficialLetter.letter_number == letter_number.strip(),
                    CrmOfficialLetter.status == "failed",
                )
            ).first()
            if failed_retry is not None:
                letter_id = failed_retry.id
        self._assert_letter_number(letter_number, exclude_id=letter_id)
        if supersedes_letter_id:
            previous = self.session.get(CrmOfficialLetter, supersedes_letter_id)
            if previous is None or previous.complaint_case_id != case_id or previous.status != "finalized":
                raise OfficialLetterValidationError(
                    "A revised letter must supersede a finalized letter for the same complaint"
                )
        if not recipient_name.strip() or not recipient_location.strip():
            raise OfficialLetterValidationError("Recipient name and location are required")
        if not subject_prefix.strip():
            raise OfficialLetterValidationError("Subject prefix is required")
        if letter_id:
            letter = self.session.get(CrmOfficialLetter, letter_id)
            if letter is None or letter.complaint_case_id != case_id:
                raise OfficialLetterNotFound("Official letter draft was not found")
            if letter.status in {"finalized", "superseded"}:
                raise OfficialLetterValidationError("A finalized letter cannot be overwritten; create a revised letter")
        else:
            maximum = self.session.scalar(
                select(func.max(CrmOfficialLetter.revision)).where(CrmOfficialLetter.complaint_case_id == case_id)
            ) or 0
            letter = CrmOfficialLetter(
                complaint_case_id=case.id, reply_revision_id=revision.id,
                template_id=template.id, signature_profile_id=signature.id,
                letter_number=letter_number.strip(), letter_date=letter_date,
                recipient_name=recipient_name.strip(), recipient_location=recipient_location.strip(),
                subject_prefix=subject_prefix.strip(), cc_entries=cc_entries.strip() or "Office Record.",
                status="preview", revision=int(maximum) + 1,
                supersedes_letter_id=supersedes_letter_id,
                complaint_number_snapshot=case.complaint_number or str(case.id),
                complaint_text_snapshot=case.remarks or "",
                reply_text_snapshot=revision.reply_text,
                generated_by=actor,
            )
        settings_record = self.ensure_defaults()
        values = {
            "letter_number": letter_number.strip(),
            "letter_date": _format_letter_date(letter_date, settings_record.date_format),
            "recipient_name": recipient_name.strip(),
            "recipient_location": recipient_location.strip(),
            "subject_prefix": subject_prefix.strip(),
            "complaint_number": case.complaint_number or str(case.id),
            "complaint_details": case.remarks or "No complaint details were recorded.",
            "approved_reply": revision.reply_text,
            "signatory_designation": signature.designation,
            "signatory_location": signature.office_location,
            "cc_entries": cc_entries.strip() or "Office Record.",
        }
        _template_record, template_path = self.template_path(template.id)
        _signature_record, signature_path = self.signature_path(signature.id)
        template_bytes = template_path.read_bytes()
        signature_bytes = signature_path.read_bytes()
        try:
            odt = render_official_letter_odt(
                template_bytes, values=values, signature_bytes=signature_bytes,
                signature_target_path=template.signature_target_path,
            )
            pdf, pdf_renderer = render_official_letter_pdf(
                odt, template=template_bytes, values=values, signature_bytes=signature_bytes,
            )
        except Exception as exc:
            letter.status = "failed"
            letter.error = str(exc)[:4000]
            self.session.add(letter)
            if commit:
                self.session.commit()
            raise OfficialLetterError(f"Official letter generation failed: {exc}") from exc
        now = utcnow()
        letter.reply_revision_id = revision.id
        letter.template_id = template.id
        letter.signature_profile_id = signature.id
        letter.letter_number = letter_number.strip()
        letter.letter_date = letter_date
        letter.recipient_name = recipient_name.strip()
        letter.recipient_location = recipient_location.strip()
        letter.subject_prefix = subject_prefix.strip()
        letter.cc_entries = cc_entries.strip() or "Office Record."
        letter.status = "finalized" if finalize else "preview"
        letter.error = None
        letter.complaint_text_snapshot = case.remarks or ""
        letter.reply_text_snapshot = revision.reply_text
        letter.generated_by = actor
        letter.generated_at = now
        letter.finalized_at = now if finalize else None
        letter.configuration_snapshot_json = {
            "template": {"id": str(template.id), "name": template.name, "version": template.version, "sha256": template.sha256},
            "signature": {"id": str(signature.id), "name": signature.name, "officer_name": signature.officer_name, "designation": signature.designation, "office_location": signature.office_location, "sha256": signature.image_sha256},
            "reply_revision": {"id": str(revision.id), "approval_status": revision.approval_status, "content_hash": revision.content_hash},
            "values": values,
            "pdf_renderer": pdf_renderer,
            "pdf_page_count": _pdf_page_count(pdf),
            "warnings": (["The generated official letter spans more than one legal-size page."] if _pdf_page_count(pdf) > 1 else []),
        }
        self.session.add(letter)
        self.session.flush()
        base = _safe_filename(f"{letter.complaint_number_snapshot} - DEO Official Letter")
        self._write_letter_artifact(letter, kind="odt", name=f"{base}.odt", content_type=ODT_MIME, content=odt)
        self._write_letter_artifact(letter, kind="pdf", name=f"{base}.pdf", content_type="application/pdf", content=pdf)
        if finalize:
            settings = self.ensure_defaults()
            leading = letter.letter_number.split("/", 1)[0].strip()
            if leading.isdigit() and int(leading) > settings.last_numeric_number:
                settings.last_numeric_number = int(leading)
                settings.updated_by = actor
                settings.updated_at = now
                self.session.add(settings)
            if supersedes_letter_id:
                previous = self.session.get(CrmOfficialLetter, supersedes_letter_id)
                if previous and previous.complaint_case_id == case_id and previous.status == "finalized":
                    previous.status = "superseded"
                    self.session.add(previous)
        if commit:
            self.session.commit()
            self.session.refresh(letter)
        return self._letter_payload(letter, include_artifacts=True)

    def finalize(self, letter_id: uuid.UUID, *, actor: str) -> dict[str, Any]:
        letter = self.session.get(CrmOfficialLetter, letter_id)
        if letter is None:
            raise OfficialLetterNotFound("Official letter was not found")
        if letter.status == "finalized":
            return self._letter_payload(letter, include_artifacts=True)
        if letter.status != "preview":
            raise OfficialLetterValidationError("Only a preview letter can be finalized")
        self._assert_letter_number(letter.letter_number, exclude_id=letter.id)
        letter.status = "finalized"
        letter.finalized_at = utcnow()
        letter.generated_by = actor
        settings = self.ensure_defaults()
        leading = letter.letter_number.split("/", 1)[0].strip()
        if leading.isdigit() and int(leading) > settings.last_numeric_number:
            settings.last_numeric_number = int(leading)
            settings.updated_by = actor
            settings.updated_at = utcnow()
            self.session.add(settings)
        if letter.supersedes_letter_id:
            previous = self.session.get(CrmOfficialLetter, letter.supersedes_letter_id)
            if previous and previous.complaint_case_id == letter.complaint_case_id and previous.status == "finalized":
                previous.status = "superseded"
                self.session.add(previous)
        self.session.add(letter)
        self.session.commit()
        return self._letter_payload(letter, include_artifacts=True)

    def _artifact_payload(self, artifact: CrmOfficialLetterArtifact) -> dict[str, Any]:
        return {
            "id": str(artifact.id), "kind": artifact.kind, "name": artifact.name,
            "content_type": artifact.content_type, "size_bytes": artifact.size_bytes,
            "sha256": artifact.sha256, "created_at": _iso(artifact.created_at),
            "download_url": f"/api/v1/crm/official-letters/artifacts/{artifact.id}/download",
            "preview_url": f"/api/v1/crm/official-letters/artifacts/{artifact.id}/preview" if artifact.kind == "pdf" else None,
        }

    def _letter_payload(self, record: CrmOfficialLetter, *, include_artifacts: bool = False) -> dict[str, Any]:
        payload = {
            "id": str(record.id), "complaint_case_id": str(record.complaint_case_id),
            "reply_revision_id": str(record.reply_revision_id), "template_id": str(record.template_id),
            "signature_profile_id": str(record.signature_profile_id), "letter_number": record.letter_number,
            "letter_date": _iso(record.letter_date), "recipient_name": record.recipient_name,
            "recipient_location": record.recipient_location, "subject_prefix": record.subject_prefix,
            "cc_entries": record.cc_entries, "status": record.status, "revision": record.revision,
            "supersedes_letter_id": str(record.supersedes_letter_id) if record.supersedes_letter_id else None,
            "complaint_number": record.complaint_number_snapshot, "generated_by": record.generated_by,
            "error": record.error, "created_at": _iso(record.created_at),
            "generated_at": _iso(record.generated_at), "finalized_at": _iso(record.finalized_at),
            "editor_url": f"/crm/replies/{record.complaint_case_id}/",
            "letter_url": f"/crm/replies/{record.complaint_case_id}/official-letter/?letter={record.id}",
            "warnings": list((record.configuration_snapshot_json or {}).get("warnings") or []),
            "pdf_page_count": (record.configuration_snapshot_json or {}).get("pdf_page_count"),
        }
        if include_artifacts:
            artifacts = self.session.exec(
                select(CrmOfficialLetterArtifact).where(CrmOfficialLetterArtifact.official_letter_id == record.id)
            ).all()
            payload["artifacts"] = [self._artifact_payload(item) for item in artifacts]
            payload["configuration_snapshot"] = record.configuration_snapshot_json
        return payload

    def list_letters(self, *, status: str = "", search: str = "", page: int = 1, page_size: int = 25) -> dict[str, Any]:
        statement = select(CrmOfficialLetter)
        count = select(func.count()).select_from(CrmOfficialLetter)
        filters = []
        if status:
            filters.append(CrmOfficialLetter.status == status)
        if search.strip():
            like = f"%{search.strip()}%"
            filters.append(
                (CrmOfficialLetter.letter_number.ilike(like))
                | (CrmOfficialLetter.complaint_number_snapshot.ilike(like))
                | (CrmOfficialLetter.recipient_name.ilike(like))
            )
        statement = statement.where(*filters)
        count = count.where(*filters)
        total = int(self.session.scalar(count) or 0)
        rows = self.session.exec(
            statement.order_by(CrmOfficialLetter.created_at.desc())
            .offset((page - 1) * page_size).limit(page_size)
        ).all()
        return {"items": [self._letter_payload(item, include_artifacts=True) for item in rows], "total": total, "page": page, "page_size": page_size}

    def detail(self, letter_id: uuid.UUID) -> dict[str, Any]:
        record = self.session.get(CrmOfficialLetter, letter_id)
        if record is None:
            raise OfficialLetterNotFound("Official letter was not found")
        return self._letter_payload(record, include_artifacts=True)

    def artifact_path(self, artifact_id: uuid.UUID) -> tuple[CrmOfficialLetterArtifact, Path]:
        record = self.session.get(CrmOfficialLetterArtifact, artifact_id)
        if record is None:
            raise OfficialLetterNotFound("Official letter artifact was not found")
        path = Path(record.path).expanduser().resolve(strict=False)
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise OfficialLetterError("Official letter artifact path is outside managed storage") from exc
        if not path.exists() or not path.is_file():
            raise OfficialLetterNotFound("Official letter artifact file is unavailable")
        if _sha256(path.read_bytes()) != record.sha256:
            raise OfficialLetterError("Official letter artifact checksum verification failed")
        return record, path

    def template_path(self, template_id: uuid.UUID) -> tuple[CrmOfficialLetterTemplate, Path]:
        record = self.session.get(CrmOfficialLetterTemplate, template_id)
        if record is None:
            raise OfficialLetterNotFound("Official letter template was not found")
        path = Path(record.file_path).expanduser().resolve(strict=False)
        try:
            path.relative_to(self.templates_root)
        except ValueError as exc:
            raise OfficialLetterError("Official letter template path is outside managed storage") from exc
        if not path.exists() or _sha256(path.read_bytes()) != record.sha256:
            raise OfficialLetterError("Official letter template file is unavailable or changed")
        return record, path

    def signature_path(self, signature_id: uuid.UUID) -> tuple[CrmOfficialLetterSignatureProfile, Path]:
        record = self.session.get(CrmOfficialLetterSignatureProfile, signature_id)
        if record is None:
            raise OfficialLetterNotFound("Signature profile was not found")
        path = Path(record.image_path).expanduser().resolve(strict=False)
        try:
            path.relative_to(self.signatures_root)
        except ValueError as exc:
            raise OfficialLetterError("Signature image path is outside managed storage") from exc
        if not path.exists() or _sha256(path.read_bytes()) != record.image_sha256:
            raise OfficialLetterError("Signature image is unavailable or changed")
        return record, path

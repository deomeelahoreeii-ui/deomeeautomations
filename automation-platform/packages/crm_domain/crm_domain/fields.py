from __future__ import annotations

import re
from dataclasses import dataclass

from crm_domain.identifiers import normalize_complaint_number


PAPERLESS_COMPLAINT_FIELDS: dict[str, str] = {
    "complaint_number": "Complaint Number",
    "revision": "Revision",
    "source": "Source",
    "direction": "Direction",
    "document_role": "Document Role",
    "status": "Status",
    "complainant_name": "Complainant Name",
    "complainant_mobile": "Complainant Mobile Number",
    "complainant_cnic": "Complainant CNIC",
    "complainant_address": "Complainant Address",
    "district": "District",
    "tehsil": "Tehsil",
    "department": "Department",
    "category": "Complaint Category",
    "sub_category": "Complaint Sub-Category",
    "remarks": "Remarks",
    "reported_entity_address": "The Reported Entity Address",
    "parent_case": "Parent Case",
}

FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "complaint_number": ("complaint no", "complaint number", "complaint id", "complaint code"),
    "complainant_name": ("person name", "applicant name", "complainant name", "name"),
    "complainant_mobile": ("mobile no", "mobile number", "phone no", "contact no", "contact"),
    "complainant_cnic": ("cnic no", "cnic"),
    "complainant_address": ("person address", "applicant address", "complainant address", "address"),
    "district": ("complaint district", "district"),
    "tehsil": ("tehsil",),
    "department": ("department", "level one", "level 1"),
    "category": ("complaint category", "category"),
    "sub_category": ("complaint sub category", "sub category", "subcategory"),
    "remarks": ("complaint remarks", "complaint details", "remarks", "description"),
    "gender": ("gender",),
    "caller_district": ("caller district",),
    "escalation_level": ("escalation level",),
    "portal_status": ("complaint status",),
    "portal_sub_status": ("complaint sub status",),
    "status_change_comment": ("status change comment",),
    "reported_entity_address": ("reported entity address", "the reported entity address"),
    "portal_created_date": ("created date",),
    "portal_last_activity": ("last activity",),
}


@dataclass(frozen=True)
class ExtractedField:
    field_name: str
    raw_value: str
    normalized_value: str
    confidence: float
    source_locator: str


def _normalized_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _compact(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip(" \t:|-")


def _normalize_value(field_name: str, value: str) -> str:
    compact = _compact(value)
    if field_name == "complaint_number":
        return normalize_complaint_number(compact) or ""
    if field_name == "complainant_mobile":
        digits = re.sub(r"\D+", "", compact)
        return digits if 10 <= len(digits) <= 15 else ""
    if field_name == "complainant_cnic":
        digits = re.sub(r"\D+", "", compact)
        return digits if len(digits) == 13 else ""
    if field_name == "tehsil":
        return _compact(re.split(r"\b(?:zila|district)\b", compact, maxsplit=1, flags=re.IGNORECASE)[0])
    return compact


def extract_field_observations(text: str) -> list[ExtractedField]:
    """Extract conservative labeled fields while retaining line provenance.

    OCR output is deliberately treated as an observation, not authoritative
    complaint data. Unlabelled free text is retained by DocumentExtraction and
    is not guessed into canonical fields here.
    """

    observations: list[ExtractedField] = []
    seen: set[tuple[str, str]] = set()
    alias_pairs = sorted(
        ((alias, field_name) for field_name, names in FIELD_ALIASES.items() for alias in names),
        key=lambda item: len(item[0]),
        reverse=True,
    )
    alias_to_field = {_normalized_label(alias): field for alias, field in alias_pairs}
    label_pattern = re.compile(
        r"(?<![A-Za-z0-9])(?:"
        + "|".join(re.escape(alias).replace(r"\ ", r"\s+") for alias, _field in alias_pairs)
        + r")(?=\s|[:|\-]|$)",
        re.IGNORECASE,
    )
    lines = (text or "").splitlines()
    for line_number, raw_line in enumerate(lines, start=1):
        line = _compact(raw_line)
        if not line:
            continue
        matches = list(label_pattern.finditer(line))
        for position, match in enumerate(matches):
            field_name = alias_to_field.get(_normalized_label(match.group(0)))
            if not field_name:
                continue
            end = matches[position + 1].start() if position + 1 < len(matches) else len(line)
            raw_value = _compact(line[match.end() : end])
            if field_name == "remarks" and not raw_value:
                continuation: list[str] = []
                for following in lines[line_number:]:
                    value = _compact(following)
                    if not value:
                        continue
                    if re.match(r"^(?:generated\s+on|complaint\s+label\s+ocr)\b", value, re.IGNORECASE):
                        break
                    continuation.append(value)
                raw_value = _compact(" ".join(continuation))
            normalized_value = _normalize_value(field_name, raw_value)
            if not normalized_value or (field_name, normalized_value) in seen:
                continue
            seen.add((field_name, normalized_value))
            observations.append(
                ExtractedField(
                    field_name=field_name,
                    raw_value=raw_value,
                    normalized_value=normalized_value,
                    confidence=0.98 if field_name == "complaint_number" else 0.85,
                    source_locator=f"line:{line_number}",
                )
            )
    return observations


def extract_mapping_observations(
    values: dict[str, object], *, source_locator: str
) -> list[ExtractedField]:
    observations: list[ExtractedField] = []
    for raw_label, raw_value in values.items():
        label = _normalized_label(str(raw_label))
        field_name = next(
            (
                name
                for name, aliases in FIELD_ALIASES.items()
                if label in {_normalized_label(alias) for alias in aliases}
            ),
            None,
        )
        if field_name is None or raw_value in (None, ""):
            continue
        value = _compact(str(raw_value))
        normalized = _normalize_value(field_name, value)
        if normalized:
            observations.append(
                ExtractedField(
                    field_name=field_name,
                    raw_value=value,
                    normalized_value=normalized,
                    confidence=1.0,
                    source_locator=source_locator,
                )
            )
    return observations


def paperless_custom_field_values(
    values: dict[str, object],
    *,
    document_role: str,
    parent_document_id: int | str | None = None,
) -> dict[str, object]:
    """Map native canonical names to Paperless field names.

    Select-option IDs are intentionally resolved by the Paperless adapter at
    publication time; domain records store stable labels only.
    """

    result = {
        PAPERLESS_COMPLAINT_FIELDS[key]: value
        for key, value in values.items()
        if key in PAPERLESS_COMPLAINT_FIELDS and value not in (None, "")
    }
    result["Document Role"] = document_role
    if document_role != "Main Complaint":
        result.pop("Status", None)
        if parent_document_id is not None:
            result["Parent Case"] = parent_document_id
    return result

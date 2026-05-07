import asyncio
import datetime
import json
import os
import re
import shutil
import sys
import time
import uuid
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path

import nats  # Removed httpx
import pandas as pd
from dotenv import load_dotenv
from nats.js.api import RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError
from openpyxl.styles import Alignment, Border, Font, Side
from openpyxl.utils import get_column_letter
from prefect import flow, get_run_logger, task

# Import your custom scraper task
from scraper import scrape_portal_data

# ==========================================
# 1. CONFIGURATION & FOLDER PATHS
# ==========================================
BASE_DIR = Path(__file__).parent.resolve()
load_dotenv(BASE_DIR / ".env")


def _resolve_project_path(value: str | Path) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else (BASE_DIR / candidate).resolve()

OUTPUT_DIR = BASE_DIR / "output-files"
ARCHIVE_DIR = BASE_DIR / "archived-files"
UNMAPPED_REPORT_DIR = BASE_DIR / "unmapped-officer-reports"
MANUAL_UNFILTERED_DIR = BASE_DIR / "drop-manually-unfilterd-files"
MASTER_FILE = _resolve_project_path(
    os.getenv("MASTER_FILE_PATH", "List-school-M-EE-09-04-2026.xlsx")
)
WHATSAPP_RECIPIENTS_FILE = _resolve_project_path(
    os.getenv("WHATSAPP_RECIPIENTS_FILE", "whatsapp_recipients.csv")
)
OFFICERS_LIST_FILE = _resolve_project_path(
    os.getenv("OFFICERS_LIST_FILE", "officers_list.csv")
)
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
NATS_SUBJECT = os.getenv("NATS_SUBJECT", "whatsapp.pending")
NATS_STREAM = os.getenv("NATS_STREAM", "pending_stream")
DEFAULT_SEND_DELAY_MS = int(os.getenv("WA_SEND_DELAY_MS", "1500"))
GENERATE_SCREENSHOT = os.getenv("GENERATE_SCREENSHOT", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DEFAULT_ATTACHMENT_TEXT_MODE = os.getenv("WA_ATTACHMENT_TEXT_MODE", "separate")
UNFILTERED_DORMANT_ACTIVITY_COLUMNS = [
    "Simple Activities",
    "Patient Activities",
    "Vector Surveillance Activities",
    "Larvae Case Response",
    "TPV Activities",
    "Total Activities",
]


def _clean_optional_text(value) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None

    return text


def _parse_bool(value, default: bool = True) -> bool:
    if value is None:
        return default

    text = str(value).strip().lower()
    if not text:
        return default

    return text not in {"0", "false", "no", "off"}


INCLUDE_EXCEL_FOR_DDEO = _parse_bool(os.getenv("WA_INCLUDE_EXCEL_FOR_DDEO"), True)
INCLUDE_EXCEL_FOR_AEO = _parse_bool(os.getenv("WA_INCLUDE_EXCEL_FOR_AEO"), False)


def _parse_delay_ms(value) -> int | None:
    text = _clean_optional_text(value)
    if text is None:
        return None

    parsed = int(text)
    if parsed < 0:
        raise ValueError("delay_ms must be a non-negative integer")
    return parsed


def _normalize_attachment_text_mode(value, default: str = "separate") -> str:
    text = _clean_optional_text(value)
    normalized_default = (default or "separate").strip().lower()

    if normalized_default not in {"caption", "separate"}:
        normalized_default = "separate"

    if text is None:
        return normalized_default

    normalized = text.strip().lower()
    if normalized not in {"caption", "separate"}:
        raise ValueError(
            "attachment_text_mode must be either 'caption' or 'separate'"
        )

    return normalized


def _should_include_excel_for_roles(role_labels: list[str]) -> bool:
    normalized_roles = {role.strip().upper() for role in role_labels}

    if "DDEO" in normalized_roles:
        return INCLUDE_EXCEL_FOR_DDEO

    if "AEO" in normalized_roles:
        return INCLUDE_EXCEL_FOR_AEO

    return False


def _resolve_from_recipients_file(value: str | None) -> Path | None:
    text = _clean_optional_text(value)
    if text is None:
        return None

    candidate = Path(text)
    if not candidate.is_absolute():
        candidate = (WHATSAPP_RECIPIENTS_FILE.parent / candidate).resolve()

    return candidate


def _normalize_target(target: str, recipient_type: str | None) -> tuple[str, str]:
    cleaned_target = _clean_optional_text(target)
    if cleaned_target is None:
        raise ValueError("target is required")

    raw_type = _clean_optional_text(recipient_type)
    normalized_type = raw_type.lower() if raw_type else (
        "group" if cleaned_target.endswith("@g.us") else "contact"
    )

    if normalized_type == "group":
        if not cleaned_target.endswith("@g.us"):
            raise ValueError(
                f"group targets must end with @g.us, received: {cleaned_target}"
            )
        return cleaned_target, normalized_type

    if normalized_type == "contact":
        if cleaned_target.endswith("@s.whatsapp.net"):
            return cleaned_target, normalized_type

        digits = re.sub(r"\D", "", cleaned_target)
        if not digits:
            raise ValueError(f"invalid contact target: {cleaned_target}")

        return f"{digits}@s.whatsapp.net", normalized_type

    raise ValueError(f"unsupported recipient type: {normalized_type}")


def _render_template(template: str | None, variables: dict[str, str]) -> str | None:
    text = _clean_optional_text(template)
    if text is None:
        return None

    return re.sub(
        r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}",
        lambda match: str(variables.get(match.group(1), "")),
        text,
    )


def _normalize_dataframe_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.copy()
    dataframe.columns = (
        dataframe.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
    )
    return dataframe.loc[:, ~dataframe.columns.str.startswith("unnamed")]


def _read_dataframe_by_content(file_path: Path, *, dtype=str) -> pd.DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_path.stat().st_size == 0:
        raise ValueError(f"File is empty: {file_path}")

    with file_path.open("rb") as handle:
        signature = handle.read(8)

    if signature.startswith(b"PK"):
        return pd.read_excel(file_path, dtype=dtype, engine="openpyxl")

    if signature.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return pd.read_excel(file_path, dtype=dtype, engine="xlrd")

    csv_encodings = ("utf-8", "utf-8-sig", "cp1252", "latin1")
    last_error = None

    for encoding in csv_encodings:
        try:
            return pd.read_csv(file_path, dtype=dtype, encoding=encoding)
        except (UnicodeDecodeError, pd.errors.EmptyDataError) as exc:
            last_error = exc

    suffix = file_path.suffix.lower()
    if suffix == ".xlsx":
        return pd.read_excel(file_path, dtype=dtype, engine="openpyxl")
    if suffix == ".xls":
        return pd.read_excel(file_path, dtype=dtype, engine="xlrd")

    raise last_error or ValueError(f"Unable to read file: {file_path}")


def _read_table_dataframe(file_path: Path) -> pd.DataFrame:
    dataframe = _read_dataframe_by_content(file_path, dtype=str)
    return _normalize_dataframe_columns(dataframe)


def _read_recipients_dataframe() -> pd.DataFrame:
    recipients_df = _read_table_dataframe(WHATSAPP_RECIPIENTS_FILE)
    try:
        if "target" not in recipients_df.columns:
            raise KeyError("The recipients file must include a 'target' column.")
    except Exception as exc:
        raise ValueError(
            f"Invalid fixed recipients file {WHATSAPP_RECIPIENTS_FILE}: {exc}"
        ) from exc

    return recipients_df


def _normalize_emis(value) -> str | None:
    text = _clean_optional_text(value)
    if text is None:
        return None

    digits = re.sub(r"\D", "", text)
    return digits or None


def _normalize_pk_mobile(value) -> str:
    text = _clean_optional_text(value)
    if text is None:
        raise ValueError("mobile number is required")

    cleaned = text.replace("'", "").replace('"', "").replace(" ", "")
    cleaned = cleaned.replace("-", "").replace("(", "").replace(")", "")
    cleaned = cleaned.replace("+", "")

    if re.search(r"[eE]", cleaned):
        try:
            cleaned = str(Decimal(cleaned).quantize(Decimal("1")))
        except InvalidOperation as exc:
            raise ValueError(f"invalid scientific mobile number: {text}") from exc

    digits = re.sub(r"\D", "", cleaned)

    if digits.startswith("0092"):
        digits = digits[2:]
    elif digits.startswith("92") and len(digits) == 12:
        pass
    elif digits.startswith("03") and len(digits) == 11:
        digits = f"92{digits[1:]}"
    elif digits.startswith("3") and len(digits) == 10:
        digits = f"92{digits}"
    else:
        raise ValueError(f"unsupported Pakistani mobile format: {text}")

    if not re.fullmatch(r"92[0-9]{10}", digits):
        raise ValueError(f"invalid Pakistani mobile number: {text}")

    if digits[2] != "3":
        raise ValueError(f"Pakistani mobile number must start with 3 after country code: {text}")

    return digits


def _create_job_payload(
    *,
    target: str,
    recipient_type: str,
    recipient_name: str | None,
    attachment_text_mode: str,
    text: str | None,
    excel_path: Path | None,
    image_path: Path | None,
    excel_filename: str | None,
    delay_ms: int,
) -> dict:
    if excel_path and not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_path}")

    if image_path and not image_path.exists():
        raise FileNotFoundError(f"Image file not found: {image_path}")

    if not text and excel_path is None and image_path is None:
        raise ValueError("Each recipient needs text, excel_path, or image_path.")

    return {
        "job_id": str(uuid.uuid4()),
        "target": target,
        "type": recipient_type,
        "recipient_name": recipient_name,
        "attachment_text_mode": attachment_text_mode,
        "text": text,
        "excel_path": str(excel_path) if excel_path else None,
        "excel_filename": excel_filename,
        "image_path": str(image_path) if image_path else None,
        "delay_ms": delay_ms,
    }


def build_fixed_whatsapp_payloads(
    excel_path: Path, image_path: Path | None, message_body: str
) -> list[dict]:
    recipients_df = _read_recipients_dataframe()
    payloads: list[dict] = []
    errors: list[str] = []

    generated_excel_path = excel_path.resolve()
    generated_image_path = image_path.resolve() if image_path and image_path.exists() else None

    for row_number, row in enumerate(
        recipients_df.to_dict(orient="records"), start=2
    ):
        try:
            if not _parse_bool(row.get("enabled"), default=True):
                continue

            target, recipient_type = _normalize_target(
                row.get("target"), row.get("type")
            )
            recipient_name = _clean_optional_text(row.get("name"))
            variables = {
                "name": recipient_name or "",
                "target": target,
                "type": recipient_type,
            }

            text = _render_template(row.get("text"), variables) or message_body
            excel_override = _resolve_from_recipients_file(row.get("excel_path"))
            image_override = _resolve_from_recipients_file(row.get("image_path"))

            final_excel_path = excel_override or generated_excel_path
            final_image_path = image_override or generated_image_path

            if final_excel_path and not final_excel_path.exists():
                raise FileNotFoundError(f"Excel file not found: {final_excel_path}")

            if image_override and not image_override.exists():
                raise FileNotFoundError(f"Image file not found: {image_override}")

            excel_filename = (
                _clean_optional_text(row.get("excel_filename"))
                or final_excel_path.name
                if final_excel_path
                else None
            )
            delay_ms = _parse_delay_ms(row.get("delay_ms")) or DEFAULT_SEND_DELAY_MS
            attachment_text_mode = _normalize_attachment_text_mode(
                row.get("attachment_text_mode"),
                default=DEFAULT_ATTACHMENT_TEXT_MODE,
            )
            payloads.append(
                _create_job_payload(
                    target=target,
                    recipient_type=recipient_type,
                    recipient_name=recipient_name,
                    attachment_text_mode=attachment_text_mode,
                    text=text,
                    excel_path=final_excel_path,
                    image_path=final_image_path,
                    excel_filename=excel_filename,
                    delay_ms=delay_ms,
                )
            )
        except Exception as exc:
            errors.append(
                f"Row {row_number}: {row.get('target', '(missing target)')} -> {exc}"
            )

    if errors:
        raise ValueError("Recipient file validation failed:\n" + "\n".join(errors))

    return payloads


def _read_officers_dataframe() -> pd.DataFrame:
    officers_df = _read_table_dataframe(OFFICERS_LIST_FILE)
    required_columns = {
        "emis",
        "school_name",
        "ddeo_name",
        "ddeo_cell_number",
        "aeo_name",
        "aeo_cell_number",
    }
    missing_columns = sorted(required_columns.difference(officers_df.columns))

    if missing_columns:
        raise ValueError(
            f"Officers list is missing required columns: {', '.join(missing_columns)}"
        )

    officers_df["emis_normalized"] = officers_df["emis"].map(_normalize_emis)
    officers_df["school_name_normalized"] = (
        officers_df["school_name"].astype(str).str.strip()
    )
    return officers_df


def _build_officer_message(
    *,
    title_text: str,
    role_labels: list[str],
    recipient_name: str,
    schools: list[dict[str, str]],
) -> str:
    normalized_roles = {role.strip().upper() for role in role_labels}
    show_tehsil_summary = "DDEO" in normalized_roles
    tehsil_counts = Counter()
    markaz_counts = Counter()
    school_lines: list[str] = []

    for index, school in enumerate(schools, start=1):
        school_name = school["school_name"]
        tehsil = school.get("tehsil")
        markaz = school.get("markaz")

        if tehsil:
            tehsil_counts[tehsil] += 1

        if markaz:
            markaz_counts[markaz] += 1

        location_parts = []
        if tehsil:
            location_parts.append(f"Tehsil: {tehsil}")
        if markaz:
            location_parts.append(f"Markaz: {markaz}")

        location_suffix = f" ({', '.join(location_parts)})" if location_parts else ""
        school_lines.append(f"{index}. {school_name}{location_suffix}")

    intro_lines = [
        f"🚨 *{title_text}*",
        "",
        f"*For:* {recipient_name}",
        f"*Role:* {', '.join(role_labels)}",
        f"*Dormant schools mapped to you in this run:* {len(schools)}",
    ]

    if show_tehsil_summary and tehsil_counts:
        intro_lines.extend(
            [
                "",
                "📍 *Tehsil Summary*",
                *[
                    f"{index}. {tehsil}: {count}"
                    for index, (tehsil, count) in enumerate(
                        sorted(tehsil_counts.items()), start=1
                    )
                ],
            ]
        )

    if markaz_counts:
        intro_lines.extend(
            [
                "",
                "🏫 *Markaz Summary*",
                *[
                    f"{index}. {markaz}: {count}"
                    for index, (markaz, count) in enumerate(
                        sorted(markaz_counts.items()), start=1
                    )
                ],
            ]
        )

    intro_lines.extend(
        [
            "",
            "📋 *Schools*",
            *(school_lines or ["1. Not available"]),
        ]
    )

    return "\n".join(intro_lines)


def build_dynamic_officer_payloads(
    final_df: pd.DataFrame,
    excel_path: Path,
    image_path: Path | None,
    title_text: str,
) -> list[dict]:
    officers_df = _read_officers_dataframe()
    dormant_df = final_df.copy()

    school_emis_column = next(
        (column for column in dormant_df.columns if column.strip().lower() == "school emis"),
        None,
    )
    school_name_column = next(
        (column for column in dormant_df.columns if column.strip().lower() == "school name"),
        None,
    )

    if school_emis_column is None or school_name_column is None:
        raise ValueError("Processed report is missing 'School EMIS' or 'School Name' columns.")

    dormant_df["emis_normalized"] = dormant_df[school_emis_column].map(_normalize_emis)
    merged_df = dormant_df.merge(
        officers_df,
        on="emis_normalized",
        how="left",
        suffixes=("_dormant", "_officer"),
    )

    contacts_by_phone: dict[str, dict] = {}
    unmatched_emis: set[str] = set()
    skipped_contact_rows: list[str] = []

    for row in merged_df.to_dict(orient="records"):
        emis_value = row.get("emis_normalized")
        school_name = _clean_optional_text(row.get(school_name_column)) or "Unknown School"

        if _clean_optional_text(row.get("ddeo_name")) is None and _clean_optional_text(
            row.get("aeo_name")
        ) is None:
            if emis_value:
                unmatched_emis.add(emis_value)
            continue

        officer_specs = [
            ("DDEO", row.get("ddeo_name"), row.get("ddeo_cell_number")),
            ("AEO", row.get("aeo_name"), row.get("aeo_cell_number")),
        ]

        for role_label, officer_name, officer_mobile in officer_specs:
            if _clean_optional_text(officer_name) is None or _clean_optional_text(
                officer_mobile
            ) is None:
                continue

            try:
                normalized_mobile = _normalize_pk_mobile(officer_mobile)
            except Exception as exc:
                skipped_contact_rows.append(
                    f"{role_label} {officer_name} / {officer_mobile} / EMIS {emis_value}: {exc}"
                )
                continue

            contact_entry = contacts_by_phone.setdefault(
                normalized_mobile,
                {
                    "names": set(),
                    "roles": set(),
                    "schools": [],
                    "school_keys_seen": set(),
                },
            )

            contact_entry["names"].add(_clean_optional_text(officer_name))
            contact_entry["roles"].add(role_label)

            school_key = (
                school_name,
                _clean_optional_text(row.get("tehsil")) or "",
                _clean_optional_text(row.get("markaz")) or "",
            )

            if school_key not in contact_entry["school_keys_seen"]:
                contact_entry["school_keys_seen"].add(school_key)
                contact_entry["schools"].append(
                    {
                        "school_name": school_name,
                        "tehsil": _clean_optional_text(row.get("tehsil")) or "",
                        "markaz": _clean_optional_text(row.get("markaz")) or "",
                    }
                )

    payloads = []

    for normalized_mobile, details in sorted(contacts_by_phone.items()):
        recipient_name = ", ".join(sorted(name for name in details["names"] if name))
        role_labels = sorted(details["roles"])
        include_excel = _should_include_excel_for_roles(role_labels)
        officer_message = _build_officer_message(
            title_text=title_text,
            role_labels=role_labels,
            recipient_name=recipient_name or "Officer",
            schools=sorted(
                details["schools"],
                key=lambda school: (
                    school["tehsil"],
                    school["markaz"],
                    school["school_name"],
                ),
            ),
        )

        payloads.append(
            _create_job_payload(
                target=normalized_mobile,
                recipient_type="contact",
                recipient_name=recipient_name or None,
                attachment_text_mode=_normalize_attachment_text_mode(
                    DEFAULT_ATTACHMENT_TEXT_MODE
                ),
                text=officer_message,
                excel_path=excel_path.resolve() if include_excel else None,
                image_path=image_path.resolve() if image_path and image_path.exists() else None,
                excel_filename=excel_path.name,
                delay_ms=DEFAULT_SEND_DELAY_MS,
            )
        )

    if unmatched_emis:
        sample_unmatched = ", ".join(sorted(unmatched_emis)[:10])
        print(
            f"Warning: {len(unmatched_emis)} dormant school EMIS values had no officer mapping in {OFFICERS_LIST_FILE.name}. Sample: {sample_unmatched}"
        )

    if skipped_contact_rows:
        sample_skips = " | ".join(skipped_contact_rows[:5])
        print(
            f"Warning: skipped {len(skipped_contact_rows)} officer contact rows due to invalid mobile data. Sample: {sample_skips}"
        )

    return payloads


def _find_column(dataframe: pd.DataFrame, column_name: str) -> str | None:
    normalized_name = column_name.strip().lower()
    return next(
        (
            column
            for column in dataframe.columns
            if str(column).strip().lower() == normalized_name
        ),
        None,
    )


def _build_officer_mapping_audit(final_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    officers_df = _read_officers_dataframe()
    dormant_df = final_df.copy()

    school_emis_column = _find_column(dormant_df, "School EMIS")
    school_name_column = _find_column(dormant_df, "School Name")
    tehsil_column = _find_column(dormant_df, "Tehsil")
    markaz_column = _find_column(dormant_df, "Markaz")

    if school_emis_column is None or school_name_column is None:
        raise ValueError("Processed report is missing 'School EMIS' or 'School Name' columns.")

    dormant_df["emis_normalized"] = dormant_df[school_emis_column].map(_normalize_emis)
    merged_df = dormant_df.merge(
        officers_df,
        on="emis_normalized",
        how="left",
        suffixes=("_dormant", "_officer"),
    )

    audit_rows = []
    invalid_contact_rows = []

    for row in merged_df.to_dict(orient="records"):
        emis_value = _clean_optional_text(row.get(school_emis_column)) or row.get("emis_normalized")
        school_name = _clean_optional_text(row.get(school_name_column)) or "Unknown School"
        ddeo_name = _clean_optional_text(row.get("ddeo_name"))
        aeo_name = _clean_optional_text(row.get("aeo_name"))
        ddeo_cell = _clean_optional_text(row.get("ddeo_cell_number"))
        aeo_cell = _clean_optional_text(row.get("aeo_cell_number"))

        status_parts = []
        if ddeo_name:
            status_parts.append("DDEO mapped")
        if aeo_name:
            status_parts.append("AEO mapped")
        if not status_parts:
            status_parts.append("No officer mapping")

        audit_rows.append(
            {
                "School EMIS": emis_value,
                "School Name": school_name,
                "Tehsil": _clean_optional_text(row.get(tehsil_column)) or "",
                "Markaz": _clean_optional_text(row.get(markaz_column)) or "",
                "DDEO Name": ddeo_name or "",
                "DDEO Cell Number": ddeo_cell or "",
                "AEO Name": aeo_name or "",
                "AEO Cell Number": aeo_cell or "",
                "Mapping Status": ", ".join(status_parts),
            }
        )

        for role_label, officer_name, officer_mobile in (
            ("DDEO", ddeo_name, ddeo_cell),
            ("AEO", aeo_name, aeo_cell),
        ):
            if officer_name is None or officer_mobile is None:
                continue

            try:
                _normalize_pk_mobile(officer_mobile)
            except Exception as exc:
                invalid_contact_rows.append(
                    {
                        "School EMIS": emis_value,
                        "School Name": school_name,
                        "Role": role_label,
                        "Officer Name": officer_name,
                        "Mobile Number": officer_mobile,
                        "Issue": str(exc),
                    }
                )

    audit_df = pd.DataFrame(audit_rows)
    unmatched_df = audit_df[audit_df["Mapping Status"] == "No officer mapping"].copy()
    invalid_contacts_df = pd.DataFrame(invalid_contact_rows)

    return unmatched_df, invalid_contacts_df, audit_df


# ==========================================
# 2. PREFECT TASKS (Data Engineering)
# ==========================================
@task(name="Extract: Read Raw Data")
def extract_raw_data(file_path: Path) -> pd.DataFrame:
    logger = get_run_logger()
    user_df = _read_dataframe_by_content(file_path, dtype=str)
    logger.info(
        f"Loaded raw report from {file_path.name}: "
        f"{len(user_df)} rows, {len(user_df.columns)} columns."
    )

    user_df.columns = user_df.columns.str.strip()
    activity_columns_by_name = {
        column.strip().lower(): column for column in user_df.columns
    }
    activity_columns = [
        activity_columns_by_name.get(column_name.strip().lower())
        for column_name in UNFILTERED_DORMANT_ACTIVITY_COLUMNS
    ]

    if all(activity_columns):
        before_filter_count = len(user_df)
        activity_values = user_df[activity_columns].apply(
            lambda column: pd.to_numeric(
                column.astype(str).str.strip().replace("", pd.NA),
                errors="coerce",
            )
        )
        user_df = user_df[activity_values.eq(0).all(axis=1)].copy()
        logger.info(
            "Applied unfiltered-report dormant filter: "
            f"{len(user_df)} of {before_filter_count} rows have zero activity."
        )
    else:
        missing_activity_columns = [
            column_name
            for column_name, resolved_column in zip(
                UNFILTERED_DORMANT_ACTIVITY_COLUMNS, activity_columns
            )
            if resolved_column is None
        ]
        logger.info(
            "Activity columns not found; treating report as already filtered. "
            f"Missing columns: {', '.join(missing_activity_columns)}"
        )

    if "Username" not in user_df.columns:
        raise KeyError("Could not find 'Username' column!")

    user_df["Extracted_EMIS"] = (
        user_df["Username"].astype(str).str.extract(r"^(\d+)", expand=False)
    )
    return user_df[["Extracted_EMIS"]].copy()


@task(name="Transform: Merge, Sort & Reset Serials")
def transform_data(user_df_clean: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    master_df = pd.read_excel(MASTER_FILE)
    master_df.columns = master_df.columns.str.strip()

    master_df["School EMIS"] = master_df["School EMIS"].astype(str).str.strip()
    user_df_clean["Extracted_EMIS"] = (
        user_df_clean["Extracted_EMIS"].astype(str).str.strip()
    )

    merged_df = pd.merge(
        user_df_clean,
        master_df,
        left_on="Extracted_EMIS",
        right_on="School EMIS",
        how="inner",
    )
    final_df = merged_df[master_df.columns].drop_duplicates()

    # Sort by Tehsil
    tehsil_col = next(
        (c for c in final_df.columns if c.strip().lower() == "tehsil"), None
    )
    if tehsil_col:
        final_df = final_df.sort_values(by=tehsil_col, ascending=True).reset_index(
            drop=True
        )

    # Reset Serial Numbers
    sr_col = next(
        (
            c
            for c in final_df.columns
            if "sr" in c.strip().lower() or "serial" in c.strip().lower()
        ),
        None,
    )
    if sr_col:
        final_df[sr_col] = range(1, len(final_df) + 1)

    logger.info(f"Processed {len(final_df)} unique records.")
    return final_df


@task(name="Load: Generate Formatted Excel")
def load_excel_report(final_df: pd.DataFrame, output_path: Path, title_text: str):
    logger = get_run_logger()
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, startrow=2, sheet_name="Report")

        workbook = writer.book
        worksheet = writer.sheets["Report"]
        num_columns = len(final_df.columns)

        # Title Row
        worksheet["A1"] = title_text
        worksheet["A1"].font = Font(bold=True, size=14, color="000080")
        if num_columns > 0:
            worksheet.merge_cells(
                start_row=1, start_column=1, end_row=1, end_column=num_columns
            )
            worksheet["A1"].alignment = Alignment(
                horizontal="center", vertical="center"
            )

        # Apply Thin Borders & Widths
        thin_border = Border(
            left=Side(style="thin", color="000000"),
            right=Side(style="thin", color="000000"),
            top=Side(style="thin", color="000000"),
            bottom=Side(style="thin", color="000000"),
        )

        for col_idx in range(1, num_columns + 1):
            header_cell = worksheet.cell(row=3, column=col_idx)
            header_cell.font = Font(bold=True)
            header_cell.border = thin_border

            column_letter = get_column_letter(col_idx)
            max_length = 0

            for row_idx in range(3, worksheet.max_row + 1):
                cell = worksheet.cell(row=row_idx, column=col_idx)
                cell.border = thin_border
                if cell.value is not None:
                    max_length = max(max_length, len(str(cell.value)))

            worksheet.column_dimensions[column_letter].width = max_length + 2


@task(name="Load: Generate High-Res Screenshot")
def load_screenshot(final_df: pd.DataFrame, image_path: Path, title_text: str):
    import dataframe_image as dfi

    logger = get_run_logger()
    styled_df = (
        final_df.style.set_caption(title_text)
        .set_table_styles(
            [
                {
                    "selector": "caption",
                    "props": [
                        ("color", "#000080"),
                        ("font-size", "18px"),
                        ("font-weight", "bold"),
                        ("text-align", "center"),
                        ("padding-bottom", "15px"),
                    ],
                },
                {
                    "selector": "th",
                    "props": [
                        ("background-color", "#f8f9fa"),
                        ("font-weight", "bold"),
                        ("border", "1px solid black"),
                        ("text-align", "left"),
                    ],
                },
                {
                    "selector": "td",
                    "props": [("border", "1px solid black"), ("text-align", "left")],
                },
            ]
        )
        .hide(axis="index")
    )

    dfi.export(styled_df, str(image_path), max_rows=-1, dpi=300)


@task(name="Load: Generate Officer Mapping Audit")
def load_officer_mapping_audit(final_df: pd.DataFrame, output_path: Path, title_text: str):
    logger = get_run_logger()
    unmatched_df, invalid_contacts_df, audit_df = _build_officer_mapping_audit(final_df)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if invalid_contacts_df.empty:
        invalid_contacts_df = pd.DataFrame(
            columns=[
                "School EMIS",
                "School Name",
                "Role",
                "Officer Name",
                "Mobile Number",
                "Issue",
            ]
        )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        unmatched_df.to_excel(writer, index=False, sheet_name="Unmapped Schools")
        invalid_contacts_df.to_excel(writer, index=False, sheet_name="Invalid Contacts")
        audit_df.to_excel(writer, index=False, sheet_name="All Dormant Mapping")

        workbook = writer.book
        for worksheet in workbook.worksheets:
            worksheet.freeze_panes = "A2"
            for cell in worksheet[1]:
                cell.font = Font(bold=True)

            for column_cells in worksheet.columns:
                column_letter = get_column_letter(column_cells[0].column)
                max_length = max(
                    len(str(cell.value)) if cell.value is not None else 0
                    for cell in column_cells
                )
                worksheet.column_dimensions[column_letter].width = min(
                    max(max_length + 2, 12), 45
                )

    logger.info(
        f"Officer mapping audit saved to {output_path}. "
        f"Unmapped schools: {len(unmatched_df)}, invalid contacts: {len(invalid_contacts_df)}."
    )


# --- NEW: Helper to create a nice text message ---
# --- UPDATED: Helper to create a Tehsil-wise text message ---
def create_whatsapp_summary(df: pd.DataFrame, title_text: str) -> str:
    total_users = len(df)
    msg = f"🚨 *{title_text}*\n\n"
    msg += f"📊 *Total Dormant Users:* {total_users}\n"

    # Find the Tehsil column dynamically (ignoring case/spaces)
    tehsil_col = next((c for c in df.columns if c.strip().lower() == "tehsil"), None)

    if tehsil_col:
        # Get counts per tehsil and sort alphabetically
        tehsil_counts = df[tehsil_col].value_counts().sort_index()
        for tehsil, count in tehsil_counts.items():
            # Clean up the tehsil name just in case there are weird spaces in the Excel
            clean_tehsil = str(tehsil).strip().title()
            msg += f"Tehsil {clean_tehsil} = {count}\n"
    else:
        msg += "*(Tehsil breakdown not available in this report)*\n"

    msg += "\nPlease review the attached Excel report for details."
    return msg


async def _publish_to_nats(payloads: list[dict]):
    """Async helper to connect and publish to JetStream."""
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    try:
        try:
            owning_stream = await js.find_stream_name_by_subject(NATS_SUBJECT)
        except NotFoundError:
            owning_stream = None

        stream_config = StreamConfig(
            name=owning_stream or NATS_STREAM,
            subjects=[NATS_SUBJECT],
            retention=RetentionPolicy.WORK_QUEUE,
            storage=StorageType.FILE,
        )

        if owning_stream:
            stream_info = await js.stream_info(owning_stream)
            existing_subjects = set(stream_info.config.subjects or [])
            if NATS_SUBJECT not in existing_subjects:
                stream_config.subjects = sorted([*existing_subjects, NATS_SUBJECT])
                await js.update_stream(stream_config)
        else:
            await js.add_stream(stream_config)

        for payload in payloads:
            await js.publish(NATS_SUBJECT, json.dumps(payload).encode("utf-8"))
    finally:
        await nc.close()


@task(name="Notify: Push to NATS Queue", retries=3, retry_delay_seconds=10)
def send_whatsapp_alert(
    final_df: pd.DataFrame,
    excel_path: Path,
    image_path: Path | None,
    title_text: str,
    message_body: str,
):
    logger = get_run_logger()
    try:
        dynamic_payloads = build_dynamic_officer_payloads(
            final_df, excel_path, image_path, title_text
        )
    except Exception as exc:
        logger.error(f"Dynamic officer recipient build failed. Fixed recipients will still be sent. Error: {exc}")
        dynamic_payloads = []

    fixed_payloads = build_fixed_whatsapp_payloads(excel_path, image_path, message_body)
    payloads = [*dynamic_payloads, *fixed_payloads]

    if not payloads:
        logger.warning("No enabled WhatsApp recipients found. Nothing was queued.")
        return

    asyncio.run(_publish_to_nats(payloads))

    logger.info(
        "Queued WhatsApp payloads successfully: "
        f"{len(dynamic_payloads)} dynamic officer recipients, "
        f"{len(fixed_payloads)} fixed recipients, "
        f"{len(payloads)} total."
    )


@task(name="Cleanup: Archive Raw File")
def archive_raw_file(file_path: Path):
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    archive_path = ARCHIVE_DIR / file_path.name
    if archive_path.exists():
        archive_path = (
            ARCHIVE_DIR / f"{file_path.stem}_{int(time.time())}{file_path.suffix}"
        )
    shutil.move(str(file_path), str(archive_path))


def _create_run_directories(current_time_obj: datetime.datetime) -> tuple[Path, Path]:
    timestamp_folder_name = current_time_obj.strftime("%Y-%m-%d_%H-%M-%S")
    run_output_dir = OUTPUT_DIR / timestamp_folder_name
    run_unmapped_report_dir = UNMAPPED_REPORT_DIR / timestamp_folder_name

    counter = 2
    while run_output_dir.exists() or run_unmapped_report_dir.exists():
        suffix = f"_{counter}"
        run_output_dir = OUTPUT_DIR / f"{timestamp_folder_name}{suffix}"
        run_unmapped_report_dir = UNMAPPED_REPORT_DIR / f"{timestamp_folder_name}{suffix}"
        counter += 1

    run_output_dir.mkdir(parents=True, exist_ok=False)
    run_unmapped_report_dir.mkdir(parents=True, exist_ok=False)
    return run_output_dir, run_unmapped_report_dir


def _process_raw_report_file(file_path: Path, logger) -> None:
    # Step 1: Extract & Transform
    raw_df = extract_raw_data(file_path)
    clean_df = transform_data(raw_df)

    # Step 2: Check if zero schools left (Graceful exit)
    if len(clean_df) == 0:
        logger.info("TARGET REACHED: ZERO INACTIVE SCHOOLS LEFT! Stopping this file.")
        archive_raw_file(file_path)
        return

    # Step 3: Setup dynamic Output Folders & Filenames
    current_time_obj = datetime.datetime.now()
    run_output_dir, run_unmapped_report_dir = _create_run_directories(current_time_obj)

    title_text = f"Anti-Dengue Dormant Users - {current_time_obj.strftime('%d-%b-%Y - %I:%M %p')}"

    date_str = current_time_obj.strftime("%d-%m-%Y")
    time_str = current_time_obj.strftime("%I-%M %p")
    excel_filename = f"Anti-Dengue App Dormant Users - {date_str} - {time_str}.xlsx"

    excel_path = run_output_dir / excel_filename
    image_path = run_output_dir / "Inactive_Schools_Report_Screenshot.png"
    mapping_audit_path = (
        run_unmapped_report_dir
        / f"Officer Mapping Audit - {date_str} - {time_str}.xlsx"
    )
    generated_image_path = None

    # Step 4: Load outputs
    load_excel_report(clean_df, excel_path, title_text)
    load_officer_mapping_audit(clean_df, mapping_audit_path, title_text)

    if GENERATE_SCREENSHOT:
        try:
            load_screenshot(clean_df, image_path, title_text)
            generated_image_path = image_path
        except Exception as e:
            logger.error(f"Screenshot failed, but pipeline will continue. Error: {e}")

    # Step 5: Cleanup & Notify
    archive_raw_file(file_path)

    body_text = create_whatsapp_summary(clean_df, title_text)

    try:
        send_whatsapp_alert(
            clean_df,
            excel_path,
            generated_image_path,
            title_text,
            body_text,
        )
    except Exception as e:
        logger.error(f"CRITICAL: Failed to push to NATS. Error: {e}")


def _iter_manual_unfiltered_files() -> list[Path]:
    if not MANUAL_UNFILTERED_DIR.exists():
        return []

    return sorted(
        (
            path
            for path in MANUAL_UNFILTERED_DIR.iterdir()
            if path.is_file()
            and not path.name.startswith(".")
            and not path.name.startswith("~$")
        ),
        key=lambda path: (path.stat().st_mtime, path.name),
    )


# ==========================================
# 3. PREFECT FLOW (The Pipeline Orchestrator)
# ==========================================
@flow(name="Hourly Dengue Report Pipeline", log_prints=True)
def process_file_flow():
    logger = get_run_logger()
    logger.info("Starting Hourly Pipeline...")

    # Step 1: Scrape the data
    file_path = scrape_portal_data()
    _process_raw_report_file(file_path, logger)
    logger.info("Pipeline completed successfully.")


@flow(name="Manual Unfiltered Dengue Report Pipeline", log_prints=True)
def process_manual_unfiltered_flow():
    logger = get_run_logger()
    logger.info(f"Starting manual unfiltered pipeline from {MANUAL_UNFILTERED_DIR}...")

    manual_files = _iter_manual_unfiltered_files()
    if not manual_files:
        logger.info("No manual unfiltered files found to process.")
        return

    processed_count = 0
    failed_count = 0

    for file_path in manual_files:
        logger.info(f"Processing manual unfiltered file: {file_path.name}")
        try:
            _process_raw_report_file(file_path, logger)
            processed_count += 1
        except Exception as exc:
            failed_count += 1
            logger.error(
                f"Failed to process manual file {file_path}. "
                "The file was left in place for review. Error: "
                f"{exc}"
            )

    logger.info(
        "Manual unfiltered pipeline completed: "
        f"{processed_count} processed, {failed_count} failed."
    )


if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    UNMAPPED_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_UNFILTERED_DIR.mkdir(parents=True, exist_ok=True)

    if not MASTER_FILE.exists():
        print(f"CRITICAL: Master file '{MASTER_FILE.name}' not found.")
        exit(1)

    command = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "portal"
    if command in {"portal", "run", "download"}:
        process_file_flow()
    elif command in {"manual-unfiltered", "manual-unfilterd", "manual"}:
        process_manual_unfiltered_flow()
    else:
        print(
            "Unknown command. Use 'python main.py' for portal download or "
            "'python main.py manual-unfiltered' for files in "
            f"{MANUAL_UNFILTERED_DIR.name}."
        )
        exit(2)

from __future__ import annotations

import argparse
import os
import re
import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import requests
from dotenv import load_dotenv
from urllib3.exceptions import InsecureRequestWarning


INPUT_DIR = "phase1-crm/unprocessed-crm/sheets"
OUTPUT_BASE_DIR = "phase1-crm/unprocessed-crm/filtered"
COMPLAINT_COL_NAME = "Complaint No"
STATUS_FIELD_NAME = "Status"
COMPLAINT_NUMBER_FIELD_NAME = "Complaint Number"
SOURCE_FIELD_NAME = "Source"
DOCUMENT_ROLE_FIELD_NAME = "Document Role"
SUBMITTED_STATUS_VALUE = "Submitted"
NOT_RELEVANT_STATUS_VALUE = "Not Relevant"
CRM_SOURCE_VALUE = "CRM Portal"
MAIN_COMPLAINT_ROLE_VALUE = "Main Complaint"
SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".xlsm", ".xltx", ".xltm"}
HEADER_SCAN_ROWS = 20


def paperless_api_base_url(base_url: str) -> str:
    parts = urlsplit(base_url)
    path = "/" if parts.path.rstrip("/") == "/dashboard" else parts.path
    return urlunsplit((parts.scheme, parts.netloc, path.rstrip("/"), "", ""))


def get_paperless_client() -> tuple[str, dict[str, str], bool]:
    load_dotenv()
    url = paperless_api_base_url(
        os.getenv("PAPERLESS_URL", "https://paperless.lab.internal/dashboard")
    )
    username = os.getenv("PAPERLESS_USERNAME")
    password = os.getenv("PAPERLESS_PASSWORD")
    verify_ssl = os.getenv("PAPERLESS_VERIFY_SSL", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    if not verify_ssl:
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    if not username or not password:
        raise RuntimeError("PAPERLESS_USERNAME or PAPERLESS_PASSWORD is missing in .env")

    print("Authenticating with Paperless-ngx...")
    response = requests.post(
        f"{url}/api/token/",
        data={"username": username, "password": password},
        timeout=10,
        verify=verify_ssl,
    )
    response.raise_for_status()
    token = response.json()["token"]
    return url, {"Authorization": f"Token {token}", "Accept": "application/json"}, verify_ssl


def fetch_paperless_metadata(url: str, headers: dict[str, str], verify_ssl: bool) -> dict:
    load_dotenv()
    doc_type_name = os.getenv("PAPERLESS_DOCUMENT_TYPE_COMPLAINT", "Complaint").lower()
    metadata = {
        "complaint_type_id": None,
        "complaint_number_field_id": None,
        "source_field_id": None,
        "status_field_id": None,
        "document_role_field_id": None,
        "status_option_ids_by_label": {},
        "status_option_labels_by_id": {},
        "option_labels_by_field_id": {},
    }

    doc_types = requests.get(
        f"{url}/api/document_types/",
        params={"page_size": 100},
        headers=headers,
        timeout=10,
        verify=verify_ssl,
    )
    doc_types.raise_for_status()
    for item in doc_types.json().get("results", []):
        if str(item.get("name", "")).strip().lower() == doc_type_name:
            metadata["complaint_type_id"] = item.get("id")
            break

    custom_fields = requests.get(
        f"{url}/api/custom_fields/",
        params={"page_size": 100},
        headers=headers,
        timeout=10,
        verify=verify_ssl,
    )
    custom_fields.raise_for_status()
    for field in custom_fields.json().get("results", []):
        field_name = str(field.get("name", "")).strip().lower()
        field_id = field.get("id")
        if field.get("data_type") == "select" and field_id is not None:
            labels = {
                str(opt.get("id")): str(opt.get("label", "")).strip()
                for opt in field.get("extra_data", {}).get("select_options", [])
                if opt.get("id") is not None
            }
            metadata["option_labels_by_field_id"][str(field_id)] = labels
        if field_name == COMPLAINT_NUMBER_FIELD_NAME.lower():
            metadata["complaint_number_field_id"] = field_id
        elif field_name == SOURCE_FIELD_NAME.lower():
            metadata["source_field_id"] = field_id
        elif field_name == DOCUMENT_ROLE_FIELD_NAME.lower():
            metadata["document_role_field_id"] = field_id
        elif field_name == STATUS_FIELD_NAME.lower():
            metadata["status_field_id"] = field_id
            for option in field.get("extra_data", {}).get("select_options", []):
                label = str(option.get("label", "")).strip()
                option_id = option.get("id")
                if label and option_id is not None:
                    metadata["status_option_ids_by_label"][label.lower()] = option_id
                    metadata["status_option_labels_by_id"][str(option_id)] = label
    return metadata


def field_text(value) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if item is not None)
    return str(value).strip()


def custom_field_value(document: dict, field_id) -> object:
    if field_id is None:
        return None
    for field in document.get("custom_fields", []):
        if str(field.get("field")) == str(field_id):
            return field.get("value")
    return None


def custom_field_label(document: dict, field_id, metadata: dict) -> str:
    value = custom_field_value(document, field_id)
    if value in (None, ""):
        return ""
    labels = metadata["option_labels_by_field_id"].get(str(field_id), {})
    return labels.get(str(value), field_text(value))


def status_label(value, metadata: dict) -> str:
    if value in (None, ""):
        return ""
    return metadata["status_option_labels_by_id"].get(str(value), str(value).strip())


def status_matches(value, label: str, metadata: dict) -> bool:
    resolved = status_label(value, metadata).strip().lower()
    if resolved == label.lower():
        return True
    option_id = metadata["status_option_ids_by_label"].get(label.lower())
    return option_id is not None and str(value).strip() == str(option_id)


def title_has_exact_complaint(title: str, complaint_no: str) -> bool:
    return bool(re.search(rf"(?<!\d){re.escape(complaint_no)}(?!\d)", title or ""))


def is_matching_crm_complaint(document: dict, complaint_no: str, metadata: dict) -> bool:
    if metadata["complaint_type_id"] and document.get("document_type") != metadata["complaint_type_id"]:
        return False
    role = custom_field_label(document, metadata["document_role_field_id"], metadata)
    if role and role.strip().lower() != MAIN_COMPLAINT_ROLE_VALUE.lower():
        return False
    source = custom_field_label(document, metadata["source_field_id"], metadata)
    if source and source.strip().lower() != CRM_SOURCE_VALUE.lower():
        return False
    complaint_number = field_text(
        custom_field_value(document, metadata["complaint_number_field_id"])
    )
    if complaint_number:
        return complaint_number.strip() == complaint_no
    return title_has_exact_complaint(str(document.get("title", "")), complaint_no)


def categorize_complaint(
    complaint_no: str,
    url: str,
    headers: dict[str, str],
    metadata: dict,
    verify_ssl: bool,
) -> str:
    try:
        response = requests.get(
            f"{url}/api/documents/",
            params={"query": complaint_no, "page_size": 100},
            headers=headers,
            timeout=10,
            verify=verify_ssl,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"  [!] API Error for {complaint_no}: {exc}")
        return "fresh"

    found = False
    active_status = ""
    for document in response.json().get("results", []):
        if not is_matching_crm_complaint(document, complaint_no, metadata):
            continue
        found = True
        value = custom_field_value(document, metadata["status_field_id"])
        if status_matches(value, SUBMITTED_STATUS_VALUE, metadata):
            return "submitted"
        if status_matches(value, NOT_RELEVANT_STATUS_VALUE, metadata):
            return "uploaded_not_relevant"
        active_status = status_label(value, metadata)
    if found:
        if active_status:
            print(f"    Paperless Status '{active_status}' treated as UPLOADED PENDING.")
        return "uploaded_pending"
    return "fresh"


def resolve_project_path(project_root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def files_from_args(project_root: Path, args: argparse.Namespace) -> list[Path]:
    if args.input_file:
        input_file = resolve_project_path(project_root, args.input_file)
        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        if input_file.suffix.lower() not in SUPPORTED_SUFFIXES:
            raise ValueError(f"Unsupported input file type: {input_file}")
        return [input_file]
    input_dir = resolve_project_path(project_root, args.input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)
    return [
        path
        for path in sorted(input_dir.iterdir())
        if path.is_file()
        and not path.name.startswith("~$")
        and path.suffix.lower() in SUPPORTED_SUFFIXES
    ]


def clean_column_name(value) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\ufeff", "")).strip()


def normalized_column_name(value) -> str:
    return clean_column_name(value).casefold()


def complaint_column_in(columns) -> str | None:
    expected = normalized_column_name(COMPLAINT_COL_NAME)
    for column in columns:
        if normalized_column_name(column) == expected:
            return column
    return None


def normalize_dataframe_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.copy()
    dataframe.columns = [clean_column_name(column) for column in dataframe.columns]
    complaint_column = complaint_column_in(dataframe.columns)
    if complaint_column and complaint_column != COMPLAINT_COL_NAME:
        dataframe = dataframe.rename(columns={complaint_column: COMPLAINT_COL_NAME})
    return dataframe


def read_tabular_file(
    file_path: Path, header: int | None = 0, nrows: int | None = None
) -> pd.DataFrame:
    if file_path.suffix.lower() == ".csv":
        return pd.read_csv(file_path, header=header, nrows=nrows)
    return pd.read_excel(file_path, header=header, nrows=nrows)


def detect_header_row(file_path: Path) -> int | None:
    raw = read_tabular_file(file_path, header=None, nrows=HEADER_SCAN_ROWS)
    expected = normalized_column_name(COMPLAINT_COL_NAME)
    for index, row in raw.iterrows():
        if any(normalized_column_name(value) == expected for value in row):
            return int(index)
    return None


def read_crm_dataframe(file_path: Path) -> tuple[pd.DataFrame, int | None]:
    dataframe = normalize_dataframe_columns(read_tabular_file(file_path))
    if complaint_column_in(dataframe.columns):
        return dataframe, None

    header_row = detect_header_row(file_path)
    if header_row is None:
        return dataframe, None

    dataframe = normalize_dataframe_columns(
        read_tabular_file(file_path, header=header_row)
    )
    return dataframe, header_row


def process_file(
    file_path: Path,
    url: str,
    headers: dict[str, str],
    metadata: dict,
    output_dirs: dict[str, Path],
    verify_ssl: bool,
) -> None:
    print(f"\n--- Processing: {file_path.name} ---")
    try:
        dataframe, detected_header_row = read_crm_dataframe(file_path)
    except Exception as exc:
        print(f"  [!] Failed to read file {file_path.name}: {exc}")
        return
    if detected_header_row is not None:
        print(f"  Detected header row {detected_header_row + 1}.")
    if COMPLAINT_COL_NAME not in dataframe.columns:
        print(f"  [!] Skipped: Column '{COMPLAINT_COL_NAME}' not found.")
        return

    categorized = {
        "fresh": [],
        "uploaded_pending": [],
        "uploaded_not_relevant": [],
        "submitted": [],
    }
    total = len(dataframe)
    for index, row in dataframe.iterrows():
        complaint_no = str(row[COMPLAINT_COL_NAME]).strip()
        category = categorize_complaint(complaint_no, url, headers, metadata, verify_ssl)
        categorized[category].append(row)
        print(f"  [{index + 1}/{total}] {complaint_no} -> {category.upper()}")
        time.sleep(0.1)

    output_names = {
        "fresh": "fresh",
        "uploaded_pending": "uploaded_pending",
        "uploaded_not_relevant": "uploaded_not_relevant",
        "submitted": "submitted",
    }
    for category, rows in categorized.items():
        if not rows:
            continue
        output_path = output_dirs[category] / f"{output_names[category]}_{file_path.stem}.xlsx"
        pd.DataFrame(rows).to_excel(output_path, index=False)
        print(f"  => Saved {len(rows)} {output_names[category].upper()} records to {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Filter CRM CSV/Excel sheets against Paperless.")
    parser.add_argument("--input-file", default="")
    parser.add_argument("--input-dir", default=INPUT_DIR)
    parser.add_argument("--output-dir", default=OUTPUT_BASE_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parent
    output_base = resolve_project_path(project_root, args.output_dir)
    output_dirs = {
        "fresh": output_base / "fresh",
        "uploaded_pending": output_base / "uploaded" / "pending",
        "uploaded_not_relevant": output_base / "uploaded" / "not-relevant",
        "submitted": output_base / "submitted",
    }
    for path in output_dirs.values():
        path.mkdir(parents=True, exist_ok=True)

    url, headers, verify_ssl = get_paperless_client()
    metadata = fetch_paperless_metadata(url, headers, verify_ssl)
    files = files_from_args(project_root, args)
    if not files:
        print(f"No CSV or Excel files found in '{args.input_file or args.input_dir}'.")
        return 0
    for file_path in files:
        process_file(file_path, url, headers, metadata, output_dirs, verify_ssl)
    print("\n--- All operations complete ---")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from urllib3.exceptions import InsecureRequestWarning


STATUS_FIELD_NAME = "Status"
COMPLAINT_NUMBER_FIELD_NAME = "Complaint Number"
SOURCE_FIELD_NAME = "Source"
DOCUMENT_ROLE_FIELD_NAME = "Document Role"
SUBMITTED_STATUS_VALUE = "Submitted"
NOT_RELEVANT_STATUS_VALUE = "Not Relevant"
CRM_SOURCE_VALUE = "CRM Portal"
MAIN_COMPLAINT_ROLE_VALUE = "Main Complaint"


class PaperlessConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class ComplaintLookupResult:
    category: str
    reason: str
    matched_document_ids: list[int | str] = field(default_factory=list)
    matched_statuses: list[str] = field(default_factory=list)
    error: str = ""


@dataclass
class PaperlessMetadata:
    complaint_type_id: int | str | None = None
    complaint_number_field_id: int | str | None = None
    source_field_id: int | str | None = None
    status_field_id: int | str | None = None
    document_role_field_id: int | str | None = None
    status_option_ids_by_label: dict[str, int | str] = field(default_factory=dict)
    status_option_labels_by_id: dict[str, str] = field(default_factory=dict)
    option_labels_by_field_id: dict[str, dict[str, str]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def paperless_api_base_url(base_url: str) -> str:
    parts = urlsplit(base_url.strip())
    path = parts.path.rstrip("/")
    if path == "/dashboard":
        path = ""
    return urlunsplit((parts.scheme, parts.netloc, path, "", "")).rstrip("/")


def field_text(value: object) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if item is not None)
    return str(value).strip()


def custom_field_value(document: dict[str, Any], field_id: object) -> object:
    if field_id is None:
        return None
    for custom_field in document.get("custom_fields", []) or []:
        if str(custom_field.get("field")) == str(field_id):
            return custom_field.get("value")
    return None


def custom_field_label(
    document: dict[str, Any],
    field_id: object,
    metadata: PaperlessMetadata,
) -> str:
    value = custom_field_value(document, field_id)
    if value in (None, ""):
        return ""
    labels = metadata.option_labels_by_field_id.get(str(field_id), {})
    return labels.get(str(value), field_text(value))


def status_label(value: object, metadata: PaperlessMetadata) -> str:
    if value in (None, ""):
        return ""
    return metadata.status_option_labels_by_id.get(str(value), str(value).strip())


def status_matches(value: object, label: str, metadata: PaperlessMetadata) -> bool:
    resolved = status_label(value, metadata).casefold()
    if resolved == label.casefold():
        return True
    option_id = metadata.status_option_ids_by_label.get(label.casefold())
    return option_id is not None and str(value).strip() == str(option_id)


def title_has_exact_complaint(title: str, complaint_no: str) -> bool:
    return bool(re.search(rf"(?<!\d){re.escape(complaint_no)}(?!\d)", title or ""))


def is_matching_crm_complaint(
    document: dict[str, Any],
    complaint_no: str,
    metadata: PaperlessMetadata,
) -> bool:
    if metadata.complaint_type_id is not None and str(document.get("document_type")) != str(
        metadata.complaint_type_id
    ):
        return False

    role = custom_field_label(document, metadata.document_role_field_id, metadata)
    if role and role.casefold() != MAIN_COMPLAINT_ROLE_VALUE.casefold():
        return False

    source = custom_field_label(document, metadata.source_field_id, metadata)
    if source and source.casefold() != CRM_SOURCE_VALUE.casefold():
        return False

    complaint_number = field_text(
        custom_field_value(document, metadata.complaint_number_field_id)
    )
    if complaint_number:
        return complaint_number == complaint_no
    return title_has_exact_complaint(str(document.get("title", "")), complaint_no)


def categorize_matching_documents(
    documents: list[dict[str, Any]],
    complaint_no: str,
    metadata: PaperlessMetadata,
) -> ComplaintLookupResult:
    matches = [
        document
        for document in documents
        if is_matching_crm_complaint(document, complaint_no, metadata)
    ]
    if not matches:
        return ComplaintLookupResult(
            category="fresh",
            reason="No matching CRM main complaint was found in Paperless.",
        )

    document_ids = [document.get("id", "") for document in matches]
    values = [custom_field_value(document, metadata.status_field_id) for document in matches]
    labels = [status_label(value, metadata) or "Unspecified" for value in values]
    has_submitted = any(
        status_matches(value, SUBMITTED_STATUS_VALUE, metadata) for value in values
    )
    has_not_relevant = any(
        status_matches(value, NOT_RELEVANT_STATUS_VALUE, metadata) for value in values
    )

    if has_submitted and has_not_relevant:
        return ComplaintLookupResult(
            category="manual_review",
            reason="Conflicting Submitted and Not Relevant Paperless records were found.",
            matched_document_ids=document_ids,
            matched_statuses=labels,
        )
    if has_submitted:
        return ComplaintLookupResult(
            category="submitted",
            reason="A matching Paperless complaint has status Submitted.",
            matched_document_ids=document_ids,
            matched_statuses=labels,
        )
    if has_not_relevant:
        return ComplaintLookupResult(
            category="uploaded_not_relevant",
            reason="A matching Paperless complaint has status Not Relevant.",
            matched_document_ids=document_ids,
            matched_statuses=labels,
        )
    return ComplaintLookupResult(
        category="uploaded_pending",
        reason="A matching Paperless complaint exists and remains active/pending.",
        matched_document_ids=document_ids,
        matched_statuses=labels,
    )


class PaperlessClient:
    def __init__(
        self,
        *,
        base_url: str,
        username: str = "",
        password: str = "",
        token: str = "",
        verify_ssl: bool = True,
        timeout_seconds: float = 15,
        document_type_name: str = "Complaint",
        max_pages: int = 10,
    ) -> None:
        self.base_url = paperless_api_base_url(base_url)
        self.username = username
        self.password = password
        self.token = token
        self.verify_ssl = verify_ssl
        self.timeout_seconds = timeout_seconds
        self.document_type_name = document_type_name
        self.max_pages = max(1, max_pages)
        self.session = requests.Session()
        self.metadata: PaperlessMetadata | None = None

        if not self.verify_ssl:
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

    def connect(self) -> PaperlessMetadata:
        if not self.base_url:
            raise PaperlessConfigurationError("PAPERLESS_URL is not configured.")
        if self.token:
            token = self.token
        else:
            if not self.username or not self.password:
                raise PaperlessConfigurationError(
                    "Configure PAPERLESS_TOKEN or PAPERLESS_USERNAME and PAPERLESS_PASSWORD."
                )
            response = self.session.post(
                f"{self.base_url}/api/token/",
                data={"username": self.username, "password": self.password},
                timeout=self.timeout_seconds,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            token = str(response.json().get("token") or "")
            if not token:
                raise PaperlessConfigurationError("Paperless did not return an API token.")

        self.session.headers.update(
            {"Authorization": f"Token {token}", "Accept": "application/json"}
        )
        self.metadata = self.fetch_metadata()
        return self.metadata

    def _get_all(self, path: str, *, params: dict[str, object] | None = None) -> list[dict[str, Any]]:
        url: str | None = f"{self.base_url}{path}"
        request_params = dict(params or {})
        request_params.setdefault("page_size", 100)
        results: list[dict[str, Any]] = []
        pages = 0
        while url and pages < self.max_pages:
            response = self.session.get(
                url,
                params=request_params if pages == 0 else None,
                timeout=self.timeout_seconds,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            body = response.json()
            results.extend(body.get("results", []) or [])
            next_url = body.get("next")
            url = urljoin(self.base_url + "/", str(next_url)) if next_url else None
            pages += 1
        if url:
            raise requests.RequestException(
                f"Paperless pagination exceeded the configured limit of {self.max_pages} pages."
            )
        return results

    def fetch_metadata(self) -> PaperlessMetadata:
        metadata = PaperlessMetadata()
        for item in self._get_all("/api/document_types/"):
            if str(item.get("name", "")).strip().casefold() == self.document_type_name.casefold():
                metadata.complaint_type_id = item.get("id")
                break

        for custom_field in self._get_all("/api/custom_fields/"):
            field_name = str(custom_field.get("name", "")).strip().casefold()
            field_id = custom_field.get("id")
            if custom_field.get("data_type") == "select" and field_id is not None:
                labels = {
                    str(option.get("id")): str(option.get("label", "")).strip()
                    for option in custom_field.get("extra_data", {}).get("select_options", [])
                    if option.get("id") is not None
                }
                metadata.option_labels_by_field_id[str(field_id)] = labels
            if field_name == COMPLAINT_NUMBER_FIELD_NAME.casefold():
                metadata.complaint_number_field_id = field_id
            elif field_name == SOURCE_FIELD_NAME.casefold():
                metadata.source_field_id = field_id
            elif field_name == DOCUMENT_ROLE_FIELD_NAME.casefold():
                metadata.document_role_field_id = field_id
            elif field_name == STATUS_FIELD_NAME.casefold():
                metadata.status_field_id = field_id
                for option in custom_field.get("extra_data", {}).get("select_options", []):
                    label = str(option.get("label", "")).strip()
                    option_id = option.get("id")
                    if label and option_id is not None:
                        metadata.status_option_ids_by_label[label.casefold()] = option_id
                        metadata.status_option_labels_by_id[str(option_id)] = label

        if metadata.complaint_type_id is None:
            raise PaperlessConfigurationError(
                f"Paperless document type '{self.document_type_name}' was not found."
            )
        if metadata.status_field_id is None:
            raise PaperlessConfigurationError(
                f"Paperless custom field '{STATUS_FIELD_NAME}' was not found."
            )
        if metadata.complaint_number_field_id is None:
            metadata.warnings.append(
                "Complaint Number custom field was not found; exact title matching will be used."
            )
        if metadata.source_field_id is None:
            metadata.warnings.append(
                "Source custom field was not found; source filtering cannot be applied."
            )
        if metadata.document_role_field_id is None:
            metadata.warnings.append(
                "Document Role custom field was not found; role filtering cannot be applied."
            )
        return metadata

    def lookup_complaint(self, complaint_no: str) -> ComplaintLookupResult:
        if self.metadata is None:
            raise RuntimeError("PaperlessClient.connect() must be called before lookup.")
        try:
            documents = self._get_all(
                "/api/documents/",
                params={"query": complaint_no, "page_size": 100},
            )
        except requests.RequestException as exc:
            return ComplaintLookupResult(
                category="manual_review",
                reason="Paperless lookup failed; the row was not treated as fresh.",
                error=str(exc),
            )
        return categorize_matching_documents(documents, complaint_no, self.metadata)

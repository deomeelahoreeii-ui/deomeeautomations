from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import re
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import duckdb
import orjson
from yarl import URL


LOGGER_NAME = "pmdu_automation"


@dataclass(frozen=True)
class PaperlessSettings:
    base_url: str
    username: str
    password: str
    artifact_dir: Path
    duckdb_path: Path
    timeout_seconds: int
    task_timeout_seconds: int
    max_cases: int | None
    document_type_complaint: str
    document_type_attachment: str
    correspondent_name: str
    source_label: str
    field_config_path: Path
    dry_run: bool


DEFAULT_FIELD_CONFIG: dict[str, Any] = {
    "defaults": {
        "Complaint Number": "{complaint_code}",
        "Revision": "{version}",
        "Source": "{source_label}",
        "Direction": "Incoming",
        "The Reported Entity Address": "{identity.address}",
    },
    "main_complaint": {
        "Document Role": "Main Complaint",
        "Status": "Pending",
        "Complainant Name": "{identity.citizen_name}",
        "Complainant Mobile Number": "{identity.citizen_contact}",
        "Department": "{identity.level_one}",
        "Complaint Category": "{identity.category}",
    },
    "attachment": {
        "Document Role": "Complaint Details",
        "Parent Case": "{parent_document_id}",
    },
    "clear_fields": {
        "attachment": ["Status"],
    },
    "filters": {
        "roles": ["main_complaint", "attachment"],
        "paperless_added_date": None,
        "paperless_created_date": None,
        "complaint_codes": [],
    },
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())


def resolve_project_path(value: str | None, project_root: Path) -> Path:
    if not value:
        return Path()
    path = Path(value)
    if path.is_absolute():
        return path
    return project_root / path


def normalize_stored_path(value: str | None, project_root: Path) -> str:
    if not value:
        return ""
    path = Path(value)
    if not path.is_absolute():
        return value
    if path.exists():
        return relative_path(path, project_root)
    for marker in ("/scrap-pmdu/", "/deomeeautomations/scrap-pmdu/"):
        if marker in value:
            suffix = value.split(marker, 1)[1]
            return relative_path(project_root / suffix, project_root)
    return value


def stable_hash(value: Any) -> str:
    return hashlib.sha256(orjson.dumps(value, option=orjson.OPT_SORT_KEYS)).hexdigest()


def unsupported_paperless_attachment_reason(path: Path) -> str:
    content_type = mimetypes.guess_type(path.name)[0] or ""
    if content_type.startswith(("audio/", "video/")):
        return f"Paperless does not support attachment content type {content_type}"
    return ""


def latest_snapshots(artifact_dir: Path, max_cases: int | None) -> list[Path]:
    snapshots: list[Path] = []
    for complaint_dir in sorted(path for path in artifact_dir.iterdir() if path.is_dir()):
        versions = []
        for version_dir in complaint_dir.glob("v*"):
            if not version_dir.is_dir():
                continue
            try:
                version_number = int(version_dir.name.removeprefix("v"))
            except ValueError:
                continue
            snapshot_path = version_dir / "snapshot.json"
            if snapshot_path.exists():
                versions.append((version_number, snapshot_path))
        if versions:
            snapshots.append(max(versions, key=lambda item: item[0])[1])
    if max_cases is not None:
        return snapshots[:max_cases]
    return snapshots


def load_snapshot(path: Path) -> dict[str, Any]:
    return orjson.loads(path.read_bytes())


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_field_config(path: Path) -> dict[str, Any]:
    logger = logging.getLogger(LOGGER_NAME)
    if not path.exists():
        logger.info("Paperless field config not found; using built-in defaults: %s", path)
        return deepcopy(DEFAULT_FIELD_CONFIG)

    data = orjson.loads(path.read_bytes())
    if not isinstance(data, dict):
        raise ValueError(f"Paperless field config must be a JSON object: {path}")
    config = deep_merge(DEFAULT_FIELD_CONFIG, data)
    logger.info("Paperless field config loaded: %s", path)
    return config


def init_paperless_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paperless_documents (
            complaint_code TEXT,
            version INTEGER,
            artifact_role TEXT,
            local_path TEXT,
            sha256 TEXT,
            paperless_document_id INTEGER,
            parent_document_id INTEGER,
            upload_status TEXT,
            uploaded_at TIMESTAMP,
            error TEXT,
            PRIMARY KEY (complaint_code, version, artifact_role, local_path)
        )
        """
    )
    columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info('paperless_documents')").fetchall()
    }
    if "metadata_fingerprint" not in columns:
        conn.execute("ALTER TABLE paperless_documents ADD COLUMN metadata_fingerprint TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paperless_status_history (
            complaint_code TEXT,
            version INTEGER,
            paperless_document_id INTEGER,
            status_before TEXT,
            status_after TEXT,
            reason TEXT,
            synced_at TIMESTAMP
        )
        """
    )


def existing_uploaded_document(
    conn: duckdb.DuckDBPyConnection,
    complaint_code: str,
    version: int,
    artifact_role: str,
    sha256: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT paperless_document_id, metadata_fingerprint
        FROM paperless_documents
        WHERE complaint_code = ?
          AND version = ?
          AND artifact_role = ?
          AND sha256 = ?
          AND upload_status = 'uploaded'
          AND paperless_document_id IS NOT NULL
        ORDER BY uploaded_at DESC
        LIMIT 1
        """,
        [complaint_code, version, artifact_role, sha256],
    ).fetchone()
    if not row:
        return None
    return {
        "document_id": int(row[0]),
        "metadata_fingerprint": row[1] or "",
    }


def has_prior_uploaded_main_version(
    conn: duckdb.DuckDBPyConnection,
    complaint_code: str,
    version: int,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM paperless_documents
        WHERE complaint_code = ?
          AND artifact_role = 'main_complaint'
          AND version <> ?
          AND upload_status = 'uploaded'
          AND paperless_document_id IS NOT NULL
        LIMIT 1
        """,
        [complaint_code, version],
    ).fetchone()
    return bool(row)


def record_status_history(
    conn: duckdb.DuckDBPyConnection,
    complaint_code: str,
    version: int,
    paperless_document_id: int,
    status_before: str,
    status_after: str,
    reason: str,
) -> None:
    conn.execute(
        """
        INSERT INTO paperless_status_history (
            complaint_code,
            version,
            paperless_document_id,
            status_before,
            status_after,
            reason,
            synced_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            complaint_code,
            version,
            paperless_document_id,
            status_before,
            status_after,
            reason,
            datetime.now(timezone.utc).replace(tzinfo=None),
        ],
    )


def record_paperless_document(
    conn: duckdb.DuckDBPyConnection,
    complaint_code: str,
    version: int,
    artifact_role: str,
    local_path: Path,
    sha256: str,
    paperless_document_id: int | None,
    parent_document_id: int | None,
    upload_status: str,
    metadata_fingerprint: str = "",
    error: str = "",
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO paperless_documents (
            complaint_code,
            version,
            artifact_role,
            local_path,
            sha256,
            paperless_document_id,
            parent_document_id,
            upload_status,
            uploaded_at,
            metadata_fingerprint,
            error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            complaint_code,
            version,
            artifact_role,
            str(local_path),
            sha256,
            paperless_document_id,
            parent_document_id,
            upload_status,
            datetime.now(timezone.utc).replace(tzinfo=None),
            metadata_fingerprint,
            error,
        ],
    )


def migrate_paperless_paths(conn: duckdb.DuckDBPyConnection, project_root: Path) -> None:
    rows = conn.execute(
        """
        SELECT complaint_code, version, artifact_role, local_path
        FROM paperless_documents
        WHERE local_path LIKE '/%'
        """
    ).fetchall()
    for complaint_code, version, artifact_role, local_path in rows:
        normalized = normalize_stored_path(local_path, project_root)
        if normalized and normalized != local_path:
            try:
                conn.execute(
                    """
                    UPDATE paperless_documents
                    SET local_path = ?
                    WHERE complaint_code = ?
                      AND version = ?
                      AND artifact_role = ?
                      AND local_path = ?
                    """,
                    [normalized, complaint_code, version, artifact_role, local_path],
                )
            except duckdb.ConstraintException:
                # A relative row may already exist; future matching no longer depends on local_path.
                continue


class PaperlessClient:
    def __init__(self, settings: PaperlessSettings) -> None:
        self.settings = settings
        self.base_url = str(URL(settings.base_url).with_path(""))
        self.session: aiohttp.ClientSession | None = None
        self.token = ""

    async def __aenter__(self) -> "PaperlessClient":
        timeout = aiohttp.ClientTimeout(total=self.settings.timeout_seconds)
        self.session = aiohttp.ClientSession(timeout=timeout)
        await self.login()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.session:
            await self.session.close()

    def url(self, path: str) -> str:
        return str(URL(self.base_url).join(URL(path.lstrip("/"))))

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Token {self.token}"}

    async def login(self) -> None:
        assert self.session is not None
        async with self.session.post(
            self.url("/api/token/"),
            data={
                "username": self.settings.username,
                "password": self.settings.password,
            },
        ) as response:
            response.raise_for_status()
            data = await response.json()
        self.token = data["token"]

    async def get_json(self, path: str) -> Any:
        assert self.session is not None
        async with self.session.get(self.url(path), headers=self.headers) as response:
            response.raise_for_status()
            return await response.json()

    async def patch_json(self, path: str, payload: dict[str, Any]) -> Any:
        assert self.session is not None
        async with self.session.patch(
            self.url(path),
            headers={**self.headers, "Content-Type": "application/json"},
            json=payload,
        ) as response:
            if response.status >= 400:
                body = await response.text()
                raise RuntimeError(
                    f"Paperless PATCH {path} failed: status={response.status} body={body}"
                )
            return await response.json()

    async def paginated_results(self, path: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        next_path: str | None = path
        while next_path:
            data = await self.get_json(next_path)
            if isinstance(data, list):
                return data
            results.extend(data.get("results", []))
            next_url = data.get("next")
            if not next_url:
                break
            next_path = str(URL(next_url).relative())
        return results

    async def upload_document(
        self,
        file_path: Path,
        title: str,
        document_type_id: int,
        correspondent_id: int | None,
    ) -> int:
        assert self.session is not None
        content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        form = aiohttp.FormData()
        form.add_field(
            "document",
            file_path.read_bytes(),
            filename=file_path.name,
            content_type=content_type,
        )
        form.add_field("title", title)
        form.add_field("document_type", str(document_type_id))
        if correspondent_id:
            form.add_field("correspondent", str(correspondent_id))

        async with self.session.post(
            self.url("/api/documents/post_document/"),
            headers=self.headers,
            data=form,
        ) as response:
            if response.status >= 400:
                body = await response.text()
                raise RuntimeError(
                    "Paperless upload failed: "
                    f"status={response.status} file={file_path} "
                    f"content_type={content_type} body={body}"
                )
            data = await response.json()

        task_id = extract_task_id(data)
        if not task_id:
            raise RuntimeError(f"Paperless upload did not return a task id: {data!r}")
        return await self.wait_for_document_id(task_id)

    async def find_document_by_title(self, title: str) -> int | None:
        quoted_title = title.lower()
        for document in await self.paginated_results("/api/documents/?page_size=100"):
            if str(document.get("title", "")).strip().lower() == quoted_title:
                return int(document["id"])
        return None

    async def get_document(self, document_id: int) -> dict[str, Any]:
        return await self.get_json(f"/api/documents/{document_id}/")

    async def wait_for_document_id(self, task_id: str) -> int:
        logger = logging.getLogger(LOGGER_NAME)
        deadline = asyncio.get_running_loop().time() + self.settings.task_timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            tasks = await self.get_json("/api/tasks/?page_size=100")
            if isinstance(tasks, dict):
                task_items = tasks.get("results", [])
            else:
                task_items = tasks
            for task in task_items:
                if task.get("task_id") != task_id:
                    continue
                status = task.get("status")
                if status == "SUCCESS":
                    related_document = task.get("related_document")
                    if related_document:
                        return int(related_document)
                    match = re.search(r"document id (\d+)", str(task.get("result", "")))
                    if match:
                        return int(match.group(1))
                    raise RuntimeError(f"Paperless task succeeded without document id: {task!r}")
                if status in {"FAILURE", "REVOKED"}:
                    result_text = str(task.get("result", ""))
                    related_document = task.get("related_document")
                    if status == "FAILURE" and related_document and "duplicate" in result_text.lower():
                        logger.info(
                            "Paperless task %s reported duplicate; using related document %s.",
                            task_id,
                            related_document,
                        )
                        return int(related_document)
                    raise RuntimeError(f"Paperless task failed: {task!r}")
            logger.info("Waiting for Paperless task %s.", task_id)
            await asyncio.sleep(2)
        raise TimeoutError(f"Timed out waiting for Paperless task {task_id}")


def extract_task_id(data: Any) -> str | None:
    if isinstance(data, str):
        return data
    if isinstance(data, dict):
        for key in ("task_id", "task"):
            if data.get(key):
                return str(data[key])
    if isinstance(data, list) and data:
        return extract_task_id(data[0])
    return None


def by_name(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(item.get("name", "")).strip().lower(): item for item in items}


def select_option_id(field: dict[str, Any], label: str) -> str | None:
    wanted = label.strip().lower()
    options = field.get("extra_data", {}).get("select_options", [])
    for option in options:
        if str(option.get("label", "")).strip().lower() == wanted:
            return str(option.get("id"))
    return None


def select_option_label(field: dict[str, Any], value: Any) -> str:
    wanted = str(value).strip()
    options = field.get("extra_data", {}).get("select_options", [])
    for option in options:
        if str(option.get("id", "")).strip() == wanted:
            return str(option.get("label", "")).strip()
    return wanted


def custom_field_payload(
    fields_by_name: dict[str, dict[str, Any]],
    field_name: str,
    value: Any,
) -> dict[str, Any] | None:
    logger = logging.getLogger(LOGGER_NAME)
    if value in (None, ""):
        return None
    field = fields_by_name.get(field_name.lower())
    if not field:
        logger.warning("Paperless custom field missing: %s", field_name)
        return None

    data_type = field.get("data_type")
    if data_type == "select":
        option_id = select_option_id(field, str(value))
        if not option_id:
            logger.warning(
                "Paperless select option missing for field %s: %s",
                field_name,
                value,
            )
            return None
        value = option_id
    elif data_type == "float":
        value = float(value)
    elif data_type == "documentlink":
        value = [int(value)] if value else []

    return {"field": int(field["id"]), "value": value}


TEMPLATE_RE = re.compile(r"\{([^{}]+)\}")


def context_value(context: dict[str, Any], dotted_path: str) -> Any:
    value: Any = context
    for part in dotted_path.split("."):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            return None
    return value


def resolve_config_value(value: Any, context: dict[str, Any]) -> Any:
    if not isinstance(value, str):
        return value

    exact_match = TEMPLATE_RE.fullmatch(value.strip())
    if exact_match:
        return context_value(context, exact_match.group(1).strip())

    def replace(match: re.Match[str]) -> str:
        resolved = context_value(context, match.group(1).strip())
        return "" if resolved is None else str(resolved)

    return TEMPLATE_RE.sub(replace, value)


def role_field_config(field_config: dict[str, Any], role: str) -> dict[str, Any]:
    values: dict[str, Any] = {}
    defaults = field_config.get("defaults", {})
    role_values = field_config.get(role, {})
    if isinstance(defaults, dict):
        values.update(defaults)
    if isinstance(role_values, dict):
        values.update(role_values)
    return values


def build_custom_fields(
    snapshot: dict[str, Any],
    fields_by_name: dict[str, dict[str, Any]],
    role: str,
    settings: PaperlessSettings,
    field_config: dict[str, Any],
    parent_document_id: int | None = None,
    skip_field_names: set[str] | None = None,
) -> list[dict[str, Any]]:
    identity = snapshot.get("identity", {})
    context = {
        **snapshot,
        "identity": identity,
        "parent_document_id": parent_document_id,
        "source_label": settings.source_label,
    }

    payload: list[dict[str, Any]] = []
    skipped = {field.lower() for field in (skip_field_names or set())}
    for field_name, raw_value in role_field_config(field_config, role).items():
        if field_name.lower() in skipped:
            continue
        value = resolve_config_value(raw_value, context)
        item = custom_field_payload(fields_by_name, field_name, value)
        if item:
            payload.append(item)
    return payload


def custom_field_id(fields_by_name: dict[str, dict[str, Any]], field_name: str) -> int | None:
    field = fields_by_name.get(field_name.lower())
    if not field:
        return None
    return int(field["id"])


def custom_field_label(
    custom_fields: list[dict[str, Any]],
    fields_by_name: dict[str, dict[str, Any]],
    field_name: str,
) -> str:
    field = fields_by_name.get(field_name.lower())
    if not field:
        return ""
    field_id = int(field["id"])
    for item in custom_fields:
        if int(item.get("field", 0)) != field_id:
            continue
        value = item.get("value")
        if value in (None, ""):
            return ""
        if field.get("data_type") == "select":
            return select_option_label(field, value)
        return str(value)
    return ""


def merge_custom_fields(
    existing_custom_fields: list[dict[str, Any]],
    new_custom_fields: list[dict[str, Any]],
    fields_by_name: dict[str, dict[str, Any]],
    clear_field_names: list[str],
) -> list[dict[str, Any]]:
    new_field_ids = {int(item["field"]) for item in new_custom_fields}
    clear_field_ids = {
        field_id
        for field_id in (
            custom_field_id(fields_by_name, field_name)
            for field_name in clear_field_names
        )
        if field_id is not None
    }
    retained = [
        item
        for item in existing_custom_fields
        if int(item["field"]) not in new_field_ids
        and int(item["field"]) not in clear_field_ids
    ]
    return retained + new_custom_fields


def clear_fields_for_role(field_config: dict[str, Any], role: str) -> list[str]:
    clear_fields = field_config.get("clear_fields", {})
    if not isinstance(clear_fields, dict):
        return []
    role_clear_fields = clear_fields.get(role, [])
    if isinstance(role_clear_fields, str):
        return [role_clear_fields]
    if isinstance(role_clear_fields, list):
        return [str(item) for item in role_clear_fields if item]
    return []


def list_filter(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value}
    if isinstance(value, list):
        return {str(item) for item in value if item is not None and str(item)}
    return {str(value)}


def document_date(value: Any) -> str:
    if not value:
        return ""
    return str(value)[:10]


def filters_have_existing_document_constraints(field_config: dict[str, Any]) -> bool:
    filters = field_config.get("filters", {})
    if not isinstance(filters, dict):
        return False
    return bool(filters.get("paperless_added_date") or filters.get("paperless_created_date"))


def artifact_allowed_by_config(
    field_config: dict[str, Any],
    role: str,
    snapshot: dict[str, Any],
    existing_document: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    filters = field_config.get("filters", {})
    if not isinstance(filters, dict):
        return True, ""

    roles = list_filter(filters.get("roles"))
    if roles and role not in roles:
        return False, f"role {role!r} not in configured roles"

    complaint_codes = list_filter(filters.get("complaint_codes"))
    if complaint_codes and str(snapshot.get("complaint_code")) not in complaint_codes:
        return False, "complaint code not in configured complaint_codes"

    added_dates = list_filter(filters.get("paperless_added_date"))
    created_dates = list_filter(filters.get("paperless_created_date"))
    if added_dates or created_dates:
        if not existing_document:
            return False, "date filters require an existing Paperless document"
        if added_dates and document_date(existing_document.get("added")) not in added_dates:
            return False, "Paperless added date did not match filter"
        if created_dates and document_date(existing_document.get("created")) not in created_dates:
            return False, "Paperless created date did not match filter"

    return True, ""


async def resolve_metadata(client: PaperlessClient, settings: PaperlessSettings) -> dict[str, Any]:
    document_types = by_name(await client.paginated_results("/api/document_types/?page_size=100"))
    custom_fields = by_name(await client.paginated_results("/api/custom_fields/?page_size=100"))
    correspondents = by_name(await client.paginated_results("/api/correspondents/?page_size=100"))

    complaint_type = document_types.get(settings.document_type_complaint.lower())
    attachment_type = document_types.get(settings.document_type_attachment.lower())
    if not complaint_type:
        raise RuntimeError(f"Paperless document type missing: {settings.document_type_complaint}")
    if not attachment_type:
        raise RuntimeError(f"Paperless document type missing: {settings.document_type_attachment}")

    correspondent = correspondents.get(settings.correspondent_name.lower())
    if not correspondent:
        logging.getLogger(LOGGER_NAME).warning(
            "Paperless correspondent not found, uploads will omit correspondent: %s",
            settings.correspondent_name,
        )

    return {
        "complaint_type_id": int(complaint_type["id"]),
        "attachment_type_id": int(attachment_type["id"]),
        "correspondent_id": int(correspondent["id"]) if correspondent else None,
        "custom_fields": custom_fields,
    }


async def upload_artifact(
    client: PaperlessClient,
    settings: PaperlessSettings,
    conn: duckdb.DuckDBPyConnection,
    metadata: dict[str, Any],
    field_config: dict[str, Any],
    snapshot: dict[str, Any],
    artifact_role: str,
    path: Path,
    title: str,
    project_root: Path,
    parent_document_id: int | None = None,
) -> int:
    logger = logging.getLogger(LOGGER_NAME)
    complaint_code = snapshot["complaint_code"]
    version = int(snapshot["version"])
    digest = sha256_file(path)
    existing_id = existing_uploaded_document(
        conn,
        complaint_code,
        version,
        artifact_role,
        digest,
    )
    existing_document_id = existing_id["document_id"] if existing_id else None
    existing_fingerprint = existing_id["metadata_fingerprint"] if existing_id else ""
    if settings.dry_run:
        if existing_document_id:
            logger.info(
                "DRY RUN Paperless metadata patch existing %s: doc_id=%s path=%s",
                artifact_role,
                existing_document_id,
                path,
            )
            return existing_document_id
        logger.info("DRY RUN Paperless upload %s: %s", artifact_role, path)
        return 0

    document_type_id = (
        metadata["complaint_type_id"]
        if artifact_role == "main_complaint"
        else metadata["attachment_type_id"]
    )
    try:
        document_id = existing_document_id
        document_existed_before_upload = bool(document_id)
        status_reason = "preserved_existing"
        if document_id:
            logger.info(
                "Paperless already uploaded %s: doc_id=%s path=%s; refreshing metadata",
                artifact_role,
                document_id,
                path,
            )
        else:
            document_id = await client.find_document_by_title(title)
            document_existed_before_upload = bool(document_id)
        if document_id and not existing_id:
            logger.info("Found existing Paperless document by title: doc_id=%s title=%s", document_id, title)
        existing_document = await client.get_document(document_id) if document_id else None
        allowed, reason = artifact_allowed_by_config(
            field_config,
            artifact_role,
            snapshot,
            existing_document,
        )
        if not allowed:
            logger.info("Skipping Paperless %s for %s: %s", artifact_role, title, reason)
            return document_id or 0
        if not document_id and filters_have_existing_document_constraints(field_config):
            logger.info(
                "Skipping new Paperless upload for %s because date filters only apply to existing documents.",
                title,
            )
            return 0
        if not document_id:
            status_reason = (
                "new_version"
                if artifact_role == "main_complaint"
                and has_prior_uploaded_main_version(conn, complaint_code, version)
                else "first_upload"
            )
            document_id = await client.upload_document(
                file_path=path,
                title=title,
                document_type_id=document_type_id,
                correspondent_id=metadata["correspondent_id"],
            )
            existing_document = await client.get_document(document_id)
            document_existed_before_upload = False
        preserve_status = (
            artifact_role == "main_complaint"
            and document_existed_before_upload
        )
        status_before = ""
        if artifact_role == "main_complaint" and existing_document:
            status_before = custom_field_label(
                existing_document.get("custom_fields", []),
                metadata["custom_fields"],
                "Status",
            )
        custom_fields = build_custom_fields(
            snapshot=snapshot,
            fields_by_name=metadata["custom_fields"],
            role=artifact_role,
            settings=settings,
            field_config=field_config,
            parent_document_id=parent_document_id,
            skip_field_names={"Status"} if preserve_status else None,
        )
        clear_field_names = clear_fields_for_role(field_config, artifact_role)
        merged_custom_fields = merge_custom_fields(
            existing_custom_fields=existing_document.get("custom_fields", []),
            new_custom_fields=custom_fields,
            fields_by_name=metadata["custom_fields"],
            clear_field_names=clear_field_names,
        )
        status_after = ""
        if artifact_role == "main_complaint":
            status_after = custom_field_label(
                merged_custom_fields,
                metadata["custom_fields"],
                "Status",
            )
        patch_payload: dict[str, Any] = {
            "title": title,
            "document_type": document_type_id,
            "custom_fields": merged_custom_fields,
        }
        if metadata["correspondent_id"]:
            patch_payload["correspondent"] = metadata["correspondent_id"]
        metadata_fingerprint = stable_hash(
            {
                "title": title,
                "document_type": document_type_id,
                "correspondent": metadata["correspondent_id"],
                "custom_fields": merged_custom_fields,
                "parent_document_id": parent_document_id,
            }
        )
        stored_local_path = relative_path(path, project_root)
        if existing_fingerprint == metadata_fingerprint:
            logger.info(
                "Paperless metadata unchanged for %s doc_id=%s title=%s; skipping patch",
                artifact_role,
                document_id,
                title,
            )
            record_paperless_document(
                conn,
                complaint_code,
                version,
                artifact_role,
                Path(stored_local_path),
                digest,
                document_id,
                parent_document_id,
                "uploaded",
                metadata_fingerprint=metadata_fingerprint,
            )
            if artifact_role == "main_complaint":
                record_status_history(
                    conn,
                    complaint_code,
                    version,
                    document_id,
                    status_before,
                    status_after,
                    status_reason,
                )
            return document_id

        await client.patch_json(f"/api/documents/{document_id}/", patch_payload)
        record_paperless_document(
            conn,
            complaint_code,
            version,
            artifact_role,
            Path(stored_local_path),
            digest,
            document_id,
            parent_document_id,
            "uploaded",
            metadata_fingerprint=metadata_fingerprint,
        )
        if artifact_role == "main_complaint":
            record_status_history(
                conn,
                complaint_code,
                version,
                document_id,
                status_before,
                status_after,
                status_reason,
            )
        logger.info("Uploaded Paperless %s doc_id=%s title=%s", artifact_role, document_id, title)
        return document_id
    except Exception as exc:
        record_paperless_document(
            conn,
            complaint_code,
            version,
            artifact_role,
            Path(relative_path(path, project_root)),
            digest,
            None,
            parent_document_id,
            "failed",
            error=repr(exc),
        )
        raise


def attachment_title(snapshot: dict[str, Any], attachment: dict[str, Any], path: Path) -> str:
    filename = attachment.get("filename") or path.name
    return f"PMDU - {snapshot['complaint_code']} - Attachment - {filename}"


async def sync_one_snapshot(
    client: PaperlessClient,
    settings: PaperlessSettings,
    conn: duckdb.DuckDBPyConnection,
    metadata: dict[str, Any],
    field_config: dict[str, Any],
    snapshot_path: Path,
    project_root: Path,
) -> None:
    snapshot = load_snapshot(snapshot_path)
    complaint_code = snapshot["complaint_code"]
    version = int(snapshot["version"])
    pdf_path = resolve_project_path(snapshot.get("generated_pdf"), project_root)
    if not pdf_path.exists():
        raise FileNotFoundError(f"Generated PDF missing for {complaint_code}: {pdf_path}")

    main_title = f"PMDU - {complaint_code} - Main Complaint - v{version}"
    parent_document_id = await upload_artifact(
        client,
        settings,
        conn,
        metadata,
        field_config,
        snapshot,
        "main_complaint",
        pdf_path,
        main_title,
        project_root,
    )

    seen_attachments: set[str] = set()
    for attachment in snapshot.get("attachments", []):
        local_path = attachment.get("local_path")
        if not local_path:
            continue
        attachment_key = f"{local_path}|{attachment.get('sha256', '')}"
        if attachment_key in seen_attachments:
            logging.getLogger(LOGGER_NAME).info(
                "Skipping duplicate attachment entry for %s: %s",
                complaint_code,
                local_path,
            )
            continue
        seen_attachments.add(attachment_key)
        attachment_path = resolve_project_path(local_path, project_root)
        if not attachment_path.exists():
            logging.getLogger(LOGGER_NAME).warning("Attachment file missing: %s", attachment_path)
            continue
        unsupported_reason = unsupported_paperless_attachment_reason(attachment_path)
        if unsupported_reason:
            logging.getLogger(LOGGER_NAME).warning(
                "Skipping unsupported Paperless attachment for %s: %s (%s)",
                complaint_code,
                attachment_path,
                unsupported_reason,
            )
            record_paperless_document(
                conn,
                complaint_code,
                version,
                "attachment",
                Path(relative_path(attachment_path, project_root)),
                sha256_file(attachment_path),
                None,
                parent_document_id,
                "skipped_unsupported",
                error=unsupported_reason,
            )
            continue
        try:
            await upload_artifact(
                client,
                settings,
                conn,
                metadata,
                field_config,
                snapshot,
                "attachment",
                attachment_path,
                attachment_title(snapshot, attachment, attachment_path),
                project_root,
                parent_document_id=parent_document_id,
            )
        except Exception:
            logging.getLogger(LOGGER_NAME).exception(
                "Paperless attachment sync failed for %s; continuing with remaining files: %s",
                complaint_code,
                attachment_path,
            )


def roles_for_preflight(field_config: dict[str, Any]) -> list[str]:
    filters = field_config.get("filters", {})
    configured = list_filter(filters.get("roles")) if isinstance(filters, dict) else set()
    roles = ["main_complaint", "attachment"]
    if configured:
        roles = [role for role in roles if role in configured]
    return roles


def validate_field_values(
    settings: PaperlessSettings,
    metadata: dict[str, Any],
    field_config: dict[str, Any],
    snapshots: list[Path],
) -> list[str]:
    issues: set[str] = set()
    fields_by_name = metadata["custom_fields"]
    if settings.correspondent_name and not metadata["correspondent_id"]:
        issues.add(f"Paperless correspondent missing: {settings.correspondent_name}")

    for snapshot_path in snapshots:
        snapshot = load_snapshot(snapshot_path)
        for role in roles_for_preflight(field_config):
            context = {
                **snapshot,
                "identity": snapshot.get("identity", {}),
                "parent_document_id": 1,
                "source_label": settings.source_label,
            }
            for field_name, raw_value in role_field_config(field_config, role).items():
                field = fields_by_name.get(field_name.lower())
                if not field:
                    issues.add(f"Paperless custom field missing: {field_name}")
                    continue
                value = resolve_config_value(raw_value, context)
                if value in (None, ""):
                    continue
                data_type = field.get("data_type")
                if data_type == "select" and not select_option_id(field, str(value)):
                    issues.add(f"Paperless select option missing for field {field_name}: {value}")
                elif data_type == "float":
                    try:
                        float(value)
                    except (TypeError, ValueError):
                        issues.add(f"Paperless float field value invalid for {field_name}: {value}")
    return sorted(issues)


async def run_paperless_check_async(settings: PaperlessSettings, project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    snapshots = latest_snapshots(settings.artifact_dir, settings.max_cases)
    field_config = load_field_config(settings.field_config_path)
    logger.info("Paperless check URL: %s", settings.base_url)
    logger.info("Paperless check snapshots found: %s", len(snapshots))
    async with PaperlessClient(settings) as client:
        metadata = await resolve_metadata(client, settings)
        issues = validate_field_values(settings, metadata, field_config, snapshots)
    if issues:
        for issue in issues:
            logger.error("Paperless preflight issue: %s", issue)
        raise RuntimeError(f"Paperless preflight failed with {len(issues)} issue(s).")
    logger.info("Paperless preflight check passed.")


def run_paperless_check(settings: PaperlessSettings, project_root: Path) -> None:
    asyncio.run(run_paperless_check_async(settings, project_root))


async def run_paperless_async(settings: PaperlessSettings, project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    snapshots = latest_snapshots(settings.artifact_dir, settings.max_cases)
    field_config = load_field_config(settings.field_config_path)
    logger.info("Paperless URL: %s", settings.base_url)
    logger.info("Paperless artifact snapshots found: %s", len(snapshots))
    logger.info("Paperless dry run: %s", settings.dry_run)
    logger.info("Paperless field config: %s", settings.field_config_path)

    conn = duckdb.connect(str(settings.duckdb_path))
    try:
        init_paperless_db(conn)
        migrate_paperless_paths(conn, project_root)
        async with PaperlessClient(settings) as client:
            metadata = await resolve_metadata(client, settings)
            uploaded = 0
            failed = 0
            for index, snapshot_path in enumerate(snapshots, start=1):
                logger.info("Paperless progress %s/%s: %s", index, len(snapshots), snapshot_path)
                try:
                    await sync_one_snapshot(
                        client,
                        settings,
                        conn,
                        metadata,
                        field_config,
                        snapshot_path,
                        project_root,
                    )
                    uploaded += 1
                except Exception:
                    failed += 1
                    logger.exception("Paperless sync failed for %s", snapshot_path)
            logger.info("Paperless sync complete: cases_processed=%s failed=%s", uploaded, failed)
    finally:
        conn.close()


def run_paperless(settings: PaperlessSettings, project_root: Path) -> None:
    asyncio.run(run_paperless_async(settings, project_root))

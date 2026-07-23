from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Automation Platform"
    log_level: str = "INFO"
    log_format: str = "json"

    database_url: str = "sqlite:///./data/automation-platform.db"
    celery_broker_url: str = "amqp://automation:automation-local-only@localhost:5672//"
    celery_result_backend: str | None = None
    celery_filesystem_folder: Path = Path("./data/celery-broker")

    deomee_root: Path = Path("/home/ahmad/code/deomeeautomations")
    crm_project_root: Path | None = None

    paperless_url: str = "https://paperless.lab.internal/dashboard"
    paperless_username: str = ""
    paperless_password: str = ""
    paperless_token: str = ""
    paperless_verify_ssl: bool = True
    paperless_ca_bundle: Path | None = None
    paperless_allow_insecure_fallback: bool = True
    paperless_timeout_seconds: float = 15.0
    paperless_document_type_complaint: str = "Complaint"
    paperless_document_type_attachment: str = "Attachment"
    paperless_correspondent_name: str = "CEO, (DEA), Lahore"
    paperless_task_timeout_seconds: int = 180
    paperless_max_pages: int = 10
    crm_job_stale_minutes: int = 15

    # Frappe Helpdesk is the operational complaint-case system. Paperless remains
    # the document archive; this integration stores only durable links and case data.
    frappe_helpdesk_enabled: bool = False
    frappe_helpdesk_url: str = "http://127.0.0.1:8082"
    frappe_helpdesk_public_url: str = ""
    frappe_helpdesk_site: str = "helpdesk.localhost"
    frappe_helpdesk_api_key: str = ""
    frappe_helpdesk_api_secret: str = ""
    frappe_helpdesk_timeout_seconds: float = 15.0
    frappe_helpdesk_preview_token_ttl_seconds: int = 1800
    frappe_helpdesk_verify_ssl: bool = True
    frappe_helpdesk_ca_bundle: Path | None = None
    frappe_helpdesk_default_raised_by: str = "complaints@deomee.local"
    frappe_helpdesk_default_priority: str = "Medium"
    frappe_helpdesk_default_status: str = "New"
    frappe_helpdesk_sync_states: str = "published"
    frappe_helpdesk_crm_public_url: str = "http://localhost:4321"
    frappe_helpdesk_paperless_url_template: str = ""
    frappe_helpdesk_fallback_agent_email: str = "ahmadkakarr@gmail.com"
    frappe_helpdesk_triage_team: str = "Complaint Triage"
    frappe_helpdesk_team_prefix: str = "Complaints"
    frappe_helpdesk_approved_reply_statuses: str = "Approved,Issued"

    antidengue_project_root: Path | None = None
    antidengue_python_bin: Path | None = None
    antidengue_pocketbase_db_path: Path | None = None
    antidengue_submission_deadline: str = "12:30 PM"
    antidengue_scheduler_enabled: bool = True
    antidengue_scheduler_interval_seconds: int = 10
    antidengue_local_retention_hours: int = 24
    antidengue_failed_workspace_retention_hours: int = 168
    antidengue_retention_enabled: bool = True
    antidengue_retention_interval_seconds: int = 3600

    # Shared ntfy transport. Publishing and subscriber URLs are deliberately
    # separate: containers can publish over their private network while phones
    # use localhost, LAN, Tailscale, or Cloudflare without application changes.
    ntfy_enabled: bool = False
    ntfy_port: int = 2586
    ntfy_publish_url: str = "http://127.0.0.1:2586"
    ntfy_public_base_url: str = "http://localhost:2586"
    ntfy_exposure_mode: str = "local"
    ntfy_token: str = ""
    ntfy_timeout_seconds: float = 5.0

    # Module-level routing switch. Other modules can add their own topic field
    # while sharing the transport above.
    antidengue_ntfy_enabled: bool = False
    antidengue_ntfy_topic: str = "automation-antidengue"

    artifact_root: Path = Path("./data/artifacts")
    source_file_max_bytes: int = 50 * 1024 * 1024
    legacy_uv_bin: str = "/home/ahmad/.local/bin/uv"

    whatsapp_nats_url: str = "nats://127.0.0.1:4222"
    whatsapp_command_subject: str = "whatsapp.pending"
    whatsapp_health_subject: str = "whatsapp.worker.health"
    whatsapp_group_directory_subject: str = "whatsapp.worker.directory.groups"
    whatsapp_identity_directory_subject: str = "whatsapp.worker.directory.identities"
    whatsapp_group_members_subject: str = "whatsapp.worker.group-members"
    whatsapp_qr_image_path: Path = Path("./data/whatsapp/login-qr.png")
    whatsapp_inbound_ingest_token: str = ""
    whatsapp_inbound_media_subject: str = "whatsapp.worker.inbound.media"
    whatsapp_inbound_history_provider: str = "wwebjs"
    whatsapp_inbound_history_subject: str = "whatsapp.worker.inbound.history"
    whatsapp_web_history_subject: str = "whatsapp.web.inbound.history"
    whatsapp_inbound_history_timeout_seconds: int = 15
    whatsapp_inbound_media_max_bytes: int = 75 * 1024 * 1024
    whatsapp_inbound_media_timeout_seconds: int = 180

    object_storage_enabled: bool = False
    object_storage_provider: str = "s3"
    object_storage_endpoint_url: str = ""
    object_storage_access_key: str = ""
    object_storage_secret_key: str = ""
    object_storage_region: str = "us-east-1"
    object_storage_path_style: bool = True
    object_storage_verify_ssl: bool = True
    object_storage_ca_bundle: Path | None = None
    object_storage_auto_create_buckets: bool = True
    object_storage_raw_bucket: str = "whatsapp-inbound-raw"
    object_storage_manifest_bucket: str = "whatsapp-inbound-manifests"
    object_storage_derived_bucket: str = "whatsapp-inbound-derived"
    platform_storage_raw_bucket: str = "automation-raw"
    platform_storage_manifest_bucket: str = "automation-manifests"
    platform_storage_derived_bucket: str = "automation-derived"
    object_storage_connect_timeout_seconds: float = 5.0
    object_storage_read_timeout_seconds: float = 120.0

    whatsapp_processing_classifier_version: str = "crm-classifier-v1"
    whatsapp_processing_crm_rule_version: str = "crm-104-v1"
    crm_complaint_prefixes: str = "104"
    crm_complaint_suffix_digits: int = 7
    whatsapp_processing_text_limit: int = 100_000

    api_cors_origins: str = (
        "http://localhost:4321,http://127.0.0.1:4321,"
        "http://localhost:5173,http://127.0.0.1:5173"
    )
    api_allowed_hosts: str = "localhost,127.0.0.1,testserver,api"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("paperless_ca_bundle", "object_storage_ca_bundle", "frappe_helpdesk_ca_bundle", mode="before")
    @classmethod
    def normalize_optional_ca_bundle(cls, value: object) -> object:
        """Treat empty optional CA-bundle settings as unset."""
        if value is None:
            return None

        if isinstance(value, str) and not value.strip():
            return None

        return value

    @field_validator("frappe_helpdesk_url", "frappe_helpdesk_public_url", "frappe_helpdesk_crm_public_url")
    @classmethod
    def normalize_optional_service_url(cls, value: str) -> str:
        normalized = value.strip().rstrip("/")
        if normalized and not normalized.startswith(("http://", "https://")):
            raise ValueError("service URLs must start with http:// or https://")
        return normalized

    @field_validator("frappe_helpdesk_timeout_seconds")
    @classmethod
    def validate_frappe_timeout(cls, value: float) -> float:
        if value <= 0 or value > 300:
            raise ValueError("frappe_helpdesk_timeout_seconds must be between 0 and 300")
        return value

    @field_validator("frappe_helpdesk_preview_token_ttl_seconds")
    @classmethod
    def validate_frappe_preview_ttl(cls, value: int) -> int:
        if value < 60 or value > 86400:
            raise ValueError(
                "frappe_helpdesk_preview_token_ttl_seconds must be between 60 and 86400"
            )
        return value

    @property
    def frappe_helpdesk_sync_state_list(self) -> list[str]:
        return [value.strip() for value in self.frappe_helpdesk_sync_states.split(",") if value.strip()]

    @property
    def frappe_helpdesk_approved_reply_status_list(self) -> list[str]:
        return [
            value.strip()
            for value in self.frappe_helpdesk_approved_reply_statuses.split(",")
            if value.strip()
        ]

    @field_validator("ntfy_exposure_mode")
    @classmethod
    def validate_ntfy_exposure_mode(cls, value: str) -> str:
        normalized = value.strip().lower()
        allowed = {"local", "lan", "tailscale", "cloudflare"}
        if normalized not in allowed:
            raise ValueError(f"ntfy_exposure_mode must be one of: {', '.join(sorted(allowed))}")
        return normalized

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("log_level must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
        return normalized

    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"json", "text"}:
            raise ValueError("log_format must be json or text")
        return normalized

    @field_validator("ntfy_port")
    @classmethod
    def validate_ntfy_port(cls, value: int) -> int:
        if not 1 <= value <= 65535:
            raise ValueError("ntfy_port must be between 1 and 65535")
        return value

    @field_validator("ntfy_publish_url", "ntfy_public_base_url")
    @classmethod
    def normalize_ntfy_url(cls, value: str) -> str:
        normalized = value.strip().rstrip("/")
        if not normalized.startswith(("http://", "https://")):
            raise ValueError("ntfy URLs must start with http:// or https://")
        return normalized

    @property
    def crm_root(self) -> Path:
        return (
            self.crm_project_root or self.deomee_root / "crm-management-system"
        ).resolve()

    @property
    def antidengue_root(self) -> Path:
        return (
            self.antidengue_project_root or self.deomee_root / "antidengue"
        ).resolve()

    @property
    def antidengue_python(self) -> Path:
        # Do not resolve this symlink: invoking the venv entry point is what
        # activates its site-packages. Resolving it launches uv's bare base
        # interpreter instead, which cannot import the legacy dependencies.
        return (
            self.antidengue_python_bin
            or self.antidengue_root / ".venv" / "bin" / "python"
        ).absolute()

    @property
    def pocketbase_db_path(self) -> Path:
        return (
            self.antidengue_pocketbase_db_path
            or self.deomee_root / "antidengue-pocketbase" / "pb_data" / "data.db"
        ).resolve()

    @property
    def source_file_root(self) -> Path:
        return (self.artifact_root / "source-files").resolve()

    @property
    def crm_filter_artifact_root(self) -> Path:
        return (self.artifact_root / "crm-sheet-filter").resolve()

    @property
    def crm_pdf_filter_artifact_root(self) -> Path:
        return (self.artifact_root / "crm-pdf-filter").resolve()

    @property
    def crm_sheet_pdf_artifact_root(self) -> Path:
        return (self.artifact_root / "crm-sheet-to-pdf").resolve()

    @property
    def whatsapp_inbound_media_root(self) -> Path:
        return (self.artifact_root / "whatsapp-inbound-media").resolve()

    @property
    def whatsapp_inbound_export_root(self) -> Path:
        return (self.artifact_root / "whatsapp-inbound-exports").resolve()

    @property
    def platform_storage_buckets(self) -> list[str]:
        return list(dict.fromkeys([
            self.platform_storage_raw_bucket,
            self.platform_storage_manifest_bucket,
            self.platform_storage_derived_bucket,
        ]))

    @property
    def crm_complaint_prefix_list(self) -> list[str]:
        return [value.strip() for value in self.crm_complaint_prefixes.split(",") if value.strip()]

    @property
    def cors_origins(self) -> list[str]:
        return [
            origin.strip()
            for origin in self.api_cors_origins.split(",")
            if origin.strip()
        ]

    @property
    def allowed_hosts(self) -> list[str]:
        return [host.strip() for host in self.api_allowed_hosts.split(",") if host.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()

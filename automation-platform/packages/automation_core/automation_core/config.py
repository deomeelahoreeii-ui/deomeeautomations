from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Automation Platform"
    database_url: str = "sqlite:///./data/automation-platform.db"
    celery_broker_url: str = "amqp://guest:guest@localhost:5672//"
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
    paperless_max_pages: int = 10
    antidengue_project_root: Path | None = None
    antidengue_python_bin: Path | None = None
    antidengue_pocketbase_db_path: Path | None = None
    antidengue_submission_deadline: str = "12:30 PM"
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
    api_cors_origins: str = (
        "http://localhost:4321,http://127.0.0.1:4321,"
        "http://localhost:5173,http://127.0.0.1:5173"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def crm_root(self) -> Path:
        return (self.crm_project_root or self.deomee_root / "crm-management-system").resolve()

    @property
    def antidengue_root(self) -> Path:
        return (self.antidengue_project_root or self.deomee_root / "antidengue").resolve()

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
    def cors_origins(self) -> list[str]:
        return [
            origin.strip()
            for origin in self.api_cors_origins.split(",")
            if origin.strip()
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()

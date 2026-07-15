from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any


LOGGER_NAME = "crm_management"
DEFAULT_PAPERLESS_DUCKDB = "paperless.duckdb"


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(LOGGER_NAME)


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line_number, raw_line in enumerate(path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            logging.getLogger(LOGGER_NAME).warning(
                "Skipping malformed .env line %s: missing '='", line_number
            )
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = value


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer, got {value!r}") from None


def resolve_artifact_dir(project_root: Path, value: str | None = None) -> Path:
    configured = value or os.getenv("ARTIFACT_DIR") or "artifacts"
    path = Path(configured).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def resolve_paperless_duckdb_path(project_root: Path) -> Path:
    configured = os.getenv("CRM_DUCKDB_PATH", DEFAULT_PAPERLESS_DUCKDB).strip()
    path = Path(configured).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def load_paperless_settings(
    project_root: Path,
    sync_mode: str | None = None,
    artifact_dir: str | None = None,
):
    from paperless import PaperlessSettings

    load_dotenv(project_root / ".env")
    paperless_url = os.getenv(
        "PAPERLESS_URL", "http://10.200.0.1:8010/dashboard"
    ).strip()
    paperless_username = os.getenv("PAPERLESS_USERNAME", "").strip()
    paperless_password = os.getenv("PAPERLESS_PASSWORD", "")
    missing = [
        name
        for name, value in (
            ("PAPERLESS_USERNAME", paperless_username),
            ("PAPERLESS_PASSWORD", paperless_password),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            "Missing required Paperless environment variables: "
            + ", ".join(missing)
            + ". Add them to .env."
        )
    return PaperlessSettings(
        base_url=paperless_url,
        username=paperless_username,
        password=paperless_password,
        artifact_dir=resolve_artifact_dir(project_root, artifact_dir),
        duckdb_path=resolve_paperless_duckdb_path(project_root),
        timeout_seconds=env_int("PAPERLESS_TIMEOUT_SECONDS", 120),
        task_timeout_seconds=env_int("PAPERLESS_TASK_TIMEOUT_SECONDS", 300),
        max_cases=(
            env_int("PAPERLESS_MAX_CASES", 0)
            if os.getenv("PAPERLESS_MAX_CASES")
            else None
        ),
        document_type_complaint=os.getenv(
            "PAPERLESS_DOCUMENT_TYPE_COMPLAINT", "Complaint"
        ).strip(),
        document_type_attachment=os.getenv(
            "PAPERLESS_DOCUMENT_TYPE_ATTACHMENT", "Attachment"
        ).strip(),
        correspondent_name=os.getenv(
            "PAPERLESS_CORRESPONDENT_NAME", "CEO, (DEA), Lahore"
        ).strip(),
        source_label=os.getenv("PAPERLESS_SOURCE_LABEL", "CRM Portal").strip(),
        field_config_path=(
            project_root
            / os.getenv("PAPERLESS_FIELD_CONFIG", "paperless_field_defaults.json")
        ).resolve(),
        dry_run=env_bool("PAPERLESS_DRY_RUN", False),
        verify_ssl=env_bool("PAPERLESS_VERIFY_SSL", True),
        sync_mode=(sync_mode or os.getenv("PAPERLESS_SYNC_MODE", "upload")).strip(),
    )


def parse_paperless_flags(args: list[str]) -> dict[str, str | None]:
    artifact_dir: str | None = None
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--artifact-dir":
            if index + 1 >= len(args):
                raise ValueError("--artifact-dir requires a folder path")
            artifact_dir = args[index + 1]
            index += 1
        elif arg.startswith("--artifact-dir="):
            artifact_dir = arg.split("=", 1)[1]
        else:
            raise ValueError("Unknown Paperless flag: " + arg + ". Use --artifact-dir.")
        index += 1
    return {"artifact_dir": artifact_dir}


def main() -> int:
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent
    try:
        command = sys.argv[1] if len(sys.argv) > 1 else "paperless"
        if command in {"paperless", "paperless-upload"}:
            from paperless import run_paperless

            paperless_options = parse_paperless_flags(sys.argv[2:])
            run_paperless(
                load_paperless_settings(
                    project_root,
                    sync_mode="upload",
                    **paperless_options,
                ),
                project_root,
            )
        elif command in {"paperless-patch", "paperless-refresh"}:
            from paperless import run_paperless

            paperless_options = parse_paperless_flags(sys.argv[2:])
            run_paperless(
                load_paperless_settings(
                    project_root,
                    sync_mode="patch",
                    **paperless_options,
                ),
                project_root,
            )
        elif command == "paperless-check":
            from paperless import run_paperless_check

            paperless_options = parse_paperless_flags(sys.argv[2:])
            run_paperless_check(
                load_paperless_settings(project_root, **paperless_options),
                project_root,
            )
        else:
            logger.error(
                "Unknown command: %s. Use paperless, paperless-patch, or paperless-check.",
                command,
            )
            return 2
        return 0
    except Exception:
        logger.exception("CRM automation failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

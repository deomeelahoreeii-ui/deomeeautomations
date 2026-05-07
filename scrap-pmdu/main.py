from __future__ import annotations

import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import duckdb
from playwright.sync_api import (
    BrowserContext,
    Page,
    sync_playwright,
)
from playwright.sync_api import (
    Error as PlaywrightError,
)
from playwright.sync_api import (
    TimeoutError as PlaywrightTimeoutError,
)

LOGGER_NAME = "pmdu_automation"
DEFAULT_LOGIN_URL = "https://admin.pmdu.gov.pk/guardian/users/login"
DEFAULT_COMPLAINTS_URL = "https://admin.pmdu.gov.pk/pkcp/complaints/list"
DEFAULT_CHROMIUM_EXECUTABLE = "/usr/bin/chromium"
COMPLAINT_CODE_RE = re.compile(
    r"\b[A-Z]{2,}[-/][A-Z0-9-/]{4,}\b|\b\d{4,}[-/]\d{2,}[-/]\d+\b"
)


@dataclass(frozen=True)
class Settings:
    login_url: str
    username: str
    password: str
    complaints_url: str
    chromium_executable_path: str
    headless: bool
    slow_mo_ms: int
    login_timeout_ms: int
    navigation_timeout_ms: int
    artifact_dir: Path
    browser_width: int
    browser_height: int
    raw_html_dir: Path
    duckdb_path: Path
    scrape_retries: int
    scrape_rate_limit_ms: int
    max_details: int | None


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


def load_settings(project_root: Path, require_credentials: bool = True) -> Settings:
    load_dotenv(project_root / ".env")

    username = os.getenv("PMDU_USERNAME", "").strip()
    password = os.getenv("PMDU_PASSWORD", "")
    missing = [
        name
        for name, value in (
            ("PMDU_USERNAME", username),
            ("PMDU_PASSWORD", password),
        )
        if not value
    ]
    if require_credentials and missing:
        raise RuntimeError(
            "Missing required environment variables: "
            + ", ".join(missing)
            + ". Add them to .env."
        )

    return Settings(
        login_url=os.getenv("PMDU_LOGIN_URL", DEFAULT_LOGIN_URL).strip(),
        username=username,
        password=password,
        complaints_url=os.getenv("PMDU_COMPLAINTS_URL", DEFAULT_COMPLAINTS_URL).strip(),
        chromium_executable_path=os.getenv(
            "CHROMIUM_EXECUTABLE_PATH", DEFAULT_CHROMIUM_EXECUTABLE
        ).strip(),
        headless=env_bool("PMDU_HEADLESS", False),
        slow_mo_ms=env_int("PMDU_SLOW_MO_MS", 100),
        login_timeout_ms=env_int("PMDU_LOGIN_TIMEOUT_MS", 300_000),
        navigation_timeout_ms=env_int("PMDU_NAVIGATION_TIMEOUT_MS", 60_000),
        artifact_dir=(
            project_root / os.getenv("PMDU_ARTIFACT_DIR", "artifacts")
        ).resolve(),
        browser_width=env_int("PMDU_BROWSER_WIDTH", 1600),
        browser_height=env_int("PMDU_BROWSER_HEIGHT", 950),
        raw_html_dir=(
            project_root / os.getenv("PMDU_RAW_HTML_DIR", "raw_html")
        ).resolve(),
        duckdb_path=(
            project_root / os.getenv("PMDU_DUCKDB_PATH", "pmdu_scrape.duckdb")
        ).resolve(),
        scrape_retries=env_int("PMDU_SCRAPE_RETRIES", 3),
        scrape_rate_limit_ms=env_int("PMDU_SCRAPE_RATE_LIMIT_MS", 1_500),
        max_details=(
            env_int("PMDU_MAX_DETAILS", 0) if os.getenv("PMDU_MAX_DETAILS") else None
        ),
    )


def load_phase2_settings(project_root: Path):
    from phase2 import Phase2Settings

    base_settings = load_settings(project_root, require_credentials=False)
    return Phase2Settings(
        raw_html_dir=base_settings.raw_html_dir,
        artifact_dir=base_settings.artifact_dir,
        duckdb_path=base_settings.duckdb_path,
        chromium_executable_path=base_settings.chromium_executable_path,
        headless=base_settings.headless,
        navigation_timeout_ms=base_settings.navigation_timeout_ms,
        attachment_retries=env_int("PMDU_ATTACHMENT_RETRIES", 3),
        attachment_concurrency=env_int("PMDU_ATTACHMENT_CONCURRENCY", 4),
        attachment_timeout_seconds=env_int("PMDU_ATTACHMENT_TIMEOUT_SECONDS", 120),
        pdf_enabled=env_bool("PMDU_PDF_ENABLED", True),
        pdf_format=os.getenv("PMDU_PDF_FORMAT", "Letter").strip() or "Letter",
        json_indent=env_bool("PMDU_JSON_INDENT", True),
        max_files=(
            env_int("PMDU_PHASE2_MAX_FILES", 0)
            if os.getenv("PMDU_PHASE2_MAX_FILES")
            else None
        ),
    )


def load_paperless_settings(project_root: Path):
    from paperless import PaperlessSettings

    base_settings = load_settings(project_root, require_credentials=False)
    paperless_url = os.getenv("PAPERLESS_URL", "http://localhost:8000/").strip()
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
        artifact_dir=base_settings.artifact_dir,
        duckdb_path=base_settings.duckdb_path,
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
        source_label=os.getenv("PAPERLESS_SOURCE_LABEL", "PMDU Portal").strip(),
        field_config_path=(
            project_root
            / os.getenv("PAPERLESS_FIELD_CONFIG", "paperless_field_defaults.json")
        ).resolve(),
        dry_run=env_bool("PAPERLESS_DRY_RUN", False),
    )


def load_notify_settings(
    project_root: Path,
    recipient_roles: frozenset[str] | None = None,
    include_group_summary: bool = False,
    files: str = "none",
    scope: str = "all",
    tehsil: str | None = None,
    action: str = "preview",
    command: str = "notify-preview",
    message_mode: str | None = None,
):
    from notify_under_investigation import NotifySettings

    return NotifySettings(
        paperless=load_paperless_settings(project_root),
        config_path=(
            project_root
            / os.getenv("UNDER_INVESTIGATION_NOTIFY_CONFIG", "notification_config.json")
        ).resolve(),
        duckdb_path=(
            project_root / os.getenv("PMDU_DUCKDB_PATH", "pmdu_scrape.duckdb")
        ).resolve(),
        action=action,
        command=command,
        recipient_roles=recipient_roles,
        include_group_summary=include_group_summary,
        files=files,
        scope=scope,
        tehsil=tehsil,
        message_mode=message_mode,
    )


def parse_notify_flags(args: list[str]) -> dict[str, Any]:
    to_value = "group"
    files = "none"
    scope = "all"
    tehsil: str | None = None
    message_mode: str | None = None
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--to":
            if index + 1 >= len(args):
                raise ValueError(
                    "--to requires a value: group, aeo, ddeo, deo, officers, or all"
                )
            to_value = args[index + 1]
            index += 1
        elif arg.startswith("--to="):
            to_value = arg.split("=", 1)[1]
        elif arg == "--files":
            if index + 1 >= len(args):
                raise ValueError(
                    "--files requires a value: none, pdf, pdf-and-attachments, or combined-pdf"
                )
            files = args[index + 1]
            index += 1
        elif arg.startswith("--files="):
            files = arg.split("=", 1)[1]
        elif arg == "--scope":
            if index + 1 >= len(args):
                raise ValueError("--scope requires a value")
            scope = args[index + 1]
            index += 1
        elif arg.startswith("--scope="):
            scope = arg.split("=", 1)[1]
        elif arg == "--tehsil":
            if index + 1 >= len(args):
                raise ValueError("--tehsil requires a value, for example: City")
            tehsil = args[index + 1]
            index += 1
        elif arg.startswith("--tehsil="):
            tehsil = arg.split("=", 1)[1]
        elif arg == "--mode":
            if index + 1 >= len(args):
                raise ValueError(
                    "--mode requires a value: per_complaint or per_recipient"
                )
            message_mode = args[index + 1]
            index += 1
        elif arg.startswith("--mode="):
            message_mode = arg.split("=", 1)[1]
        else:
            raise ValueError(
                "Unknown notify flag: "
                + arg
                + ". Use --to, --files, --scope, --tehsil, or --mode."
            )
        index += 1

    if scope not in {"all", "new", "new-or-updated", "newly-under-investigation"}:
        raise ValueError(
            "--scope must be one of: all, new, new-or-updated, newly-under-investigation"
        )

    # UPDATE HERE: Added "combined-pdf"
    if files not in {"none", "pdf", "pdf-and-attachments", "combined-pdf"}:
        raise ValueError(
            "--files must be one of: none, pdf, pdf-and-attachments, combined-pdf"
        )

    if message_mode is not None and message_mode not in {
        "per_complaint",
        "per_recipient",
    }:
        raise ValueError("--mode must be either per_complaint or per_recipient")

    role_values = {
        "aeo": frozenset({"aeo"}),
        "ddeo": frozenset({"ddeo"}),
        "deo": frozenset({"deo_mee"}),
        "officers": frozenset({"deo_mee", "ddeo", "aeo"}),
        "all": frozenset({"deo_mee", "ddeo", "aeo"}),
        "group": frozenset(),
    }
    if to_value not in role_values:
        raise ValueError("--to must be one of: group, aeo, ddeo, deo, officers, all")
    return {
        "recipient_roles": role_values[to_value],
        "include_group_summary": to_value in {"group", "all"},
        "files": files,
        "scope": scope,
        "tehsil": tehsil,
        "message_mode": message_mode,
    }


def fill_first_available(
    page: Page, label: str, selectors: list[str], value: str
) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=5_000)
            locator.fill(value)
            logger.info("Filled %s using selector: %s", label, selector)
            return
        except PlaywrightTimeoutError:
            logger.debug("Selector not visible for %s: %s", label, selector)
        except PlaywrightError as exc:
            logger.debug("Selector failed for %s: %s (%s)", label, selector, exc)

    raise RuntimeError(f"Could not find a visible {label} field on the login page.")


def capture_artifacts(page: Page, settings: Settings, name: str) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    settings.artifact_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    screenshot_path = settings.artifact_dir / f"{timestamp}-{name}.png"
    html_path = settings.artifact_dir / f"{timestamp}-{name}.html"

    try:
        page.screenshot(path=str(screenshot_path), full_page=True)
        html_path.write_text(page.content())
        logger.info("Saved screenshot: %s", screenshot_path)
        logger.info("Saved page HTML: %s", html_path)
    except PlaywrightError as exc:
        logger.warning("Could not capture artifacts for %s: %s", name, exc)


def wait_for_manual_login(page: Page, settings: Settings) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    logger.info(
        "Manual step required: verify the CAPTCHA in Chromium and click Sign In."
    )
    logger.info(
        "Waiting up to %.0f seconds for the URL to leave the login page.",
        settings.login_timeout_ms / 1000,
    )

    def logged_in() -> bool:
        current_url = page.url.rstrip("/")
        login_url = settings.login_url.rstrip("/")
        admin_origin = "https://admin.pmdu.gov.pk"
        return (
            (current_url == admin_origin or current_url.startswith(f"{admin_origin}/"))
            and current_url != login_url
            and "/guardian/users/login" not in current_url
        )

    deadline = time.monotonic() + (settings.login_timeout_ms / 1000)
    last_url = ""
    while time.monotonic() < deadline:
        current_url = page.url
        if current_url != last_url:
            logger.info("Current browser URL: %s", current_url)
            last_url = current_url
        if logged_in():
            logger.info("Login verified by URL change: %s", current_url)
            return
        page.wait_for_timeout(1_000)

    capture_artifacts(page, settings, "login-timeout")
    raise TimeoutError(
        "Timed out waiting for login. Complete CAPTCHA and click Sign In before "
        "PMDU_LOGIN_TIMEOUT_MS expires."
    )


def open_complaints_list(page: Page, settings: Settings) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    logger.info("Opening complaints list: %s", settings.complaints_url)
    response = page.goto(
        settings.complaints_url,
        wait_until="domcontentloaded",
        timeout=settings.navigation_timeout_ms,
    )
    if response is None:
        logger.warning("Navigation completed without a response object.")
    else:
        logger.info(
            "Complaints list response: status=%s url=%s",
            response.status,
            response.url,
        )

    try:
        page.wait_for_load_state("networkidle", timeout=settings.navigation_timeout_ms)
        logger.info("Complaints list reached network idle.")
    except PlaywrightTimeoutError:
        logger.warning(
            "Timed out waiting for network idle; continuing with current DOM."
        )

    logger.info("Final complaints list URL: %s", page.url)
    capture_artifacts(page, settings, "complaints-list")


def init_db(settings: Settings, project_root: Path) -> duckdb.DuckDBPyConnection:
    logger = logging.getLogger(LOGGER_NAME)
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(settings.duckdb_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scrape_queue (
            complaint_code TEXT PRIMARY KEY,
            detail_url TEXT,
            status TEXT,
            saved_path TEXT,
            scraped_at TIMESTAMP,
            listing_received_at TEXT,
            last_seen_at TIMESTAMP,
            last_scraped_listing_received_at TEXT
        )
        """
    )
    queue_columns = {
        row[1] for row in conn.execute("PRAGMA table_info('scrape_queue')").fetchall()
    }
    for column_name, column_type in (
        ("listing_received_at", "TEXT"),
        ("last_seen_at", "TIMESTAMP"),
        ("last_scraped_listing_received_at", "TEXT"),
    ):
        if column_name not in queue_columns:
            conn.execute(
                f"ALTER TABLE scrape_queue ADD COLUMN {column_name} {column_type}"
            )
    migrate_scrape_queue_paths(conn, project_root)
    logger.info("DuckDB queue ready: %s", settings.duckdb_path)
    return conn


def normalize_detail_url(page_url: str, href: str) -> str:
    return urljoin(page_url, href.strip())


def relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())


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


def migrate_scrape_queue_paths(
    conn: duckdb.DuckDBPyConnection, project_root: Path
) -> None:
    rows = conn.execute(
        """
        SELECT complaint_code, saved_path
        FROM scrape_queue
        WHERE saved_path IS NOT NULL
          AND saved_path LIKE '/%'
        """
    ).fetchall()
    for complaint_code, saved_path in rows:
        normalized = normalize_stored_path(saved_path, project_root)
        if normalized and normalized != saved_path:
            conn.execute(
                "UPDATE scrape_queue SET saved_path = ? WHERE complaint_code = ?",
                [normalized, complaint_code],
            )


def sanitize_filename(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return sanitized.strip("._") or "unknown"


def extract_code_from_text(text: str) -> str | None:
    match = COMPLAINT_CODE_RE.search(text)
    if match:
        return match.group(0).strip()
    return None


def extract_listing_rows(page: Page) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = page.evaluate(
        """
        () => {
            const table = document.querySelector("table.dataTable") || document.querySelector("table");
            if (!table) return [];
            const headers = Array.from(table.querySelectorAll("thead th")).map((th) => th.innerText.trim());

            const rows = [];
            for (const tr of table.querySelectorAll("tbody tr")) {
                const cells = Array.from(tr.querySelectorAll("td")).map((td) => td.innerText.trim());
                const text = cells.join(" | ");
                const candidates = [];

                for (const anchor of tr.querySelectorAll("a[href]")) {
                    candidates.push({
                        href: anchor.getAttribute("href") || "",
                        onclick: anchor.getAttribute("onclick") || "",
                        title: anchor.getAttribute("title") || "",
                        text: anchor.innerText || "",
                        html: anchor.outerHTML || "",
                    });
                }

                for (const button of tr.querySelectorAll("button, [role='button']")) {
                    candidates.push({
                        href: button.getAttribute("href") || "",
                        onclick: button.getAttribute("onclick") || "",
                        title: button.getAttribute("title") || "",
                        text: button.innerText || "",
                        html: button.outerHTML || "",
                    });
                }

                rows.push({ headers, cells, text, candidates });
            }
            return rows;
        }
        """
    )

    extracted: list[dict[str, str]] = []
    for row in rows:
        indexed_cells = [str(cell).strip() for cell in row.get("cells", [])]
        cells = [cell for cell in indexed_cells if cell]
        headers = [str(header).strip().lower() for header in row.get("headers", [])]
        row_text = str(row.get("text", "")).strip()
        complaint_code = None
        listing_received_at = ""
        for index, header in enumerate(headers):
            if (
                "complaint" in header
                and "code" in header
                and index < len(indexed_cells)
            ):
                complaint_code = indexed_cells[index]
            if "received" in header and "date" in header and index < len(indexed_cells):
                listing_received_at = indexed_cells[index]
        if not complaint_code:
            complaint_code = next(
                (
                    code
                    for code in (extract_code_from_text(cell) for cell in cells)
                    if code
                ),
                None,
            )
        if not complaint_code:
            complaint_code = extract_code_from_text(row_text)

        detail_url = None
        for candidate in row.get("candidates", []):
            candidate_blob = " ".join(
                str(candidate.get(key, ""))
                for key in ("href", "onclick", "title", "text", "html")
            )
            href = str(candidate.get("href", "")).strip()
            quoted_urls = [
                match
                for match in re.findall(r"""['"]([^'"]+)['"]""", candidate_blob)
                if "complaint" in match.lower()
            ]
            onclick_match = re.search(
                r"""(?:window\.)?location(?:\.href)?\s*=\s*['"]([^'"]+)['"]""",
                candidate_blob,
            )
            if onclick_match:
                quoted_urls.insert(0, onclick_match.group(1))

            possible_url = None
            if href and not href.lower().startswith(("javascript:", "#")):
                possible_url = href
            elif quoted_urls:
                possible_url = quoted_urls[0]
            if not possible_url:
                continue
            looks_like_detail = (
                "complaint" in possible_url.lower()
                and "list" not in possible_url.lower()
            ) or any(
                marker in candidate_blob.lower()
                for marker in ("view", "detail", "eye", "fa-eye", "glyphicon-eye")
            )
            if looks_like_detail:
                detail_url = normalize_detail_url(page.url, possible_url)
                break

        if complaint_code and detail_url:
            extracted.append(
                {
                    "complaint_code": complaint_code,
                    "detail_url": detail_url,
                    "listing_received_at": listing_received_at,
                }
            )

    return extracted


def upsert_queue_rows(
    conn: duckdb.DuckDBPyConnection,
    rows: list[dict[str, str]],
) -> tuple[int, int, int]:
    inserted = 0
    requeued = 0
    seen_existing = 0
    for row in rows:
        listing_received_at = row.get("listing_received_at", "")
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        existing = conn.execute(
            """
            SELECT
                status,
                listing_received_at,
                last_scraped_listing_received_at
            FROM scrape_queue
            WHERE complaint_code = ?
            """,
            [row["complaint_code"]],
        ).fetchone()
        if existing:
            seen_existing += 1
            status, previous_received_at, last_scraped_received_at = existing
            baseline_received_at = (
                last_scraped_received_at or previous_received_at or ""
            )
            received_changed = (
                bool(listing_received_at)
                and bool(baseline_received_at)
                and listing_received_at != baseline_received_at
            )
            next_status = "pending" if received_changed else status
            next_last_scraped_received_at = last_scraped_received_at
            if (
                not received_changed
                and status == "scraped"
                and listing_received_at
                and not last_scraped_received_at
            ):
                next_last_scraped_received_at = listing_received_at
            if received_changed:
                requeued += 1
            elif status != "scraped":
                next_status = status
            conn.execute(
                """
                UPDATE scrape_queue
                SET detail_url = ?,
                    listing_received_at = ?,
                    last_seen_at = ?,
                    status = ?,
                    last_scraped_listing_received_at = ?
                WHERE complaint_code = ?
                """,
                [
                    row["detail_url"],
                    listing_received_at,
                    now,
                    next_status,
                    next_last_scraped_received_at,
                    row["complaint_code"],
                ],
            )
        else:
            inserted += 1
            conn.execute(
                """
                INSERT INTO scrape_queue (
                    complaint_code,
                    detail_url,
                    status,
                    saved_path,
                    scraped_at,
                    listing_received_at,
                    last_seen_at,
                    last_scraped_listing_received_at
                )
                VALUES (?, ?, 'pending', NULL, NULL, ?, ?, NULL)
                """,
                [row["complaint_code"], row["detail_url"], listing_received_at, now],
            )
    return inserted, requeued, seen_existing


def set_show_entries_to_all(page: Page, settings: Settings) -> bool:
    logger = logging.getLogger(LOGGER_NAME)
    selectors = [
        "select[name$='_length']",
        "div.dataTables_length select",
        "label:has-text('Show') select",
    ]
    for selector in selectors:
        select = page.locator(selector).first
        try:
            select.wait_for(state="visible", timeout=3_000)
        except PlaywrightTimeoutError:
            continue

        options: list[dict[str, str]] = select.evaluate(
            """
            (node) => Array.from(node.options).map((option) => ({
                value: option.value,
                label: option.label || option.textContent || "",
            }))
            """
        )
        all_option = next(
            (
                option
                for option in options
                if option["value"] == "-1" or option["label"].strip().lower() == "all"
            ),
            None,
        )
        if not all_option:
            logger.info("Show entries dropdown has no All option; will paginate.")
            return False

        logger.info("Changing Show entries dropdown to All via %s.", selector)
        select.select_option(value=all_option["value"])
        try:
            page.wait_for_load_state(
                "networkidle", timeout=settings.navigation_timeout_ms
            )
        except PlaywrightTimeoutError:
            logger.warning("Timed out waiting after selecting All; continuing.")
        page.wait_for_timeout(1_000)
        return True

    logger.info("No visible Show entries dropdown found; will paginate current table.")
    return False


def is_next_button_enabled(page: Page) -> bool:
    return bool(
        page.evaluate(
            """
            () => {
                const candidates = Array.from(document.querySelectorAll(
                    ".dataTables_paginate .next, a.paginate_button.next, li.next, button[aria-label='Next']"
                ));
                const next = candidates.find((node) => /next/i.test(node.innerText || node.getAttribute("aria-label") || "Next"));
                if (!next) return false;
                const className = next.getAttribute("class") || "";
                const disabled = next.hasAttribute("disabled")
                    || next.getAttribute("aria-disabled") === "true"
                    || className.includes("disabled");
                return !disabled;
            }
            """
        )
    )


def click_next_page(page: Page, settings: Settings) -> bool:
    selectors = [
        ".dataTables_paginate .next:not(.disabled)",
        "a.paginate_button.next:not(.disabled)",
        "li.next:not(.disabled) a",
        "button[aria-label='Next']:not([disabled])",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=1_500)
            locator.click()
            try:
                page.wait_for_load_state(
                    "networkidle", timeout=settings.navigation_timeout_ms
                )
            except PlaywrightTimeoutError:
                pass
            page.wait_for_timeout(750)
            return True
        except PlaywrightTimeoutError:
            continue
        except PlaywrightError:
            continue
    return False


def build_scrape_queue(
    page: Page,
    settings: Settings,
    conn: duckdb.DuckDBPyConnection,
) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    logger.info("Waiting for listing table to render.")
    page.locator("table").first.wait_for(
        state="visible", timeout=settings.navigation_timeout_ms
    )

    used_all = set_show_entries_to_all(page, settings)
    page_number = 1
    total_inserted = 0
    total_requeued = 0
    total_existing = 0
    seen_codes: set[str] = set()

    while True:
        rows = extract_listing_rows(page)
        new_rows = [row for row in rows if row["complaint_code"] not in seen_codes]
        for row in new_rows:
            seen_codes.add(row["complaint_code"])

        inserted, requeued, existing = upsert_queue_rows(conn, new_rows)
        total_inserted += inserted
        total_requeued += requeued
        total_existing += existing
        logger.info(
            "Listing page %s: extracted=%s inserted=%s requeued_received_changed=%s already_known=%s total_unique=%s",
            page_number,
            len(rows),
            inserted,
            requeued,
            existing,
            len(seen_codes),
        )

        if used_all:
            break
        if not is_next_button_enabled(page):
            break
        if not click_next_page(page, settings):
            logger.warning(
                "Next pagination button looked enabled but could not be clicked."
            )
            break
        page_number += 1

    logger.info(
        "Queue build complete: inserted=%s requeued_received_changed=%s already_known=%s total_unique_on_listing=%s",
        total_inserted,
        total_requeued,
        total_existing,
        len(seen_codes),
    )


def pending_queue_rows(
    conn: duckdb.DuckDBPyConnection,
    settings: Settings,
) -> list[tuple[str, str, str]]:
    query = """
        SELECT complaint_code, detail_url, COALESCE(listing_received_at, '')
        FROM scrape_queue
        WHERE status != 'scraped'
           OR status IS NULL
        ORDER BY complaint_code
    """
    params: list[Any] = []
    if settings.max_details is not None:
        query += " LIMIT ?"
        params.append(settings.max_details)
    return conn.execute(query, params).fetchall()


def mark_scraped(
    conn: duckdb.DuckDBPyConnection,
    complaint_code: str,
    saved_path: Path,
    project_root: Path,
) -> None:
    conn.execute(
        """
        UPDATE scrape_queue
        SET status = 'scraped',
            saved_path = ?,
            scraped_at = ?,
            last_scraped_listing_received_at = listing_received_at
        WHERE complaint_code = ?
        """,
        [
            relative_path(saved_path, project_root),
            datetime.now(timezone.utc).replace(tzinfo=None),
            complaint_code,
        ],
    )


def mark_failed(conn: duckdb.DuckDBPyConnection, complaint_code: str) -> None:
    conn.execute(
        """
        UPDATE scrape_queue
        SET status = 'failed',
            scraped_at = ?
        WHERE complaint_code = ?
        """,
        [datetime.now(timezone.utc).replace(tzinfo=None), complaint_code],
    )


def wait_for_detail_render(page: Page, settings: Settings) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    for text in ("Complaint Information", "Complaint History", "Complaint Action"):
        try:
            page.get_by_text(text, exact=False).first.wait_for(
                state="visible",
                timeout=15_000,
            )
            logger.info("Detail section visible: %s", text)
        except PlaywrightTimeoutError:
            logger.warning("Detail section not visible before timeout: %s", text)

    try:
        page.wait_for_load_state("networkidle", timeout=settings.navigation_timeout_ms)
    except PlaywrightTimeoutError:
        logger.warning("Detail page did not reach network idle; saving current render.")
    page.wait_for_timeout(500)


def scrape_detail_page(
    context: BrowserContext,
    settings: Settings,
    conn: duckdb.DuckDBPyConnection,
    complaint_code: str,
    detail_url: str,
    listing_received_at: str,
) -> bool:
    logger = logging.getLogger(LOGGER_NAME)
    safe_code = sanitize_filename(complaint_code)
    saved_path = settings.raw_html_dir / f"{safe_code}.html"

    for attempt in range(1, settings.scrape_retries + 1):
        page = context.new_page()
        page.set_default_timeout(settings.navigation_timeout_ms)
        try:
            logger.info(
                "Scraping detail page %s received_at=%s attempt %s/%s: %s",
                complaint_code,
                listing_received_at or "<unknown>",
                attempt,
                settings.scrape_retries,
                detail_url,
            )
            response = page.goto(
                detail_url,
                wait_until="domcontentloaded",
                timeout=settings.navigation_timeout_ms,
            )
            if "/guardian/users/login" in page.url:
                raise RuntimeError(
                    "Detail page redirected to login; session is not valid"
                )
            if response is None:
                logger.warning("No response object for %s.", complaint_code)
            else:
                logger.info(
                    "Detail response for %s: status=%s url=%s",
                    complaint_code,
                    response.status,
                    response.url,
                )
                if response.status >= 400:
                    raise RuntimeError(f"HTTP {response.status} loading detail page")

            wait_for_detail_render(page, settings)
            settings.raw_html_dir.mkdir(parents=True, exist_ok=True)
            saved_path.write_text(page.content(), encoding="utf-8")
            mark_scraped(
                conn, complaint_code, saved_path, Path(__file__).resolve().parent
            )
            logger.info("Saved rendered HTML for %s: %s", complaint_code, saved_path)
            return True
        except Exception as exc:
            logger.warning(
                "Failed scraping %s attempt %s/%s: %s",
                complaint_code,
                attempt,
                settings.scrape_retries,
                exc,
            )
            if attempt == settings.scrape_retries:
                mark_failed(conn, complaint_code)
                return False
            page.wait_for_timeout(min(2_000 * attempt, 10_000))
        finally:
            page.close()

    return False


def scrape_pending_details(
    context: BrowserContext,
    settings: Settings,
    conn: duckdb.DuckDBPyConnection,
) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    pending = pending_queue_rows(conn, settings)
    logger.info("Pending detail pages to scrape: %s", len(pending))
    if settings.max_details is not None:
        logger.info(
            "PMDU_MAX_DETAILS is set: scraping at most %s rows.", settings.max_details
        )

    scraped = 0
    failed = 0
    for index, (complaint_code, detail_url, listing_received_at) in enumerate(
        pending, start=1
    ):
        logger.info("Detail progress %s/%s: %s", index, len(pending), complaint_code)
        if scrape_detail_page(
            context,
            settings,
            conn,
            complaint_code,
            detail_url,
            listing_received_at,
        ):
            scraped += 1
        else:
            failed += 1
        if index < len(pending):
            logger.info("Rate limit sleep: %sms", settings.scrape_rate_limit_ms)
            time.sleep(settings.scrape_rate_limit_ms / 1000)

    logger.info("Detail scraping complete: scraped=%s failed=%s", scraped, failed)


def run() -> None:
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent
    settings = load_settings(project_root)

    logger.info("Starting PMDU automation.")
    logger.info("Login URL: %s", settings.login_url)
    logger.info("Complaints URL: %s", settings.complaints_url)
    logger.info("Chromium executable: %s", settings.chromium_executable_path)
    logger.info("Headless mode: %s", settings.headless)
    logger.info(
        "Browser window size: %sx%s", settings.browser_width, settings.browser_height
    )
    logger.info("Raw HTML output directory: %s", settings.raw_html_dir)
    logger.info("DuckDB path: %s", settings.duckdb_path)
    logger.info("Scrape retries: %s", settings.scrape_retries)
    logger.info("Scrape rate limit: %sms", settings.scrape_rate_limit_ms)

    if not Path(settings.chromium_executable_path).exists():
        raise RuntimeError(
            f"Chromium executable not found: {settings.chromium_executable_path}"
        )

    conn = init_db(settings, project_root)
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                executable_path=settings.chromium_executable_path,
                headless=settings.headless,
                slow_mo=settings.slow_mo_ms,
                args=[
                    f"--window-size={settings.browser_width},{settings.browser_height}",
                    "--start-maximized",
                    "--force-device-scale-factor=1",
                ],
            )
            context = browser.new_context(
                no_viewport=True,
            )
            page = context.new_page()
            page.set_default_timeout(settings.navigation_timeout_ms)

            try:
                logger.info("Opening login page.")
                page.goto(
                    settings.login_url,
                    wait_until="domcontentloaded",
                    timeout=settings.navigation_timeout_ms,
                )
                logger.info("Login page loaded: %s", page.url)

                fill_first_available(
                    page,
                    "username",
                    [
                        "input[name='username']",
                        "input[name='email']",
                        "input[name='user_name']",
                        "input[type='email']",
                        "input[type='text']",
                    ],
                    settings.username,
                )
                fill_first_available(
                    page,
                    "password",
                    [
                        "input[name='password']",
                        "input[type='password']",
                    ],
                    settings.password,
                )
                capture_artifacts(page, settings, "login-filled")
                wait_for_manual_login(page, settings)
                open_complaints_list(page, settings)
                build_scrape_queue(page, settings, conn)
                scrape_pending_details(context, settings, conn)
                logger.info("Automation completed.")
                page.wait_for_timeout(5_000)
            finally:
                context.close()
                browser.close()
                logger.info("Browser closed.")
    finally:
        conn.close()
        logger.info("DuckDB connection closed.")


def main() -> int:
    logger = configure_logging()
    try:
        command = sys.argv[1] if len(sys.argv) > 1 else "phase1"
        if command in {"phase1", "scrape"}:
            run()
        elif command in {"phase2", "artifacts", "parse"}:
            from phase2 import run_phase2

            project_root = Path(__file__).resolve().parent
            run_phase2(load_phase2_settings(project_root), project_root)
        elif command in {"phase3", "paperless", "paperless-sync"}:
            from paperless import run_paperless

            project_root = Path(__file__).resolve().parent
            run_paperless(load_paperless_settings(project_root), project_root)
        elif command == "paperless-check":
            from paperless import run_paperless_check

            project_root = Path(__file__).resolve().parent
            run_paperless_check(load_paperless_settings(project_root), project_root)
        elif command in {"notify-preview", "notify-send"}:
            from notify_under_investigation import run_notify

            project_root = Path(__file__).resolve().parent
            notify_options = parse_notify_flags(sys.argv[2:])
            run_notify(
                load_notify_settings(
                    project_root,
                    action="send" if command == "notify-send" else "preview",
                    command=command,
                    **notify_options,
                ),
                project_root,
            )
        else:
            logger.error(
                "Unknown command: %s. Use phase1, phase2, paperless, paperless-check, notify-preview, or notify-send.",
                command,
            )
            return 2
        return 0
    except Exception:
        logger.exception("Automation failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

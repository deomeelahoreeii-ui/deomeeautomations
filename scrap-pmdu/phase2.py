from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import duckdb
import orjson
from lxml import html
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from yarl import URL


LOGGER_NAME = "pmdu_automation"
ADMIN_BASE_URL = "https://admin.pmdu.gov.pk/"
PDF_READY_SELECTORS = (
    "#print_button",
    ".complaint-history",
    ".attachment-list",
    "table.table_compact",
    "h4:has-text('Complaint Processing History')",
)
PDF_FONT_FALLBACK_CSS = """
<style id="codex-pdf-font-fallback">
html, body, table, td, th, p, div, span, bdi {
    font-family: "Noto Nastaliq Urdu", "Noto Naskh Arabic", "Noto Sans Arabic",
        "Arial Unicode MS", Arial, Helvetica, sans-serif !important;
}
tr, td, th {
    break-inside: avoid;
    page-break-inside: avoid;
}
table {
    border-collapse: collapse;
}
</style>
"""
PRINT_CSS_URL = "https://admin.pmdu.gov.pk/assets/css/print.css"


@dataclass(frozen=True)
class Phase2Settings:
    raw_html_dir: Path
    artifact_dir: Path
    duckdb_path: Path
    chromium_executable_path: str
    headless: bool
    navigation_timeout_ms: int
    attachment_retries: int
    attachment_concurrency: int
    attachment_timeout_seconds: int
    pdf_enabled: bool
    pdf_format: str
    json_indent: bool
    max_files: int | None


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    value = value.replace("\xa0", " ")
    lines = [re.sub(r"[ \t\r\f\v]+", " ", line).strip() for line in value.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def normalize_key(value: str) -> str:
    value = clean_text(value).lower()
    value = re.sub(r"[\u200e\u200f:*/()]+", " ", value)
    value = value.replace("&", "and")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def field_name(label: str) -> str | None:
    key = normalize_key(label)
    aliases = {
        "complaint code": "complaint_code",
        "date of complaint": "complaint_date",
        "complaint date": "complaint_date",
        "current status": "current_status",
        "province": "province",
        "district": "district",
        "tehsil": "tehsil",
        "address": "address",
        "complaint address": "address",
        "gps address": "gps_address",
        "level one": "level_one",
        "complaint category level 1": "level_one",
        "category level 1": "level_one",
        "level two": "level_two",
        "complaint sub category level 2": "level_two",
        "sub category level 2": "level_two",
        "category": "category",
        "subject": "subject",
        "complaint subject": "subject",
        "citizen name": "citizen_name",
        "complainant name": "citizen_name",
        "citizen contact": "citizen_contact",
        "contact": "citizen_contact",
        "mobile": "citizen_contact",
        "phone": "citizen_contact",
        "location of complaint": "district",
    }
    if key in aliases:
        return aliases[key]
    for alias, name in aliases.items():
        if alias in key:
            return name
    return None


def text_content(node: Any) -> str:
    return clean_text(node.text_content() if node is not None else "")


def relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path.resolve())


def resolve_stored_path(value: str | None, project_root: Path) -> Path:
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


def extract_label_value_text(text: str) -> tuple[str, str] | None:
    text = clean_text(text)
    if ":" not in text:
        return None
    label, value = text.split(":", 1)
    return clean_text(label), clean_text(value)


def extract_identity_from_lists(doc: html.HtmlElement) -> dict[str, str]:
    identity: dict[str, str] = {}
    for li in doc.xpath("//li[.//span[contains(@class, 'semi-bold')] or contains(., ':')]"):
        item_text = text_content(li)
        parsed = extract_label_value_text(item_text)
        if not parsed:
            continue
        label, value = parsed
        name = field_name(label)
        if name and value and not identity.get(name):
            identity[name] = value
    return identity


def extract_identity_from_tables(doc: html.HtmlElement) -> dict[str, str]:
    identity: dict[str, str] = {}
    for row in doc.xpath("//table//tr"):
        cells = row.xpath("./th|./td")
        index = 0
        while index < len(cells) - 1:
            label = text_content(cells[index])
            value = text_content(cells[index + 1])
            name = field_name(label)
            if name and value and not identity.get(name):
                identity[name] = value
            index += 2
    return identity


def split_location(value: str) -> tuple[str, str | None]:
    value = clean_text(value)
    match = re.match(r"(?P<district>.*?)\s*\((?P<province>[^,()]+)", value)
    if match:
        return clean_text(match.group("district")), clean_text(match.group("province"))
    return value, None


def parse_level_two_category(value: str) -> tuple[str, str | None]:
    value = clean_text(value)
    match = re.match(r"(?P<level_two>.*?)\s*\((?P<category>.*?)\)\s*$", value)
    if match:
        return clean_text(match.group("level_two")), clean_text(match.group("category"))
    return value, None


def finalize_identity(identity: dict[str, str], source_html: Path) -> dict[str, str]:
    code = identity.get("complaint_code") or source_html.stem
    identity["complaint_code"] = clean_text(code)

    location = identity.get("district", "")
    if "(" in location:
        district, province = split_location(location)
        if district:
            identity["district"] = district
        if province and not identity.get("province"):
            identity["province"] = province

    level_two = identity.get("level_two", "")
    if "(" in level_two and not identity.get("category"):
        parsed_level_two, category = parse_level_two_category(level_two)
        identity["level_two"] = parsed_level_two
        if category:
            identity["category"] = category

    for key in (
        "complaint_date",
        "current_status",
        "province",
        "district",
        "tehsil",
        "address",
        "gps_address",
        "level_one",
        "level_two",
        "category",
        "subject",
        "citizen_name",
        "citizen_contact",
    ):
        identity.setdefault(key, "")
    return identity


def find_heading_section(doc: html.HtmlElement, heading_text: str) -> html.HtmlElement | None:
    heading = doc.xpath(
        "//*[self::h1 or self::h2 or self::h3 or self::h4 or self::h5 or self::h6]"
        "[contains(translate(normalize-space(.), "
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), $needle)]",
        needle=heading_text.lower(),
    )
    if not heading:
        return None
    node = heading[0]
    parent = node.getparent()
    if parent is not None and "row" in (parent.get("class") or ""):
        return parent
    grandparent = parent.getparent() if parent is not None else None
    if grandparent is not None and "row" in (grandparent.get("class") or ""):
        return grandparent
    return parent


def extract_body(doc: html.HtmlElement) -> str:
    section = find_heading_section(doc, "Contents")
    if section is not None:
        body_nodes = section.xpath(".//bdi|.//*[contains(@class, 'alert')]//p|.//p")
        body_text = "\n\n".join(
            text_content(node)
            for node in body_nodes
            if text_content(node) and "Contents" not in text_content(node)
        )
        if body_text:
            return clean_text(body_text)

    candidates = doc.xpath(
        "//*[contains(@class, 'attachment-section')][.//bdi][1]//bdi"
        "|//p[contains(., 'Complaint Subject:')]/following::bdi[1]"
    )
    return clean_text("\n\n".join(text_content(node) for node in candidates))


def normalize_attachment_url(raw_url: str) -> str | None:
    raw_url = clean_text(raw_url)
    if not raw_url:
        return None
    url = URL(raw_url)
    if not url.is_absolute():
        url = URL(ADMIN_BASE_URL).join(url)
    if url.scheme not in {"http", "https"} or not url.host:
        return None
    return str(url)


def filename_from_url(url: str) -> str:
    parsed = URL(url)
    name = Path(parsed.path).name
    return sanitize_filename(name or "attachment")


def sanitize_filename(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return sanitized.strip("._") or "attachment"


def extract_attachments(doc: html.HtmlElement) -> list[dict[str, str]]:
    attachments: list[dict[str, str]] = []
    seen: set[str] = set()
    anchors = doc.xpath(
        "//a[@href and (contains(@href, 'cdn.pmdu.gov.pk') "
        "or contains(@href, 'complaint_attachments'))]"
    )
    for anchor in anchors:
        url = normalize_attachment_url(anchor.get("href", ""))
        if not url or url in seen:
            continue
        seen.add(url)
        filename = anchor.get("download") or text_content(anchor) or filename_from_url(url)
        attachments.append(
            {
                "filename": sanitize_filename(filename),
                "cdn_url": url,
            }
        )
    return attachments


def extract_history(doc: html.HtmlElement) -> list[dict[str, str]]:
    history: list[dict[str, str]] = []
    tables = doc.xpath(
        "//table[.//th[contains(translate(normalize-space(.), "
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'dated')]"
        " and .//th[contains(translate(normalize-space(.), "
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'remarks')]]"
    )
    for table in tables:
        headers = [normalize_key(text_content(th)) for th in table.xpath(".//thead//th")]
        if not headers:
            first_row = table.xpath(".//tr[1]/th")
            headers = [normalize_key(text_content(th)) for th in first_row]
        for row in table.xpath(".//tbody/tr|.//tr[position() > 1]"):
            cells = row.xpath("./td")
            if len(cells) < 5:
                continue
            values = [text_content(cell) for cell in cells]
            if len(values) >= 6:
                entry = {
                    "date": values[1],
                    "from": values[2],
                    "to": values[3],
                    "status": values[4],
                    "remarks": values[5],
                }
            else:
                mapped = dict(zip(headers, values, strict=False))
                entry = {
                    "date": mapped.get("dated", mapped.get("date", "")),
                    "from": mapped.get("from", ""),
                    "to": mapped.get("to", ""),
                    "status": mapped.get("status", ""),
                    "remarks": mapped.get("remarks", ""),
                }
            if any(entry.values()) and entry not in history:
                history.append(entry)
    return history


def parse_complaint_html(source_html: Path) -> dict[str, Any]:
    raw = source_html.read_bytes()
    doc = html.fromstring(raw)
    identity = extract_identity_from_tables(doc)
    identity.update({k: v for k, v in extract_identity_from_lists(doc).items() if v})
    identity = finalize_identity(identity, source_html)
    return {
        "complaint_code": identity["complaint_code"],
        "identity": identity,
        "body": extract_body(doc),
        "attachments": extract_attachments(doc),
        "history": extract_history(doc),
        "source_html": source_html,
    }


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalized_content_hash(parsed: dict[str, Any]) -> str:
    body = clean_text(parsed.get("body", ""))
    history = parsed.get("history", [])
    payload = body.encode("utf-8") + b"\n" + orjson.dumps(
        history,
        option=orjson.OPT_SORT_KEYS,
    )
    return sha256_bytes(payload)


def init_phase2_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS complaints (
            complaint_code TEXT PRIMARY KEY,
            complainant_name TEXT,
            contact TEXT,
            address TEXT,
            province TEXT,
            district TEXT,
            tehsil TEXT,
            level_one TEXT,
            level_two TEXT,
            category TEXT,
            first_seen DATE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS versions (
            id INTEGER PRIMARY KEY,
            complaint_code TEXT,
            version INTEGER,
            status TEXT,
            content_hash TEXT,
            pdf_hash TEXT,
            pdf_generation_success BOOLEAN,
            snapshot_path TEXT,
            pdf_path TEXT,
            parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (complaint_code) REFERENCES complaints(complaint_code)
        )
        """
    )
    version_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info('versions')").fetchall()
    }
    if "pdf_generation_success" not in version_columns:
        conn.execute(
            "ALTER TABLE versions ADD COLUMN pdf_generation_success BOOLEAN DEFAULT FALSE"
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS parse_failures (
            source_html TEXT PRIMARY KEY,
            error TEXT,
            failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS phase2_source_files (
            source_html TEXT PRIMARY KEY,
            raw_sha256 TEXT,
            complaint_code TEXT,
            content_hash TEXT,
            version INTEGER,
            result TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def migrate_phase2_paths(conn: duckdb.DuckDBPyConnection, project_root: Path) -> None:
    rows = conn.execute(
        """
        SELECT id, snapshot_path, pdf_path
        FROM versions
        WHERE snapshot_path LIKE '/%'
           OR pdf_path LIKE '/%'
        """
    ).fetchall()
    for version_id, snapshot_path, pdf_path in rows:
        normalized_snapshot = normalize_stored_path(snapshot_path, project_root)
        normalized_pdf = normalize_stored_path(pdf_path, project_root)
        if normalized_snapshot != snapshot_path or normalized_pdf != pdf_path:
            conn.execute(
                "UPDATE versions SET snapshot_path = ?, pdf_path = ? WHERE id = ?",
                [normalized_snapshot, normalized_pdf, version_id],
            )
    try:
        queue_rows = conn.execute(
            """
            SELECT complaint_code, saved_path
            FROM scrape_queue
            WHERE saved_path IS NOT NULL
              AND saved_path LIKE '/%'
            """
        ).fetchall()
    except duckdb.CatalogException:
        queue_rows = []
    for complaint_code, saved_path in queue_rows:
        normalized = normalize_stored_path(saved_path, project_root)
        if normalized and normalized != saved_path:
            conn.execute(
                "UPDATE scrape_queue SET saved_path = ? WHERE complaint_code = ?",
                [normalized, complaint_code],
            )


def insert_complaint_identity(conn: duckdb.DuckDBPyConnection, identity: dict[str, str]) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO complaints (
            complaint_code,
            complainant_name,
            contact,
            address,
            province,
            district,
            tehsil,
            level_one,
            level_two,
            category,
            first_seen
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)
        """,
        [
            identity.get("complaint_code", ""),
            identity.get("citizen_name", ""),
            identity.get("citizen_contact", ""),
            identity.get("address", ""),
            identity.get("province", ""),
            identity.get("district", ""),
            identity.get("tehsil", ""),
            identity.get("level_one", ""),
            identity.get("level_two", ""),
            identity.get("category", ""),
        ],
    )


def existing_version(
    conn: duckdb.DuckDBPyConnection,
    complaint_code: str,
    content_hash: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            id,
            version,
            snapshot_path,
            pdf_path,
            COALESCE(pdf_generation_success, FALSE) AS pdf_generation_success
        FROM versions
        WHERE complaint_code = ?
          AND content_hash = ?
        ORDER BY version DESC
        LIMIT 1
        """,
        [complaint_code, content_hash],
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "version": row[1],
        "snapshot_path": row[2],
        "pdf_path": row[3],
        "pdf_generation_success": row[4],
    }


def next_version(conn: duckdb.DuckDBPyConnection, complaint_code: str) -> int:
    row = conn.execute(
        """
        SELECT version
        FROM versions
        WHERE complaint_code = ?
        ORDER BY version DESC
        LIMIT 1
        """,
        [complaint_code],
    ).fetchone()
    return int(row[0]) + 1 if row else 1


def next_version_id(conn: duckdb.DuckDBPyConnection) -> int:
    row = conn.execute(
        """
        SELECT id
        FROM versions
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    return int(row[0]) + 1 if row else 1


def record_parse_failure(conn: duckdb.DuckDBPyConnection, source_html: Path, error: Exception) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO parse_failures (source_html, error, failed_at)
        VALUES (?, ?, ?)
        """,
        [str(source_html), repr(error), datetime.now(timezone.utc).replace(tzinfo=None)],
    )


async def download_one_attachment(
    session: aiohttp.ClientSession,
    attachment: dict[str, str],
    attachments_dir: Path,
    retries: int,
) -> dict[str, str]:
    logger = logging.getLogger(LOGGER_NAME)
    cdn_url = attachment["cdn_url"]
    filename = sanitize_filename(attachment.get("filename") or filename_from_url(cdn_url))
    target = attachments_dir / filename
    if target.exists():
        return {
            **attachment,
            "filename": filename,
            "local_path": str(target),
            "sha256": sha256_file(target),
        }

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(cdn_url) as response:
                response.raise_for_status()
                data = await response.read()
            target.write_bytes(data)
            return {
                **attachment,
                "filename": filename,
                "local_path": str(target),
                "sha256": sha256_bytes(data),
            }
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Attachment download failed attempt %s/%s url=%s error=%s",
                attempt,
                retries,
                cdn_url,
                exc,
            )
            if attempt < retries:
                await asyncio.sleep(min(2**attempt, 10))

    return {
        **attachment,
        "filename": filename,
        "local_path": "",
        "sha256": "",
        "error": repr(last_error),
    }


async def download_attachments(
    attachments: list[dict[str, str]],
    attachments_dir: Path,
    settings: Phase2Settings,
) -> list[dict[str, str]]:
    attachments_dir.mkdir(parents=True, exist_ok=True)
    timeout = aiohttp.ClientTimeout(total=settings.attachment_timeout_seconds)
    connector = aiohttp.TCPConnector(limit=settings.attachment_concurrency)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [
            download_one_attachment(
                session=session,
                attachment=attachment,
                attachments_dir=attachments_dir,
                retries=settings.attachment_retries,
            )
            for attachment in attachments
        ]
        if not tasks:
            return []
        return list(await asyncio.gather(*tasks))


def run_async_downloads(
    attachments: list[dict[str, str]],
    attachments_dir: Path,
    settings: Phase2Settings,
) -> list[dict[str, str]]:
    coroutine = download_attachments(attachments, attachments_dir, settings)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)

    result: list[dict[str, str]] = []
    error: BaseException | None = None

    def runner() -> None:
        nonlocal result, error
        try:
            result = asyncio.run(coroutine)
        except BaseException as exc:
            error = exc

    thread = threading.Thread(target=runner, name="phase2-attachment-downloads")
    thread.start()
    thread.join()
    if error:
        raise error
    return result


class PdfRenderer:
    def __init__(self, settings: Phase2Settings) -> None:
        self.settings = settings
        self.playwright: Any = None
        self.browser: Any = None
        self.context: Any = None

    def __enter__(self) -> "PdfRenderer":
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            executable_path=self.settings.chromium_executable_path,
            headless=True,
            args=[
                "--font-render-hinting=medium",
                "--enable-font-antialiasing",
            ],
        )
        self.context = self.browser.new_context(
            base_url=ADMIN_BASE_URL,
            locale="en-US",
            ignore_https_errors=True,
            bypass_csp=True,
        )
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    def render(self, source_html: Path, pdf_path: Path) -> str:
        logger = logging.getLogger(LOGGER_NAME)
        settings = self.settings
        if self.context is None:
            raise RuntimeError("PDF renderer is not open")

        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        html_text = html_for_pdf(source_html)
        page = self.context.new_page()
        page.set_default_timeout(settings.navigation_timeout_ms)
        failed_requests: list[str] = []
        page.on(
            "requestfailed",
            lambda request: failed_requests.append(f"{request.url} :: {request.failure}"),
        )

        def route_fonts(route: Any) -> None:
            request = route.request
            if request.resource_type == "font" or re.search(
                r"\.(?:woff2?|ttf|otf)(?:\?|$)",
                request.url,
                re.IGNORECASE,
            ):
                try:
                    route.fulfill(response=route.fetch(timeout=settings.navigation_timeout_ms))
                    return
                except PlaywrightError as exc:
                    logger.debug("Font route fetch failed, continuing request: %s", exc)
            route.continue_()

        page.route("**/*", route_fonts)
        try:
            page.set_content(html_text, wait_until="networkidle")
            page.emulate_media(media="print")
            wait_for_pdf_render(page, settings)
            page.pdf(
                path=str(pdf_path),
                format=settings.pdf_format,
                print_background=True,
                display_header_footer=True,
                header_template=chrome_header_template("Complaint Details - PMDU"),
                footer_template=chrome_footer_template(source_html),
                margin={
                    "top": "10mm",
                    "right": "10mm",
                    "bottom": "10mm",
                    "left": "10mm",
                },
                prefer_css_page_size=True,
            )
        finally:
            if failed_requests:
                logger.warning(
                    "PDF render had %s failed asset requests. First failures: %s",
                    len(failed_requests),
                    failed_requests[:5],
                )
            page.close()
        logger.info("Generated PDF: %s", pdf_path)
        return sha256_file(pdf_path)


def generate_pdf(source_html: Path, pdf_path: Path, settings: Phase2Settings) -> str:
    with PdfRenderer(settings) as renderer:
        return renderer.render(source_html, pdf_path)


def write_json(path: Path, data: dict[str, Any], indent: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    option = orjson.OPT_INDENT_2 if indent else 0
    path.write_bytes(orjson.dumps(data, option=option))


def html_for_pdf(source_html: Path) -> str:
    html_text = source_html.read_text(encoding="utf-8", errors="replace")
    doc = html.fromstring(html_text)
    printable = doc.xpath("//*[contains(concat(' ', normalize-space(@class), ' '), ' printable ')]")
    if not printable:
        body_html = html_text
    else:
        for node in printable[0].xpath(".//*[contains(concat(' ', normalize-space(@class), ' '), ' titlesec ')]//p"):
            node.attrib.pop("style", None)
        body_html = html.tostring(printable[0], encoding="unicode", method="html")

    head_parts = [f'<base href="{ADMIN_BASE_URL}">']
    for node in doc.xpath("//head/link[@rel='stylesheet' or @href] | //head/style"):
        head_parts.append(html.tostring(node, encoding="unicode", method="html"))
    if PRINT_CSS_URL not in "".join(head_parts):
        head_parts.append(f'<link rel="stylesheet" href="{PRINT_CSS_URL}">')
    head_parts.append(PDF_FONT_FALLBACK_CSS)
    head_parts.append(
        """
        <style id="codex-printthis-page">
        html, body { background: #fff !important; }
        body { margin: 0 !important; }
        .printable { display: block !important; }
        .titlesec p {
            background: transparent !important;
            color: #aaa !important;
            padding: 0 !important;
        }
        a[href]:after { content: none !important; }
        </style>
        """
    )
    return (
        "<!DOCTYPE html><html><head>"
        + "\n".join(head_parts)
        + "</head><body>"
        + body_html
        + "</body></html>"
    )


def chrome_header_template(title: str) -> str:
    return f"""
    <div style="font-size:8px; width:100%; padding:0 10mm; color:#000;">
      <span class="date" style="float:left;"></span>
      <span style="position:absolute; left:0; right:0; text-align:center;">{title}</span>
    </div>
    """


def chrome_footer_template(source_html: Path) -> str:
    return f"""
    <div style="font-size:8px; width:100%; padding:0 10mm; color:#000;">
      <span style="float:left;">{source_html.resolve().as_uri()}</span>
      <span style="float:right;"><span class="pageNumber"></span>/<span class="totalPages"></span></span>
    </div>
    """


def wait_for_pdf_render(page: Any, settings: Phase2Settings) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    try:
        page.wait_for_load_state("networkidle", timeout=settings.navigation_timeout_ms)
    except PlaywrightError as exc:
        logger.warning("PDF render did not reach networkidle before timeout: %s", exc)

    for selector in PDF_READY_SELECTORS:
        try:
            page.locator(selector).first.wait_for(state="visible", timeout=5_000)
            logger.info("PDF readiness selector visible: %s", selector)
        except PlaywrightError:
            logger.debug("Optional PDF readiness selector not visible: %s", selector)

    try:
        page.evaluate(
            """
            async () => {
                if (document.fonts && document.fonts.ready) {
                    await document.fonts.ready;
                }
            }
            """
        )
        logger.info("PDF font readiness completed.")
    except PlaywrightError as exc:
        logger.warning("Could not wait for document.fonts.ready: %s", exc)

    page.wait_for_timeout(1_000)


def insert_version(
    conn: duckdb.DuckDBPyConnection,
    complaint_code: str,
    version: int,
    content_hash: str,
    pdf_hash: str,
    pdf_generation_success: bool,
    snapshot_path: Path,
    pdf_path: Path,
    project_root: Path,
) -> None:
    conn.execute(
        """
        INSERT INTO versions (
            id,
            complaint_code,
            version,
            status,
            content_hash,
            pdf_hash,
            pdf_generation_success,
            snapshot_path,
            pdf_path,
            parsed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            next_version_id(conn),
            complaint_code,
            version,
            "generated",
            content_hash,
            pdf_hash,
            pdf_generation_success,
            relative_path(snapshot_path, project_root),
            relative_path(pdf_path, project_root),
            datetime.now(timezone.utc).replace(tzinfo=None),
        ],
    )


def update_existing_version_artifacts(
    conn: duckdb.DuckDBPyConnection,
    version_id: int,
    pdf_hash: str,
    pdf_generation_success: bool,
    snapshot_path: Path,
    pdf_path: Path,
    project_root: Path,
) -> None:
    conn.execute(
        """
        UPDATE versions
        SET status = ?,
            pdf_hash = ?,
            pdf_generation_success = ?,
            snapshot_path = ?,
            pdf_path = ?,
            parsed_at = ?
        WHERE id = ?
        """,
        [
            "generated",
            pdf_hash,
            pdf_generation_success,
            relative_path(snapshot_path, project_root),
            relative_path(pdf_path, project_root),
            datetime.now(timezone.utc).replace(tzinfo=None),
            version_id,
        ],
    )


def artifact_paths_exist(snapshot_path: Path, pdf_path: Path, pdf_required: bool) -> bool:
    if not snapshot_path.exists():
        return False
    if pdf_required and not pdf_path.exists():
        return False
    return True


def unchanged_source_is_ready(
    conn: duckdb.DuckDBPyConnection,
    source_html: Path,
    raw_sha256: str,
    settings: Phase2Settings,
    project_root: Path,
) -> bool:
    source_key = relative_path(source_html, project_root)
    row = conn.execute(
        """
        SELECT complaint_code, version, raw_sha256
        FROM phase2_source_files
        WHERE source_html = ?
        """,
        [source_key],
    ).fetchone()
    if not row or row[2] != raw_sha256:
        return False

    complaint_code, version, _ = row
    artifact_row = conn.execute(
        """
        SELECT snapshot_path, pdf_path
        FROM versions
        WHERE complaint_code = ?
          AND version = ?
          AND status = 'generated'
        ORDER BY parsed_at DESC
        LIMIT 1
        """,
        [complaint_code, version],
    ).fetchone()
    if not artifact_row:
        return False
    snapshot_path = resolve_stored_path(artifact_row[0], project_root)
    pdf_path = resolve_stored_path(artifact_row[1], project_root)
    return artifact_paths_exist(snapshot_path, pdf_path, settings.pdf_enabled)


def record_source_file(
    conn: duckdb.DuckDBPyConnection,
    source_html: Path,
    raw_sha256: str,
    outcome: dict[str, Any],
    project_root: Path,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO phase2_source_files (
            source_html,
            raw_sha256,
            complaint_code,
            content_hash,
            version,
            result,
            processed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            relative_path(source_html, project_root),
            raw_sha256,
            outcome.get("complaint_code", ""),
            outcome.get("content_hash", ""),
            int(outcome.get("version") or 0),
            outcome.get("status", ""),
            datetime.now(timezone.utc).replace(tzinfo=None),
        ],
    )


def process_one_html(
    source_html: Path,
    settings: Phase2Settings,
    conn: duckdb.DuckDBPyConnection,
    project_root: Path,
    pdf_renderer: PdfRenderer | None = None,
) -> dict[str, Any]:
    logger = logging.getLogger(LOGGER_NAME)
    parsed = parse_complaint_html(source_html)
    complaint_code = sanitize_filename(parsed["complaint_code"])
    parsed["complaint_code"] = complaint_code
    parsed["identity"]["complaint_code"] = complaint_code

    insert_complaint_identity(conn, parsed["identity"])
    content_hash = normalized_content_hash(parsed)
    duplicate = existing_version(conn, complaint_code, content_hash)
    if duplicate:
        version = int(duplicate["version"])
        snapshot_path = resolve_stored_path(duplicate["snapshot_path"], project_root)
        pdf_path = resolve_stored_path(duplicate["pdf_path"], project_root)
        if artifact_paths_exist(snapshot_path, pdf_path, settings.pdf_enabled):
            logger.info(
                "Skipping duplicate version for %s content_hash=%s existing_v=%s snapshot=%s",
                complaint_code,
                content_hash,
                version,
                snapshot_path,
            )
            return {
                "status": "skipped",
                "complaint_code": complaint_code,
                "content_hash": content_hash,
                "version": version,
            }

        logger.info(
            "Rebuilding missing artifacts for duplicate %s v%s content_hash=%s",
            complaint_code,
            version,
            content_hash,
        )
    else:
        version = next_version(conn, complaint_code)

    version_dir = settings.artifact_dir / complaint_code / f"v{version}"
    attachments_dir = version_dir / "attachments"
    snapshot_path = version_dir / "snapshot.json"
    pdf_path = version_dir / "complaint.pdf"
    version_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Creating artifact version %s for %s", version, complaint_code)
    downloaded_attachments = run_async_downloads(
        parsed["attachments"],
        attachments_dir,
        settings,
    )

    pdf_hash = ""
    pdf_generation_success = False
    if settings.pdf_enabled:
        try:
            pdf_hash = (
                pdf_renderer.render(source_html, pdf_path)
                if pdf_renderer
                else generate_pdf(source_html, pdf_path, settings)
            )
            pdf_generation_success = True
        except PlaywrightError as exc:
            logger.exception("PDF generation failed for %s: %s", complaint_code, exc)
        except Exception as exc:
            logger.exception("PDF generation failed for %s: %s", complaint_code, exc)

    parsed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    snapshot = {
        "complaint_code": complaint_code,
        "version": version,
        "parsed_at": parsed_at,
        "identity": parsed["identity"],
        "body": parsed["body"],
        "history": parsed["history"],
        "attachments": [
            {
                **attachment,
                "local_path": relative_path(Path(attachment["local_path"]), project_root)
                if attachment.get("local_path")
                else "",
            }
            for attachment in downloaded_attachments
        ],
        "source_html": relative_path(source_html, project_root),
        "generated_pdf": relative_path(pdf_path, project_root) if pdf_path.exists() else "",
        "content_hash": content_hash,
        "pdf_hash": pdf_hash,
        "pdf_generation_success": pdf_generation_success,
    }
    write_json(snapshot_path, snapshot, settings.json_indent)
    write_json(settings.artifact_dir / complaint_code / "meta.json", parsed["identity"], True)
    if duplicate:
        update_existing_version_artifacts(
            conn,
            int(duplicate["id"]),
            pdf_hash,
            pdf_generation_success,
            snapshot_path,
            pdf_path,
            project_root,
        )
    else:
        insert_version(
            conn,
            complaint_code,
            version,
            content_hash,
            pdf_hash,
            pdf_generation_success,
            snapshot_path,
            pdf_path,
            project_root,
        )
    logger.info("Wrote snapshot: %s", snapshot_path)
    return {
        "status": "rebuilt" if duplicate else "generated",
        "complaint_code": complaint_code,
        "content_hash": content_hash,
        "version": version,
    }


def run_phase2(settings: Phase2Settings, project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    html_files = sorted(settings.raw_html_dir.glob("*.html"))
    if settings.max_files is not None:
        html_files = html_files[: settings.max_files]

    logger.info("Phase Two raw HTML directory: %s", settings.raw_html_dir)
    logger.info("Phase Two artifact directory: %s", settings.artifact_dir)
    logger.info("Phase Two DuckDB path: %s", settings.duckdb_path)
    logger.info("Phase Two HTML files found: %s", len(html_files))

    settings.artifact_dir.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(settings.duckdb_path))
    try:
        init_phase2_db(conn)
        migrate_phase2_paths(conn, project_root)
        generated = 0
        rebuilt = 0
        skipped = 0
        unchanged = 0
        failed = 0
        renderer_manager: PdfRenderer | None = None
        try:
            for index, source_html in enumerate(html_files, start=1):
                logger.info("Phase Two progress %s/%s: %s", index, len(html_files), source_html)
                try:
                    raw_sha256 = sha256_file(source_html)
                    if unchanged_source_is_ready(
                        conn,
                        source_html,
                        raw_sha256,
                        settings,
                        project_root,
                    ):
                        unchanged += 1
                        logger.info("Skipping unchanged raw HTML: %s", source_html)
                        continue
                    if settings.pdf_enabled and renderer_manager is None:
                        renderer_manager = PdfRenderer(settings)
                        renderer_manager.__enter__()
                    outcome = process_one_html(
                        source_html,
                        settings,
                        conn,
                        project_root,
                        renderer_manager,
                    )
                    record_source_file(conn, source_html, raw_sha256, outcome, project_root)
                    result = outcome["status"]
                    if result == "generated":
                        generated += 1
                    elif result == "rebuilt":
                        rebuilt += 1
                    elif result == "skipped":
                        skipped += 1
                except Exception as exc:
                    failed += 1
                    record_parse_failure(conn, source_html, exc)
                    logger.exception("Failed processing raw HTML %s", source_html)
        finally:
            if renderer_manager:
                renderer_manager.__exit__(None, None, None)
        logger.info(
            "Phase Two complete: generated=%s rebuilt_missing=%s skipped_duplicates=%s skipped_unchanged=%s failed=%s",
            generated,
            rebuilt,
            skipped,
            unchanged,
            failed,
        )
    finally:
        conn.close()

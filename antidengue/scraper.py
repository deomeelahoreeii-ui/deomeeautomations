import datetime
import json
import logging
import os
import re
import shutil
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

# ==========================================
# 1. CONFIGURATION & PATHS
# ==========================================
BASE_DIR = Path(__file__).parent.resolve()
load_dotenv(BASE_DIR / ".env")
WATCH_DIR = BASE_DIR / "drop-raw-files"
DEBUG_DIR = BASE_DIR / "scraper-debug"
SESSION_FILE = BASE_DIR / "playwright_session.json"
LAST_RUN_FILE = BASE_DIR / "last_scrape_metadata.json"
LOGIN_LOCK_FILE = BASE_DIR / "login_failed.lock"  # The Kill-Switch file

PORTAL_USER = os.getenv("PORTAL_USER", "")
PORTAL_PASS = os.getenv("PORTAL_PASS", "")
BASE_URL = os.getenv("BASE_URL", "https://dashboard-tracking.punjab.gov.pk/")
FILTERED_TARGET_URL = os.getenv(
    "FILTERED_TARGET_URL",
    os.getenv(
        "TARGET_URL",
        "https://dashboard-tracking.punjab.gov.pk/user_wise_larva_report?district_id=18&date_range=0&dormant_users=true&line_listing=true&status=false&title=Dormant+Users",
    ),
)
UNFILTERED_TARGET_URL = os.getenv(
    "UNFILTERED_TARGET_URL",
    "https://dashboard-tracking.punjab.gov.pk/user_wise_larva_report?district_id=18&date_range=0",
)
REPORT_SOURCE = os.getenv("REPORT_SOURCE", "unfiltered").strip().lower()
PLAYWRIGHT_HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
PLAYWRIGHT_SLOW_MO_MS = int(os.getenv("PLAYWRIGHT_SLOW_MO_MS", "300"))
DOWNLOAD_RETRIES = int(os.getenv("DOWNLOAD_RETRIES", "5"))
DOWNLOAD_RETRY_DELAY_MS = int(os.getenv("DOWNLOAD_RETRY_DELAY_MS", "10000"))
PORTAL_LOGIN_MODE = (
    os.getenv("PORTAL_LOGIN_MODE", "manual").strip().lower().replace("-", "_")
)
if PORTAL_LOGIN_MODE not in {"manual", "auto", "remote_approve"}:
    PORTAL_LOGIN_MODE = "manual"
PORTAL_LOGIN_WAIT_SECONDS = int(os.getenv("PORTAL_LOGIN_WAIT_SECONDS", "90"))
PORTAL_LOGIN_REMOTE_WAIT_SECONDS = int(
    os.getenv("PORTAL_LOGIN_REMOTE_WAIT_SECONDS", "300")
)
PORTAL_LOGIN_AUTO_RETRIES = int(os.getenv("PORTAL_LOGIN_AUTO_RETRIES", "1"))
PORTAL_LOGIN_REQUEST_FILE = Path(
    os.getenv("PORTAL_LOGIN_REQUEST_FILE", str(BASE_DIR / "portal_login_request.json"))
)
PORTAL_LOGIN_APPROVAL_FILE = Path(
    os.getenv("PORTAL_LOGIN_APPROVAL_FILE", str(BASE_DIR / "portal_login_approval.json"))
)


# ==========================================
# 2. CAPTCHA SOLVER & HELPERS
# ==========================================
def record_scrape_time():
    """Maintains a persistent record of the last successful scrape."""
    timestamp = datetime.datetime.now().isoformat()
    with open(LAST_RUN_FILE, "w") as f:
        json.dump({"last_successful_scrape": timestamp}, f, indent=4)
    return timestamp


def solve_math_captcha(page, logger=None):
    """Reads the math problem from the DOM, solves it, and inputs the answer."""
    logger = logger or logging.getLogger("antidengue.scraper")

    # Wait for the captcha text to appear
    captcha_element = page.locator("text=/What is \\d+/")
    captcha_text = captcha_element.inner_text()
    logger.info(f"Detected CAPTCHA: {captcha_text}")

    # Extract the numbers and operator using Regex
    match = re.search(r"What is\s+(\d+)\s+([\+\-\*])\s+(\d+)\s*\?", captcha_text)
    if not match:
        raise ValueError(f"Could not parse CAPTCHA text: {captcha_text}")

    num1 = int(match.group(1))
    operator = match.group(2)
    num2 = int(match.group(3))

    # Calculate the result
    if operator == "+":
        result = num1 + num2
    elif operator == "-":
        result = num1 - num2
    elif operator == "*":
        result = num1 * num2
    else:
        raise ValueError(f"Unknown operator: {operator}")

    logger.info(f"Solved CAPTCHA: {num1} {operator} {num2} = {result}")

    # Type the result into the CAPTCHA input field using the exact HTML ID
    page.locator("#captcha").fill(str(result))


def _safe_download_filename(suggested_filename: str | None) -> str:
    filename = (suggested_filename or "").strip()
    if not filename:
        filename = "user_wise_dormancy_report.xls"

    return Path(filename).name


def _get_target_url() -> str:
    if REPORT_SOURCE in {"unfiltered", "all"}:
        return UNFILTERED_TARGET_URL
    if REPORT_SOURCE in {"filtered", "dormant"}:
        return FILTERED_TARGET_URL
    if REPORT_SOURCE.startswith("http://") or REPORT_SOURCE.startswith("https://"):
        return REPORT_SOURCE

    raise ValueError(
        "REPORT_SOURCE must be 'unfiltered', 'filtered', or a full report URL."
    )


def _download_error_details(download) -> str:
    try:
        failure = download.failure()
    except Exception as exc:
        failure = f"could not read download failure: {exc}"

    try:
        temporary_path = download.path()
    except Exception as exc:
        temporary_path = f"could not read temporary path: {exc}"

    return f"suggested={download.suggested_filename!r}, temp={temporary_path}, failure={failure!r}"


def _save_download(download, destination: Path, logger) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    download.save_as(str(destination))
    file_size = destination.stat().st_size

    if file_size > 0:
        return file_size

    try:
        temporary_path = Path(download.path())
        if temporary_path.exists() and temporary_path.stat().st_size > 0:
            shutil.copy2(temporary_path, destination)
            logger.info(
                f"Copied report from Playwright temporary file: {temporary_path}"
            )
            return destination.stat().st_size
    except Exception as exc:
        logger.warning(f"Could not inspect Playwright temporary download file: {exc}")

    logger.warning(
        f"Playwright saved a zero-byte download: {_download_error_details(download)}"
    )
    return file_size


def _copy_latest_browser_download(destination: Path, logger) -> int:
    candidates = sorted(
        WATCH_DIR.glob("*"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0,
        reverse=True,
    )

    for candidate in candidates:
        if candidate == destination or not candidate.is_file():
            continue

        file_size = candidate.stat().st_size
        if file_size <= 0:
            continue

        shutil.copy2(candidate, destination)
        logger.info(f"Recovered report from browser download file: {candidate}")
        return destination.stat().st_size

    return 0


def _response_looks_like_report(response) -> bool:
    headers = response.headers
    content_disposition = headers.get("content-disposition", "").lower()
    content_type = headers.get("content-type", "").lower()
    url = response.url.lower()

    return (
        "attachment" in content_disposition
        or "csv" in content_type
        or "excel" in content_type
        or "spreadsheet" in content_type
        or "export" in url
        or "csv" in url
    )


def _save_debug_snapshot(page, label: str, logger) -> tuple[Path | None, Path | None]:
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    safe_label = re.sub(r"[^a-zA-Z0-9_-]+", "-", label).strip("-") or "snapshot"
    screenshot_path = DEBUG_DIR / f"{timestamp}_{safe_label}.png"
    html_path = DEBUG_DIR / f"{timestamp}_{safe_label}.html"

    try:
        page.screenshot(path=str(screenshot_path), full_page=True)
    except Exception as exc:
        logger.warning(f"Could not save scraper screenshot: {exc}")
        screenshot_path = None

    try:
        html_path.write_text(page.content(), encoding="utf-8")
    except Exception as exc:
        logger.warning(f"Could not save scraper HTML snapshot: {exc}")
        html_path = None

    logger.info(f"Saved scraper debug snapshot: {screenshot_path}, {html_path}")
    return screenshot_path, html_path


def _visible_login_messages(page) -> str:
    messages: list[str] = []
    for selector in (
        ".alert",
        ".error",
        ".toast",
        "[role='alert']",
        ".alert-danger",
        ".alert-warning",
        ".invalid-feedback",
    ):
        try:
            for text in page.locator(selector).all_inner_texts():
                normalized = " ".join(text.split())
                if normalized and normalized not in messages:
                    messages.append(normalized)
        except Exception:
            continue
    return " | ".join(messages)


def _login_form_visible(page) -> bool:
    try:
        return page.locator("#user_username").is_visible()
    except Exception:
        return False


def _fill_login_form(page, logger) -> None:
    logger.info("Filling portal credentials and CAPTCHA...")
    page.locator("#user_username").fill(PORTAL_USER)
    page.locator("#user_password").fill(PORTAL_PASS)
    solve_math_captcha(page, logger)


def _click_sign_in(page, logger) -> None:
    logger.info("Clicking Sign In...")
    candidates = [
        page.get_by_role("button", name=re.compile(r"sign\s*in", re.I)).first,
        page.locator("button:has-text('Sign In')").first,
        page.locator("input[type='submit']").first,
    ]
    last_error: Exception | None = None
    for candidate in candidates:
        try:
            if candidate.count() > 0 and candidate.is_visible():
                candidate.click()
                return
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Could not find a visible Sign In button: {last_error}")


def _wait_for_login_success(page, timeout_seconds: int, logger) -> bool:
    deadline = datetime.datetime.now() + datetime.timedelta(seconds=timeout_seconds)
    while datetime.datetime.now() < deadline:
        if not _login_form_visible(page):
            page.wait_for_timeout(2000)
            if not _login_form_visible(page):
                return True
        page.wait_for_timeout(1000)
    messages = _visible_login_messages(page)
    if messages:
        logger.warning(f"Portal login page message(s): {messages}")
    return False


def _write_login_request(page, logger) -> None:
    screenshot_path, html_path = _save_debug_snapshot(page, "login-awaiting-approval", logger)
    PORTAL_LOGIN_REQUEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    request = {
        "status": "pending",
        "requested_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "mode": PORTAL_LOGIN_MODE,
        "base_url": BASE_URL,
        "approval_file": str(PORTAL_LOGIN_APPROVAL_FILE),
        "screenshot": str(screenshot_path) if screenshot_path else "",
        "html": str(html_path) if html_path else "",
    }
    PORTAL_LOGIN_REQUEST_FILE.write_text(
        json.dumps(request, indent=2),
        encoding="utf-8",
    )
    logger.info(f"Remote login approval request saved: {PORTAL_LOGIN_REQUEST_FILE}")


def _read_remote_approval() -> str:
    if not PORTAL_LOGIN_APPROVAL_FILE.exists():
        return ""
    try:
        raw = PORTAL_LOGIN_APPROVAL_FILE.read_text(encoding="utf-8").strip()
        if not raw:
            return ""
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return raw.lower()
        return str(payload.get("action") or payload.get("status") or "").strip().lower()
    except OSError:
        return ""


def _mark_login_request(status: str, detail: str = "") -> None:
    if not PORTAL_LOGIN_REQUEST_FILE.exists():
        return
    try:
        payload = json.loads(PORTAL_LOGIN_REQUEST_FILE.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            payload = {}
    except (OSError, json.JSONDecodeError):
        payload = {}
    payload.update(
        {
            "status": status,
            "detail": detail,
            "updated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        }
    )
    try:
        PORTAL_LOGIN_REQUEST_FILE.write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )
    except OSError:
        return


def _save_login_success(context, logger) -> None:
    logger.info("✅ Login successful! Saving session state...")
    context.storage_state(path=str(SESSION_FILE))
    _mark_login_request("completed")


def _wait_for_remote_approval(page, logger) -> str:
    PORTAL_LOGIN_APPROVAL_FILE.unlink(missing_ok=True)
    _write_login_request(page, logger)
    logger.info("======================================================")
    logger.info("⏳ WAITING FOR REMOTE LOGIN APPROVAL")
    logger.info(f"Approve by writing {{\"action\":\"approve\"}} to: {PORTAL_LOGIN_APPROVAL_FILE}")
    logger.info("You can also click Sign In manually in the browser while this waits.")
    logger.info(f"Waiting up to {PORTAL_LOGIN_REMOTE_WAIT_SECONDS} seconds...")
    logger.info("======================================================")

    deadline = datetime.datetime.now() + datetime.timedelta(
        seconds=PORTAL_LOGIN_REMOTE_WAIT_SECONDS
    )
    while datetime.datetime.now() < deadline:
        if not _login_form_visible(page):
            return "manual_clicked"
        action = _read_remote_approval()
        if action in {"approve", "approved", "click", "sign_in", "signin", "yes"}:
            return "approve"
        if action in {"deny", "denied", "cancel", "cancelled", "no"}:
            return "deny"
        page.wait_for_timeout(1000)
    return "timeout"


def _manual_login(page, context, logger, *, reason: str = "") -> None:
    _save_debug_snapshot(page, "login-manual-review", logger)
    logger.info("======================================================")
    logger.info("⚠️ SCRIPT PAUSED FOR MANUAL VERIFICATION")
    if reason:
        logger.info(reason)
    logger.info("Please review the credentials and CAPTCHA in the browser.")
    logger.info("If everything is correct, manually click 'Sign In'.")
    logger.info(f"You have {PORTAL_LOGIN_WAIT_SECONDS} seconds to do this...")
    logger.info("======================================================")
    if not _wait_for_login_success(page, PORTAL_LOGIN_WAIT_SECONDS, logger):
        raise PlaywrightTimeoutError("Login timeout: The login form never disappeared.")
    _save_login_success(context, logger)


def _auto_login(page, context, logger) -> None:
    max_attempts = max(1, PORTAL_LOGIN_AUTO_RETRIES + 1)
    for attempt in range(1, max_attempts + 1):
        logger.info(f"Auto login attempt {attempt}/{max_attempts}...")
        _fill_login_form(page, logger)
        _save_debug_snapshot(page, f"login-auto-before-click-attempt-{attempt}", logger)
        _click_sign_in(page, logger)
        if _wait_for_login_success(page, PORTAL_LOGIN_WAIT_SECONDS, logger):
            _save_login_success(context, logger)
            return
        _save_debug_snapshot(page, f"login-auto-failed-attempt-{attempt}", logger)
        if attempt < max_attempts:
            logger.warning("Auto login did not reach dashboard. Retrying with a fresh CAPTCHA...")
            page.goto(BASE_URL, wait_until="domcontentloaded")
            page.wait_for_timeout(1000)
            if not _login_form_visible(page):
                _save_login_success(context, logger)
                return

    _manual_login(
        page,
        context,
        logger,
        reason="Auto Sign In did not succeed cleanly; falling back to manual review.",
    )


def _remote_approved_login(page, context, logger) -> None:
    _fill_login_form(page, logger)
    action = _wait_for_remote_approval(page, logger)
    if action == "manual_clicked":
        if not _wait_for_login_success(page, PORTAL_LOGIN_WAIT_SECONDS, logger):
            raise PlaywrightTimeoutError(
                "Login timeout after manual click during remote approval."
            )
        _save_login_success(context, logger)
        return
    if action == "deny":
        _mark_login_request("denied", "Remote operator denied portal Sign In.")
        raise RuntimeError("Remote operator denied portal Sign In.")
    if action == "timeout":
        _manual_login(
            page,
            context,
            logger,
            reason="Remote approval timed out; falling back to local manual review.",
        )
        return

    _click_sign_in(page, logger)
    if not _wait_for_login_success(page, PORTAL_LOGIN_WAIT_SECONDS, logger):
        _save_debug_snapshot(page, "login-remote-approved-failed", logger)
        _manual_login(
            page,
            context,
            logger,
            reason="Remote-approved Sign In did not succeed; falling back to manual review.",
        )
        return
    _save_login_success(context, logger)


def _perform_login(page, context, logger) -> None:
    logger.info(f"Portal login mode: {PORTAL_LOGIN_MODE}")
    if PORTAL_LOGIN_MODE == "auto":
        _auto_login(page, context, logger)
    elif PORTAL_LOGIN_MODE == "remote_approve":
        _remote_approved_login(page, context, logger)
    else:
        _fill_login_form(page, logger)
        _manual_login(page, context, logger)


# ==========================================
# 3. PORTAL SCRAPER ENTRY POINT
# ==========================================
def scrape_portal_data(logger=None) -> Path:
    logger = logger or logging.getLogger("antidengue.scraper")
    WATCH_DIR.mkdir(parents=True, exist_ok=True)

    if not PORTAL_USER or not PORTAL_PASS:
        raise RuntimeError(
            "PORTAL_USER and PORTAL_PASS must be set in the environment or .env file."
        )

    # 🛑 CRITICAL CHECK: Ensure we haven't failed previously
    if LOGIN_LOCK_FILE.exists():
        logger.error("🚨 SCRIPT HALTED: A previous login attempt failed.")
        logger.error(
            f"To allow the script to run again, you must manually delete this file: {LOGIN_LOCK_FILE}"
        )
        raise RuntimeError(
            "Login Lock Active. Script permanently halted until manual reset."
        )

    # Check last run time
    if LAST_RUN_FILE.exists():
        with open(LAST_RUN_FILE, "r") as f:
            last_run_data = json.load(f)
            logger.info(
                f"Last successful scrape was recorded at: {last_run_data.get('last_successful_scrape')}"
            )

    with sync_playwright() as p:
        # VISUAL DEBUGGING MODE: headless=False with a 300ms delay between actions
        browser = p.chromium.launch(
            headless=PLAYWRIGHT_HEADLESS,
            slow_mo=PLAYWRIGHT_SLOW_MO_MS,
            executable_path="/usr/bin/chromium",
            downloads_path=str(WATCH_DIR),
        )

        # Load session state if it exists to bypass login
        context_args = {}
        if SESSION_FILE.exists():
            logger.info("Loading existing browser session...")
            context_args["storage_state"] = str(SESSION_FILE)

        context = browser.new_context(accept_downloads=True, **context_args)
        page = context.new_page()

        # 🪄 Tell Playwright to wait up to 5 minutes (300,000 ms) for EVERYTHING
        page.set_default_timeout(300000)
        page.set_default_navigation_timeout(300000)

        # 1. Navigate to the Base URL to check authentication status
        logger.info(f"Navigating to Base URL: {BASE_URL}")
        page.goto(BASE_URL, wait_until="domcontentloaded")

        # 2. Check if we are on the login page
        if page.locator("#user_username").is_visible():
            logger.info("Login page detected. Initiating Login sequence...")
            try:
                _perform_login(page, context, logger)
            except Exception as exc:
                logger.error(
                    f"❌ Login failed (or timed out). The dashboard was not reached: {exc}"
                )
                _save_debug_snapshot(page, "login-failed", logger)
                _mark_login_request("failed", str(exc))
                logger.error(
                    "Engaging Kill-Switch: Creating lock file to prevent further attempts."
                )
                LOGIN_LOCK_FILE.touch()
                browser.close()
                raise RuntimeError("Login failed. Pipeline completely halted.")

        else:
            logger.info("Valid session detected. Bypassing login.")

        # 3. Wait 2 seconds, then navigate to Target URL
        target_url = _get_target_url()
        logger.info(
            f"Waiting 2 seconds before navigating to {REPORT_SOURCE} Target URL..."
        )
        page.wait_for_timeout(2000)

        logger.info(f"Navigating to Target URL: {target_url}")
        page.goto(target_url, wait_until="networkidle")

        # 4. Wait another 2 seconds before downloading
        logger.info("Waiting 2 seconds before clicking download...")
        page.wait_for_timeout(2000)

        # 5. Download the CSV File
        logger.info("Locating the 'Filter & Download CSV' button by exact ID...")
        download_button = page.locator("#btn-export-csv")
        download_button.wait_for(state="visible", timeout=60000)
        captured_report_responses: list[tuple[str, bytes]] = []

        def capture_report_response(response):
            if not _response_looks_like_report(response):
                return

            try:
                body = response.body()
            except Exception as exc:
                logger.warning(
                    f"Could not read report response body from {response.url}: {exc}"
                )
                return

            if body:
                captured_report_responses.append((response.url, body))
                logger.info(
                    f"Captured report HTTP response body from {response.url} "
                    f"({len(body)} bytes)"
                )

        page.on("response", capture_report_response)

        downloaded_file_path = None
        for attempt in range(1, DOWNLOAD_RETRIES + 1):
            # Wait up to 60 seconds for the database to generate the CSV report.
            with page.expect_download(timeout=60000) as download_info:
                download_button.click()

            download = download_info.value

            # 6. Save the file
            downloaded_file_path = WATCH_DIR / _safe_download_filename(
                download.suggested_filename
            )
            file_size = _save_download(download, downloaded_file_path, logger)

            if file_size <= 0:
                file_size = _copy_latest_browser_download(downloaded_file_path, logger)

            if file_size <= 0 and captured_report_responses:
                response_url, response_body = captured_report_responses[-1]
                downloaded_file_path.write_bytes(response_body)
                file_size = downloaded_file_path.stat().st_size
                logger.info(
                    f"Recovered report from captured HTTP response: {response_url} "
                    f"({file_size} bytes)"
                )

            if file_size > 0:
                logger.info(
                    f"File successfully downloaded to: {downloaded_file_path} "
                    f"({file_size} bytes)"
                )
                break

            logger.warning(
                "Downloaded file was empty on attempt "
                f"{attempt}/{DOWNLOAD_RETRIES}: {downloaded_file_path}"
            )
            if attempt < DOWNLOAD_RETRIES:
                downloaded_file_path.unlink(missing_ok=True)
                logger.info(
                    "Waiting "
                    f"{DOWNLOAD_RETRY_DELAY_MS / 1000:.0f} seconds before retrying "
                    "the download..."
                )
                page.wait_for_timeout(DOWNLOAD_RETRY_DELAY_MS)
        else:
            _save_debug_snapshot(page, "empty-report-download", logger)
            raise RuntimeError(
                "The portal returned an empty report file after "
                f"{DOWNLOAD_RETRIES} download attempts. The last zero-byte file was "
                f"left at: {downloaded_file_path}"
            )

        # Record the successful execution timestamp
        record_scrape_time()

        browser.close()

        return downloaded_file_path

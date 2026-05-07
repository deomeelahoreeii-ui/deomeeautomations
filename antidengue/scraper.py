import datetime
import json
import os
import re
import shutil
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from prefect import get_run_logger, task

# ==========================================
# 1. CONFIGURATION & PATHS
# ==========================================
BASE_DIR = Path(__file__).parent.resolve()
load_dotenv(BASE_DIR / ".env")
WATCH_DIR = BASE_DIR / "drop-raw-files"
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


# ==========================================
# 2. CAPTCHA SOLVER & HELPERS
# ==========================================
def record_scrape_time():
    """Maintains a persistent record of the last successful scrape."""
    timestamp = datetime.datetime.now().isoformat()
    with open(LAST_RUN_FILE, "w") as f:
        json.dump({"last_successful_scrape": timestamp}, f, indent=4)
    return timestamp


def solve_math_captcha(page):
    """Reads the math problem from the DOM, solves it, and inputs the answer."""
    logger = get_run_logger()

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
            logger.info(f"Copied report from Playwright temporary file: {temporary_path}")
            return destination.stat().st_size
    except Exception as exc:
        logger.warning(f"Could not inspect Playwright temporary download file: {exc}")

    logger.warning(f"Playwright saved a zero-byte download: {_download_error_details(download)}")
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


# ==========================================
# 3. PREFECT TASK: SCRAPER
# ==========================================
# Note: retries=0 here to ensure Prefect doesn't immediately try again on failure
@task(name="Extract: Scrape Data Portal", retries=0)
def scrape_portal_data() -> Path:
    logger = get_run_logger()
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

            page.locator("#user_username").fill(PORTAL_USER)
            page.locator("#user_password").fill(PORTAL_PASS)
            solve_math_captcha(page)

            # MANUAL INTERVENTION PAUSE
            logger.info("======================================================")
            logger.info("⚠️ SCRIPT PAUSED FOR MANUAL VERIFICATION")
            logger.info("Please review the credentials and CAPTCHA in the browser.")
            logger.info("If everything is correct, manually click 'Sign In'.")
            logger.info("You have 90 seconds to do this...")
            logger.info("======================================================")

            try:
                # ROBUST LOGIN VERIFICATION LOOP
                # Instead of relying on URLs, we physically monitor the login form
                login_success = False
                for _ in range(90):
                    # Check if the username field has disappeared from the screen
                    if not page.locator("#user_username").is_visible():
                        # Wait 2 seconds to ensure it's not just momentarily hidden during a failed page reload
                        page.wait_for_timeout(2000)

                        # If it's STILL hidden, we have successfully left the login page
                        if not page.locator("#user_username").is_visible():
                            login_success = True
                            break

                    # Pause for 1 second before checking again
                    page.wait_for_timeout(1000)

                if not login_success:
                    raise PlaywrightTimeoutError(
                        "Login timeout: The login form never disappeared."
                    )

                logger.info("✅ Login successful! Saving session state...")
                context.storage_state(path=str(SESSION_FILE))

            except PlaywrightTimeoutError:
                # This triggers if 90s pass, or if you click sign in and an error keeps you on the sign_in page
                logger.error(
                    "❌ Login failed (or timed out). The dashboard was not reached."
                )
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
        logger.info(f"Waiting 2 seconds before navigating to {REPORT_SOURCE} Target URL...")
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
                logger.warning(f"Could not read report response body from {response.url}: {exc}")
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
            raise RuntimeError(
                "The portal returned an empty report file after "
                f"{DOWNLOAD_RETRIES} download attempts. The last zero-byte file was "
                f"left at: {downloaded_file_path}"
            )

        # Record the successful execution timestamp
        record_scrape_time()

        browser.close()

        return downloaded_file_path

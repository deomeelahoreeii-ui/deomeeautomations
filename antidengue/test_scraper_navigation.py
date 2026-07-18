import logging

import pytest
from playwright.sync_api import Error as PlaywrightError

import scraper


class FakePage:
    def __init__(self, failures: list[Exception]):
        self.failures = list(failures)
        self.goto_calls: list[tuple[str, str]] = []
        self.waits: list[int] = []

    def goto(self, url: str, *, wait_until: str):
        self.goto_calls.append((url, wait_until))
        if self.failures:
            raise self.failures.pop(0)
        return "response"

    def wait_for_timeout(self, milliseconds: int):
        self.waits.append(milliseconds)


def test_navigation_retries_dns_failure_three_times(monkeypatch):
    page = FakePage(
        [PlaywrightError("net::ERR_NAME_NOT_RESOLVED") for _ in range(3)]
    )
    monkeypatch.setattr(scraper, "PORTAL_NAVIGATION_RETRIES", 3)
    monkeypatch.setattr(scraper, "PORTAL_NAVIGATION_RETRY_DELAY_MS", 2000)

    response = scraper._navigate_with_retry(
        page,
        "https://dashboard-tracking.punjab.gov.pk/",
        wait_until="domcontentloaded",
        logger=logging.getLogger("test"),
    )

    assert response == "response"
    assert len(page.goto_calls) == 4
    assert page.waits == [2000, 2000, 2000]


def test_navigation_raises_after_retry_budget(monkeypatch):
    page = FakePage(
        [PlaywrightError("net::ERR_NAME_NOT_RESOLVED") for _ in range(4)]
    )
    monkeypatch.setattr(scraper, "PORTAL_NAVIGATION_RETRIES", 3)
    monkeypatch.setattr(scraper, "PORTAL_NAVIGATION_RETRY_DELAY_MS", 1000)

    with pytest.raises(PlaywrightError, match="ERR_NAME_NOT_RESOLVED"):
        scraper._navigate_with_retry(
            page,
            "https://dashboard-tracking.punjab.gov.pk/",
            wait_until="domcontentloaded",
            logger=logging.getLogger("test"),
        )

    assert len(page.goto_calls) == 4
    assert page.waits == [1000, 1000, 1000]


def test_navigation_does_not_retry_non_transient_error(monkeypatch):
    page = FakePage([PlaywrightError("net::ERR_CERT_AUTHORITY_INVALID")])
    monkeypatch.setattr(scraper, "PORTAL_NAVIGATION_RETRIES", 3)

    with pytest.raises(PlaywrightError, match="ERR_CERT_AUTHORITY_INVALID"):
        scraper._navigate_with_retry(
            page,
            "https://dashboard-tracking.punjab.gov.pk/",
            wait_until="domcontentloaded",
            logger=logging.getLogger("test"),
        )

    assert len(page.goto_calls) == 1
    assert page.waits == []

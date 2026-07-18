import datetime as dt
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from portal_reports import (
    HOTSPOT_DISTANCE,
    SIMPLE_ACTIVITY_LIST,
    PortalReportBundle,
    PortalReportCapture,
    PortalReportWindow,
)


def test_scheduled_cutoff_freezes_local_day_window() -> None:
    scheduled_utc = dt.datetime(2026, 7, 18, 3, 30, tzinfo=dt.UTC)

    window = PortalReportWindow.for_cutoff(scheduled_utc)

    assert window.as_dict() == {
        "timezone": "Asia/Karachi",
        "start": "2026-07-18T00:00:00+05:00",
        "end": "2026-07-18T08:30:00+05:00",
        "datefrom": "2026-07-18T00:00",
        "dateto": "2026-07-18T08:30",
    }


def test_hotspot_export_url_contains_cumulative_window_and_district() -> None:
    window = PortalReportWindow.for_cutoff(
        dt.datetime(2026, 7, 18, 8, 30)
    )

    url = HOTSPOT_DISTANCE.export_url(
        "https://dashboard-tracking.punjab.gov.pk/",
        window,
        district_id="18",
    )
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)

    assert parsed.path == "/reports/activities/hotspot_distance_report"
    assert query["district_id"] == ["18"]
    assert query["datefrom"] == ["2026-07-18T00:00"]
    assert query["dateto"] == ["2026-07-18T08:30"]
    assert query["format"] == ["xls"]
    assert query["pagination"] == ["No"]


def test_simple_activity_export_url_contains_frozen_window() -> None:
    window = PortalReportWindow.for_cutoff(dt.datetime(2026, 7, 18, 17, 11))
    parsed = urlparse(SIMPLE_ACTIVITY_LIST.export_url(
        "https://dashboard-tracking.punjab.gov.pk/", window, district_id="18"
    ))
    query = parse_qs(parsed.query, keep_blank_values=True)
    assert parsed.path == "/activities/simples/line_list"
    assert query["period"] == ["simple_activities"]
    assert query["datefrom"] == ["2026-07-18T00:00"]
    assert query["dateto"] == ["2026-07-18T17:11"]
    assert query["format"] == ["xls"]


def test_report_bundle_preserves_independent_report_statuses(tmp_path: Path) -> None:
    window = PortalReportWindow.for_cutoff(
        dt.datetime(2026, 7, 18, 8, 30)
    )
    hotspot_path = tmp_path / HOTSPOT_DISTANCE.filename(window)
    hotspot_path.write_bytes(b"xls-content")
    bundle = PortalReportBundle(
        window=window,
        captures=[
            PortalReportCapture.completed(
                HOTSPOT_DISTANCE,
                requested_url="https://example.test/export",
                path=hotspot_path,
                content_type="application/vnd.ms-excel",
            )
        ],
    )

    report = bundle.as_dict()["reports"][0]
    assert report["report_key"] == "hotspot_distance"
    assert report["status"] == "completed"
    assert report["size_bytes"] == len(b"xls-content")
    assert len(report["sha256"]) == 64

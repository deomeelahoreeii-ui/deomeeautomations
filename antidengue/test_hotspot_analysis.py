from pathlib import Path
import json
import pandas as pd

from hotspot_analysis import analyze_hotspot_distance_report, write_hotspot_review_report


def _write_report(path: Path) -> None:
    path.write_text(
        """
        <table>
          <thead><tr>
            <th>ID</th><th>District</th><th>Town</th><th>Sub Department</th>
            <th>Tag</th><th>Hotspot Name</th><th>Hotspot(Lat,Long)</th>
            <th>Activity(Lat,Long)</th><th>Distance Difference (In meters)</th>
            <th>Submitted by</th><th>Activity Date/Time</th>
          </tr></thead>
          <tbody>
            <tr><td>1</td><td>Lahore</td><td>City</td><td>SED</td><td>Schools</td>
              <td>School A</td><td>31.1,74.1</td><td>31.2,74.2</td><td>25.5</td>
              <td>35210001.lahore.sed</td><td>on 07/18/2026 at 08:00AM</td></tr>
            <tr><td>2</td><td>Lahore</td><td>City</td><td>SED</td><td>Schools</td>
              <td>School B</td><td>31.1,74.1</td><td>31.5,74.5</td><td>850.0</td>
              <td>35210002.lahore.sed</td><td>on 07/18/2026 at 08:10AM</td></tr>
          </tbody>
        </table>
        """,
        encoding="utf-8",
    )


def test_analysis_validates_schema_without_calling_activity_fake(
    tmp_path: Path,
    monkeypatch,
) -> None:
    report = tmp_path / "hotspot.xls"
    _write_report(report)
    monkeypatch.delenv("HOTSPOT_DISTANCE_REVIEW_THRESHOLD_METERS", raising=False)

    result = analyze_hotspot_distance_report(report)

    assert result["schema_version"] == "hotspot_distance.v1"
    assert result["row_count"] == 2
    assert result["review_candidate_count"] is None
    assert result["classification"] == "threshold_not_configured"


def test_analysis_applies_configured_review_threshold(tmp_path: Path, monkeypatch) -> None:
    report = tmp_path / "hotspot.xls"
    _write_report(report)
    monkeypatch.setenv("HOTSPOT_DISTANCE_REVIEW_THRESHOLD_METERS", "500")

    result = analyze_hotspot_distance_report(report)

    assert result["review_threshold_meters"] == 500
    assert result["review_candidate_count"] == 1
    assert result["review_candidate_sample"][0]["activity_id"] == "2"


def test_analysis_applies_frozen_published_rule_version(tmp_path: Path, monkeypatch) -> None:
    report = tmp_path / "hotspot.xls"
    _write_report(report)
    snapshot = tmp_path / "runtime.json"
    snapshot.write_text(json.dumps({
        "schema_version": 1,
        "source": "automation-platform-postgresql",
        "activity_rules": [{
            "id": "rule-version-2",
            "rule_key": "distance-rule",
            "version": 2,
            "name": "Distant early activity",
            "classification": "review_required",
            "match_mode": "all",
            "distance_enabled": True,
            "distance_operator": "gte",
            "distance_threshold_meters": 50,
            "time_enabled": True,
            "time_operator": "between",
            "time_start": "08:00",
            "time_end": "08:11",
        }],
    }), encoding="utf-8")
    monkeypatch.setenv("ANTIDENGUE_RUNTIME_SNAPSHOT", str(snapshot))
    monkeypatch.delenv("HOTSPOT_DISTANCE_REVIEW_THRESHOLD_METERS", raising=False)

    result = analyze_hotspot_distance_report(report)

    assert result["classification"] == "review_required"
    assert result["review_candidate_count"] == 1
    assert result["rule_results"][0]["rule_version"] == 2
    assert result["rule_results"][0]["matching_activity_count"] == 1


def test_review_workbook_contains_routable_classified_rows(tmp_path: Path, monkeypatch) -> None:
    report = tmp_path / "hotspot.xls"
    _write_report(report)
    wing_id = "11111111-1111-1111-1111-111111111111"
    tehsil_id = "22222222-2222-2222-2222-222222222222"
    snapshot = tmp_path / "runtime.json"
    snapshot.write_text(json.dumps({
        "schema_version": 1,
        "source": "automation-platform-postgresql",
        "activity_rules": [{
            "id": "rule-version-1", "rule_key": "distance-rule", "version": 1,
            "name": "Distance review", "classification": "review_required",
            "match_mode": "all", "distance_enabled": True,
            "distance_operator": "gte", "distance_threshold_meters": 50,
            "time_enabled": False,
        }],
        "master_schools": [{
            "School EMIS": "35210002", "School Name": "School B",
            "Tehsil": "CITY", "Markaz": "CITY MARKAZ",
            "_wing_id": wing_id, "_tehsil_id": tehsil_id, "_markaz_id": "",
        }],
    }), encoding="utf-8")
    monkeypatch.setenv("ANTIDENGUE_RUNTIME_SNAPSHOT", str(snapshot))
    destination = tmp_path / "Hotspot Distance Review.xlsx"

    result = write_hotspot_review_report(report, destination)

    assert result["candidate_count"] == 1
    frame = pd.read_excel(destination)
    assert frame.loc[0, "School EMIS"] == 35210002
    assert frame.loc[0, "School Name"] == "School B"
    assert frame.loc[0, "Wing ID"] == wing_id
    assert frame.loc[0, "Tehsil ID"] == tehsil_id


def test_later_correct_hotspot_activity_closes_one_issue_and_keeps_audit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    report = tmp_path / "hotspot-correction.xls"
    report.write_text(
        """
        <table><thead><tr>
          <th>ID</th><th>District</th><th>Town</th><th>Sub Department</th>
          <th>Tag</th><th>Hotspot Name</th><th>Hotspot(Lat,Long)</th>
          <th>Activity(Lat,Long)</th><th>Distance Difference (In meters)</th>
          <th>Submitted by</th><th>Activity Date/Time</th>
        </tr></thead><tbody>
          <tr><td>1</td><td>Lahore</td><td>City</td><td>SED</td><td>Schools</td><td>School A</td><td>31.1,74.1</td><td>31.2,74.2</td><td>150</td><td>35210001.user</td><td>on 07/20/2026 at 08:00AM</td></tr>
          <tr><td>2</td><td>Lahore</td><td>City</td><td>SED</td><td>Schools</td><td>School A</td><td>31.1,74.1</td><td>31.1,74.1</td><td>25</td><td>35210001.user</td><td>on 07/20/2026 at 08:10AM</td></tr>
        </tbody></table>
        """,
        encoding="utf-8",
    )
    snapshot = tmp_path / "runtime.json"
    snapshot.write_text(json.dumps({
        "schema_version": 1,
        "source": "automation-platform-postgresql",
        "activity_rules": [{
            "id": "r1", "rule_key": "distance", "version": 1,
            "name": "Distance", "classification": "review_required",
            "match_mode": "all", "distance_enabled": True,
            "distance_operator": "gte", "distance_threshold_meters": 100,
            "time_enabled": False,
        }],
        "master_schools": [{
            "School EMIS": "35210001", "School Name": "School A",
            "Tehsil": "CITY", "Markaz": "M1", "_wing_id": "w1",
            "_tehsil_id": "t1", "_markaz_id": "m1",
        }],
    }), encoding="utf-8")
    monkeypatch.setenv("ANTIDENGUE_RUNTIME_SNAPSHOT", str(snapshot))
    destination = tmp_path / "Hotspot Distance Review.xlsx"

    result = write_hotspot_review_report(report, destination)

    assert result["historical_candidate_count"] == 1
    assert result["corrected_candidate_count"] == 1
    assert result["candidate_count"] == 0
    book = pd.ExcelFile(destination)
    assert book.sheet_names == [
        "Review Required", "Corrected Issues", "Valid Unused Activities", "All Activity Audit"
    ]
    corrected = pd.read_excel(destination, sheet_name="Corrected Issues")
    assert corrected.loc[0, "ID"] == 1
    assert corrected.loc[0, "Correction Activity ID"] == 2
    audit = pd.read_excel(destination, sheet_name="All Activity Audit")
    assert set(audit["Compliance Status"]) == {"Corrected issue", "Corrective activity"}


def test_distance_only_pass_does_not_clear_another_active_hotspot_rule(
    tmp_path: Path,
    monkeypatch,
) -> None:
    report = tmp_path / "hotspot-combined-rule.xls"
    report.write_text(
        """
        <table><thead><tr>
          <th>ID</th><th>District</th><th>Town</th><th>Sub Department</th>
          <th>Tag</th><th>Hotspot Name</th><th>Hotspot(Lat,Long)</th>
          <th>Activity(Lat,Long)</th><th>Distance Difference (In meters)</th>
          <th>Submitted by</th><th>Activity Date/Time</th>
        </tr></thead><tbody>
          <tr><td>1</td><td>Lahore</td><td>City</td><td>SED</td><td>Schools</td><td>School A</td><td>31.1,74.1</td><td>31.2,74.2</td><td>150</td><td>35210001.user</td><td>on 07/20/2026 at 08:00AM</td></tr>
          <tr><td>2</td><td>Lahore</td><td>City</td><td>SED</td><td>Schools</td><td>School A</td><td>31.1,74.1</td><td>31.1,74.1</td><td>25</td><td>35210001.user</td><td>on 07/20/2026 at 08:10AM</td></tr>
        </tbody></table>
        """,
        encoding="utf-8",
    )
    snapshot = tmp_path / "runtime.json"
    snapshot.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "automation-platform-postgresql",
                "activity_rules": [
                    {
                        "id": "r1",
                        "rule_key": "distance-or-time",
                        "version": 1,
                        "name": "Distance or restricted submission time",
                        "classification": "review_required",
                        "match_mode": "any",
                        "distance_enabled": True,
                        "distance_operator": "gte",
                        "distance_threshold_meters": 100,
                        "time_enabled": True,
                        "time_operator": "between",
                        "time_start": "08:00",
                        "time_end": "09:00",
                    }
                ],
                "master_schools": [
                    {
                        "School EMIS": "35210001",
                        "School Name": "School A",
                        "Tehsil": "CITY",
                        "Markaz": "M1",
                        "_wing_id": "w1",
                        "_tehsil_id": "t1",
                        "_markaz_id": "m1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("ANTIDENGUE_RUNTIME_SNAPSHOT", str(snapshot))
    destination = tmp_path / "Hotspot Distance Review.xlsx"

    result = write_hotspot_review_report(report, destination)

    assert result["historical_candidate_count"] == 2
    assert result["corrected_candidate_count"] == 0
    assert result["candidate_count"] == 2

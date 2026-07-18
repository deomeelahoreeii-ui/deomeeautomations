from pathlib import Path
import json
import pandas as pd

from simple_activity_analysis import analyze_simple_activity_report, write_simple_activity_review_report


def _report(path: Path) -> None:
    path.write_text("""<table><thead><tr><th>ID</th><th>Submitted by</th><th>Picture Time Difference(Sec)</th><th>Latitude</th><th>Longitude</th></tr></thead><tbody>
    <tr><td>1</td><td>35210001.user</td><td>299</td><td>31.1</td><td>74.1</td></tr>
    <tr><td>2</td><td>35210002.user</td><td>300</td><td>31.2</td><td>74.2</td></tr>
    <tr><td>3</td><td>35210003.user</td><td>-1</td><td>31.3</td><td>74.3</td></tr>
    <tr><td>4</td><td>35210004.user</td><td>bad</td><td>31.4</td><td>74.4</td></tr>
    </tbody></table>""", encoding="utf-8")


def test_default_five_minute_boundary_and_invalid_values(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "simple.xls"; _report(path)
    monkeypatch.delenv("ANTIDENGUE_RUNTIME_SNAPSHOT", raising=False)
    result = analyze_simple_activity_report(path)
    assert result["review_candidate_count"] == 1
    assert result["invalid_time_difference_count"] == 2


def test_published_rule_and_school_routing_are_frozen(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "simple.xls"; _report(path)
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps({
        "schema_version": 1, "source": "automation-platform-postgresql",
        "simple_activity_rules": [{"id":"r1", "rule_key":"timing", "version":2, "name":"Five minutes", "operator":"lte", "minimum_seconds":300}],
        "master_schools": [{"School EMIS":"35210002", "School Name":"School B", "Tehsil":"CITY", "Markaz":"M1", "_wing_id":"w1", "_tehsil_id":"t1", "_markaz_id":"m1"}],
    }), encoding="utf-8")
    monkeypatch.setenv("ANTIDENGUE_RUNTIME_SNAPSHOT", str(snapshot))
    destination = tmp_path / "Simple Activity Timing Review.xlsx"
    result = write_simple_activity_review_report(path, destination)
    assert result["candidate_count"] == 2
    assert result["rule_results"][0]["rule_version"] == 2
    frame = pd.read_excel(destination)
    routed = frame[frame["School EMIS"] == 35210002].iloc[0]
    assert routed["School Name"] == "School B"
    assert routed["Wing ID"] == "w1"

from pathlib import Path

from automation_core.command_runner import artifact_kind


def test_artifact_kind_classifies_pipeline_outputs() -> None:
    output_dir = Path("/tmp/output")

    assert artifact_kind(
        output_dir / "run/group-route-excels/01 - Heads.xlsx",
        output_dir=output_dir,
    ) == "dispatch_draft"
    assert artifact_kind(output_dir / "run/run_summary.json", output_dir=output_dir) == "manifest"
    assert artifact_kind(output_dir / "run/Officer Delivery Audit.xlsx", output_dir=output_dir) == "audit"
    assert artifact_kind(output_dir / "run/Activity Evidence.xlsx", output_dir=output_dir) == "evidence"
    assert artifact_kind(output_dir / "run/Anti-Dengue App Dormant Users.xlsx", output_dir=output_dir) == "report"

import asyncio
import json
import sqlite3
from pathlib import Path

import pandas as pd

import main
from main import (
    _build_activity_evidence,
    _filter_dormant_report_rows,
    _persist_run_summary_to_pocketbase,
    _quality_gate,
    _should_enforce_officer_delivery_quality,
    _read_master_dataframe_from_pocketbase,
    _read_officers_dataframe_from_pocketbase,
    _read_recipients_dataframe_from_pocketbase,
    build_dynamic_officer_payloads_with_audit,
)


def test_filter_uses_total_activities_when_tpv_column_is_missing():
    raw_df = pd.DataFrame(
        {
            "Username": [
                "35210001.lahore.sed(Active Head)",
                "35210002.lahore.sed(Inactive Head)",
                "35210003.lahore.sed(Another Active Head)",
            ],
            "Simple Activities": ["2", "0", "0"],
            "Patient Activities": ["0", "0", "1"],
            "Vector Surveillance Activities": ["0", "0", "0"],
            "Larvae Case Response": ["0", "0", "0"],
            "Total Activities": ["2", "0", "1"],
        }
    )

    filtered_df, diagnostics = _filter_dormant_report_rows(raw_df)

    assert filtered_df["Username"].tolist() == ["35210002.lahore.sed(Inactive Head)"]
    assert diagnostics["filter_applied"] is True
    assert diagnostics["filter_strategy"] == "Total Activities"
    assert diagnostics["missing_activity_columns"] == ["TPV Activities"]
    assert diagnostics["invalid_total_activity_rows"] == 0
    assert diagnostics["dormant_activity_conflict_rows"] == 0


def test_filter_records_total_and_detail_activity_conflicts():
    raw_df = pd.DataFrame(
        {
            "Username": [
                "35210001.lahore.sed(Contradictory)",
                "35210002.lahore.sed(Unknown Total)",
            ],
            "Simple Activities": ["1", "0"],
            "Patient Activities": ["0", "0"],
            "Vector Surveillance Activities": ["0", "0"],
            "Larvae Case Response": ["0", "0"],
            "Total Activities": ["0", "not-a-number"],
        }
    )

    _, diagnostics = _filter_dormant_report_rows(raw_df)

    assert diagnostics["invalid_total_activity_rows"] == 1
    assert diagnostics["dormant_activity_conflict_rows"] == 1


def test_filter_falls_back_to_available_detail_columns_when_total_is_missing():
    raw_df = pd.DataFrame(
        {
            "Username": [
                "35210001.lahore.sed(Active Head)",
                "35210002.lahore.sed(Inactive Head)",
            ],
            "Simple Activities": ["1", "0"],
            "Patient Activities": ["0", "0"],
            "Vector Surveillance Activities": ["0", "0"],
            "Larvae Case Response": ["0", "0"],
        }
    )

    filtered_df, diagnostics = _filter_dormant_report_rows(raw_df)

    assert filtered_df["Username"].tolist() == ["35210002.lahore.sed(Inactive Head)"]
    assert diagnostics["filter_applied"] is True
    assert diagnostics["filter_strategy"] == "available_detail_activity_columns"
    assert diagnostics["missing_activity_columns"] == [
        "TPV Activities",
        "Total Activities",
    ]


def test_filter_treats_report_as_already_filtered_when_no_activity_columns_exist():
    raw_df = pd.DataFrame(
        {
            "Username": [
                "35210001.lahore.sed(Portal Filtered Head)",
                "35210002.lahore.sed(Portal Filtered Head)",
            ],
            "Department": [
                "School Education Department",
                "School Education Department",
            ],
        }
    )

    filtered_df, diagnostics = _filter_dormant_report_rows(raw_df)

    assert filtered_df["Username"].tolist() == raw_df["Username"].tolist()
    assert diagnostics["filter_applied"] is False
    assert diagnostics["filter_strategy"] == "already_filtered"


def test_activity_evidence_includes_raw_activity_values():
    final_df = pd.DataFrame(
        {
            "School EMIS": ["35210002"],
            "School Name": ["GPS TEST SCHOOL"],
            "Tehsil": ["CANTT"],
            "Markaz": ["TEST MARKAZ"],
        }
    )
    filtered_user_df = pd.DataFrame(
        {
            "Username": ["35210002.lahore.sed(Inactive Head)"],
            "Extracted_EMIS": ["35210002"],
            "Simple Activities": ["0"],
            "Patient Activities": ["0"],
            "Vector Surveillance Activities": ["0"],
            "Larvae Case Response": ["0"],
            "Total Activities": ["0"],
        }
    )

    evidence_df = _build_activity_evidence(final_df, filtered_user_df)

    assert evidence_df.loc[0, "School Name"] == "GPS TEST SCHOOL"
    assert evidence_df.loc[0, "Username"] == "35210002.lahore.sed(Inactive Head)"
    assert evidence_df.loc[0, "Total Activities"] == "0"


def test_quality_gate_blocks_unfiltered_report_without_activity_columns(
    tmp_path, monkeypatch
):
    master_path = tmp_path / "master.xlsx"
    pd.DataFrame({"School EMIS": ["1", "2"], "School Name": ["A", "B"]}).to_excel(
        master_path, index=False
    )
    monkeypatch.setattr(main, "MASTER_FILE", master_path)
    monkeypatch.setattr(main, "REPORT_SOURCE", "unfiltered")
    monkeypatch.setattr(main, "POCKETBASE_ENABLED", False)

    result = _quality_gate(
        final_df=pd.DataFrame({"School EMIS": ["1"], "School Name": ["A"]}),
        extraction_diagnostics={
            "filter_applied": False,
            "filter_strategy": "already_filtered",
            "available_activity_columns": [],
            "missing_activity_columns": [
                "Simple Activities",
                "Patient Activities",
                "Vector Surveillance Activities",
                "Larvae Case Response",
                "TPV Activities",
                "Total Activities",
            ],
        },
        unmatched_df=pd.DataFrame(),
        invalid_contacts_df=pd.DataFrame(),
    )

    assert result["passed"] is False
    assert any("no local dormant filter" in error for error in result["errors"])


def test_quality_gate_treats_mapping_as_warning_when_officer_delivery_is_disabled(
    tmp_path, monkeypatch
):
    master_path = tmp_path / "master.xlsx"
    pd.DataFrame({"School EMIS": ["1"], "School Name": ["A"]}).to_excel(
        master_path, index=False
    )
    monkeypatch.setattr(main, "MASTER_FILE", master_path)
    monkeypatch.setattr(main, "REPORT_SOURCE", "unfiltered")
    monkeypatch.setattr(main, "POCKETBASE_ENABLED", False)

    result = _quality_gate(
        final_df=pd.DataFrame({"School EMIS": ["1"], "School Name": ["A"]}),
        extraction_diagnostics={
            "filter_applied": True,
            "filter_strategy": "Total Activities",
            "available_activity_columns": ["Total Activities"],
            "missing_activity_columns": [],
        },
        unmatched_df=pd.DataFrame({"School EMIS": ["1"]}),
        invalid_contacts_df=pd.DataFrame(),
        enforce_officer_delivery_quality=False,
    )

    assert result["passed"] is True
    assert result["errors"] == []
    assert result["officer_delivery_quality_enforced"] is False
    assert not any("Officer mapping" in warning for warning in result["warnings"])
    assert result["officer_mapping_check_applicable"] is False


def test_quality_gate_treats_missing_tpv_as_optional_when_total_is_available(
    tmp_path, monkeypatch
):
    master_path = tmp_path / "master.xlsx"
    pd.DataFrame({"School EMIS": ["1", "2"], "School Name": ["A", "B"]}).to_excel(
        master_path, index=False
    )
    monkeypatch.setattr(main, "MASTER_FILE", master_path)
    monkeypatch.setattr(main, "REPORT_SOURCE", "unfiltered")
    monkeypatch.setattr(main, "POCKETBASE_ENABLED", False)

    result = _quality_gate(
        final_df=pd.DataFrame({"School EMIS": ["1"], "School Name": ["A"]}),
        extraction_diagnostics={
            "filter_applied": True,
            "filter_strategy": "Total Activities",
            "available_activity_columns": [
                "Simple Activities",
                "Patient Activities",
                "Vector Surveillance Activities",
                "Larvae Case Response",
                "Total Activities",
            ],
            "missing_activity_columns": ["TPV Activities"],
            "exported_emis_values": ["1", "2"],
            "dormant_emis_values": ["1"],
        },
        unmatched_df=pd.DataFrame(),
        invalid_contacts_df=pd.DataFrame(),
        enforce_officer_delivery_quality=False,
        observed_at=main.datetime.datetime(2026, 7, 18, 9, 0),
    )

    assert result["passed"] is True
    assert result["activity_schema_mode"] == "total_activities"
    assert result["optional_missing_activity_columns"] == ["TPV Activities"]
    assert not any("schema" in warning.lower() for warning in result["warnings"])


def test_quality_gate_reconciles_export_and_defers_high_dormancy_before_cutoff(
    tmp_path, monkeypatch
):
    master_path = tmp_path / "master.xlsx"
    pd.DataFrame(
        {"School EMIS": ["1", "2", "3", "4"], "School Name": list("ABCD")}
    ).to_excel(master_path, index=False)
    monkeypatch.setattr(main, "MASTER_FILE", master_path)
    monkeypatch.setattr(main, "REPORT_SOURCE", "unfiltered")
    monkeypatch.setattr(main, "POCKETBASE_ENABLED", False)
    monkeypatch.setattr(main, "QUALITY_MIN_EXPORT_COVERAGE", 0.70)

    result = _quality_gate(
        final_df=pd.DataFrame(
            {"School EMIS": ["1", "2", "3"], "School Name": list("ABC")}
        ),
        extraction_diagnostics={
            "filter_applied": True,
            "filter_strategy": "Total Activities",
            "available_activity_columns": ["Total Activities"],
            "missing_activity_columns": list(main.DETAILED_ACTIVITY_COLUMNS),
            "exported_emis_values": ["1", "2", "3", "999"],
            "dormant_emis_values": ["1", "2", "3", "999"],
        },
        unmatched_df=pd.DataFrame(),
        invalid_contacts_df=pd.DataFrame(),
        enforce_officer_delivery_quality=False,
        observed_at=main.datetime.datetime(2026, 7, 18, 3, 16),
    )

    assert result["reconciled_export_school_count"] == 3
    assert result["export_coverage_ratio"] == 0.75
    assert result["dormancy_ratio"] == 1.0
    assert result["unknown_export_emis_sample"] == ["999"]
    assert result["master_missing_from_export_emis_sample"] == ["4"]
    assert result["dormancy_rate_check_applicable"] is False
    assert not any(issue["code"] == "high_dormancy_rate" for issue in result["issues"])


def test_quality_gate_blocks_partial_detail_schema_without_total(
    tmp_path, monkeypatch
):
    master_path = tmp_path / "master.xlsx"
    pd.DataFrame({"School EMIS": ["1"], "School Name": ["A"]}).to_excel(
        master_path, index=False
    )
    monkeypatch.setattr(main, "MASTER_FILE", master_path)
    monkeypatch.setattr(main, "REPORT_SOURCE", "unfiltered")
    monkeypatch.setattr(main, "POCKETBASE_ENABLED", False)

    result = _quality_gate(
        final_df=pd.DataFrame({"School EMIS": ["1"], "School Name": ["A"]}),
        extraction_diagnostics={
            "filter_applied": True,
            "filter_strategy": "available_detail_activity_columns",
            "available_activity_columns": ["Simple Activities"],
            "missing_activity_columns": [
                "Patient Activities",
                "Vector Surveillance Activities",
                "Larvae Case Response",
                "TPV Activities",
                "Total Activities",
            ],
            "exported_emis_values": ["1"],
            "dormant_emis_values": ["1"],
        },
        unmatched_df=pd.DataFrame(),
        invalid_contacts_df=pd.DataFrame(),
        enforce_officer_delivery_quality=False,
        observed_at=main.datetime.datetime(2026, 7, 18, 9, 0),
    )

    assert result["passed"] is False
    assert result["activity_schema_mode"] == "partial_detail_columns"
    assert any(
        issue["code"] == "unreliable_activity_schema"
        for issue in result["issues"]
    )


def test_quality_gate_blocks_contradictory_total_activity(tmp_path, monkeypatch):
    master_path = tmp_path / "master.xlsx"
    pd.DataFrame({"School EMIS": ["1"], "School Name": ["A"]}).to_excel(
        master_path, index=False
    )
    monkeypatch.setattr(main, "MASTER_FILE", master_path)
    monkeypatch.setattr(main, "REPORT_SOURCE", "unfiltered")
    monkeypatch.setattr(main, "POCKETBASE_ENABLED", False)

    result = _quality_gate(
        final_df=pd.DataFrame({"School EMIS": ["1"], "School Name": ["A"]}),
        extraction_diagnostics={
            "filter_applied": True,
            "filter_strategy": "Total Activities",
            "available_activity_columns": [
                "Simple Activities",
                "Total Activities",
            ],
            "missing_activity_columns": [
                "Patient Activities",
                "Vector Surveillance Activities",
                "Larvae Case Response",
                "TPV Activities",
            ],
            "dormant_activity_conflict_rows": 1,
            "exported_emis_values": ["1"],
            "dormant_emis_values": ["1"],
        },
        unmatched_df=pd.DataFrame(),
        invalid_contacts_df=pd.DataFrame(),
        enforce_officer_delivery_quality=False,
        observed_at=main.datetime.datetime(2026, 7, 18, 3, 0),
    )

    assert result["passed"] is False
    assert any(
        issue["code"] == "contradictory_activity_totals"
        for issue in result["issues"]
    )


def test_officer_delivery_quality_is_only_enforced_for_live_direct_delivery():
    enabled = {
        "allow_individual": True,
        "manual_only": False,
        "require_preview": False,
    }
    disabled = {**enabled, "allow_individual": False}

    assert _should_enforce_officer_delivery_quality(
        dry_run=False, dispatch_settings=enabled
    )
    assert not _should_enforce_officer_delivery_quality(
        dry_run=True, dispatch_settings=enabled
    )
    assert not _should_enforce_officer_delivery_quality(
        dry_run=False, dispatch_settings=disabled
    )
    assert not _should_enforce_officer_delivery_quality(
        dry_run=False, dispatch_settings={**enabled, "manual_only": True}
    )
    assert not _should_enforce_officer_delivery_quality(
        dry_run=False, dispatch_settings={**enabled, "require_preview": True}
    )


def test_pocketbase_master_reader_builds_report_master_dataframe(tmp_path, monkeypatch):
    db_path = tmp_path / "pb.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            create table schools (
              id text primary key,
              emis text,
              name text,
              school_type text,
              deos_wise text,
              school_level text,
              district_ref text,
              wing_ref text,
              tehsil_ref text,
              markaz_ref text,
              active integer
            );
            create table districts (id text primary key, name text);
            create table wings (id text primary key, name text);
            create table tehsils (id text primary key, name text);
            create table markazes (id text primary key, name text);
            insert into districts values ('d1', 'LAHORE');
            insert into wings values ('w1', 'DEO MEE');
            insert into tehsils values ('t1', 'SHALIMAR');
            insert into markazes values ('m1', 'BAGHBANPURA - MALE');
            insert into schools values ('s1', '35210258', 'GPS NAWAB', 'Male', 'M-EE', 'Primary', 'd1', 'w1', 't1', 'm1', 1);
            """
        )
    monkeypatch.setattr(main, "POCKETBASE_DB_PATH", db_path)

    master_df = _read_master_dataframe_from_pocketbase()

    assert master_df.columns.tolist() == main.MASTER_REPORT_COLUMNS
    assert master_df.loc[0, "School EMIS"] == "35210258"
    assert master_df.loc[0, "District"] == "LAHORE"
    assert master_df.loc[0, "DEOs Wise"] == "M-EE"


def test_pocketbase_fixed_recipient_reader_returns_runtime_columns(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "pb.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            create table whatsapp_recipients (
              id text primary key,
              enabled integer,
              name text,
              type text,
              target text,
              text text,
              image_path text,
              excel_path text,
              excel_filename text,
              attachment_text_mode text,
              delay_ms integer
            );
            insert into whatsapp_recipients values (
              'r1', 1, 'Anti Dengue Group', 'group', '120363319976862432@g.us',
              '', '', '', '', 'caption', 1500
            );
            """
        )
    monkeypatch.setattr(main, "POCKETBASE_DB_PATH", db_path)

    recipients_df = _read_recipients_dataframe_from_pocketbase()

    assert recipients_df.loc[0, "target"] == "120363319976862432@g.us"
    assert recipients_df.loc[0, "attachment_text_mode"] == "caption"


def test_dynamic_officer_payloads_include_officer_delivery_audit(tmp_path, monkeypatch):
    final_df = pd.DataFrame(
        {
            "School EMIS": ["35210258", "35210287"],
            "School Name": [
                "GPS NAWAB BAGHBAN PURA LAHORE",
                "GPS CUSTOM COLONY WAHGA",
            ],
        }
    )
    officers_df = pd.DataFrame(
        {
            "emis_normalized": ["35210258", "35210287"],
            "ddeo_name": [
                "Ghulam Mustafa Ahmadani",
                "Ghulam Mustafa Ahmadani",
            ],
            "ddeo_cell_number": ["3005363198", "3005363198"],
            "aeo_name": ["Bilal Adeel Kazmi", "Shumaila Kaleem"],
            "aeo_cell_number": ["3331461236", "3338395682"],
            "tehsil": ["SHALIMAR", "SHALIMAR"],
            "markaz": ["BAGHBANPURA - MALE", "GHARHI SHAHU-MALE"],
        }
    )
    excel_path = tmp_path / "report.xlsx"
    pd.DataFrame({"ok": [1]}).to_excel(excel_path, index=False)

    monkeypatch.setattr(main, "_read_officers_dataframe", lambda: officers_df)

    payloads, audit_rows = build_dynamic_officer_payloads_with_audit(
        final_df,
        excel_path,
        image_path=None,
        title_text="Anti-Dengue Dormant Users",
    )

    assert len(payloads) == 3
    ddeo_audit = next(
        row for row in audit_rows if row["Mobile Number"] == "923005363198"
    )
    assert ddeo_audit["Officer Name"] == "Ghulam Mustafa Ahmadani"
    assert ddeo_audit["Roles"] == "DDEO"
    assert ddeo_audit["School Count"] == 2
    assert ddeo_audit["Target"] == "923005363198@s.whatsapp.net"
    assert "GPS NAWAB BAGHBAN PURA LAHORE" in ddeo_audit["Schools"]
    assert "GPS CUSTOM COLONY WAHGA" in ddeo_audit["Schools"]


def test_pocketbase_officer_mapping_reader_pivots_assignments(tmp_path, monkeypatch):
    db_path = tmp_path / "pb.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            create table schools (
              id text primary key,
              emis text,
              name text,
              tehsil text,
              markaz text,
              active integer
            );
            create table officers (
              id text primary key,
              name text,
              role text,
              normalized_mobile text,
              active integer
            );
            create table school_assignments (
              id text primary key,
              school_emis text,
              officer_mobile text,
              officer_role text,
              active integer
            );
            insert into schools values ('s1', '35210258', 'GPS NAWAB', 'SHALIMAR', 'BAGHBANPURA - MALE', 1);
            insert into officers values ('o1', 'Ghulam Mustafa Ahmadani', 'DDEO', '923005363198', 1);
            insert into officers values ('o2', 'Bilal Adeel Kazmi', 'AEO', '923331461236', 1);
            insert into school_assignments values ('a1', '35210258', '923005363198', 'DDEO', 1);
            insert into school_assignments values ('a2', '35210258', '923331461236', 'AEO', 1);
            """
        )
    monkeypatch.setattr(main, "POCKETBASE_DB_PATH", db_path)

    officers_df = _read_officers_dataframe_from_pocketbase()

    assert len(officers_df) == 1
    row = officers_df.iloc[0]
    assert row["emis_normalized"] == "35210258"
    assert row["ddeo_name"] == "Ghulam Mustafa Ahmadani"
    assert row["ddeo_cell_number"] == "923005363198"
    assert row["aeo_name"] == "Bilal Adeel Kazmi"
    assert row["aeo_cell_number"] == "923331461236"


def test_pocketbase_officer_mapping_reader_uses_normalized_relations(tmp_path, monkeypatch):
    db_path = tmp_path / "pb.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            create table schools (
              id text primary key,
              emis text,
              name text,
              wing_ref text,
              tehsil_ref text,
              markaz_ref text,
              active integer
            );
            create table ddeo_jurisdictions (
              id text primary key,
              ddeo_ref text,
              wing_ref text,
              tehsil_ref text,
              active integer
            );
            create table aeo_jurisdictions (
              id text primary key,
              aeo_ref text,
              wing_ref text,
              tehsil_ref text,
              markaz_ref text,
              active integer
            );
            create table school_ddeo_overrides (
              id text primary key,
              school_ref text,
              ddeo_ref text,
              active integer
            );
            create table school_aeo_overrides (
              id text primary key,
              school_ref text,
              aeo_ref text,
              active integer
            );
            create table ddeo_officers (
              id text primary key,
              name text,
              normalized_mobile text,
              active integer
            );
            create table aeo_officers (
              id text primary key,
              name text,
              normalized_mobile text,
              active integer
            );
            create table tehsils (
              id text primary key,
              name text
            );
            create table markazes (
              id text primary key,
              name text
            );
            insert into tehsils values ('t1', 'SHALIMAR');
            insert into markazes values ('m1', 'BAGHBANPURA - MALE');
            insert into schools values ('s1', '35210258', 'GPS NAWAB', 'w1', 't1', 'm1', 1);
            insert into ddeo_officers values ('d1', 'Ghulam Mustafa Ahmadani', '923005363198', 1);
            insert into aeo_officers values ('a1', 'Bilal Adeel Kazmi', '923331461236', 1);
            insert into ddeo_jurisdictions values ('dj1', 'd1', 'w1', 't1', 1);
            insert into aeo_jurisdictions values ('aj1', 'a1', 'w1', 't1', 'm1', 1);
            """
        )
    monkeypatch.setattr(main, "POCKETBASE_DB_PATH", db_path)

    officers_df = _read_officers_dataframe_from_pocketbase()

    assert len(officers_df) == 1
    row = officers_df.iloc[0]
    assert row["emis_normalized"] == "35210258"
    assert row["tehsil"] == "SHALIMAR"
    assert row["markaz"] == "BAGHBANPURA - MALE"
    assert row["ddeo_name"] == "Ghulam Mustafa Ahmadani"
    assert row["ddeo_cell_number"] == "923005363198"
    assert row["aeo_name"] == "Bilal Adeel Kazmi"
    assert row["aeo_cell_number"] == "923331461236"


def test_pocketbase_run_summary_persistence_writes_history(tmp_path, monkeypatch):
    db_path = tmp_path / "pb.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            create table report_runs (
              id text primary key,
              started_at text,
              finished_at text,
              source text,
              status text,
              raw_file_name text,
              raw_file_sha256 text,
              summary text
            );
            create table dormant_records (
              id text primary key,
              run_key text,
              school_emis text,
              school_name text,
              tehsil text,
              markaz text,
              row_data text
            );
            create table delivery_events (
              id text primary key,
              run_key text,
              job_id text,
              target text,
              recipient_name text,
              role text,
              status text,
              cause text,
              payload text
            );
            create table data_quality_issues (
              id text primary key,
              run_key text,
              severity text,
              category text,
              message text,
              context text
            );
            create table run_stage_events (
              id text primary key,
              run_key text,
              stage text,
              status text,
              message text,
              payload text,
              created_at text
            );
            create table dispatch_plan_items (
              id text primary key,
              run_key text,
              job_id text,
              channel text,
              recipient_name text,
              target text,
              route_kind text,
              message_mode text,
              row_count integer,
              excel_path text,
              status text,
              cause text,
              payload text,
              created_at text,
              updated_at text
            );
            """
        )
    monkeypatch.setattr(main, "POCKETBASE_DB_PATH", db_path)
    monkeypatch.setattr(main, "POCKETBASE_ENABLED", True)
    dormant_df = pd.DataFrame(
        {
            "School EMIS": ["35210258"],
            "School Name": ["GPS NAWAB"],
            "Tehsil": ["SHALIMAR"],
            "Markaz": ["BAGHBANPURA - MALE"],
        }
    )
    summary = {
        "run_started_at": "2026-07-02T09:00:00",
        "input": {
            "path": "/tmp/user_wise_dormancy_report.xls",
            "name": "user_wise_dormancy_report.xls",
            "sha256": "abc",
        },
        "quality_gate": {"passed": True, "warnings": ["schema drift"], "errors": []},
        "lifecycle": [
            {
                "stage": "started",
                "status": "started",
                "message": "Run started.",
                "payload": {"dry_run": False},
                "created_at": "2026-07-02T09:00:00",
            },
            {
                "stage": "dispatch_finished",
                "status": "ok",
                "message": "WhatsApp dispatch completed.",
                "payload": {"total_payloads": 1},
                "created_at": "2026-07-02T09:01:00",
            },
        ],
        "whatsapp": {
            "queued": True,
            "delivery": {"statuses": {}, "delivered": 1, "failed": 0, "missing": []},
            "officer_delivery_audit": [
                {
                    "Job ID": "job-1",
                    "Target": "923005363198@s.whatsapp.net",
                    "Officer Name": "Ghulam Mustafa Ahmadani",
                    "Roles": "DDEO",
                    "Delivery Status": "delivered",
                    "Delivery Error": "",
                }
            ],
            "dispatch_plan": [
                {
                    "job_id": "job-1",
                    "channel": "individual",
                    "recipient_name": "Ghulam Mustafa Ahmadani",
                    "target": "923005363198@s.whatsapp.net",
                    "route_kind": "",
                    "message_mode": "individual",
                    "row_count": 1,
                    "excel_path": "",
                    "status": "delivered",
                    "cause": "",
                    "payload": {"text": "Assalam o Alaikum"},
                    "created_at": "2026-07-02T09:00:30",
                    "updated_at": "2026-07-02T09:01:00",
                }
            ],
        },
    }

    _persist_run_summary_to_pocketbase(summary, dormant_df)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("select count(*) from report_runs").fetchone()[0] == 1
        assert conn.execute("select count(*) from dormant_records").fetchone()[0] == 1
        assert conn.execute("select count(*) from delivery_events").fetchone()[0] == 1
        assert conn.execute("select count(*) from data_quality_issues").fetchone()[0] == 1
        assert conn.execute("select count(*) from run_stage_events").fetchone()[0] == 2
        assert conn.execute("select count(*) from dispatch_plan_items").fetchone()[0] == 1


def test_delivery_failure_summary_includes_officer_and_whatsapp_status():
    failed_rows = [
        {
            "Job ID": "job-1",
            "Officer Name": "Ghulam Mustafa Ahmadani",
            "Mobile Number": "923005363198",
            "Roles": "DDEO",
            "School Count": 3,
            "Delivery Error": "WhatsApp reported ERROR",
        }
    ]
    delivery_statuses = {
        "job-1": {
            "operationResult": [
                {
                    "id": "MSG1",
                    "remoteJid": "182218282557475@lid",
                    "statusName": "ERROR",
                }
            ]
        }
    }

    summary = main._build_delivery_failure_summary(
        failed_rows,
        delivery_statuses,
        "Anti-Dengue Dormant Users",
    )

    assert "Ghulam Mustafa Ahmadani" in summary
    assert "923005363198" in summary
    assert "WhatsApp reported ERROR" in summary
    assert "MSG1 / ERROR / 182218282557475@lid" in summary


def test_dispatch_plan_items_include_group_route_metadata():
    payload = {
        "job_id": "group-job-1",
        "target": "120363319976862432@g.us",
        "recipient_name": "Heads Tehsil Shalimar",
        "text": "Anti-Dengue summary",
        "excel_path": "/tmp/shalimar.xlsx",
        "dispatch_route": {
            "route_kind": "tehsil",
            "message_mode": "tehsil_markaz_summary",
            "row_count": 5,
        },
    }

    plan = main._build_dispatch_plan_items(
        dynamic_payloads=[],
        dynamic_payloads_to_queue=[],
        fixed_payloads=[payload],
        officer_delivery_audit=[],
        delivery_statuses={
            "group-job-1": {
                "status": "sent_pending_confirmation",
                "error": "server ack pending",
            }
        },
    )

    assert len(plan) == 1
    assert plan[0]["channel"] == "group"
    assert plan[0]["route_kind"] == "tehsil"
    assert plan[0]["message_mode"] == "tehsil_markaz_summary"
    assert plan[0]["row_count"] == 5
    assert plan[0]["status"] == "sent_pending_confirmation"
    assert plan[0]["cause"] == "server ack pending"


def test_group_ack_timeout_with_message_id_is_pending_confirmation():
    status = {
        "status": "failed",
        "type": "group",
        "error": (
            "Timed out waiting for WhatsApp SERVER_ACK acknowledgement for message "
            "MSG1 to 120363319976862432@g.us; last status was PENDING"
        ),
        "operationResult": [
            {
                "id": "MSG1",
                "remoteJid": "120363319976862432@g.us",
                "statusName": "PENDING",
            }
        ],
    }

    assert main._delivery_status_is_pending_confirmation(status)


def test_group_route_message_mode_controls_summary_shape():
    rows = [
        {
            "emis": "35210001",
            "school_name": "GPS CANTT ONE",
            "tehsil": "CANTT",
            "markaz": "BARKI - MALE",
        },
        {
            "emis": "35210002",
            "school_name": "GPS CANTT TWO",
            "tehsil": "CANTT",
            "markaz": "BEDIAN-MALE",
        },
        {
            "emis": "35220001",
            "school_name": "GPS CITY ONE",
            "tehsil": "CITY",
            "markaz": "SANDA - MALE",
        },
    ]
    route = {
        "name": "Anti Dengue Group",
        "target": "120363319976862432@g.us",
        "route_kind": "district",
        "message_mode": "tehsil_markaz_summary",
    }

    text = main._build_group_route_message(
        route,
        rows,
        title_text="Anti-Dengue Dormant Users",
        default_message="fallback",
    )

    assert "*TEHSIL SUMMARY*" in text
    assert "*MARKAZ SUMMARY*" in text
    assert "CANTT: 2 schools" in text
    assert "BARKI - MALE: 1 school" in text
    assert "GPS CANTT ONE" not in text
    assert "GPS CITY ONE" not in text

    route["message_mode"] = "markaz_summary"
    markaz_text = main._build_group_route_message(
        route,
        rows,
        title_text="Anti-Dengue Dormant Users",
        default_message="fallback",
    )

    assert "*TEHSIL SUMMARY*" not in markaz_text
    assert "*MARKAZ SUMMARY*" in markaz_text
    assert "GPS CANTT ONE" not in markaz_text


def test_group_route_payloads_generate_scoped_excel_for_route(tmp_path, monkeypatch):
    final_df = pd.DataFrame(
        {
            "Sr. No.": [1, 2],
            "School EMIS": ["35210001", "35220001"],
            "School Name": ["GPS CANTT ONE", "GPS CITY ONE"],
            "Tehsil": ["CANTT", "CITY"],
            "Markaz": ["BARKI - MALE", "SANDA - MALE"],
        }
    )
    officers_df = pd.DataFrame(
        {
            "emis_normalized": ["35210001", "35220001"],
            "ddeo_name": ["DDEO CANTT", "DDEO CITY"],
            "ddeo_cell_number": ["923001111111", "923002222222"],
            "aeo_name": ["AEO BARKI", "AEO SANDA"],
            "aeo_cell_number": ["923003333333", "923004444444"],
            "tehsil": ["CANTT", "CITY"],
            "markaz": ["BARKI - MALE", "SANDA - MALE"],
        }
    )
    routes_df = pd.DataFrame(
        [
            {
                "enabled": True,
                "name": "Heads Tehsil Cantt",
                "target": "120363319976862432@g.us",
                "type": "group",
                "route_kind": "tehsil",
                "tehsil": "CANTT",
                "markaz": "",
                "message_mode": "full_report",
                "attach_excel": True,
                "manual_only": False,
                "attachment_text_mode": "separate",
                "delay_ms": 0,
                "text": "",
            }
        ]
    )

    monkeypatch.setattr(main, "_read_officers_dataframe", lambda: officers_df)
    monkeypatch.setattr(main, "_runtime_has_group_routes", lambda: True)
    monkeypatch.setattr(
        main,
        "_read_group_routes_dataframe",
        lambda purpose="normal": routes_df,
    )

    payloads = main.build_group_route_payloads(
        final_df,
        tmp_path / "district-full-report.xlsx",
        image_path=None,
        title_text="Anti-Dengue Dormant Users",
        message_body="district fallback",
    )

    assert len(payloads) == 1
    payload = payloads[0]
    route_excel_path = Path(payload["excel_path"])
    assert route_excel_path.exists()
    assert route_excel_path.name != "district-full-report.xlsx"
    assert "Heads Tehsil Cantt" in route_excel_path.name
    assert payload["excel_filename"] == route_excel_path.name
    assert payload["dispatch_route"]["row_count"] == 1
    assert "GPS CANTT ONE" in payload["text"]
    assert "GPS CITY ONE" not in payload["text"]

    scoped_report = pd.read_excel(route_excel_path, header=2, dtype=str).fillna("")
    assert scoped_report["School EMIS"].tolist() == ["35210001"]
    assert scoped_report["School Name"].tolist() == ["GPS CANTT ONE"]


def test_duplicate_portal_export_detection_uses_postgres_runtime_history(
    tmp_path, monkeypatch
):
    snapshot_path = tmp_path / "runtime.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "automation-platform-postgresql",
                "previous_portal_runs": [
                    {
                        "started_at": "2026-07-06T09:19:05",
                        "status": "completed",
                        "raw_file_name": "user_wise_dormancy_report.xls",
                        "raw_file_sha256": "same-hash",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(main, "RUNTIME_SNAPSHOT_PATH", snapshot_path)

    result = main._detect_duplicate_portal_export(
        extraction_diagnostics={"source_file_sha256": "same-hash"},
        current_time_obj=main.datetime.datetime.fromisoformat(
            "2026-07-06T12:06:39"
        ),
        input_source="portal",
    )

    assert result["duplicate"] is True
    assert result["previous_run_started_at"] == "2026-07-06T09:19:05"
    assert result["previous_status"] == "completed"


def test_runtime_snapshot_supplies_master_officers_and_routes(tmp_path, monkeypatch):
    snapshot_path = tmp_path / "runtime.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "automation-platform-postgresql",
                "master_schools": [
                    {
                        "Sr. No.": 1,
                        "District": "LAHORE",
                        "Tehsil": "CANTT",
                        "Markaz": "BARKI - MALE",
                        "School Name": "GPS TEST",
                        "School EMIS": "35210001",
                        "School Type": "Male",
                        "DEOs Wise": "M-EE",
                        "School Level": "Primary",
                    }
                ],
                "officer_mappings": [
                    {
                        "emis": "35210001",
                        "school_name": "GPS TEST",
                        "tehsil": "CANTT",
                        "markaz": "BARKI - MALE",
                        "ddeo_name": "DDEO TEST",
                        "ddeo_cell_number": "923001111111",
                        "aeo_name": "AEO TEST",
                        "aeo_cell_number": "923002222222",
                    }
                ],
                "group_routes": [],
                "dispatch_settings": {},
                "previous_portal_runs": [],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(main, "RUNTIME_SNAPSHOT_PATH", snapshot_path)

    master = main._read_master_dataframe()
    officers = main._read_officers_dataframe()

    assert master.loc[0, "School EMIS"] == "35210001"
    assert officers.loc[0, "emis_normalized"] == "35210001"
    assert main._runtime_has_group_routes() is True


def test_duplicate_portal_export_detection_does_not_block_manual_input(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "pb.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            create table report_runs (
              id text primary key,
              started_at text,
              source text,
              status text,
              raw_file_name text,
              raw_file_sha256 text,
              summary text
            )
            """
        )
        conn.execute(
            """
            insert into report_runs
            (id, started_at, source, status, raw_file_name, raw_file_sha256, summary)
            values ('r1', '2026-07-06T09:19:05', 'portal', 'completed',
                    'user_wise_dormancy_report.xls', 'same-hash', '{}')
            """
        )

    monkeypatch.setattr(main, "POCKETBASE_DB_PATH", db_path)
    monkeypatch.setattr(main, "POCKETBASE_ENABLED", True)

    result = main._detect_duplicate_portal_export(
        extraction_diagnostics={"source_file_sha256": "same-hash"},
        current_time_obj=main.datetime.datetime.fromisoformat(
            "2026-07-06T12:06:39"
        ),
        input_source="manual",
    )

    assert result["duplicate"] is False


def test_stale_duplicate_portal_export_completes_as_no_change(caplog):
    summary = {
        "quality_gate": {"passed": True},
        "whatsapp": {
            "queued": False,
            "skipped_reason": "stale duplicate portal export",
            "outcome": "no_change",
        },
    }

    assert main._derive_pocketbase_run_status(summary) == "completed_no_change"
    main._raise_if_summary_not_completed(summary, main._get_logger())


def test_jetstream_publish_retry_reuses_message_id(monkeypatch):
    class FakeJetStream:
        def __init__(self):
            self.calls = []

        async def publish(self, subject, payload, headers):
            self.calls.append((subject, payload, headers))
            if len(self.calls) == 1:
                raise RuntimeError("temporary publish failure")

    jetstream = FakeJetStream()
    monkeypatch.setattr(main, "NATS_PUBLISH_ATTEMPTS", 3)
    monkeypatch.setattr(main, "NATS_PUBLISH_RETRY_DELAY_SECONDS", 0)

    asyncio.run(
        main._publish_jetstream_payload(
            jetstream,
            {"job_id": "job-123", "text": "AntiDengue update"},
        )
    )

    assert len(jetstream.calls) == 2
    assert jetstream.calls[0][2] == {"Nats-Msg-Id": "job-123"}
    assert jetstream.calls[1][2] == {"Nats-Msg-Id": "job-123"}
    assert jetstream.calls[0][1] == jetstream.calls[1][1]


def test_antidengue_runtime_has_no_prefect_dependency():
    project_root = Path(main.__file__).resolve().parent
    runtime_source = "\n".join(
        (project_root / name).read_text(encoding="utf-8")
        for name in ("main.py", "scraper.py", "pyproject.toml")
    ).lower()

    assert "prefect" not in runtime_source
    assert not hasattr(main.process_file_flow, "fn")


def _portal_bundle(*, dormant: str, hotspot: str, timing: str) -> dict:
    return {
        "reports": [
            {"report_key": "dormant_users", "status": "completed", "sha256": dormant},
            {"report_key": "hotspot_distance", "status": "completed", "sha256": hotspot},
            {"report_key": "simple_activity_list", "status": "completed", "sha256": timing},
        ]
    }


def _write_previous_bundle_summary(
    output_dir, bundle: dict, *, dry_run: bool = False, status: str = "completed"
) -> None:
    run_dir = output_dir / "2026-07-20_09-00-00"
    run_dir.mkdir(parents=True)
    whatsapp = {
        "queued": not dry_run,
        "dry_run": dry_run,
        "total_payloads": 3,
    }
    if status == "completed_no_change":
        whatsapp.update(
            {
                "queued": False,
                "dry_run": True,
                "skipped_reason": "stale duplicate portal export",
                "outcome": "no_change",
            }
        )
    (run_dir / "run_summary.json").write_text(
        json.dumps(
            {
                "run_started_at": "2026-07-20T09:00:00",
                "report_source": "unfiltered",
                "input": {
                    "name": "user_wise_dormancy_report.xls",
                    "sha256": "dormant-same",
                },
                "portal_acquisition": bundle,
                "quality_gate": {"passed": True},
                "whatsapp": whatsapp,
            }
        ),
        encoding="utf-8",
    )


def test_complete_portal_bundle_duplicate_is_no_change(tmp_path, monkeypatch):
    output_dir = tmp_path / "output-files"
    previous = _portal_bundle(
        dormant="dormant-same", hotspot="hotspot-same", timing="timing-same"
    )
    _write_previous_bundle_summary(output_dir, previous)
    monkeypatch.setattr(main, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(main, "RUNTIME_SNAPSHOT_PATH", None)

    result = main._detect_duplicate_portal_export(
        extraction_diagnostics={"source_file_sha256": "dormant-same"},
        current_time_obj=main.datetime.datetime.fromisoformat("2026-07-20T09:30:00"),
        input_source="portal",
        portal_acquisition=previous,
    )

    assert result["duplicate"] is True
    assert result["primary_duplicate"] is True
    assert result["duplicate_scope"] == "complete_report_bundle"
    assert result["changed_report_keys"] == []




def test_test_run_dry_run_does_not_consume_future_send_now(tmp_path, monkeypatch):
    output_dir = tmp_path / "output-files"
    bundle = _portal_bundle(
        dormant="dormant-same", hotspot="hotspot-same", timing="timing-same"
    )
    _write_previous_bundle_summary(output_dir, bundle, dry_run=True)
    monkeypatch.setattr(main, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(main, "RUNTIME_SNAPSHOT_PATH", None)

    result = main._detect_duplicate_portal_export(
        extraction_diagnostics={"source_file_sha256": "dormant-same"},
        current_time_obj=main.datetime.datetime.fromisoformat("2026-07-20T09:30:00"),
        input_source="portal",
        portal_acquisition=bundle,
    )

    assert result["duplicate"] is False
    assert result["duplicate_scope"] == "none"


def test_incomplete_current_bundle_is_not_reported_as_no_change(tmp_path, monkeypatch):
    output_dir = tmp_path / "output-files"
    previous = _portal_bundle(
        dormant="dormant-same", hotspot="hotspot-same", timing="timing-same"
    )
    current = {
        "reports": [
            {"report_key": "dormant_users", "status": "completed", "sha256": "dormant-same"},
            {"report_key": "hotspot_distance", "status": "failed", "sha256": ""},
            {"report_key": "simple_activity_list", "status": "completed", "sha256": "timing-same"},
        ]
    }
    _write_previous_bundle_summary(output_dir, previous)
    monkeypatch.setattr(main, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(main, "RUNTIME_SNAPSHOT_PATH", None)

    result = main._detect_duplicate_portal_export(
        extraction_diagnostics={"source_file_sha256": "dormant-same"},
        current_time_obj=main.datetime.datetime.fromisoformat("2026-07-20T09:30:00"),
        input_source="portal",
        portal_acquisition=current,
    )

    assert result["duplicate"] is False
    assert result["primary_duplicate"] is True
    assert result["duplicate_scope"] == "incomplete_report_bundle"


def test_same_dormant_but_changed_activity_report_is_not_blocked(tmp_path, monkeypatch):
    output_dir = tmp_path / "output-files"
    previous = _portal_bundle(
        dormant="dormant-same", hotspot="hotspot-old", timing="timing-same"
    )
    current = _portal_bundle(
        dormant="dormant-same", hotspot="hotspot-new", timing="timing-same"
    )
    _write_previous_bundle_summary(output_dir, previous)
    monkeypatch.setattr(main, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(main, "RUNTIME_SNAPSHOT_PATH", None)

    result = main._detect_duplicate_portal_export(
        extraction_diagnostics={"source_file_sha256": "dormant-same"},
        current_time_obj=main.datetime.datetime.fromisoformat("2026-07-20T09:30:00"),
        input_source="portal",
        portal_acquisition=current,
    )

    assert result["duplicate"] is False
    assert result["primary_duplicate"] is True
    assert result["duplicate_scope"] == "primary_report_only"
    assert result["changed_report_keys"] == ["hotspot_distance"]
    warning = main._duplicate_portal_export_warning(result)
    assert "will continue" in warning


def test_failed_runtime_execution_is_not_a_duplicate_baseline(tmp_path, monkeypatch):
    output_dir = tmp_path / "output-files"
    output_dir.mkdir()
    bundle = _portal_bundle(
        dormant="dormant-same", hotspot="hotspot-same", timing="timing-same"
    )
    snapshot_path = tmp_path / "runtime-snapshot.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "schema_version": main.RUNTIME_SNAPSHOT_SCHEMA_VERSION,
                "source": "automation-platform-postgresql",
                "previous_portal_runs": [
                    {
                        "started_at": "2026-07-20T09:00:00",
                        "status": "failed",
                        "raw_file_name": "user_wise_dormancy_report.xls",
                        "raw_file_sha256": "dormant-same",
                        "portal_acquisition": bundle,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(main, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(main, "RUNTIME_SNAPSHOT_PATH", snapshot_path)

    result = main._detect_duplicate_portal_export(
        extraction_diagnostics={"source_file_sha256": "dormant-same"},
        current_time_obj=main.datetime.datetime.fromisoformat("2026-07-20T09:30:00"),
        input_source="portal",
        portal_acquisition=bundle,
    )

    assert result["duplicate"] is False
    assert result["duplicate_scope"] == "none"

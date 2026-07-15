import pandas as pd
import sqlite3
from pathlib import Path

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
    assert any("informational" in warning for warning in result["warnings"])


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
    monkeypatch.setattr(main, "_pocketbase_has_group_routes_table", lambda: True)
    monkeypatch.setattr(
        main,
        "_read_group_routes_dataframe_from_pocketbase",
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


def test_duplicate_portal_export_detection_uses_pocketbase_history(
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
        input_source="portal",
    )

    assert result["duplicate"] is True
    assert result["previous_run_started_at"] == "2026-07-06T09:19:05"
    assert result["previous_status"] == "completed"


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


def test_stale_duplicate_portal_export_is_not_marked_completed():
    summary = {
        "quality_gate": {"passed": True},
        "whatsapp": {
            "queued": False,
            "skipped_reason": "stale duplicate portal export",
        },
    }

    assert main._derive_pocketbase_run_status(summary) == "failed"

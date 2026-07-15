from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from app.config import settings


LoginMode = Literal["manual", "auto", "remote_approve"]


class AntiDengueRunRequest(BaseModel):
    login_mode: LoginMode = "manual"
    dry_run: bool = False
    command: Literal["portal", "manual-unfiltered", "manual-file"] = "portal"
    file_path: str = ""


class PortalApprovalRequest(BaseModel):
    note: str = ""


def antidengue_command(request: AntiDengueRunRequest) -> tuple[list[str], dict[str, str]]:
    python = settings.antidengue_python
    if not python.exists():
        python = Path("python3")

    command = [str(python), "main.py"]
    if request.command != "portal":
        command.append(request.command)
    if request.command == "manual-file":
        if not request.file_path:
            raise ValueError("file_path is required for manual-file runs.")
        command.extend(["--file", request.file_path])
    if request.dry_run:
        command.append("--dry-run")

    env = {
        "PORTAL_LOGIN_MODE": request.login_mode,
    }
    return command, env


def latest_summary() -> dict[str, object]:
    output_root = settings.antidengue_dir / "output-files"
    summaries = sorted(
        output_root.glob("*/run_summary.json"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0,
        reverse=True,
    )
    for path in summaries:
        try:
            summary = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        return {
            "path": str(path),
            "folder": str(path.parent),
            "modified_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat(
                timespec="seconds"
            ),
            "summary": summary,
        }
    return {"path": "", "folder": "", "modified_at": "", "summary": {}}


def _delivery_operation_result_has_message_id(operation_result: object) -> bool:
    return isinstance(operation_result, list) and any(
        isinstance(item, dict) and bool(item.get("id")) for item in operation_result
    )


def _delivery_operation_result_has_terminal_error(operation_result: object) -> bool:
    return isinstance(operation_result, list) and any(
        isinstance(item, dict) and str(item.get("statusName") or "").upper() == "ERROR"
        for item in operation_result
    )


def _delivery_status_is_pending_confirmation(status: dict[str, object]) -> bool:
    status_name = str(status.get("status") or "").strip().lower()
    if status_name == "sent_pending_confirmation":
        return True

    return (
        status_name == "failed"
        and str(status.get("type") or "").strip().lower() == "group"
        and "Timed out waiting for WhatsApp" in str(status.get("error") or "")
        and _delivery_operation_result_has_message_id(status.get("operationResult"))
        and not _delivery_operation_result_has_terminal_error(
            status.get("operationResult")
        )
    )


def _delivery_counts(delivery: dict[str, object]) -> dict[str, int]:
    statuses = delivery.get("statuses")
    if isinstance(statuses, dict) and statuses:
        delivered = 0
        pending_confirmation = 0
        failed = 0
        for status in statuses.values():
            if not isinstance(status, dict):
                continue
            status_name = str(status.get("status") or "").strip().lower()
            if status_name == "delivered":
                delivered += 1
            elif _delivery_status_is_pending_confirmation(status):
                pending_confirmation += 1
            elif status_name:
                failed += 1
        return {
            "delivered": delivered,
            "pending_confirmation": pending_confirmation,
            "sent": delivered + pending_confirmation,
            "failed": failed,
        }

    delivered = int(delivery.get("delivered") or 0)
    pending_confirmation = int(delivery.get("pending_confirmation") or 0)
    sent = int(delivery.get("sent") or delivered + pending_confirmation)
    return {
        "delivered": delivered,
        "pending_confirmation": pending_confirmation,
        "sent": sent,
        "failed": int(delivery.get("failed") or 0),
    }


def latest_summary_brief() -> dict[str, object]:
    latest = latest_summary()
    summary = latest.get("summary") if isinstance(latest.get("summary"), dict) else {}
    filter_info = summary.get("filter") if isinstance(summary.get("filter"), dict) else {}
    quality_gate = (
        summary.get("quality_gate") if isinstance(summary.get("quality_gate"), dict) else {}
    )
    outputs = summary.get("outputs") if isinstance(summary.get("outputs"), dict) else {}
    whatsapp = summary.get("whatsapp") if isinstance(summary.get("whatsapp"), dict) else {}
    delivery = whatsapp.get("delivery") if isinstance(whatsapp.get("delivery"), dict) else {}
    fallback = (
        whatsapp.get("fallback_delivery")
        if isinstance(whatsapp.get("fallback_delivery"), dict)
        else {}
    )
    fallback_delivery = (
        fallback.get("delivery") if isinstance(fallback.get("delivery"), dict) else {}
    )
    warnings = quality_gate.get("warnings") if isinstance(quality_gate.get("warnings"), list) else []
    errors = quality_gate.get("errors") if isinstance(quality_gate.get("errors"), list) else []
    delivery_counts = _delivery_counts(delivery)
    fallback_counts = _delivery_counts(fallback_delivery)
    final_school_count = int(quality_gate.get("final_school_count") or 0)
    raw_dormant_rows = int(filter_info.get("dormant_raw_rows") or 0)
    delivery_error = str(whatsapp.get("error") or "")
    skipped_reason = str(whatsapp.get("skipped_reason") or "")
    if (
        delivery_error
        and delivery_counts["failed"] == 0
        and not delivery.get("missing")
        and delivery_counts["pending_confirmation"] > 0
    ):
        delivery_error = ""
    if not delivery_error and skipped_reason and skipped_reason != "zero inactive schools":
        delivery_error = skipped_reason

    input_info = summary.get("input") if isinstance(summary.get("input"), dict) else {}
    duplicate_check = (
        input_info.get("portal_duplicate_raw_check")
        if isinstance(input_info.get("portal_duplicate_raw_check"), dict)
        else {}
    )

    return {
        "path": latest.get("path") or "",
        "folder": latest.get("folder") or "",
        "modified_at": latest.get("modified_at") or "",
        "started_at": summary.get("run_started_at") or "",
        "dry_run": bool(summary.get("dry_run") or whatsapp.get("dry_run")),
        "report_source": summary.get("report_source") or "",
        "raw_rows": int(filter_info.get("raw_rows") or 0),
        "raw_dormant_rows": raw_dormant_rows,
        "processed_school_count": final_school_count,
        "dormant_rows": final_school_count or raw_dormant_rows,
        "quality_passed": bool(quality_gate.get("passed")),
        "quality_warning_count": len(warnings),
        "quality_error_count": len(errors),
        "whatsapp_queued": bool(
            whatsapp.get("queued") or delivery_counts["sent"] or delivery_counts["failed"]
        ),
        "delivered": delivery_counts["delivered"],
        "sent": delivery_counts["sent"],
        "pending_confirmation": delivery_counts["pending_confirmation"],
        "failed": delivery_counts["failed"],
        "fallback_failed": fallback_counts["failed"],
        "missing_status_count": len(delivery.get("missing") or []),
        "total_payloads": int(whatsapp.get("total_payloads") or 0),
        "dynamic_payloads": int(whatsapp.get("dynamic_payloads") or 0),
        "queued_dynamic_payloads": int(whatsapp.get("queued_dynamic_payloads") or 0),
        "fixed_payloads": int(whatsapp.get("fixed_payloads") or 0),
        "error": delivery_error,
        "skipped_reason": skipped_reason,
        "stale_duplicate": bool(duplicate_check.get("duplicate")),
        "excel_report": outputs.get("excel_report") or "",
        "output_dir": outputs.get("output_dir") or "",
    }


def portal_login_request_path() -> Path:
    return settings.antidengue_dir / "portal_login_request.json"


def portal_login_approval_path() -> Path:
    return settings.antidengue_dir / "portal_login_approval.json"


def portal_login_status() -> dict[str, object]:
    request_path = portal_login_request_path()
    approval_path = portal_login_approval_path()
    payload: dict[str, object] = {}
    if request_path.is_file():
        try:
            loaded = json.loads(request_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = loaded
        except (OSError, json.JSONDecodeError):
            payload = {"status": "unreadable"}
    return {
        "request_path": str(request_path),
        "approval_path": str(approval_path),
        "request_exists": request_path.is_file(),
        "approval_exists": approval_path.is_file(),
        "request": payload,
    }


def write_portal_login_action(action: Literal["approve", "deny"], note: str = "") -> dict[str, object]:
    approval_path = portal_login_approval_path()
    approval_path.write_text(
        json.dumps(
            {
                "action": action,
                "note": note,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "source": "automation-control-api",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return portal_login_status()

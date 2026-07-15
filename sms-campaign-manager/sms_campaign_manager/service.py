from __future__ import annotations

import hashlib
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

from .client import SmsGateClient
from .config import GatewayConfig
from .contacts import load_contacts, sms_segments
from .db import Ledger


def campaign_key(path: Path, sheet: str, column: str, message: str, campaign_id: str = "") -> tuple[str, str, str]:
    message_hash = hashlib.sha256(message.encode()).hexdigest()
    identity = campaign_id.strip() or f"{path.resolve()}::{sheet}::{column}"
    key = hashlib.sha256(f"stable-campaign-v2\0{identity}".encode()).hexdigest()
    return key, message_hash, identity


def create_campaign(*, campaign_id: str, path: Path, sheet: str, column: str, message: str, country_code: str, description: str = "") -> int:
    if not campaign_id.strip():
        raise ValueError("Campaign name is required. Use a stable human-readable value like day3-ghss-islamia-absentees.")
    load_dotenv()
    contacts, invalid = load_contacts(path, column, sheet, country_code)
    segments, encoding = sms_segments(message)
    key, message_hash, identity = campaign_key(path, sheet, column, message, campaign_id)
    database = Path(os.getenv("SMS_GATE_DATABASE", "data/sms-campaigns.sqlite3")).expanduser()
    ledger = Ledger(database)
    ledger_campaign_id = ledger.create_or_update_campaign(key, identity, str(path.resolve()), sheet, column, message_hash, message, description)
    ledger.prepare(key, identity, str(path.resolve()), sheet, column, message_hash, message, contacts)
    ledger.refresh_campaign_status(ledger_campaign_id)
    print(f"Campaign saved: {identity}", flush=True)
    print(f"Recipients loaded into ledger: {len(contacts)} valid unique; ignored {invalid} blank/invalid row(s).", flush=True)
    print(f"Message: {len(message)} characters, {encoding}, {segments} SMS segment(s) per recipient.", flush=True)
    print(f"Campaign database id: {ledger_campaign_id}", flush=True)
    return 0


def list_campaigns(*, json_output: bool = False, include_deleted: bool = False) -> int:
    import json

    load_dotenv()
    config = GatewayConfig.from_env()
    ledger = Ledger(config.database)
    rows = [dict(row) for row in ledger.list_campaigns(include_deleted=include_deleted)]
    if json_output:
        print(json.dumps(rows, indent=2, default=str), flush=True)
        return 0
    if not rows:
        print("No SMS campaigns found.", flush=True)
        return 0
    for row in rows:
        print(
            f"{row.get('campaign_name') or row.get('campaign_key')} | "
            f"status={row.get('status') or '-'} "
            f"recipients={row.get('recipients') or 0} pending={row.get('pending') or 0} "
            f"queued={row.get('queued') or 0} sent={row.get('sent') or 0} "
            f"delivered={row.get('delivered') or 0} failed={row.get('failed') or 0} "
            f"unknown={row.get('unknown') or 0} | updated={row.get('updated_at') or row.get('created_at')}",
            flush=True,
        )
    return 0


def show_campaign(campaign_id: str, *, json_output: bool = False) -> int:
    import json

    load_dotenv()
    config = GatewayConfig.from_env()
    ledger = Ledger(config.database)
    campaign, summary = ledger.campaign_details(campaign_id)
    if campaign is None:
        raise ValueError(f"Campaign not found: {campaign_id}")
    payload = {"campaign": dict(campaign), "summary": summary}
    if json_output:
        print(json.dumps(payload, indent=2, default=str), flush=True)
        return 0
    print(f"Campaign: {campaign['campaign_name']}", flush=True)
    print(f"File: {campaign['source_file']}", flush=True)
    print(f"Sheet/column: {campaign['sheet']} / {campaign['number_column']}", flush=True)
    print(f"Status: {campaign['status']}", flush=True)
    if campaign["description"]:
        print(f"Description: {campaign['description']}", flush=True)
    print("Summary: " + ", ".join(f"{key}={value}" for key, value in sorted(summary.items())), flush=True)
    return 0


def failed_campaign_report(campaign_id: str, *, json_output: bool = False) -> int:
    import json

    load_dotenv()
    config = GatewayConfig.from_env()
    ledger = Ledger(config.database)
    rows = [dict(row) for row in ledger.campaign_failures(campaign_id)]
    if json_output:
        print(json.dumps(rows, indent=2, default=str), flush=True)
        return 0
    print(f"Failed recipients for campaign: {campaign_id}", flush=True)
    if not rows:
        print("No failed recipients recorded for this campaign.", flush=True)
        return 0
    for row in rows:
        reason = f" - {row['error']}" if row.get("error") else ""
        print(f"row {row['source_row']}: {row['phone']} attempts={row['attempts']} id={row.get('gateway_message_id') or '-'}{reason}", flush=True)
    return 0


def delete_campaign(campaign_id: str, *, hard: bool = False) -> int:
    load_dotenv()
    config = GatewayConfig.from_env()
    ledger = Ledger(config.database)
    if hard:
        deleted = ledger.hard_delete_campaign(campaign_id)
        action = "Hard-deleted"
    else:
        deleted = ledger.archive_campaign(campaign_id)
        action = "Archived"
    if not deleted:
        raise ValueError(f"Campaign not found or already archived: {campaign_id}")
    print(f"{action} campaign: {campaign_id}", flush=True)
    if not hard:
        print("Delivery history was preserved. Re-create/update with the same name to revive it.", flush=True)
    return 0


def deterministic_message_id(campaign_identity: str, phone: str, source_row: int, attempt: int) -> str:
    digest = hashlib.sha256(f"{campaign_identity}\0{phone}\0{source_row}\0{attempt}".encode()).hexdigest()[:24]
    return f"smscm-{digest}"


def _periods(now: datetime) -> tuple[str, str, str]:
    return tuple((now - delta).strftime("%Y-%m-%d %H:%M:%S") for delta in (timedelta(minutes=1), timedelta(hours=1), timedelta(days=1)))  # type: ignore[return-value]


def _parse_gateway_time(value: object) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _device_age_seconds(device: dict) -> float | None:
    seen_at = _parse_gateway_time(device.get("lastSeen") or device.get("updatedAt"))
    if seen_at is None:
        return None
    return (datetime.now(timezone.utc) - seen_at.astimezone(timezone.utc)).total_seconds()


def _wait_for_device_online(client: SmsGateClient, config: GatewayConfig) -> None:
    deadline = time.monotonic() + config.device_online_timeout_seconds
    printed_wait = False
    while True:
        device = client.configured_device()
        age = _device_age_seconds(device)
        if age is not None and age <= config.device_max_age_seconds:
            if printed_wait:
                print(f"SMSGate device is online again; last seen {age:.0f}s ago.", flush=True)
            return
        detail = "unknown" if age is None else f"{age:.0f}s ago"
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"SMSGate device is not fresh enough; last seen {detail}. "
                "Keep the Android SMSGate app open/online and verify Tailscale before retrying."
            )
        print(
            f"SMSGate device is not fresh yet; last seen {detail}. "
            "Waiting 15s before submitting more SMS...",
            flush=True,
        )
        printed_wait = True
        time.sleep(15)


def run_campaign(*, path: Path, sheet: str, column: str, message: str, campaign_id: str, country_code: str, minute_limit: int, hour_limit: int, day_limit: int, delay_seconds: float, sim_number: int, ttl: int, retry_failed: bool, max_attempts: int, execute: bool, watch_until_complete: bool = False, watch_timeout_seconds: int = 7200, watch_poll_seconds: int = 30) -> int:
    load_dotenv()
    contacts, invalid = load_contacts(path, column, sheet, country_code)
    segments, encoding = sms_segments(message)
    if segments > min(minute_limit, hour_limit, day_limit):
        raise ValueError(
            f"One recipient uses {segments} SMS segments, exceeding a configured rolling limit. "
            "Shorten the message or raise the limit."
        )
    key, message_hash, identity = campaign_key(path, sheet, column, message, campaign_id)
    database = Path(os.getenv("SMS_GATE_DATABASE", "data/sms-campaigns.sqlite3")).expanduser()
    ledger = Ledger(database)
    ledger_campaign_id, recovered = ledger.prepare(key, identity, str(path.resolve()), sheet, column, message_hash, message, contacts)
    pending = [row for row in ledger.pending(ledger_campaign_id, retry_failed) if int(row["attempt_count"]) < max_attempts]
    print(f"Loaded {len(contacts)} valid unique recipient(s); ignored {invalid} blank/invalid row(s).", flush=True)
    print(f"Message: {len(message)} characters, {encoding}, {segments} SMS segment(s) per recipient.", flush=True)
    print(f"Campaign ID: {identity}", flush=True)
    if retry_failed:
        print(f"Retry mode: {len(pending)} failed recipient(s) eligible below the {max_attempts}-attempt limit; all other statuses skipped.", flush=True)
    else:
        print(f"Idempotency: {len(contacts) - len(pending)} already attempted and skipped; {len(pending)} new/pending.", flush=True)
    if recovered:
        print(f"Recovered {recovered} previously attempted recipient(s) from legacy campaign records.", flush=True)
    print(f"Limits: {minute_limit}/minute, {hour_limit}/hour, {day_limit}/day; minimum delay {delay_seconds:g}s.", flush=True)
    if not pending:
        if retry_failed:
            print(
                "Nothing to retry: this campaign has no recipients currently marked failed. "
                "For the first send, run again with 'Retry confirmed failed recipients only' unchecked / without --retry-failed. "
                "After sending, run Refresh Delivery Statuses before retrying failures.",
                flush=True,
            )
        else:
            print(
                "Nothing to send: every recipient in this campaign is already handled or reserved in the SQLite ledger.",
                flush=True,
            )
            summary = ledger.campaign_summary(ledger_campaign_id)
            if summary and not ledger.has_open_recipients(ledger_campaign_id):
                ledger.refresh_campaign_status(ledger_campaign_id)
        return 0
    if not execute:
        for item in pending[:20]:
            print(f"  PREVIEW row {item['source_row']}: {item['phone']}", flush=True)
        if len(pending) > 20:
            print(f"  ... and {len(pending) - 20} more", flush=True)
        print("Preview only. Enable Execute after verifying consent, recipients, message, and limits.", flush=True)
        return 0
    config = GatewayConfig.from_env()
    client = SmsGateClient(config)
    try:
        ledger.update_campaign_status(ledger_campaign_id, "running")
        devices = client.devices()
        print(f"Gateway backend reachable at {config.base_url}; discovered {len(devices) if isinstance(devices, list) else 1} device record(s).", flush=True)
        print(
            f"Checking configured Android device freshness before sending "
            f"(max age {config.device_max_age_seconds}s, wait timeout {config.device_online_timeout_seconds}s)...",
            flush=True,
        )
        _wait_for_device_online(client, config)
        for index, recipient in enumerate(pending, 1):
            _wait_for_device_online(client, config)
            while True:
                counts = ledger.counts_since(_periods(datetime.now(timezone.utc)))
                exceeded = next(((used, limit, label, wait) for used, limit, label, wait in zip(counts, (minute_limit, hour_limit, day_limit), ("minute", "hour", "day"), (10, 60, 300)) if used + segments > limit), None)
                if not exceeded:
                    break
                used, limit, label, wait = exceeded
                print(f"Rate limit paused: {used}/{limit} in rolling {label}; rechecking in {wait}s.", flush=True)
                time.sleep(wait)
            phone = str(recipient["phone"])
            print(f"Processing {index}/{len(pending)}: Excel row {recipient['source_row']}, {phone}", flush=True)
            expected_status = "failed" if retry_failed else "pending"
            attempt = ledger.next_attempt_number(int(recipient["id"]))
            gateway_message_id = deterministic_message_id(identity, phone, int(recipient["source_row"]), attempt)
            if not ledger.reserve(int(recipient["id"]), phone, segments, message_hash, expected_status, gateway_message_id):
                print("  SKIP: another process already reserved this recipient.", flush=True)
                continue
            try:
                message_id = client.send(phone, message, sim_number, ttl, gateway_message_id)
            except httpx.TimeoutException as exc:
                ledger.complete(int(recipient["id"]), "unknown", gateway_message_id, error=f"Timeout after request; deterministic id retained for reconciliation: {exc}")
                print(f"  UNKNOWN: gateway timeout. Deterministic SMS ID retained for reconciliation: {gateway_message_id}", flush=True)
            except (httpx.HTTPError, ValueError) as exc:
                ledger.complete(int(recipient["id"]), "failed", error=str(exc))
                print(f"  FAILED: {exc}", flush=True)
            else:
                ledger.complete(int(recipient["id"]), "queued", message_id or gateway_message_id)
                print(f"  QUEUED successfully (gateway message ID: {message_id or gateway_message_id}).", flush=True)
            if index < len(pending) and delay_seconds > 0:
                print(f"  Safety wait: {delay_seconds:g}s before next recipient.", flush=True)
                time.sleep(delay_seconds)
    except KeyboardInterrupt:
        print("Stopped safely. Reserved/submitted records remain in SQLite; rerun resumes pending recipients.", flush=True)
        return 130
    finally:
        client.close()
    ledger.refresh_campaign_status(ledger_campaign_id)
    print("Campaign submission complete: all eligible recipients have been submitted or skipped according to the ledger.", flush=True)
    if watch_until_complete:
        return watch_campaign(ledger, ledger_campaign_id, watch_timeout_seconds, watch_poll_seconds)
    return 0


def _map_gateway_status(payload: dict) -> tuple[str, str]:
    raw = str(payload.get("status") or payload.get("state") or "queued").lower()
    recipients = payload.get("recipients", [])
    reason = ""
    if isinstance(recipients, list):
        reason = "; ".join(str(item.get("error", "")).strip() for item in recipients if isinstance(item, dict) and item.get("error"))
        recipient_states = " ".join(str(item.get("state", "")).lower() for item in recipients if isinstance(item, dict))
        raw = f"{raw} {recipient_states}".strip()
    mapped = (
        "delivered"
        if "deliver" in raw
        else "failed"
        if any(word in raw for word in ("fail", "error", "expired", "reject"))
        else "sent"
        if "sent" in raw
        else "processed"
        if "process" in raw
        else "queued"
    )
    return mapped, reason


def reconcile_statuses(campaign_id: str = "", campaign_db_id: int | None = None) -> int:
    load_dotenv()
    config = GatewayConfig.from_env()
    ledger = Ledger(config.database)
    rows = ledger.awaiting_status(campaign_id, campaign_db_id)
    scope = f" for campaign {campaign_id}" if campaign_id else f" for campaign db id {campaign_db_id}" if campaign_db_id is not None else ""
    print(f"Checking {len(rows)} queued/sent message(s){scope}...", flush=True)
    summary: dict[str, int] = {}
    client = SmsGateClient(config)
    try:
        for index, row in enumerate(rows, 1):
            message_id = str(row["gateway_message_id"])
            try:
                payload = client.status(message_id)
                mapped, reason = _map_gateway_status(payload)
                ledger.update_gateway_status(message_id, mapped, reason)
                summary[mapped] = summary.get(mapped, 0) + 1
                suffix = f" - {reason}" if reason else ""
                print(f"{index}/{len(rows)} {row['phone']}: {mapped} ({message_id}){suffix}", flush=True)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    ledger.mark_gateway_missing(message_id)
                    summary["missing_gateway"] = summary.get("missing_gateway", 0) + 1
                    print(f"{index}/{len(rows)} {row['phone']}: gateway has no record for {message_id}; stale submitting records reset to pending, others marked unknown.", flush=True)
                else:
                    print(f"{index}/{len(rows)} {row['phone']}: status check failed: {exc}", flush=True)
            except (httpx.HTTPError, ValueError) as exc:
                print(f"{index}/{len(rows)} {row['phone']}: status check failed: {exc}", flush=True)
    finally:
        client.close()
    print("Gateway summary: " + ", ".join(f"{key}={value}" for key, value in sorted(summary.items())), flush=True)
    return 0


def watch_campaign(ledger: Ledger, campaign_id: int, timeout_seconds: int, poll_seconds: int) -> int:
    deadline = time.monotonic() + timeout_seconds
    while True:
        summary = ledger.campaign_summary(campaign_id)
        print("Campaign ledger summary: " + ", ".join(f"{key}={value}" for key, value in sorted(summary.items())), flush=True)
        if not ledger.has_open_recipients(campaign_id):
            status = ledger.refresh_campaign_status(campaign_id)
            print("Campaign verified complete: every recipient is now terminal (sent/delivered/failed).", flush=True)
            print(f"Final campaign status: {status}", flush=True)
            return 0
        if time.monotonic() >= deadline:
            ledger.update_campaign_status(campaign_id, "needs_attention")
            print(
                "Campaign still has open recipients after watch timeout. "
                "No duplicate SMS were sent; rerun the same campaign later to resume pending recipients and reconcile deterministic IDs.",
                flush=True,
            )
            return 4
        print(f"Campaign still has open recipients; reconciling again in {poll_seconds}s.", flush=True)
        time.sleep(poll_seconds)
        reconcile_statuses(campaign_db_id=campaign_id)

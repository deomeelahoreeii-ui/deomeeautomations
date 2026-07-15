from __future__ import annotations

import argparse
import json
from pathlib import Path

import httpx

from .config import GatewayConfig
from .contacts import suggested_number_column, workbook_columns
from .service import create_campaign, delete_campaign, failed_campaign_report, list_campaigns, reconcile_statuses, run_campaign, show_campaign


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="sms-campaign")
    commands = root.add_subparsers(dest="command", required=True)
    campaigns = commands.add_parser("campaigns", help="Create, list, and inspect durable SMS campaigns")
    campaign_commands = campaigns.add_subparsers(dest="campaign_command", required=True)
    campaign_create = campaign_commands.add_parser("create", help="Create or update a campaign definition")
    campaign_create.add_argument("--campaign-id", required=True)
    campaign_create.add_argument("--file", required=True, type=Path)
    campaign_create.add_argument("--sheet", default="")
    campaign_create.add_argument("--column", required=True)
    campaign_create.add_argument("--message", required=True)
    campaign_create.add_argument("--country-code", default="92")
    campaign_create.add_argument("--description", default="")
    campaign_list = campaign_commands.add_parser("list", help="List campaigns")
    campaign_list.add_argument("--json", action="store_true")
    campaign_list.add_argument("--include-deleted", action="store_true")
    campaign_show = campaign_commands.add_parser("show", help="Show campaign summary")
    campaign_show.add_argument("--campaign-id", required=True)
    campaign_show.add_argument("--json", action="store_true")
    campaign_failed = campaign_commands.add_parser("failed", help="List failed recipients for one campaign")
    campaign_failed.add_argument("--campaign-id", required=True)
    campaign_failed.add_argument("--json", action="store_true")
    campaign_delete = campaign_commands.add_parser("delete", help="Archive or hard-delete a campaign")
    campaign_delete.add_argument("--campaign-id", required=True)
    campaign_delete.add_argument("--hard", action="store_true", help="Permanently delete campaign ledger rows instead of archiving")

    inspect = commands.add_parser("inspect", help="List workbook sheets and columns")
    inspect.add_argument("--file", required=True, type=Path)
    inspect.add_argument("--sheet", default="")
    inspect.add_argument("--json", action="store_true")
    devices = commands.add_parser("devices", help="Test connection and list gateway devices")
    devices.add_argument("--json", action="store_true")
    reconcile = commands.add_parser("reconcile", help="Refresh queued message delivery statuses")
    reconcile.add_argument("--campaign-id", default="", help="Optional campaign name/reference to reconcile only one campaign")
    send = commands.add_parser("send", help="Preview or execute a durable campaign")
    send.add_argument("--file", type=Path)
    send.add_argument("--sheet", default="")
    send.add_argument("--column", default="")
    send.add_argument("--message", default="")
    send.add_argument("--campaign-id", default="", help="Stable identity; empty uses file path + sheet + column")
    send.add_argument("--country-code", default="92")
    send.add_argument("--minute-limit", type=int, default=5)
    send.add_argument("--hour-limit", type=int, default=50)
    send.add_argument("--day-limit", type=int, default=200)
    send.add_argument("--delay-seconds", type=float, default=12)
    send.add_argument("--sim-number", type=int, default=1)
    send.add_argument("--ttl", type=int, default=3600)
    send.add_argument("--retry-failed", action="store_true", help="Retry only recipients confirmed failed by status reconciliation")
    send.add_argument("--max-attempts", type=int, default=3, help="Maximum total submissions per recipient in this campaign")
    send.add_argument("--watch-until-complete", action="store_true", help="After submission, keep reconciling until all recipients are sent/delivered/failed")
    send.add_argument("--watch-timeout-seconds", type=int, default=7200)
    send.add_argument("--watch-poll-seconds", type=int, default=30)
    send.add_argument("--consent-confirmed", action="store_true")
    send.add_argument("--execute", action="store_true")
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "campaigns":
            if args.campaign_command == "create":
                if not args.message.strip():
                    raise ValueError("Message cannot be blank")
                return create_campaign(campaign_id=args.campaign_id, path=args.file, sheet=args.sheet, column=args.column, message=args.message, country_code=args.country_code, description=args.description)
            if args.campaign_command == "list":
                return list_campaigns(json_output=args.json, include_deleted=args.include_deleted)
            if args.campaign_command == "show":
                return show_campaign(args.campaign_id, json_output=args.json)
            if args.campaign_command == "failed":
                return failed_campaign_report(args.campaign_id, json_output=args.json)
            if args.campaign_command == "delete":
                return delete_campaign(args.campaign_id, hard=args.hard)
        if args.command == "inspect":
            columns, selected = workbook_columns(args.file, args.sheet)
            payload = {"sheet": selected, "columns": columns, "suggested": suggested_number_column(columns)}
            print(json.dumps(payload) if args.json else f"Sheet: {selected}\nColumns: {', '.join(columns)}\nSuggested: {payload['suggested']}")
            return 0
        if args.command == "devices":
            from .client import SmsGateClient
            client = SmsGateClient(GatewayConfig.from_env())
            try:
                result = client.devices()
            finally:
                client.close()
            print(json.dumps(result, indent=2) if args.json else result)
            return 0
        if args.command == "reconcile":
            return reconcile_statuses(args.campaign_id)
        if not args.campaign_id.strip() and not args.file:
            raise ValueError("--file is required when --campaign-id is blank")
        if args.campaign_id.strip() and (not args.file or not args.column or not args.message.strip()):
            from .db import Ledger
            config = GatewayConfig.from_env()
            campaign = Ledger(config.database).campaign_by_name(args.campaign_id.strip())
            if campaign is not None:
                args.file = args.file or Path(campaign["source_file"])
                args.sheet = args.sheet or str(campaign["sheet"])
                args.column = args.column or str(campaign["number_column"])
                args.message = args.message or str(campaign["message"])
                print(f"Loaded campaign defaults from registry: {args.campaign_id}", flush=True)
        if not args.file:
            raise ValueError("--file is required; create/select a campaign or provide a file")
        if not args.column.strip():
            raise ValueError("--column is required; create/select a campaign or provide a number column")
        if args.execute and not args.consent_confirmed:
            raise ValueError("Execute requires --consent-confirmed. Do not message purchased, scraped, DNCR, or uninformed recipients.")
        for name in ("minute_limit", "hour_limit", "day_limit"):
            if getattr(args, name) <= 0:
                raise ValueError(f"--{name.replace('_', '-')} must be positive")
        if args.max_attempts < 1:
            raise ValueError("--max-attempts must be positive")
        if args.watch_timeout_seconds < 1:
            raise ValueError("--watch-timeout-seconds must be positive")
        if args.watch_poll_seconds < 5:
            raise ValueError("--watch-poll-seconds must be at least 5")
        if args.retry_failed and args.delay_seconds < 15:
            raise ValueError("Failed-message retries require at least 15 seconds between recipients")
        if not args.message.strip():
            raise ValueError("Message cannot be blank")
        return run_campaign(path=args.file, sheet=args.sheet, column=args.column, message=args.message, campaign_id=args.campaign_id, country_code=args.country_code, minute_limit=args.minute_limit, hour_limit=args.hour_limit, day_limit=args.day_limit, delay_seconds=args.delay_seconds, sim_number=args.sim_number, ttl=args.ttl, retry_failed=args.retry_failed, max_attempts=args.max_attempts, execute=args.execute, watch_until_complete=args.watch_until_complete, watch_timeout_seconds=args.watch_timeout_seconds, watch_poll_seconds=args.watch_poll_seconds)
    except httpx.HTTPStatusError as exc:
        response = exc.response
        print(
            f"ERROR: SMSGate returned HTTP {response.status_code} for "
            f"{response.request.method} {response.request.url}. "
            "Check the private-server /api prefix, credentials, and server logs."
        )
        return 3
    except httpx.HTTPError as exc:
        print(f"ERROR: could not communicate with SMSGate: {exc}")
        return 3
    except (OSError, ValueError, KeyError) as exc:
        print(f"ERROR: {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

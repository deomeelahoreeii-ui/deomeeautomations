from __future__ import annotations

import argparse
import json
import sys
import uuid
from typing import Any

from automation_core.config import get_settings
from automation_core.database import session_scope
from crm_integrations.frappe_helpdesk.service import ComplaintHelpdeskSyncService


def _print(data: Any, *, compact: bool = False) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=None if compact else 2, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m crm_integrations.frappe_helpdesk.cli",
        description="Manage one-way CRM complaint synchronization to Frappe Helpdesk.",
    )
    parser.add_argument("--compact", action="store_true", help="Print compact JSON")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("health")
    sub.add_parser("statistics")
    preview = sub.add_parser("preview")
    preview.add_argument("--limit", type=int, default=200)
    sub.add_parser("bootstrap")
    sync = sub.add_parser("sync")
    sync.add_argument("--limit", type=int, default=100)
    sync.add_argument("--force", action="store_true")
    sync_case = sub.add_parser("sync-case")
    sync_case.add_argument("case_id", type=uuid.UUID)
    sync_case.add_argument("--force", action="store_true")
    events = sub.add_parser("events")
    events.add_argument("--limit", type=int, default=100)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    try:
        with session_scope() as session:
            service = ComplaintHelpdeskSyncService(session, settings)
            if args.command == "health":
                result = service.health()
            elif args.command == "statistics":
                result = service.statistics()
            elif args.command == "preview":
                result = service.preview(limit=args.limit)
            elif args.command == "bootstrap":
                result = service.bootstrap()
            elif args.command == "sync":
                result = service.sync_many(limit=args.limit, force=args.force)
            elif args.command == "sync-case":
                result = service.sync_case(args.case_id, force=args.force)
            elif args.command == "events":
                result = {"items": service.recent_events(limit=args.limit)}
            else:
                raise AssertionError(args.command)
        _print(result, compact=args.compact)
        if isinstance(result, dict):
            if result.get("status") in {"failed", "not_configured"}:
                return 2
            counts = result.get("counts")
            if isinstance(counts, dict) and int(counts.get("failed", 0)):
                return 2
        return 0
    except Exception as exc:
        _print({"status": "error", "error": f"{type(exc).__name__}: {exc}"}, compact=args.compact)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

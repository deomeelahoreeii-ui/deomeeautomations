from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

from automation_core.config import get_settings
from automation_core.database import session_scope
from crm_integrations.frappe_helpdesk.service import ComplaintHelpdeskSyncService


def _print(data: Any, *, compact: bool = False) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=None if compact else 2, default=str))


def _write_private_text(path: str, value: str) -> None:
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(value.strip() + "\n", encoding="utf-8")
    os.chmod(target, 0o600)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m crm_integrations.frappe_helpdesk.cli",
        description="Manage safe CRM complaint synchronization to Frappe Helpdesk.",
    )
    parser.add_argument("--compact", action="store_true", help="Print compact JSON")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("health")
    sub.add_parser("statistics")
    preview = sub.add_parser("preview")
    preview.add_argument("--limit", type=int, default=200)
    preview.add_argument(
        "--token-out",
        help="Write the signed exact-selection preview token to this private file",
    )
    preview.add_argument(
        "--show-token",
        action="store_true",
        help="Print the short-lived executable token in JSON output",
    )
    sub.add_parser("bootstrap")
    sync = sub.add_parser("sync")
    sync.add_argument("--limit", type=int, default=100)
    sync.add_argument("--force", action="store_true")
    sync.add_argument("--batch-token")
    sync.add_argument("--batch-token-file")
    sync.add_argument("--case-id", action="append", type=uuid.UUID, default=[])
    sync_case = sub.add_parser("sync-case")
    sync_case.add_argument("case_id", type=uuid.UUID)
    sync_case.add_argument("--force", action="store_true")
    events = sub.add_parser("events")
    events.add_argument("--limit", type=int, default=100)
    audit = sub.add_parser("audit-links")
    audit.add_argument("--limit", type=int, default=200)
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
                token = str(result.get("batch_token") or "")
                if args.token_out:
                    if not token:
                        raise RuntimeError(
                            "Preview did not produce an executable token; check integration configuration"
                        )
                    _write_private_text(args.token_out, token)
                if not args.show_token:
                    result["batch_token"] = None
                    result["batch_token_available"] = bool(token)
                    if args.token_out:
                        result["batch_token_file"] = str(
                            Path(args.token_out).expanduser().resolve()
                        )
            elif args.command == "bootstrap":
                result = service.bootstrap()
            elif args.command == "sync":
                token = str(args.batch_token or "").strip() or None
                if args.batch_token_file:
                    token = Path(args.batch_token_file).expanduser().read_text(encoding="utf-8").strip()
                result = service.sync_many(
                    case_ids=args.case_id or None,
                    preview_token=token,
                    limit=args.limit,
                    force=args.force,
                )
            elif args.command == "sync-case":
                result = service.sync_case(args.case_id, force=args.force)
            elif args.command == "events":
                result = {"items": service.recent_events(limit=args.limit)}
            elif args.command == "audit-links":
                result = service.audit_links(limit=args.limit)
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
        payload: dict[str, Any] = {
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
        }
        changes = getattr(exc, "changes", None)
        if changes is not None:
            payload["changes"] = changes
        _print(payload, compact=args.compact)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

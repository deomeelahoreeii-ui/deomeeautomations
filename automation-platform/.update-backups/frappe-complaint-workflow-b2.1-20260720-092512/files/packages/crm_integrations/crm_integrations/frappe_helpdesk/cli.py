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
from crm_integrations.frappe_helpdesk.routing import ComplaintHelpdeskRoutingService
from crm_integrations.frappe_helpdesk.service import ComplaintHelpdeskSyncService
from crm_integrations.frappe_helpdesk.workflow import ComplaintHelpdeskWorkflowService


def _print(data: Any, *, compact: bool = False) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=None if compact else 2, default=str))


def _write_private_text(path: str, value: str) -> None:
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(value.strip() + "\n", encoding="utf-8")
    os.chmod(target, 0o600)


def _read_case_ids(path: str | None) -> list[uuid.UUID]:
    if not path:
        return []
    data = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    values = data.get("selected_case_ids", data) if isinstance(data, dict) else data
    if not isinstance(values, list):
        raise ValueError("Case ID file must contain a JSON list or selected_case_ids")
    return [uuid.UUID(str(value)) for value in values]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m crm_integrations.frappe_helpdesk.cli",
        description="Manage CRM complaint synchronization and Helpdesk workflow.",
    )
    parser.add_argument("--compact", action="store_true", help="Print compact JSON")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("health")
    sub.add_parser("statistics")
    preview = sub.add_parser("preview")
    preview.add_argument("--limit", type=int, default=200)
    preview.add_argument("--token-out")
    preview.add_argument("--show-token", action="store_true")
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

    sub.add_parser("bootstrap-teams")
    route_preview = sub.add_parser("route-preview")
    route_preview.add_argument("--limit", type=int, default=200)
    route_preview.add_argument("--token-out")
    route_preview.add_argument("--show-token", action="store_true")
    route = sub.add_parser("route")
    route.add_argument("--batch-token")
    route.add_argument("--batch-token-file")
    route.add_argument("--force", action="store_true")
    sub.add_parser("routing-statistics")

    workflow_preview = sub.add_parser("workflow-preview")
    workflow_preview.add_argument("--limit", type=int, default=200)
    workflow_preview.add_argument("--selection-out")
    workflow_pull = sub.add_parser("pull-workflow")
    workflow_pull.add_argument("--limit", type=int, default=200)
    workflow_pull.add_argument("--case-id", action="append", type=uuid.UUID, default=[])
    workflow_pull.add_argument("--selection-file")
    pull_case = sub.add_parser("pull-case")
    pull_case.add_argument("case_id", type=uuid.UUID)
    sub.add_parser("workflow-statistics")
    workflow_audit = sub.add_parser("workflow-audit")
    workflow_audit.add_argument("--limit", type=int, default=200)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    try:
        with session_scope() as session:
            sync_service = ComplaintHelpdeskSyncService(session, settings)
            routing_service = ComplaintHelpdeskRoutingService(session, settings)
            workflow_service = ComplaintHelpdeskWorkflowService(session, settings)
            if args.command == "health":
                result = sync_service.health()
            elif args.command == "statistics":
                result = sync_service.statistics()
            elif args.command == "preview":
                result = sync_service.preview(limit=args.limit)
                token = str(result.get("batch_token") or "")
                if args.token_out:
                    _write_private_text(args.token_out, token)
                if not args.show_token:
                    result["batch_token"] = None
                    result["batch_token_available"] = bool(token)
            elif args.command == "bootstrap":
                result = sync_service.bootstrap()
            elif args.command == "sync":
                token = str(args.batch_token or "").strip() or None
                if args.batch_token_file:
                    token = Path(args.batch_token_file).expanduser().read_text(encoding="utf-8").strip()
                result = sync_service.sync_many(case_ids=args.case_id or None, preview_token=token, limit=args.limit, force=args.force)
            elif args.command == "sync-case":
                result = sync_service.sync_case(args.case_id, force=args.force)
            elif args.command == "events":
                result = {"items": sync_service.recent_events(limit=args.limit)}
            elif args.command == "audit-links":
                result = sync_service.audit_links(limit=args.limit)
            elif args.command == "bootstrap-teams":
                result = routing_service.bootstrap_teams()
            elif args.command == "route-preview":
                result = routing_service.preview(limit=args.limit)
                token = str(result.get("batch_token") or "")
                if args.token_out:
                    _write_private_text(args.token_out, token)
                if not args.show_token:
                    result["batch_token"] = None
                    result["batch_token_available"] = bool(token)
            elif args.command == "route":
                token = str(args.batch_token or "").strip()
                if args.batch_token_file:
                    token = Path(args.batch_token_file).expanduser().read_text(encoding="utf-8").strip()
                if not token:
                    raise RuntimeError("Routing requires an approved preview token")
                result = routing_service.apply(token, force=args.force)
            elif args.command == "routing-statistics":
                result = routing_service.statistics()
            elif args.command == "workflow-preview":
                result = workflow_service.preview(limit=args.limit)
                if args.selection_out:
                    target = Path(args.selection_out).expanduser().resolve()
                    target.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
                    os.chmod(target, 0o600)
                    result["selection_file"] = str(target)
            elif args.command == "pull-workflow":
                case_ids = [*args.case_id, *_read_case_ids(args.selection_file)]
                result = workflow_service.pull_many(case_ids=case_ids or None, limit=args.limit)
            elif args.command == "pull-case":
                result = workflow_service.pull_case(args.case_id)
            elif args.command == "workflow-statistics":
                result = workflow_service.statistics()
            elif args.command == "workflow-audit":
                result = workflow_service.audit(limit=args.limit)
            else:
                raise AssertionError(args.command)
        _print(result, compact=args.compact)
        counts = result.get("counts") if isinstance(result, dict) else None
        return 2 if isinstance(counts, dict) and int(counts.get("failed", 0)) else 0
    except Exception as exc:
        payload: dict[str, Any] = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
        if getattr(exc, "changes", None) is not None:
            payload["changes"] = exc.changes
        _print(payload, compact=args.compact)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

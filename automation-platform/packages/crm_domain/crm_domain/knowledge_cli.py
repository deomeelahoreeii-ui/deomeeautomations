from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from automation_core.database import session_scope
from crm_domain.knowledge import ComplaintKnowledgeArchive, KnowledgeFilters


def _date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export approved complaint/reply knowledge files.")
    parser.add_argument("--format", choices=["zip", "xlsx", "csv", "json", "md", "txt"], default="zip")
    parser.add_argument("--output", required=True)
    parser.add_argument("--category", default="")
    parser.add_argument("--sub-category", default="")
    parser.add_argument("--source-system", default="")
    parser.add_argument("--search", default="")
    parser.add_argument("--reply-scope", choices=["approved", "with_reply", "awaiting", "all"], default="approved")
    parser.add_argument("--ai-eligible-only", action="store_true")
    parser.add_argument("--date-from", type=_date)
    parser.add_argument("--date-to", type=_date)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    filters = KnowledgeFilters(
        category=args.category.strip(),
        sub_category=args.sub_category.strip(),
        source_system=args.source_system.strip(),
        search=args.search.strip(),
        reply_scope=args.reply_scope,
        ai_eligible_only=args.ai_eligible_only,
        date_from=args.date_from,
        date_to=args.date_to,
    )
    target = Path(args.output).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    with session_scope() as session:
        archive = ComplaintKnowledgeArchive(session)
        content, media_type, suggested = archive.render(args.format, filters)
        target.write_bytes(content)
        records = archive.records(filters)
    print(json.dumps({
        "status": "exported",
        "output": str(target),
        "format": args.format,
        "media_type": media_type,
        "suggested_filename": suggested,
        "records": len(records),
        "filters": filters.public_dict(),
    }, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

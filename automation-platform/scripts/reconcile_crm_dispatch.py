#!/usr/bin/env python3
"""Repair CRM delivery roll-ups and retry eligible Paperless status transitions."""

from __future__ import annotations

import argparse
import json
import uuid

from sqlalchemy import select
from sqlmodel import Session

from automation_core.config import Settings
from automation_core.database import engine
from crm_domain.dispatch import CrmDispatchService
from crm_domain.models import CrmDispatchBatch


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    target = value.add_mutually_exclusive_group(required=True)
    target.add_argument("--batch", help="Dispatch batch UUID or batch number")
    target.add_argument(
        "--all-terminal",
        action="store_true",
        help="Reconcile all sending or terminal dispatch batches",
    )
    return value


def batch_ids(session: Session, batch: str | None, all_terminal: bool) -> list[uuid.UUID]:
    if all_terminal:
        return list(
            session.scalars(
                select(CrmDispatchBatch.id).where(
                    CrmDispatchBatch.status.in_(
                        ["sending", "completed", "completed_with_errors", "failed"]
                    )
                )
            ).all()
        )
    assert batch is not None
    try:
        batch_id = uuid.UUID(batch)
        found = session.get(CrmDispatchBatch, batch_id)
    except ValueError:
        found = session.scalar(
            select(CrmDispatchBatch).where(CrmDispatchBatch.batch_number == batch)
        )
    if found is None:
        raise SystemExit(f"CRM dispatch batch was not found: {batch}")
    return [found.id]


def main() -> None:
    arguments = parser().parse_args()
    settings = Settings()
    results: list[dict[str, object]] = []
    with Session(engine) as session:
        targets = batch_ids(session, arguments.batch, arguments.all_terminal)
        service = CrmDispatchService(session, settings)
        for batch_id in targets:
            result = service.refresh(batch_id)
            results.append(
                {
                    "batch_id": str(batch_id),
                    "batch_number": result["batch"]["batch_number"],
                    "batch_status": result["batch"]["status"],
                    "reconciliation": result.get("reconciliation") or {},
                }
            )
    print(json.dumps({"batches": results}, indent=2, default=str))


if __name__ == "__main__":
    main()

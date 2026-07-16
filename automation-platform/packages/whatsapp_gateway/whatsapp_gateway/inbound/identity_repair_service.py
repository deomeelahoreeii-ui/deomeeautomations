
from __future__ import annotations

import csv
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from sqlmodel import Session, select

from automation_core.time import utcnow
from whatsapp_gateway.inbound.identity_index import build_contact_identity_index
from whatsapp_gateway.inbound.identity_resolution import resolve_message_contact_id
from whatsapp_gateway.inbound.identity_types import IdentityRepairRow
from whatsapp_gateway.models import WhatsAppInboundMessage

def repair_inbound_message_identities(
    session: Session,
    *,
    apply: bool,
    output_dir: Path,
) -> dict[str, object]:
    """Audit and optionally repair contact ownership for inbound messages.

    Scans never assign identities.  This explicit service derives ownership only
    from account-scoped phone/LID aliases.  Group JIDs are never treated as
    contact identities.
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    identity_index = build_contact_identity_index(session)
    messages = session.exec(
        select(WhatsAppInboundMessage)
        .where(WhatsAppInboundMessage.from_me.is_(False))
        .order_by(WhatsAppInboundMessage.message_timestamp, WhatsAppInboundMessage.id)
    ).all()

    counts = {
        "examined": 0,
        "unchanged": 0,
        "assigned": 0,
        "corrected": 0,
        "cleared": 0,
        "ambiguous": 0,
        "unresolved": 0,
    }
    rows: list[IdentityRepairRow] = []

    for message in messages:
        counts["examined"] += 1
        previous = message.directory_contact_id
        resolved, resolution = resolve_message_contact_id(message, identity_index)
        if resolution in {"ambiguous", "unresolved"}:
            counts[resolution] += 1

        if previous == resolved:
            action = "unchanged"
            counts["unchanged"] += 1
        elif previous is None and resolved is not None:
            action = "assigned"
            counts["assigned"] += 1
        elif previous is not None and resolved is not None:
            action = "corrected"
            counts["corrected"] += 1
        else:
            action = "cleared"
            counts["cleared"] += 1

        rows.append(
            IdentityRepairRow(
                message_row_id=str(message.id),
                account_id=str(message.account_id),
                message_id=message.message_id,
                chat_scope=message.chat_scope,
                remote_jid=message.remote_jid,
                participant_jid=message.participant_jid,
                sender_jid=message.sender_jid,
                previous_contact_id=str(previous) if previous else None,
                resolved_contact_id=str(resolved) if resolved else None,
                resolution=resolution,
                action=action,
            )
        )

        if apply and previous != resolved:
            message.directory_contact_id = resolved
            message.last_ingested_at = utcnow()
            session.add(message)

    changed_rows = [row for row in rows if row.action != "unchanged"]
    backup_path = output_dir / f"identity-repair-backup-{timestamp}.json"
    report_json = output_dir / f"identity-repair-report-{timestamp}.json"
    report_csv = output_dir / f"identity-repair-report-{timestamp}.csv"

    backup_path.write_text(
        json.dumps(
            {
                "created_at": utcnow().isoformat(),
                "apply": apply,
                "rows": [asdict(row) for row in changed_rows],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    report_json.write_text(
        json.dumps(
            {
                "created_at": utcnow().isoformat(),
                "apply": apply,
                "counts": counts,
                "rows": [asdict(row) for row in rows],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fieldnames = list(asdict(rows[0]).keys()) if rows else list(IdentityRepairRow.__annotations__)
    with report_csv.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))

    if apply:
        session.commit()

    return {
        "apply": apply,
        "counts": counts,
        "backup_path": str(backup_path),
        "report_json": str(report_json),
        "report_csv": str(report_csv),
    }

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from sqlmodel import Session

from automation_core.database import engine
from whatsapp_gateway.identity_repair import repair_inbound_message_identities


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit or repair WhatsApp inbound directory_contact_id values using "
            "account-scoped phone JIDs and LID aliases."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes. Without this flag the command is a read-only audit.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/identity-repair"),
        help="Directory for the backup and audit reports.",
    )
    args = parser.parse_args()

    with Session(engine) as session:
        result = repair_inbound_message_identities(
            session,
            apply=args.apply,
            output_dir=args.output_dir.resolve(),
        )

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

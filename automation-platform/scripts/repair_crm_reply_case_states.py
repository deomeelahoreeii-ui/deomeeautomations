from __future__ import annotations

import argparse
import json
import uuid

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.database import session_scope
from crm_domain.repairs import ReplyCaseStateRepairError, repair_reply_import_case_states


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dry-run or apply the audited CRM reply-case state repair."
    )
    parser.add_argument("--batch-id", type=uuid.UUID, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--actor", default="reply-state-repair")
    args = parser.parse_args()
    try:
        with session_scope() as session:
            result = repair_reply_import_case_states(
                session,
                args.batch_id,
                apply=args.apply,
                actor=args.actor,
            )
    except ReplyCaseStateRepairError as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

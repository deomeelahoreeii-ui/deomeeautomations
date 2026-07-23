from __future__ import annotations

import argparse
import json

from sqlmodel import Session

from automation_core.database import engine
from crm_domain.repairs import audit_upward_submission_claim_backfill

# Import every model package before opening the session so relationship and
# metadata configuration matches the running API.
import automation_core.models  # noqa: F401, E402
import master_data.models  # noqa: F401, E402
import whatsapp_gateway.models  # noqa: F401, E402


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit canonical upward-submission claims created by the legacy backfill."
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--actor", default="dispatch-claim-backfill-audit")
    args = parser.parse_args()
    with Session(engine) as session:
        result = audit_upward_submission_claim_backfill(session, apply=args.apply, actor=args.actor)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

from __future__ import annotations

from main import configure_logging
from crm.reply_knowledge import sync_manual_replies_main


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(sync_manual_replies_main())

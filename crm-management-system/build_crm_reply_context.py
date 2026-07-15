from __future__ import annotations

from main import configure_logging
from crm.reply_knowledge import build_context_main


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(build_context_main())

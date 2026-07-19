#!/usr/bin/env python3
"""Check that the configured NATS endpoint supports the required JetStream API."""

from __future__ import annotations

from automation_core.nats_health import main


if __name__ == "__main__":
    raise SystemExit(main())

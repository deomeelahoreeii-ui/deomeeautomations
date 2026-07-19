"""NATS and JetStream readiness checks shared by development tooling."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence

import nats

from automation_core.config import get_settings


async def check_nats(url: str) -> None:
    """Connect to NATS and require a working JetStream account."""
    client = await nats.connect(
        url,
        connect_timeout=1,
        max_reconnect_attempts=0,
    )
    try:
        await asyncio.wait_for(client.jetstream().account_info(), timeout=1)
    finally:
        await client.close()


async def wait_for_nats(url: str, attempts: int, delay: float) -> Exception | None:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            await check_nats(url)
            return None
        except Exception as exc:  # NATS exposes several transport/API errors.
            last_error = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(delay)
    return last_error


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="NATS URL; defaults to WHATSAPP_NATS_URL")
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--delay", type=float, default=1.0)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    if args.attempts < 1:
        parser.error("--attempts must be at least 1")
    if args.delay < 0:
        parser.error("--delay cannot be negative")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    url = args.url or get_settings().whatsapp_nats_url
    error = asyncio.run(wait_for_nats(url, args.attempts, args.delay))
    if error is not None:
        if not args.quiet:
            print(f"NATS with JetStream is not ready at {url}: {error}", file=sys.stderr)
        return 1
    if not args.quiet:
        print(f"NATS with JetStream is ready at {url}.")
    return 0

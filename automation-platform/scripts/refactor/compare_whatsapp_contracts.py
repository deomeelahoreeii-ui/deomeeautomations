#!/usr/bin/env python3
"""Compare two deterministic WhatsApp contract snapshots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    args = parser.parse_args()

    before = json.loads(args.before.read_text(encoding="utf-8"))
    after = json.loads(args.after.read_text(encoding="utf-8"))
    if before != after:
        before_text = json.dumps(before, indent=2, sort_keys=True).splitlines()
        after_text = json.dumps(after, indent=2, sort_keys=True).splitlines()
        import difflib

        diff = "\n".join(
            difflib.unified_diff(
                before_text,
                after_text,
                fromfile=str(args.before),
                tofile=str(args.after),
                lineterm="",
            )
        )
        raise SystemExit(f"WhatsApp contract changed during refactor:\n{diff}")
    print("WhatsApp contract snapshots match.")


if __name__ == "__main__":
    main()

#!/usr/bin/env bash
set -euo pipefail

if curl -fsS http://127.0.0.1:8787/health; then
  echo
  exit 0
fi

echo "Automation Control API is not healthy on http://127.0.0.1:8787" >&2
exit 1

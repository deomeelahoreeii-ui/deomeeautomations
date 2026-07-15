#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ ! -f .env ]; then
  echo ".env not found. Start the Control API setup first." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
. ./.env
set +a

base_url="${CONTROL_API_PUBLIC_URL:-https://hustle03.tailbc3cfc.ts.net/control-api}"
if [ -z "${CONTROL_API_TOKEN:-}" ]; then
  echo "CONTROL_API_TOKEN is missing from .env" >&2
  exit 1
fi

printf '%s#token=%s\n' "$base_url" "$CONTROL_API_TOKEN"

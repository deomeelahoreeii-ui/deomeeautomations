#!/usr/bin/env bash
set -Eeuo pipefail

PORT="${WWEBJS_DEBUG_PORT:-9222}"
PROFILE_DIR="${WWEBJS_BRAVE_PROFILE_DIR:-$HOME/.local/share/deomee-wwebjs-brave}"
BRAVE="${WWEBJS_BROWSER_EXECUTABLE:-}"

if [[ -z "$BRAVE" ]]; then
  for candidate in /usr/bin/brave /usr/bin/brave-browser; do
    if [[ -x "$candidate" ]]; then BRAVE="$candidate"; break; fi
  done
fi
[[ -x "$BRAVE" ]] || { echo "Brave executable was not found." >&2; exit 1; }
mkdir -p "$PROFILE_DIR"
exec "$BRAVE" \
  --remote-debugging-port="$PORT" \
  --user-data-dir="$PROFILE_DIR" \
  --new-window https://web.whatsapp.com/

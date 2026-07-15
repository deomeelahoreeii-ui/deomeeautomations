#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ -f data/server.pid ]; then
  pid="$(cat data/server.pid)"
  if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid"
    rm -f data/server.pid
    echo "Automation Control API stopped."
    exit 0
  fi
fi

mapfile -t pids < <(pgrep -f "uvicorn app.main:app --host 127.0.0.1 --port 8787" || true)
if [ "${#pids[@]}" -eq 0 ]; then
  echo "Automation Control API is not running."
  exit 0
fi

kill "${pids[@]}"
rm -f data/server.pid
echo "Automation Control API stopped."

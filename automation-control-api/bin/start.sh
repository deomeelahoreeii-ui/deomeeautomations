#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mkdir -p data

if curl -fsS http://127.0.0.1:8787/health >/dev/null 2>&1; then
  echo "Automation Control API is already healthy on http://127.0.0.1:8787"
  exit 0
fi

: > data/server.log
setsid -f ../antidengue/.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8787 > data/server.log 2>&1 < /dev/null

for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8787/health >/dev/null 2>&1; then
    pgrep -f "uvicorn app.main:app --host 127.0.0.1 --port 8787" | head -n 1 > data/server.pid || true
    echo "Automation Control API started on http://127.0.0.1:8787"
    exit 0
  fi
  sleep 0.5
done

echo "Automation Control API did not become healthy. Last log lines:" >&2
tail -n 80 data/server.log >&2 || true
exit 1

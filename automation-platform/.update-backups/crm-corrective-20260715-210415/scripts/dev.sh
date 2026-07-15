#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
UV="${UV:-/home/ahmad/.local/bin/uv}"
NODE_BIN="${NODE_BIN:-/home/ahmad/.nvm/versions/node/v24.15.0/bin}"
pids=()

cleanup() {
  trap - EXIT INT TERM
  echo
  echo "Stopping automation-platform services..."
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait "${pids[@]:-}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

[[ -x "$UV" ]] || { echo "Error: uv is not executable at $UV" >&2; exit 1; }
[[ -d "$ROOT/apps/web" ]] || { echo "Error: keep dev.sh in automation-platform/scripts" >&2; exit 1; }
[[ -x "$NODE_BIN/node" ]] || { echo "Error: Node.js was not found at $NODE_BIN/node" >&2; exit 1; }

if command -v ss >/dev/null 2>&1 && ! ss -ltnH 'sport = :5672' | grep -q .; then
  echo "Error: RabbitMQ is not listening on 127.0.0.1:5672." >&2
  echo "Start it with: sudo systemctl enable --now rabbitmq.service" >&2
  exit 1
fi

cd "$ROOT"

echo "Starting FastAPI..."
"$UV" run uvicorn automation_api.main:app --reload --port 8020 &
pids+=("$!")

echo "Starting one Celery worker for AntiDengue and CRM..."
"$UV" run celery \
  -A automation_worker.main.celery_app worker \
  --queues=antidengue,crm \
  --concurrency=1 \
  --hostname=automation@%h \
  --without-gossip \
  --without-mingle \
  --loglevel=INFO &
pids+=("$!")

echo "Starting Astro web application..."
(
  cd "$ROOT/apps/web"
  export PATH="$NODE_BIN:$PATH"
  exec npm run dev -- --host 0.0.0.0 --port 4321
) &
pids+=("$!")

echo
echo "Automation Platform is starting:"
echo "  Web:      http://localhost:4321"
echo "  CRM:      http://localhost:4321/crm/"
echo "  CRM PDFs: http://localhost:4321/crm/pdfs/"
echo "  API:      http://localhost:8020"
echo "  API docs: http://localhost:8020/docs"
echo
echo "Press Ctrl+C to stop all services."

set +e
wait -n "${pids[@]}"
status=$?
set -e

echo
echo "A service exited with status $status. Stopping the remaining services."
exit "$status"

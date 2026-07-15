#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
WEB_DIR="$ROOT/apps/web"
WHATSAPP_DIR="$(cd -- "$ROOT/../whatsappbot" 2>/dev/null && pwd || true)"
UV="${UV:-/home/ahmad/.local/bin/uv}"
NODE_BIN="${NODE_BIN:-/home/ahmad/.nvm/versions/node/v24.15.0/bin}"
API_PORT="${API_PORT:-8020}"
WEB_PORT="${WEB_PORT:-4321}"
pids=()

info() {
  printf '\n==> %s\n' "$*"
}

fail() {
  printf '\nERROR: %s\n' "$*" >&2
  exit 1
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if ((${#pids[@]})); then
    echo
    echo "Stopping automation-platform services..."
    for pid in "${pids[@]}"; do
      kill "$pid" 2>/dev/null || true
    done
    wait "${pids[@]}" 2>/dev/null || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

port_in_use() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltnH "sport = :$port" 2>/dev/null | grep -q .
  elif command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
  else
    return 1
  fi
}

wait_for_port() {
  local port="$1"
  local seconds="$2"
  for ((i=0; i<seconds; i++)); do
    port_in_use "$port" && return 0
    sleep 1
  done
  return 1
}

wait_for_http() {
  local url="$1"
  local label="$2"
  local seconds="$3"
  if ! command -v curl >/dev/null 2>&1; then
    echo "curl is unavailable; skipping the $label HTTP readiness check."
    return 0
  fi
  for ((i=0; i<seconds; i++)); do
    if curl --fail --silent --max-time 1 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  fail "$label did not become ready at $url within ${seconds}s."
}

wait_for_platform_worker() {
  local output=""
  for _ in {1..30}; do
    output="$("$UV" run celery -A automation_worker.main.celery_app inspect registered --timeout=2 2>/dev/null || true)"
    if grep -q "crm_filters.run_sheet_filter_job" <<<"$output" \
      && grep -q "crm_filters.run_pdf_filter_job" <<<"$output" \
      && grep -q "crm_filters.run_sheet_to_pdf_job" <<<"$output" \
      && grep -q "whatsapp_gateway.build_inbound_export" <<<"$output"; then
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "$output" >&2
  fail "The Celery worker did not register the CRM and WhatsApp inbound export tasks."
}

whatsapp_worker_ready() {
  "$UV" run python - <<'PYWORKER' >/dev/null 2>&1
import asyncio
import json
import nats
from automation_core.config import get_settings

async def main():
    settings = get_settings()
    client = await nats.connect(
        settings.whatsapp_nats_url,
        connect_timeout=1,
        max_reconnect_attempts=0,
    )
    try:
        response = await client.request(
            settings.whatsapp_health_subject,
            b"{}",
            timeout=2,
        )
        payload = json.loads(response.data.decode("utf-8"))
        if not payload.get("ready"):
            raise SystemExit(1)
    finally:
        await client.close()

asyncio.run(main())
PYWORKER
}

cd "$ROOT"

info "Checking project prerequisites"
[[ -f pyproject.toml ]] || fail "pyproject.toml was not found under $ROOT"
[[ -f "$WEB_DIR/package.json" ]] || fail "apps/web/package.json was not found"
[[ -n "$WHATSAPP_DIR" && -f "$WHATSAPP_DIR/package.json" ]] \
  || fail "../whatsappbot/package.json was not found"

if [[ ! -x "$UV" ]] && command -v uv >/dev/null 2>&1; then
  UV="$(command -v uv)"
fi
[[ -x "$UV" ]] || fail "uv is not executable at $UV"

if [[ -d "$NODE_BIN" ]]; then
  export PATH="$NODE_BIN:$PATH"
fi
command -v node >/dev/null 2>&1 || fail "Node.js was not found in $NODE_BIN or PATH"
command -v npm >/dev/null 2>&1 || fail "npm was not found"

port_in_use "$API_PORT" && fail "API port $API_PORT is already in use. Stop the old development stack first."
port_in_use "$WEB_PORT" && fail "Web port $WEB_PORT is already in use. Stop the old development stack first."

if [[ ! -f .env ]]; then
  [[ -f .env.example ]] || fail ".env and .env.example are both missing"
  cp .env.example .env
  echo "Created .env from .env.example. Add your local credentials before using integrations."
fi

info "Synchronizing Python dependencies"
"$UV" sync
"$UV" run python -c "import reportlab; print('ReportLab', reportlab.Version, 'is available')"

info "Synchronizing frontend dependencies"
lock_file="$WEB_DIR/package-lock.json"
stamp_file="$WEB_DIR/node_modules/.deomee-package-lock.sha256"
current_hash=""
installed_hash=""
if [[ -f "$lock_file" ]] && command -v sha256sum >/dev/null 2>&1; then
  current_hash="$(sha256sum "$lock_file" | awk '{print $1}')"
  [[ -f "$stamp_file" ]] && installed_hash="$(cat "$stamp_file")"
fi
if [[ ! -x "$WEB_DIR/node_modules/.bin/astro" || -n "$current_hash" && "$current_hash" != "$installed_hash" ]]; then
  (
    cd "$WEB_DIR"
    if [[ -f package-lock.json ]]; then
      npm ci --no-audit --no-fund
    else
      npm install --no-audit --no-fund
    fi
    if [[ -n "$current_hash" ]]; then
      mkdir -p node_modules
      printf '%s\n' "$current_hash" > node_modules/.deomee-package-lock.sha256
    fi
  )
else
  echo "Frontend dependencies are already synchronized."
fi

info "Synchronizing WhatsApp worker dependencies"
if command -v pnpm >/dev/null 2>&1; then
  PNPM=(pnpm)
elif command -v corepack >/dev/null 2>&1; then
  PNPM=(corepack pnpm)
else
  fail "pnpm or corepack is required for whatsappbot"
fi
(
  cd "$WHATSAPP_DIR"
  "${PNPM[@]}" install --frozen-lockfile --prefer-offline
)
info "Checking Docker RabbitMQ"
COMPOSE_FILE="$ROOT/compose.yaml"
command -v docker >/dev/null 2>&1 \
  || fail "Docker is required because RabbitMQ runs in Docker."
docker compose version >/dev/null 2>&1 \
  || fail "Docker Compose is unavailable."
rabbitmq_container_id="$(
  docker compose -f "$COMPOSE_FILE" ps -q rabbitmq 2>/dev/null
)"
if [[ -z "$rabbitmq_container_id" ]]; then
  echo "RabbitMQ container is not running. Starting it..."
  docker compose -f "$COMPOSE_FILE" up -d rabbitmq
  rabbitmq_container_id="$(docker compose -f "$COMPOSE_FILE" ps -q rabbitmq)"
fi
[[ -n "$rabbitmq_container_id" ]] || fail "RabbitMQ container could not be started."
rabbitmq_ready=false
for _ in {1..30}; do
  if docker compose -f "$COMPOSE_FILE" exec -T rabbitmq \
    rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
    rabbitmq_ready=true
    break
  fi
  sleep 1
done
if [[ "$rabbitmq_ready" != true ]]; then
  docker compose -f "$COMPOSE_FILE" ps rabbitmq || true
  docker compose -f "$COMPOSE_FILE" logs --tail=80 rabbitmq || true
  fail "RabbitMQ container did not become healthy."
fi
echo "Docker RabbitMQ is healthy."

info "Checking NATS"
wait_for_port 4222 2 \
  || fail "NATS is not listening on port 4222. Start your NATS server before the development stack."

info "Applying database migrations"
"$UV" run alembic upgrade head

info "Recovering jobs left active by an earlier development process"
"$UV" run python scripts/recover_crm_jobs.py --all-active

info "Starting FastAPI"
"$UV" run uvicorn automation_api.main:app --reload --port "$API_PORT" &
pids+=("$!")

info "Starting one Celery worker for AntiDengue, CRM, and WhatsApp exports"
"$UV" run celery \
  -A automation_worker.main.celery_app worker \
  --queues=antidengue,crm,whatsapp \
  --concurrency=1 \
  --hostname=automation@%h \
  --without-gossip \
  --without-mingle \
  --loglevel=INFO &
pids+=("$!")

if whatsapp_worker_ready; then
  echo "An existing WhatsApp worker is already ready; it will be reused."
else
  info "Starting WhatsApp Node worker"
  (
    cd "$WHATSAPP_DIR"
    exec "${PNPM[@]}" start
  ) &
  pids+=("$!")
fi

info "Starting Astro web application"
(
  cd "$WEB_DIR"
  exec npm run dev -- --host 0.0.0.0 --port "$WEB_PORT"
) &
pids+=("$!")

info "Waiting for API, web UI, Celery, and WhatsApp worker"
wait_for_http "http://127.0.0.1:$API_PORT/health" "FastAPI" 30
wait_for_http "http://127.0.0.1:$WEB_PORT/whatsapp/inbound-files" "Astro" 45
wait_for_platform_worker
for _ in {1..45}; do
  whatsapp_worker_ready && break
  sleep 1
done
whatsapp_worker_ready || fail "The WhatsApp worker did not become ready."

echo
echo "Automation Platform is ready:"
echo "  Web:               http://localhost:$WEB_PORT"
echo "  CRM sheets:        http://localhost:$WEB_PORT/crm/"
echo "  CRM PDFs:          http://localhost:$WEB_PORT/crm/pdfs/"
echo "  Sheet rows to PDF: http://localhost:$WEB_PORT/crm/convert/"
echo "  Inbound WA files:  http://localhost:$WEB_PORT/whatsapp/inbound-files"
echo "  API:               http://localhost:$API_PORT"
echo "  API docs:          http://localhost:$API_PORT/docs"
echo
echo "Press Ctrl+C to stop services started by this script."

set +e
wait -n "${pids[@]}"
status=$?
set -e

echo
echo "A service exited with status $status. Stopping the remaining services."
exit "$status"

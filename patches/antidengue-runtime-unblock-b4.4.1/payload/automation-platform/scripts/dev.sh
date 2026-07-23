#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
WEB_DIR="$ROOT/apps/web"
WHATSAPP_DIR="$(cd -- "$ROOT/../whatsappbot" 2>/dev/null && pwd || true)"
WWEBJS_DIR="$(cd -- "$ROOT/../whatsapp-web-history-bridge" 2>/dev/null && pwd || true)"
UV="${UV:-/home/ahmad/.local/bin/uv}"
NODE_BIN="${NODE_BIN:-/home/ahmad/.nvm/versions/node/v24.15.0/bin}"
API_PORT="${API_PORT:-8020}"
WEB_PORT="${WEB_PORT:-4321}"
pids=()
bridge_started_by_dev=false
WWEBJS_METADATA_PATH=""
SNAPSHOT_MANAGER="$ROOT/scripts/manage_whatsapp_web_snapshot.py"

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
    terminate_process_trees "automation-platform services" 20 "${pids[@]}"
  fi
  if [[ "$bridge_started_by_dev" == true && -n "$WWEBJS_METADATA_PATH" && -f "$SNAPSHOT_MANAGER" ]]; then
    "$UV" run python "$SNAPSHOT_MANAGER" stop \
      --metadata "$WWEBJS_METADATA_PATH" --timeout 8 >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

process_is_running() {
  local pid="$1"
  local stat=""
  local state=""
  [[ -r "/proc/$pid/stat" ]] || return 1
  stat="$(<"/proc/$pid/stat")" || return 1
  stat="${stat##*) }"
  state="${stat%% *}"
  [[ -n "$state" && "$state" != Z && "$state" != X ]]
}

process_is_descendant_of() {
  local pid="$1"
  local ancestor="$2"
  local parent=""
  local stat=""

  while [[ "$pid" =~ ^[0-9]+$ && "$pid" -gt 1 ]]; do
    [[ "$pid" == "$ancestor" ]] && return 0
    [[ -r "/proc/$pid/stat" ]] || return 1
    stat="$(<"/proc/$pid/stat")" || return 1
    stat="${stat##*) }"
    stat="${stat#* }"
    parent="${stat%% *}"
    [[ -n "$parent" && "$parent" != "$pid" ]] || return 1
    pid="$parent"
  done
  return 1
}

collect_process_trees() {
  local -a queue=("$@")
  local -A seen=()
  local pid=""
  local child=""

  while ((${#queue[@]})); do
    pid="${queue[0]}"
    queue=("${queue[@]:1}")
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    [[ -z "${seen[$pid]:-}" ]] || continue
    seen["$pid"]=1
    [[ -d "/proc/$pid" ]] || continue
    printf '%s\n' "$pid"
    if [[ -r "/proc/$pid/task/$pid/children" ]]; then
      for child in $(<"/proc/$pid/task/$pid/children"); do
        queue+=("$child")
      done
    fi
  done
}

terminate_process_trees() {
  local label="$1"
  local grace_steps="$2"
  shift 2
  local -a roots=("$@")
  local -a targets=()
  local -a alive=()
  local pid=""
  local step=0
  local any_running=false

  ((${#roots[@]})) || return 0
  mapfile -t targets < <(collect_process_trees "${roots[@]}")
  ((${#targets[@]})) || return 0
  kill -TERM "${targets[@]}" 2>/dev/null || true

  for ((step=0; step<grace_steps; step++)); do
    alive=()
    for pid in "${targets[@]}"; do
      process_is_running "$pid" && alive+=("$pid")
    done
    ((${#alive[@]} == 0)) && return 0
    sleep 0.25
  done

  echo "Force-stopping unresponsive $label PID(s): ${alive[*]}"
  kill -KILL "${alive[@]}" 2>/dev/null || true
  for _ in {1..8}; do
    any_running=false
    for pid in "${alive[@]}"; do
      if process_is_running "$pid"; then
        any_running=true
        break
      fi
    done
    [[ "$any_running" == false ]] && return 0
    sleep 0.25
  done

  alive=()
  for pid in "${targets[@]}"; do
    process_is_running "$pid" && alive+=("$pid")
  done
  ((${#alive[@]} == 0)) \
    || echo "WARNING: $label PID(s) could not be stopped: ${alive[*]}" >&2
}

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

wait_for_frontend_module_graph() {
  local source=""
  local dependency_path=""
  local url="http://127.0.0.1:$WEB_PORT/src/scripts/alerts.ts"
  for ((i=0; i<30; i++)); do
    source="$(curl --fail --silent --max-time 2 "$url" 2>/dev/null || true)"
    dependency_path="$(sed -n 's#.*from "\(/node_modules/[^";]*\)".*#\1#p' <<<"$source" | head -n 1)"
    if [[ -n "$dependency_path" ]] \
      && curl --fail --silent --max-time 2 \
        "http://127.0.0.1:$WEB_PORT$dependency_path" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  fail "Astro served HTML but its frontend dependency graph is stale or incomplete. Restart after dependency synchronization failed for $url."
}

assert_frontend_generated_paths_writable() {
  local path=""
  local blocked_path=""
  local owner="$(id -u):$(id -g)"

  for path in \
    "$WEB_DIR/node_modules/.vite" \
    "$WEB_DIR/.astro" \
    "$WEB_DIR/dist"; do
    [[ -e "$path" ]] || continue
    blocked_path="$(find "$path" ! -writable -print -quit 2>/dev/null || true)"
    if [[ -n "$blocked_path" ]]; then
      fail "Frontend generated files are not writable: $blocked_path. This usually means a container wrote them as another user. Repair them with: sudo chown -R $owner '$WEB_DIR/node_modules/.vite' '$WEB_DIR/.astro' '$WEB_DIR/dist'"
    fi
  done
}

find_running_platform_worker_pids() {
  local process_dir=""
  local process_pid=""
  local process_args=""

  for process_dir in /proc/[0-9]*; do
    [[ -r "$process_dir/cmdline" ]] || continue
    process_pid="${process_dir##*/}"
    [[ "$process_dir/cwd" -ef "$ROOT" ]] 2>/dev/null || continue
    process_args="$( { tr '\0' ' ' < "$process_dir/cmdline"; } 2>/dev/null || true)"
    if [[ "$process_args" == *celery* && "$process_args" == *automation_worker.main.celery_app* ]]; then
      printf '%s\n' "$process_pid"
    fi
  done
}

find_running_platform_stack_pids() {
  local process_dir=""
  local process_pid=""
  local process_cwd=""
  local process_args=""

  for process_dir in /proc/[0-9]*; do
    [[ -r "$process_dir/cmdline" ]] || continue
    process_pid="${process_dir##*/}"
    if [[ "$process_dir/cwd" -ef "$ROOT" ]] 2>/dev/null; then
      process_cwd="$ROOT"
    elif [[ "$process_dir/cwd" -ef "$WEB_DIR" ]] 2>/dev/null; then
      process_cwd="$WEB_DIR"
    else
      continue
    fi
    process_args="$( { tr '\0' ' ' < "$process_dir/cmdline"; } 2>/dev/null || true)"
    if [[ "$process_cwd" == "$ROOT" ]]; then
      if [[ "$process_args" == *"uvicorn automation_api.main:app"* \
        || "$process_args" == *"antidengue_automation.scheduler_service"* \
        || ( "$process_args" == *celery* && "$process_args" == *automation_worker.main.celery_app* ) ]]; then
        printf '%s\n' "$process_pid"
      fi
    elif [[ "$process_cwd" == "$WEB_DIR" ]]; then
      if [[ "$process_args" == *"/astro dev"* || "$process_args" == *"npm run dev"* ]]; then
        printf '%s\n' "$process_pid"
      fi
    fi
  done
}

find_running_platform_supervisor_pids() {
  local process_dir=""
  local process_pid=""

  for process_dir in /proc/[0-9]*; do
    [[ -r "$process_dir/cmdline" ]] || continue
    process_pid="${process_dir##*/}"
    if ! [[ "$process_dir/cwd" -ef "$ROOT" ]] 2>/dev/null \
      && ! [[ "$process_dir/cwd" -ef "$SCRIPT_DIR" ]] 2>/dev/null; then
      continue
    fi
    [[ "$process_dir/exe" -ef "/proc/$$/exe" ]] 2>/dev/null || continue
    if grep -Fzxq -e "./dev.sh" -e "scripts/dev.sh" -e "./scripts/dev.sh" -e "$SCRIPT_DIR/dev.sh" \
      "$process_dir/cmdline" 2>/dev/null; then
      process_is_descendant_of "$process_pid" "$$" && continue
      printf '%s\n' "$process_pid"
    fi
  done
}

stop_existing_platform_stack() {
  local -a supervisor_pids=()
  local -a stack_pids=()
  local -a roots=()
  mapfile -t supervisor_pids < <(find_running_platform_supervisor_pids)
  mapfile -t stack_pids < <(find_running_platform_stack_pids)
  roots=("${supervisor_pids[@]}" "${stack_pids[@]}")
  ((${#roots[@]})) || return 0

  echo "Restarting existing Automation Platform stack; stopping PID tree(s): ${roots[*]}"
  terminate_process_trees "Automation Platform" 20 "${roots[@]}"
  for _ in {1..20}; do
    mapfile -t supervisor_pids < <(find_running_platform_supervisor_pids)
    mapfile -t stack_pids < <(find_running_platform_stack_pids)
    if ((${#supervisor_pids[@]} == 0 && ${#stack_pids[@]} == 0)) \
      && ! port_in_use "$API_PORT" && ! port_in_use "$WEB_PORT"; then
      return 0
    fi
    sleep 0.25
  done

  fail "The previous Automation Platform process tree did not stop completely."
}

stop_existing_platform_workers() {
  local -a stale_pids=()
  mapfile -t stale_pids < <(find_running_platform_worker_pids)
  ((${#stale_pids[@]})) || return 0
  echo "Stopping stale local Automation Platform Celery worker(s): ${stale_pids[*]}"
  kill -TERM "${stale_pids[@]}" 2>/dev/null || true
  for _ in {1..20}; do
    mapfile -t stale_pids < <(find_running_platform_worker_pids)
    ((${#stale_pids[@]} == 0)) && return 0
    sleep 0.25
  done
  echo "Force-stopping unresponsive local Celery worker(s): ${stale_pids[*]}"
  kill -KILL "${stale_pids[@]}" 2>/dev/null || true
}

wait_for_platform_worker() {
  local output=""
  for _ in {1..30}; do
    output="$("$UV" run celery -A automation_worker.main.celery_app inspect registered --timeout=2 2>/dev/null || true)"
    if grep -q "crm_filters.run_sheet_filter_job" <<<"$output" \
      && grep -q "crm_filters.run_pdf_filter_job" <<<"$output" \
      && grep -q "crm_filters.run_sheet_to_pdf_job" <<<"$output" \
      && grep -q "whatsapp_gateway.build_inbound_export" <<<"$output" \
      && grep -q "whatsapp_gateway.process_inbound_batch" <<<"$output" \
      && grep -q "whatsapp_gateway.compile_dispatch_preview.v2" <<<"$output" \
      && grep -q "crm_domain.publish_complaint_case" <<<"$output"; then
      if "$UV" run python - <<'PYWORKERCAP'
from sqlmodel import Session
from automation_core.database import engine
from automation_core.worker_runtime import require_compatible_workers
from whatsapp_gateway.previews.compiler.capabilities import local_compiler_runtime
required = local_compiler_runtime()
with Session(engine) as session:
    require_compatible_workers(session, required)
PYWORKERCAP
      then
        return 0
      fi
    fi
    sleep 1
  done
  printf '%s\n' "$output" >&2
  fail "The Celery worker did not register the CRM, Paperless publication, WhatsApp export, inbound processing tasks, and durable preview compiler capabilities."
}

whatsapp_worker_health_json() {
  "$UV" run python - <<'PYWORKER'
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
        print(response.data.decode("utf-8"))
    finally:
        await client.close()

asyncio.run(main())
PYWORKER
}

whatsapp_worker_ready() {
  local health_json=""
  health_json="$(whatsapp_worker_health_json 2>/dev/null)" || return 1
  HEALTH_JSON="$health_json" "$UV" run python - <<'PYWORKER' >/dev/null 2>&1
import json
import os

payload = json.loads(os.environ["HEALTH_JSON"])
if not payload.get("ready"):
    raise SystemExit(1)
PYWORKER
}

whatsapp_worker_inbound_ready() {
  local health_json=""
  health_json="$(whatsapp_worker_health_json 2>/dev/null)" || return 1
  HEALTH_JSON="$health_json" "$UV" run python - <<'PYWORKER' >/dev/null 2>&1
import json
import os
from pathlib import Path
import sqlite3

payload = json.loads(os.environ["HEALTH_JSON"])
inbound = payload.get("inboundCapture") or {}
history = payload.get("inboundHistory") or {}
if not payload.get("ready"):
    raise SystemExit(1)
if int(payload.get("deliveryProtocolVersion") or 0) < 2:
    raise SystemExit(1)
if not inbound.get("enabled"):
    raise SystemExit(1)
if int(inbound.get("schemaVersion") or 0) < 2:
    raise SystemExit(1)
if int(history.get("protocolVersion") or 0) < 2:
    raise SystemExit(1)
if history.get("syncFullHistory") is not True:
    raise SystemExit(1)
store_path = Path(str(inbound.get("filePath") or ""))
if not store_path.is_file():
    raise SystemExit(1)
with sqlite3.connect(f"file:{store_path}?mode=ro", uri=True) as connection:
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
required = {"inbound_messages", "inbound_attachments", "inbound_outbox"}
if not required.issubset(tables):
    raise SystemExit(1)
PYWORKER
}

whatsapp_worker_lock_file() {
  (
    cd "$WHATSAPP_DIR"
    node --input-type=module - <<'JSCONFIG'
import { config } from "./lib/config.js";
process.stdout.write(config.lockFile);
JSCONFIG
  )
}

find_running_whatsapp_worker_pids() {
  local process_dir=""
  local process_pid=""
  local process_cwd=""
  local process_args=""

  for process_dir in /proc/[0-9]*; do
    [[ -r "$process_dir/cmdline" ]] || continue
    process_pid="${process_dir##*/}"
    process_cwd="$(readlink "$process_dir/cwd" 2>/dev/null || true)"
    [[ "$process_cwd" == "$WHATSAPP_DIR" ]] || continue
    process_args="$( { tr '\0' ' ' < "$process_dir/cmdline"; } 2>/dev/null || true)"
    if [[ "$process_args" == *node* && "$process_args" == *worker.js* ]]; then
      printf '%s\n' "$process_pid"
    fi
  done
}

stop_existing_whatsapp_worker() {
  local lock_file=""
  local lock_pid=""
  local candidate_pid=""
  local -a worker_pids=()

  lock_file="$(whatsapp_worker_lock_file 2>/dev/null || true)"
  if [[ -n "$lock_file" && -f "$lock_file" ]]; then
    lock_pid="$(tr -cd '0-9' < "$lock_file")"
  fi

  mapfile -t worker_pids < <(find_running_whatsapp_worker_pids)

  # The Node worker is authoritative about stale-lock cleanup.  dev.sh must
  # not abort merely because a previous PID file survived a worker restart.
  if ((${#worker_pids[@]} == 0)); then
    if [[ -n "$lock_pid" ]] && kill -0 "$lock_pid" 2>/dev/null; then
      worker_pids+=("$lock_pid")
    else
      [[ -n "$lock_file" ]] && rm -f "$lock_file"
      echo "Removed stale WhatsApp worker lock; no live worker process was found."
      return 0
    fi
  fi

  echo "Stopping incompatible WhatsApp worker PID(s): ${worker_pids[*]}..."
  for candidate_pid in "${worker_pids[@]}"; do
    kill "$candidate_pid" 2>/dev/null || true
  done

  for _ in {1..30}; do
    local any_running=false
    for candidate_pid in "${worker_pids[@]}"; do
      if kill -0 "$candidate_pid" 2>/dev/null; then
        any_running=true
        break
      fi
    done
    if [[ "$any_running" == false ]]; then
      [[ -n "$lock_file" ]] && rm -f "$lock_file"
      return 0
    fi
    sleep 0.5
  done

  fail "The incompatible WhatsApp worker did not stop. Stop PID(s) ${worker_pids[*]} manually and run dev.sh again."
}



bridge_env_value() {
  local key="$1"
  local fallback="$2"
  local value=""
  if [[ -f "$WWEBJS_DIR/.env" ]]; then
    value="$(grep -E "^${key}=" "$WWEBJS_DIR/.env" | tail -1 | cut -d= -f2- || true)"
  fi
  printf '%s' "${value:-$fallback}"
}

whatsapp_web_bridge_health_json() {
  local bridge_worker bridge_subject
  bridge_worker="$(bridge_env_value WWEBJS_WORKER_ID default)"
  bridge_subject="$(bridge_env_value WWEBJS_HISTORY_SUBJECT whatsapp.web.inbound.history)"
  BRIDGE_WORKER="$bridge_worker" BRIDGE_SUBJECT="$bridge_subject" "$UV" run python - <<'PYBRIDGE'
import asyncio
import json
import os
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
            f"{os.environ['BRIDGE_SUBJECT']}.{os.environ['BRIDGE_WORKER']}",
            json.dumps({
                "action": "bridge_health",
                "workerId": os.environ["BRIDGE_WORKER"],
            }).encode("utf-8"),
            timeout=2,
        )
        print(response.data.decode("utf-8"))
    finally:
        await client.close()

asyncio.run(main())
PYBRIDGE
}

whatsapp_web_bridge_responder_ready() {
  local health_json=""
  health_json="$(whatsapp_web_bridge_health_json 2>/dev/null)" || return 1
  HEALTH_JSON="$health_json" "$UV" run python - <<'PYBRIDGE' >/dev/null 2>&1
import json
import os
payload = json.loads(os.environ["HEALTH_JSON"])
if payload.get("provider") != "wwebjs":
    raise SystemExit(1)
if int(payload.get("protocolVersion") or 0) < 4:
    raise SystemExit(1)
if str(payload.get("status") or "").lower() in {"failed", "disconnected", "auth_failure"}:
    raise SystemExit(1)
PYBRIDGE
}

whatsapp_web_bridge_any_responder() {
  whatsapp_web_bridge_health_json >/dev/null 2>&1
}

find_running_whatsapp_web_bridge_pids() {
  local process_dir=""
  local process_pid=""
  local process_cwd=""
  local process_args=""
  for process_dir in /proc/[0-9]*; do
    [[ -r "$process_dir/cmdline" ]] || continue
    process_pid="${process_dir##*/}"
    process_cwd="$(readlink "$process_dir/cwd" 2>/dev/null || true)"
    [[ "$process_cwd" == "$WWEBJS_DIR" ]] || continue
    process_args="$( { tr '\0' ' ' < "$process_dir/cmdline"; } 2>/dev/null || true)"
    if [[ "$process_args" == *node* && "$process_args" == *bridge.js* ]]; then
      printf '%s\n' "$process_pid"
    fi
  done
}

stop_existing_whatsapp_web_bridge() {
  local -a bridge_pids=()
  local pid=""
  mapfile -t bridge_pids < <(find_running_whatsapp_web_bridge_pids)
  if ((${#bridge_pids[@]} == 0)); then
    rm -f "$WWEBJS_DIR/data/bridge.lock"
    return 0
  fi
  echo "Stopping incompatible whatsapp-web.js bridge PID(s): ${bridge_pids[*]}..."
  for pid in "${bridge_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for _ in {1..30}; do
    local alive=false
    for pid in "${bridge_pids[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then alive=true; break; fi
    done
    [[ "$alive" == false ]] && { rm -f "$WWEBJS_DIR/data/bridge.lock"; return 0; }
    sleep 0.5
  done
  fail "The incompatible whatsapp-web.js bridge did not stop. Stop PID(s) ${bridge_pids[*]} manually."
}

validate_whatsapp_web_profile_snapshot() {
  local mode metadata_path
  mode="$(bridge_env_value WWEBJS_MODE visible_profile)"
  [[ "$mode" == "visible_profile" ]] || {
    cat >&2 <<MSG

ERROR: WWEBJS_MODE=$mode is not permitted for normal historical retrieval.

Use the managed one-page profile mode:
  1. Set WWEBJS_MODE=visible_profile in $WWEBJS_DIR/.env
  2. Close normal Brave once and run:
       cd "$WWEBJS_DIR"
       ./scripts/launch-brave-debug.sh
  3. Start dev.sh again.
MSG
    exit 1
  }

  metadata_path="$(bridge_env_value WWEBJS_VISIBLE_PROFILE_METADATA data/visible-profile.json)"
  [[ "$metadata_path" == /* ]] || metadata_path="$WWEBJS_DIR/$metadata_path"
  if [[ ! -f "$metadata_path" ]]; then
    cat >&2 <<MSG

ERROR: The private visible-profile snapshot has not been prepared.

Close normal Brave once, then run:
  cd "$WWEBJS_DIR"
  ./scripts/launch-brave-debug.sh

After it completes, run dev.sh again; whatsapp-web.js will launch and own one Brave page.
MSG
    exit 1
  fi
  [[ -f "$SNAPSHOT_MANAGER" ]] || fail "Snapshot lifecycle helper was not found: $SNAPSHOT_MANAGER"
  WWEBJS_METADATA_PATH="$metadata_path"
  "$UV" run python "$SNAPSHOT_MANAGER" validate --metadata "$WWEBJS_METADATA_PATH"
}

prepare_whatsapp_web_snapshot_for_start() {
  [[ -n "$WWEBJS_METADATA_PATH" ]] || fail "Visible-profile metadata path was not initialized."
  # Any browser still using this private snapshot is orphaned at this point:
  # a healthy existing bridge was already checked and would have been reused.
  "$UV" run python "$SNAPSHOT_MANAGER" stop \
    --metadata "$WWEBJS_METADATA_PATH" --timeout 8
  "$UV" run python "$SNAPSHOT_MANAGER" cleanup-stale \
    --metadata "$WWEBJS_METADATA_PATH"
}

print_whatsapp_web_bridge_status() {
  local health_json=""
  health_json="$(whatsapp_web_bridge_health_json 2>/dev/null)" || return 1
  HEALTH_JSON="$health_json" "$UV" run python - <<'PYBRIDGE'
import json
import os
payload = json.loads(os.environ["HEALTH_JSON"])
status = payload.get("status") or "unknown"
print(
    "WhatsApp Web history bridge: "
    f"status={status}, mode={payload.get('mode')}, "
    f"protocol={payload.get('protocolVersion')}, web={payload.get('webVersion') or 'pending'}"
)
if status == "qr":
    print(f"Pairing required. Scan QR image: {payload.get('qrPath')}")
elif not payload.get("ready"):
    print(f"Bridge is running but not ready: {payload.get('error') or status}")
PYBRIDGE
}

print_whatsapp_inbound_status() {
  local health_json=""
  health_json="$(whatsapp_worker_health_json 2>/dev/null)" || return 1
  HEALTH_JSON="$health_json" "$UV" run python - <<'PYWORKER'
import json
import os

payload = json.loads(os.environ["HEALTH_JSON"])
inbound = payload.get("inboundCapture") or {}
print(
    "WhatsApp inbound capture is ready: "
    f"schema={inbound.get('schemaVersion')}, "
    f"messages={inbound.get('messages', 0)}, "
    f"attachments={inbound.get('attachments', 0)}, "
    f"outbox_pending={inbound.get('outboxPending', 0)}"
)
print(f"WhatsApp inbound SQLite store: {inbound.get('filePath')}")
PYWORKER
}

cd "$ROOT"

info "Checking project prerequisites"
[[ -f pyproject.toml ]] || fail "pyproject.toml was not found under $ROOT"
[[ -f "$WEB_DIR/package.json" ]] || fail "apps/web/package.json was not found"
[[ -n "$WHATSAPP_DIR" && -f "$WHATSAPP_DIR/package.json" ]] \
  || fail "../whatsappbot/package.json was not found"
[[ -n "$WWEBJS_DIR" && -f "$WWEBJS_DIR/package.json" ]] \
  || fail "../whatsapp-web-history-bridge/package.json was not found"

if [[ ! -x "$UV" ]] && command -v uv >/dev/null 2>&1; then
  UV="$(command -v uv)"
fi
[[ -x "$UV" ]] || fail "uv is not executable at $UV"

if [[ -d "$NODE_BIN" ]]; then
  export PATH="$NODE_BIN:$PATH"
fi
command -v node >/dev/null 2>&1 || fail "Node.js was not found in $NODE_BIN or PATH"
command -v npm >/dev/null 2>&1 || fail "npm was not found"

stop_existing_platform_stack
port_in_use "$API_PORT" && fail "API port $API_PORT is used by a process outside this project."
port_in_use "$WEB_PORT" && fail "Web port $WEB_PORT is used by a process outside this project."

if [[ ! -f .env ]]; then
  [[ -f .env.example ]] || fail ".env and .env.example are both missing"
  cp .env.example .env
  echo "Created .env from .env.example. Add your local credentials before using integrations."
fi


bridge_env_created=false
if [[ ! -f "$WWEBJS_DIR/.env" ]]; then
  cp "$WWEBJS_DIR/.env.example" "$WWEBJS_DIR/.env"
  bridge_env_created=true
fi
platform_token="$(grep -E '^WHATSAPP_INBOUND_INGEST_TOKEN=' .env | tail -1 | cut -d= -f2- || true)"
worker_token="$(grep -E '^WA_PLATFORM_INGEST_TOKEN=' "$WHATSAPP_DIR/.env" 2>/dev/null | tail -1 | cut -d= -f2- || true)"
worker_id="$(grep -E '^WA_WORKER_ID=' "$WHATSAPP_DIR/.env" 2>/dev/null | tail -1 | cut -d= -f2- || true)"
bridge_token="${worker_token:-$platform_token}"
configured_bridge_token="$(bridge_env_value WWEBJS_PLATFORM_TOKEN '')"
configured_bridge_worker="$(bridge_env_value WWEBJS_WORKER_ID default)"
if [[ -n "$bridge_token" && ( -z "$configured_bridge_token" || "$configured_bridge_token" == "change-me" || "$configured_bridge_token" == "replace-with-the-same-long-random-secret" ) ]]; then
  sed -i "s|^WWEBJS_PLATFORM_TOKEN=.*$|WWEBJS_PLATFORM_TOKEN=$bridge_token|" "$WWEBJS_DIR/.env"
fi
if [[ -n "$worker_id" && "$worker_id" != "$configured_bridge_worker" && "$configured_bridge_worker" == "default" ]]; then
  sed -i "s|^WWEBJS_WORKER_ID=.*$|WWEBJS_WORKER_ID=$worker_id|" "$WWEBJS_DIR/.env"
fi
if [[ "$bridge_env_created" == true ]]; then
  echo "Created whatsapp-web-history-bridge/.env and copied the existing worker id/token when available."
fi
configured_bridge_token="$(bridge_env_value WWEBJS_PLATFORM_TOKEN '')"
if [[ -z "$configured_bridge_token" || "$configured_bridge_token" == "change-me" || "$configured_bridge_token" == "replace-with-the-same-long-random-secret" ]]; then
  fail "WWEBJS_PLATFORM_TOKEN is not configured. Set it to the same value as WA_PLATFORM_INGEST_TOKEN in whatsappbot/.env."
fi

validate_whatsapp_web_profile_snapshot

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
assert_frontend_generated_paths_writable

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


info "Synchronizing WhatsApp Web history bridge dependencies"
(
  cd "$WWEBJS_DIR"
  if [[ -f pnpm-lock.yaml ]]; then
    PUPPETEER_SKIP_DOWNLOAD=true "${PNPM[@]}" install --frozen-lockfile --prefer-offline
  else
    PUPPETEER_SKIP_DOWNLOAD=true "${PNPM[@]}" install --prefer-offline
  fi
)
info "Checking Docker development dependencies"
COMPOSE_FILE="$ROOT/compose.yaml"
command -v docker >/dev/null 2>&1 \
  || fail "Docker is required because RabbitMQ and NATS run in Docker."
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
echo "Docker RabbitMQ process is healthy."

info "Verifying RabbitMQ credentials used by Celery"
broker_url_file="$(mktemp)"
if ! "$UV" run python scripts/ensure_rabbitmq_broker.py \
  --compose-file "$COMPOSE_FILE" \
  --env-file "$ROOT/.env" \
  --emit-url "$broker_url_file"; then
  rm -f "$broker_url_file"
  fail "RabbitMQ is running, but the Celery application account could not authenticate."
fi
export CELERY_BROKER_URL="$(cat "$broker_url_file")"
rm -f "$broker_url_file"
echo "Docker RabbitMQ and Celery authentication are healthy."

info "Checking NATS with JetStream"
if (
  cd "$ROOT"
  "$UV" run python scripts/check_nats.py --quiet
); then
  echo "A compatible NATS server is already running; it will be reused."
else
  echo "NATS with JetStream is not ready. Starting it..."
  docker compose -f "$COMPOSE_FILE" up -d nats \
    || fail "The Docker NATS service could not be started. Check whether port ${NATS_PORT:-4222} is already occupied."
  if ! (
    cd "$ROOT"
    "$UV" run python scripts/check_nats.py --attempts 30 --delay 1
  ); then
    docker compose -f "$COMPOSE_FILE" ps nats || true
    docker compose -f "$COMPOSE_FILE" logs --tail=80 nats || true
    fail "NATS started but did not provide a healthy JetStream API."
  fi
fi

if (
  cd "$ROOT"
  "$UV" run python - <<'PYNTFY'
from automation_core.config import get_settings
raise SystemExit(0 if get_settings().ntfy_enabled else 1)
PYNTFY
); then
  info "Checking local ntfy notification server"
  docker compose -f "$COMPOSE_FILE" up -d ntfy
  ntfy_health_url="$(
    cd "$ROOT"
    "$UV" run python - <<'PYNTFYURL'
from automation_core.config import get_settings
print(f"{get_settings().ntfy_publish_url}/v1/health")
PYNTFYURL
  )"
  wait_for_http "$ntfy_health_url" "ntfy" 30
  echo "Local ntfy is healthy."
else
  info "ntfy transport disabled by NTFY_ENABLED=false"
fi

info "Stopping stale local platform workers before durable-task recovery"
stop_existing_platform_workers

info "Applying database migrations"
"$UV" run alembic upgrade head

info "Recovering jobs left active by an earlier development process"
"$UV" run python scripts/recover_crm_jobs.py --all-active
"$UV" run python scripts/recover_whatsapp_dispatches.py --all-active
"$UV" run python scripts/recover_antidengue_executions.py

info "Starting FastAPI"
"$UV" run uvicorn automation_api.main:app \
  --reload \
  --timeout-graceful-shutdown 5 \
  --port "$API_PORT" &
pids+=("$!")

info "Starting auto-reloading Celery worker for AntiDengue, previews, CRM, and WhatsApp exports"
platform_worker_command="$UV run celery -A automation_worker.main.celery_app worker --queues=antidengue,antidengue-preview-v2,crm,whatsapp --concurrency=1 --hostname=automation@%h --without-gossip --without-mingle --loglevel=INFO"
"$UV" run watchfiles --filter python --sigint-timeout 8 \
  "$platform_worker_command" packages apps/api &
pids+=("$!")

if whatsapp_worker_inbound_ready; then
  echo "An existing WhatsApp worker with delivery protocol v2 and bounded history is ready; it will be reused."
elif whatsapp_worker_ready; then
  info "Restarting an older WhatsApp worker without the required delivery/history protocol"
  stop_existing_whatsapp_worker
  (
    cd "$WHATSAPP_DIR"
    exec "${PNPM[@]}" start
  ) &
  pids+=("$!")
else
  info "Starting WhatsApp Node worker"
  (
    cd "$WHATSAPP_DIR"
    exec "${PNPM[@]}" start
  ) &
  pids+=("$!")
fi

if whatsapp_web_bridge_responder_ready; then
  echo "An existing whatsapp-web.js history bridge protocol v4 is running; it will be reused."
else
  mapfile -t existing_bridge_pids < <(find_running_whatsapp_web_bridge_pids)
  if whatsapp_web_bridge_any_responder || ((${#existing_bridge_pids[@]})); then
    info "Stopping an older, unresponsive, or incompatible whatsapp-web.js bridge"
    stop_existing_whatsapp_web_bridge
  fi
  info "Preparing the managed WhatsApp Web browser snapshot"
  prepare_whatsapp_web_snapshot_for_start
  info "Starting whatsapp-web.js history bridge protocol v4"
  (
    cd "$WWEBJS_DIR"
    exec node bridge.js
  ) &
  pids+=("$!")
  bridge_started_by_dev=true
fi

info "Starting Astro web application"
(
  cd "$WEB_DIR"
  # Astro otherwise daemonizes automatically when it detects an AI-agent
  # environment. Keep it in the foreground so this script supervises the
  # real web-server lifetime just as it does every other service.
  exec env ASTRO_DEV_BACKGROUND=0 npm run dev -- --force --host 0.0.0.0 --port "$WEB_PORT"
) &
pids+=("$!")

info "Waiting for API, web UI, Celery, and WhatsApp worker"
wait_for_http "http://127.0.0.1:$API_PORT/health" "FastAPI" 30
wait_for_http "http://127.0.0.1:$WEB_PORT/whatsapp/inbound-files" "Astro" 45
wait_for_frontend_module_graph
wait_for_platform_worker
for _ in {1..45}; do
  whatsapp_worker_inbound_ready && break
  sleep 1
done
if ! whatsapp_worker_inbound_ready; then
  echo "Latest WhatsApp worker health payload:" >&2
  whatsapp_worker_health_json >&2 || true
  fail "The WhatsApp worker became reachable, but inbound history protocol v2 is not ready."
fi
print_whatsapp_inbound_status
for _ in {1..30}; do
  whatsapp_web_bridge_responder_ready && break
  sleep 1
done
if ! whatsapp_web_bridge_responder_ready; then
  fail "The whatsapp-web.js history bridge did not register its NATS responder."
fi
print_whatsapp_web_bridge_status

if (
  cd "$ROOT"
  "$UV" run python - <<'PY'
from automation_core.config import get_settings
raise SystemExit(0 if get_settings().antidengue_scheduler_enabled else 1)
PY
); then
  info "Starting PostgreSQL-backed AntiDengue scheduler and safe dispatch orchestrator"
  (
    cd "$ROOT"
    exec "$UV" run python -m antidengue_automation.scheduler_service
  ) &
  pids+=("$!")
else
  info "AntiDengue scheduler disabled by ANTIDENGUE_SCHEDULER_ENABLED=false"
fi

echo
echo "Automation Platform is ready:"
echo "  Web:               http://localhost:$WEB_PORT"
echo "  AntiDengue plans: http://localhost:$WEB_PORT/antidengue/schedules"
echo "  AntiDengue runs:  http://localhost:$WEB_PORT/antidengue/run-history"
echo "  CRM sheets:        http://localhost:$WEB_PORT/crm/"
echo "  CRM PDFs:          http://localhost:$WEB_PORT/crm/pdfs/"
echo "  Sheet rows to PDF: http://localhost:$WEB_PORT/crm/convert/"
echo "  Inbound WA files:  http://localhost:$WEB_PORT/whatsapp/inbound-files"
echo "  History browser:   managed one-page Brave snapshot"
echo "  Snapshot refresh:  $WWEBJS_DIR/scripts/launch-brave-debug.sh"
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

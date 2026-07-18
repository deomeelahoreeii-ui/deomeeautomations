#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT/compose.yaml"
UV="${UV:-/home/ahmad/.local/bin/uv}"

cd "$ROOT"

set_env_value() {
  local key="$1"
  local value="$2"
  local target="$ROOT/.env"
  local temporary
  [[ -f "$target" ]] || cp "$ROOT/.env.example" "$target"
  temporary="$(mktemp "$ROOT/.env.ntfy.XXXXXX")"
  awk -v key="$key" -v value="$value" '
    BEGIN { replaced = 0 }
    $0 ~ "^" key "=" {
      if (!replaced) print key "=" value
      replaced = 1
      next
    }
    { print }
    END { if (!replaced) print key "=" value }
  ' "$target" > "$temporary"
  chmod --reference="$target" "$temporary"
  mv "$temporary" "$target"
}

configure_mode() {
  local mode="$1"
  local address_or_url="${2:-}"
  local port
  port="$(
    "$UV" run python - <<'PY'
from automation_core.config import get_settings
print(get_settings().ntfy_port)
PY
  )"
  local bind_address="127.0.0.1"
  local publish_url="http://127.0.0.1:${port}"
  local public_url="http://localhost:${port}"

  case "$mode" in
    local)
      ;;
    lan)
      [[ -n "$address_or_url" ]] || {
        echo "LAN mode requires the laptop's LAN IP, for example: $0 mode lan 192.168.1.20" >&2
        return 2
      }
      "$UV" run python - "$address_or_url" <<'PY'
import ipaddress
import sys

address = ipaddress.ip_address(sys.argv[1])
if not address.is_private or address.is_loopback:
    raise SystemExit("LAN bind address must be a non-loopback private IP")
PY
      bind_address="$address_or_url"
      publish_url="http://${address_or_url}:${port}"
      public_url="http://${address_or_url}:${port}"
      ;;
    tailscale|cloudflare)
      [[ "$address_or_url" == https://* ]] || {
        echo "$mode mode requires its future HTTPS public URL." >&2
        return 2
      }
      public_url="${address_or_url%/}"
      ;;
    *)
      echo "Mode must be local, lan, tailscale, or cloudflare." >&2
      return 2
      ;;
  esac

  set_env_value NTFY_EXPOSURE_MODE "$mode"
  set_env_value NTFY_BIND_ADDRESS "$bind_address"
  set_env_value NTFY_PUBLISH_URL "$publish_url"
  set_env_value NTFY_PUBLIC_BASE_URL "$public_url"
  docker compose -f "$COMPOSE_FILE" up -d --force-recreate ntfy
  wait_until_healthy
  show_configuration
  if [[ "$mode" == tailscale || "$mode" == cloudflare ]]; then
    echo "$mode configuration is saved; its exposure adapter must also be running."
  fi
}

configured_health_url() {
  "$UV" run python - <<'PY'
from automation_core.config import get_settings
print(f"{get_settings().ntfy_publish_url}/v1/health")
PY
}

wait_until_healthy() {
  local health_url
  health_url="$(configured_health_url)"
  for _ in {1..30}; do
    if curl --fail --silent --max-time 2 "$health_url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  docker compose -f "$COMPOSE_FILE" logs --tail=80 ntfy >&2 || true
  echo "ntfy did not become healthy at ${health_url}." >&2
  return 1
}

show_configuration() {
  "$UV" run python - <<'PY'
from automation_core.config import get_settings

s = get_settings()
print(f"enabled={str(s.ntfy_enabled).lower()}")
print(f"exposure_mode={s.ntfy_exposure_mode}")
print(f"publish_url={s.ntfy_publish_url}")
print(f"public_base_url={s.ntfy_public_base_url}")
print(f"antidengue_enabled={str(s.antidengue_ntfy_enabled).lower()}")
print(f"antidengue_topic={s.antidengue_ntfy_topic}")
print(f"authentication={'token' if s.ntfy_token else 'none'}")
PY
}

case "${1:-status}" in
  enable)
    set_env_value NTFY_ENABLED true
    set_env_value ANTIDENGUE_NTFY_ENABLED true
    docker compose -f "$COMPOSE_FILE" up -d ntfy
    wait_until_healthy
    show_configuration
    ;;
  disable)
    set_env_value NTFY_ENABLED false
    docker compose -f "$COMPOSE_FILE" stop ntfy
    show_configuration
    ;;
  mode)
    configure_mode "${2:-}" "${3:-}"
    ;;
  start)
    docker compose -f "$COMPOSE_FILE" up -d ntfy
    wait_until_healthy
    show_configuration
    ;;
  stop)
    docker compose -f "$COMPOSE_FILE" stop ntfy
    ;;
  restart)
    docker compose -f "$COMPOSE_FILE" up -d --force-recreate ntfy
    wait_until_healthy
    show_configuration
    ;;
  status)
    show_configuration
    docker compose -f "$COMPOSE_FILE" ps ntfy
    ;;
  test)
    "$UV" run python - <<'PY'
import requests

from automation_core.config import get_settings
from automation_core.notifications import ntfy_headers, ntfy_topic_url

s = get_settings()
if not s.ntfy_enabled:
    raise SystemExit("NTFY_ENABLED=false; enable the transport before testing")
response = requests.post(
    ntfy_topic_url(s.ntfy_publish_url, s.antidengue_ntfy_topic),
    data="Automation Platform local ntfy test",
    headers={"Title": "Automation Platform", "Tags": "white_check_mark", **ntfy_headers(s)},
    timeout=s.ntfy_timeout_seconds,
)
response.raise_for_status()
payload = response.json()
print(f"published id={payload.get('id')} topic={payload.get('topic')}")
PY
    ;;
  provision)
    username="${2:-automation}"
    topic="${3:-automation-antidengue}"
    generated_password="$(openssl rand -hex 32)"
    docker compose -f "$COMPOSE_FILE" exec -T \
      -e NTFY_PASSWORD="$generated_password" ntfy \
      ntfy user add --ignore-exists "$username" >/dev/null
    docker compose -f "$COMPOSE_FILE" exec -T ntfy \
      ntfy access "$username" "$topic" rw >/dev/null
    echo "Store this token as NTFY_TOKEN in .env and as the topic credential on subscriber devices:"
    docker compose -f "$COMPOSE_FILE" exec -T ntfy \
      ntfy token add --label automation-platform "$username"
    ;;
  *)
    echo "Usage: $0 {enable|disable|mode <local|lan|tailscale|cloudflare> [address-or-url]|start|stop|restart|status|test|provision [user] [topic]}" >&2
    exit 2
    ;;
esac

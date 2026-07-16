#!/usr/bin/env bash
set -Eeuo pipefail

PORT="${WWEBJS_DEBUG_PORT:-9222}"
BROWSER_URL="http://127.0.0.1:${PORT}"
BRAVE="${WWEBJS_BROWSER_EXECUTABLE:-}"
SOURCE_USER_DATA_DIR="${WWEBJS_BRAVE_SOURCE_USER_DATA_DIR:-$HOME/.config/BraveSoftware/Brave-Browser}"
DEBUG_USER_DATA_DIR="${WWEBJS_BRAVE_DEBUG_USER_DATA_DIR:-$HOME/.local/share/deomee-wwebjs-brave-visible}"
PROFILE_DIRECTORY="${WWEBJS_BRAVE_PROFILE_DIRECTORY:-}"
REFRESH_SNAPSHOT="${WWEBJS_REFRESH_PROFILE_SNAPSHOT:-true}"

if [[ -z "$BRAVE" ]]; then
  for candidate in /usr/bin/brave /usr/bin/brave-browser; do
    if [[ -x "$candidate" ]]; then BRAVE="$candidate"; break; fi
  done
fi
[[ -x "$BRAVE" ]] || { echo "Brave executable was not found." >&2; exit 1; }

if command -v curl >/dev/null 2>&1 && curl --fail --silent --max-time 2 "$BROWSER_URL/json/version" >/dev/null 2>&1; then
  echo "Brave remote debugging is already available at $BROWSER_URL"
  exit 0
fi

[[ -d "$SOURCE_USER_DATA_DIR" ]] || {
  echo "Brave source profile directory was not found: $SOURCE_USER_DATA_DIR" >&2
  exit 1
}

if [[ -z "$PROFILE_DIRECTORY" && -f "$SOURCE_USER_DATA_DIR/Local State" ]] && command -v python3 >/dev/null 2>&1; then
  PROFILE_DIRECTORY="$(python3 - "$SOURCE_USER_DATA_DIR/Local State" <<'PY' 2>/dev/null || true
import json, sys
try:
    state = json.load(open(sys.argv[1], encoding="utf-8"))
    print(state.get("profile", {}).get("last_used", "Default"))
except Exception:
    print("Default")
PY
)"
fi
PROFILE_DIRECTORY="${PROFILE_DIRECTORY:-Default}"
[[ -d "$SOURCE_USER_DATA_DIR/$PROFILE_DIRECTORY" ]] || {
  echo "Brave profile was not found: $SOURCE_USER_DATA_DIR/$PROFILE_DIRECTORY" >&2
  exit 1
}

running_brave_pids() {
  local process_dir=""
  local process_pid=""
  local process_exe=""
  local process_name=""
  for process_dir in /proc/[0-9]*; do
    [[ -e "$process_dir" ]] || continue
    process_pid="${process_dir##*/}"
    [[ -r "$process_dir/status" ]] || continue
    [[ "$(awk '/^Uid:/{print $2; exit}' "$process_dir/status" 2>/dev/null || true)" == "$(id -u)" ]] || continue
    process_exe="$(readlink "$process_dir/exe" 2>/dev/null || true)"
    process_name="${process_exe##*/}"
    case "$process_name" in
      brave|brave-browser|Brave|Brave-Browser) printf '%s\n' "$process_pid" ;;
    esac
  done
}

mapfile -t BRAVE_PIDS < <(running_brave_pids)
if ((${#BRAVE_PIDS[@]} > 0)); then
  cat >&2 <<MSG
Brave is already running without the required debugging endpoint (PID(s): ${BRAVE_PIDS[*]}).

To make a consistent local snapshot of the SAME profile where the WhatsApp files are visible:
  1. Save your work and close every Brave window.
  2. Confirm no Brave executable remains:
       ps -fp ${BRAVE_PIDS[*]}
  3. Run this script again.

The script will not kill Brave and will not modify your normal Brave profile.
MSG
  exit 2
fi

mkdir -p "$DEBUG_USER_DATA_DIR"
chmod 700 "$DEBUG_USER_DATA_DIR"

if [[ "$REFRESH_SNAPSHOT" == "true" ]]; then
  echo "Refreshing a private debugging snapshot from the visible Brave profile..."
  echo "Source:      $SOURCE_USER_DATA_DIR/$PROFILE_DIRECTORY"
  echo "Destination: $DEBUG_USER_DATA_DIR/$PROFILE_DIRECTORY"
  if command -v rsync >/dev/null 2>&1; then
    rsync -a --delete --delete-excluded \
      --exclude='Cache/' \
      --exclude='Code Cache/' \
      --exclude='GPUCache/' \
      --exclude='DawnCache/' \
      --exclude='GrShaderCache/' \
      --exclude='ShaderCache/' \
      --exclude='Crashpad/' \
      "$SOURCE_USER_DATA_DIR/$PROFILE_DIRECTORY/" \
      "$DEBUG_USER_DATA_DIR/$PROFILE_DIRECTORY/"
    [[ -f "$SOURCE_USER_DATA_DIR/Local State" ]] && rsync -a "$SOURCE_USER_DATA_DIR/Local State" "$DEBUG_USER_DATA_DIR/Local State"
  else
    echo "rsync is unavailable; copying the profile with cp. This can take longer."
    rm -rf "$DEBUG_USER_DATA_DIR/$PROFILE_DIRECTORY"
    cp -a "$SOURCE_USER_DATA_DIR/$PROFILE_DIRECTORY" "$DEBUG_USER_DATA_DIR/$PROFILE_DIRECTORY"
    [[ -f "$SOURCE_USER_DATA_DIR/Local State" ]] && cp -a "$SOURCE_USER_DATA_DIR/Local State" "$DEBUG_USER_DATA_DIR/Local State"
  fi
fi

rm -f \
  "$DEBUG_USER_DATA_DIR/SingletonCookie" \
  "$DEBUG_USER_DATA_DIR/SingletonLock" \
  "$DEBUG_USER_DATA_DIR/SingletonSocket"

echo "Launching the private Brave snapshot with remote debugging at $BROWSER_URL"
echo "Profile: $PROFILE_DIRECTORY"
echo "Keep this Brave process open while using WhatsApp history retrieval."

exec "$BRAVE" \
  --remote-debugging-port="$PORT" \
  --remote-debugging-address=127.0.0.1 \
  --remote-allow-origins="*" \
  --user-data-dir="$DEBUG_USER_DATA_DIR" \
  --profile-directory="$PROFILE_DIRECTORY" \
  --no-first-run \
  --no-default-browser-check \
  --new-window https://web.whatsapp.com/

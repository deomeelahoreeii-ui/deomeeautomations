#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BRIDGE_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
SOURCE_USER_DATA_DIR="${WWEBJS_BRAVE_SOURCE_USER_DATA_DIR:-$HOME/.config/BraveSoftware/Brave-Browser}"
SNAPSHOT_USER_DATA_DIR="${WWEBJS_VISIBLE_USER_DATA_DIR:-${WWEBJS_BRAVE_DEBUG_USER_DATA_DIR:-$HOME/.local/share/deomee-wwebjs-brave-visible}}"
PROFILE_DIRECTORY="${WWEBJS_VISIBLE_PROFILE_DIRECTORY:-${WWEBJS_BRAVE_PROFILE_DIRECTORY:-}}"
METADATA_PATH="${WWEBJS_VISIBLE_PROFILE_METADATA:-$BRIDGE_ROOT/data/visible-profile.json}"
REFRESH_SNAPSHOT="${WWEBJS_REFRESH_PROFILE_SNAPSHOT:-true}"
SKIP_RUNNING_CHECK="${WWEBJS_TEST_SKIP_RUNNING_BRAVE_CHECK:-false}"

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
  local process_dir="" process_pid="" process_exe="" process_name=""
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

if [[ "$SKIP_RUNNING_CHECK" != "true" ]]; then
  mapfile -t BRAVE_PIDS < <(running_brave_pids)
  if ((${#BRAVE_PIDS[@]} > 0)); then
    cat >&2 <<MSG
Brave is currently running (PID(s): ${BRAVE_PIDS[*]}).

To copy a consistent snapshot of the SAME profile where the WhatsApp files are visible:
  1. Save your work and close every Brave window.
  2. Confirm no Brave executable remains:
       ps -fp ${BRAVE_PIDS[*]}
  3. Run this script again.

The script will not kill Brave and will not modify your normal Brave profile.
MSG
    exit 2
  fi
fi

mkdir -p "$SNAPSHOT_USER_DATA_DIR" "$(dirname -- "$METADATA_PATH")"
chmod 700 "$SNAPSHOT_USER_DATA_DIR"

if [[ "$REFRESH_SNAPSHOT" == "true" ]]; then
  echo "Refreshing the private WhatsApp history snapshot from the visible Brave profile..."
  echo "Source:      $SOURCE_USER_DATA_DIR/$PROFILE_DIRECTORY"
  echo "Destination: $SNAPSHOT_USER_DATA_DIR/$PROFILE_DIRECTORY"
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
      "$SNAPSHOT_USER_DATA_DIR/$PROFILE_DIRECTORY/"
    [[ -f "$SOURCE_USER_DATA_DIR/Local State" ]] && rsync -a "$SOURCE_USER_DATA_DIR/Local State" "$SNAPSHOT_USER_DATA_DIR/Local State"
  else
    echo "rsync is unavailable; copying the profile with cp. This can take longer."
    rm -rf "$SNAPSHOT_USER_DATA_DIR/$PROFILE_DIRECTORY"
    cp -a "$SOURCE_USER_DATA_DIR/$PROFILE_DIRECTORY" "$SNAPSHOT_USER_DATA_DIR/$PROFILE_DIRECTORY"
    [[ -f "$SOURCE_USER_DATA_DIR/Local State" ]] && cp -a "$SOURCE_USER_DATA_DIR/Local State" "$SNAPSHOT_USER_DATA_DIR/Local State"
  fi
fi

rm -f \
  "$SNAPSHOT_USER_DATA_DIR/SingletonCookie" \
  "$SNAPSHOT_USER_DATA_DIR/SingletonLock" \
  "$SNAPSHOT_USER_DATA_DIR/SingletonSocket"

python3 - "$METADATA_PATH" "$SOURCE_USER_DATA_DIR" "$SNAPSHOT_USER_DATA_DIR" "$PROFILE_DIRECTORY" <<'PY'
import json, pathlib, sys
from datetime import datetime, timezone
path = pathlib.Path(sys.argv[1])
payload = {
    "schemaVersion": 1,
    "sourceUserDataDir": sys.argv[2],
    "userDataDir": sys.argv[3],
    "profileDirectory": sys.argv[4],
    "preparedAt": datetime.now(timezone.utc).isoformat(),
}
path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
chmod 600 "$METADATA_PATH"

cat <<MSG

Visible-profile snapshot prepared successfully.
Profile:  $PROFILE_DIRECTORY
Snapshot: $SNAPSHOT_USER_DATA_DIR
Metadata: $METADATA_PATH

Do not launch this snapshot manually. Start automation-platform/scripts/dev.sh.
whatsapp-web.js will launch and own exactly one Brave WhatsApp Web page from this snapshot,
which avoids the duplicate-tab detached-frame failure.
MSG

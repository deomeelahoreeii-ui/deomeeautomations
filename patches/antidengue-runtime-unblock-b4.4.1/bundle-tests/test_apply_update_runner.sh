#!/usr/bin/env bash
set -Eeuo pipefail

BUNDLE_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

mkdir -p "$TMP_ROOT/platform/tests" "$TMP_ROOT/capture"
cat > "$TMP_ROOT/fake-uv" <<'UVEOF'
#!/usr/bin/env bash
set -Eeuo pipefail
printf '%s\n' "$PWD" > "$CAPTURE_DIR/pwd"
printf '%s\n' "$@" > "$CAPTURE_DIR/args"
printf '%s\n' "${PYTHONPATH:-}" > "$CAPTURE_DIR/pythonpath"
UVEOF
chmod +x "$TMP_ROOT/fake-uv"

export APPLY_UPDATE_LIB_ONLY=1
# shellcheck source=../apply-update.sh
source "$BUNDLE_ROOT/apply-update.sh"

PLATFORM_ROOT="$TMP_ROOT/platform"
UV="$TMP_ROOT/fake-uv"
export CAPTURE_DIR="$TMP_ROOT/capture"
export UV_NO_DEV=1
export UV_NO_SYNC=1

run_project_pytest -q tests/example.py

[[ "$(cat "$CAPTURE_DIR/pwd")" == "$PLATFORM_ROOT" ]]
mapfile -t args < "$CAPTURE_DIR/args"
expected=(run --frozen --group dev python -m pytest -q tests/example.py)
[[ "${args[*]}" == "${expected[*]}" ]]
[[ "$(cat "$CAPTURE_DIR/pythonpath")" == "$PLATFORM_ROOT"* ]]

printf 'Installer pytest runner regression test passed.\n'

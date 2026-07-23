#!/usr/bin/env bash
set -Eeuo pipefail
BUNDLE_ID="antidengue-runtime-unblock-b4.4.1"
TARGET_INPUT="${1:-}"
BACKUP_INPUT="${2:-}"
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
[[ -n "$TARGET_INPUT" ]] || fail "Usage: ./rollback-update.sh /path/to/automation-platform [backup-directory]"
TARGET="$(cd -- "$TARGET_INPUT" 2>/dev/null && pwd)" || fail "Target does not exist"
if [[ -f "$TARGET/compose.yaml" && "$(basename "$TARGET")" == "automation-platform" ]]; then
  PLATFORM_ROOT="$TARGET"; WORKSPACE_ROOT="$(dirname "$TARGET")"
elif [[ -f "$TARGET/automation-platform/compose.yaml" ]]; then
  WORKSPACE_ROOT="$TARGET"; PLATFORM_ROOT="$TARGET/automation-platform"
else
  fail "Target must be automation-platform or its workspace parent"
fi
if [[ -n "$BACKUP_INPUT" ]]; then
  BACKUP_DIR="$(cd -- "$BACKUP_INPUT" 2>/dev/null && pwd)" || fail "Backup does not exist"
else
  BACKUP_DIR="$(find "$PLATFORM_ROOT/.update-backups" -maxdepth 1 -type d -name "${BUNDLE_ID}-*" -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)"
fi
[[ -n "$BACKUP_DIR" && -d "$BACKUP_DIR" ]] || fail "No backup for $BUNDLE_ID was found"
[[ "$(cat "$BACKUP_DIR/bundle-id.txt" 2>/dev/null || true)" == "$BUNDLE_ID" ]] || fail "Backup does not belong to $BUNDLE_ID"
while IFS= read -r rel; do
  [[ -n "$rel" ]] || continue
  mkdir -p "$WORKSPACE_ROOT/$(dirname "$rel")"
  cp -a "$BACKUP_DIR/files/$rel" "$WORKSPACE_ROOT/$rel"
done < "$BACKUP_DIR/backed-up-files.txt"
for list in created-files.txt runtime-created-files.txt; do
  [[ -f "$BACKUP_DIR/$list" ]] || continue
  while IFS= read -r rel; do
    [[ -n "$rel" ]] || continue
    rm -f "$WORKSPACE_ROOT/$rel"
  done < "$BACKUP_DIR/$list"
done
printf 'Rolled back %s using %s\n' "$BUNDLE_ID" "$BACKUP_DIR"

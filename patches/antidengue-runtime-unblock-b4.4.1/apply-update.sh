#!/usr/bin/env bash
set -Eeuo pipefail

BUNDLE_ID="antidengue-runtime-unblock-b4.4.1"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$SCRIPT_DIR/FILE_MANIFEST.tsv"
PAYLOAD="$SCRIPT_DIR/payload"
TARGET_INPUT="${1:-}"
RUN_FULL_TESTS="${RUN_FULL_TESTS:-1}"
UV="${UV:-}"
BACKUP_DIR=""
APPLY_STARTED=false
APPLY_SUCCEEDED=false

info() { printf '\n==> %s\n' "$*"; }
fail() { printf '\nERROR: %s\n' "$*" >&2; exit 1; }
sha256_file() { sha256sum "$1" | awk '{print $1}'; }

resolve_roots() {
  [[ -n "$TARGET_INPUT" ]] || fail "Usage: ./apply-update.sh /path/to/automation-platform"
  local candidate
  candidate="$(cd -- "$TARGET_INPUT" 2>/dev/null && pwd)" \
    || fail "Target does not exist: $TARGET_INPUT"
  if [[ -f "$candidate/compose.yaml" && "$(basename "$candidate")" == "automation-platform" ]]; then
    PLATFORM_ROOT="$candidate"
    WORKSPACE_ROOT="$(dirname "$candidate")"
  elif [[ -f "$candidate/automation-platform/compose.yaml" ]]; then
    WORKSPACE_ROOT="$candidate"
    PLATFORM_ROOT="$candidate/automation-platform"
  else
    fail "Target must be the automation-platform directory or its workspace parent."
  fi
  BRIDGE_ROOT="$WORKSPACE_ROOT/whatsapp-web-history-bridge"
  [[ -d "$BRIDGE_ROOT" ]] || fail "Sibling whatsapp-web-history-bridge directory is missing: $BRIDGE_ROOT"
}

restore_backup() {
  [[ -n "$BACKUP_DIR" && -d "$BACKUP_DIR" ]] || return 0
  printf '\nRolling back %s from %s\n' "$BUNDLE_ID" "$BACKUP_DIR" >&2
  if [[ -f "$BACKUP_DIR/backed-up-files.txt" ]]; then
    while IFS= read -r rel; do
      [[ -n "$rel" ]] || continue
      mkdir -p "$WORKSPACE_ROOT/$(dirname "$rel")"
      cp -a "$BACKUP_DIR/files/$rel" "$WORKSPACE_ROOT/$rel"
    done < "$BACKUP_DIR/backed-up-files.txt"
  fi
  for list in created-files.txt runtime-created-files.txt; do
    if [[ -f "$BACKUP_DIR/$list" ]]; then
      while IFS= read -r rel; do
        [[ -n "$rel" ]] || continue
        rm -f "$WORKSPACE_ROOT/$rel"
      done < "$BACKUP_DIR/$list"
    fi
  done
  printf 'Rollback complete.\n' >&2
}

on_exit() {
  local status=$?
  if [[ "$status" -ne 0 && "$APPLY_STARTED" == true && "$APPLY_SUCCEEDED" != true ]]; then
    restore_backup || true
  fi
  exit "$status"
}
resolve_uv() {
  if [[ -z "$UV" ]]; then
    UV="$(command -v uv || true)"
  fi
  [[ -x "$UV" ]] || fail "uv is required for verification. Set UV=/path/to/uv if needed."
}

preflight_files() {
  local rel baseline final mode target current
  local baseline_count=0 final_count=0
  : > /tmp/${BUNDLE_ID}-preflight.$$.txt
  while IFS=$'\t' read -r rel baseline final mode; do
    [[ "$rel" == "path" ]] && continue
    target="$WORKSPACE_ROOT/$rel"
    if [[ -e "$target" ]]; then
      current="$(sha256_file "$target")"
      if [[ "$current" == "$final" ]]; then
        final_count=$((final_count + 1))
        printf 'final\t%s\n' "$rel" >> /tmp/${BUNDLE_ID}-preflight.$$.txt
      elif [[ "$baseline" != "MISSING" && "$current" == "$baseline" ]]; then
        baseline_count=$((baseline_count + 1))
        printf 'baseline\t%s\n' "$rel" >> /tmp/${BUNDLE_ID}-preflight.$$.txt
      else
        printf 'unknown\t%s\t%s\n' "$rel" "$current" >> /tmp/${BUNDLE_ID}-preflight.$$.txt
      fi
    elif [[ "$baseline" == "MISSING" ]]; then
      baseline_count=$((baseline_count + 1))
      printf 'missing\t%s\n' "$rel" >> /tmp/${BUNDLE_ID}-preflight.$$.txt
    else
      printf 'unknown\t%s\tMISSING\n' "$rel" >> /tmp/${BUNDLE_ID}-preflight.$$.txt
    fi
  done < "$MANIFEST"

  if grep -q $'^unknown\t' /tmp/${BUNDLE_ID}-preflight.$$.txt; then
    cat /tmp/${BUNDLE_ID}-preflight.$$.txt >&2
    rm -f /tmp/${BUNDLE_ID}-preflight.$$.txt
    fail "Target differs from both the uploaded baseline and this bundle's final state. No files were changed."
  fi
  rm -f /tmp/${BUNDLE_ID}-preflight.$$.txt
  printf 'State: baseline=%s, final=%s, unknown=0\n' "$baseline_count" "$final_count"
  if [[ "$baseline_count" -eq 0 ]]; then
    ALREADY_APPLIED=true
  else
    ALREADY_APPLIED=false
  fi
}

create_backup() {
  local stamp rel baseline final mode target
  stamp="$(date +%Y%m%d-%H%M%S)"
  BACKUP_DIR="$PLATFORM_ROOT/.update-backups/${BUNDLE_ID}-${stamp}"
  mkdir -p "$BACKUP_DIR/files"
  : > "$BACKUP_DIR/backed-up-files.txt"
  : > "$BACKUP_DIR/created-files.txt"
  : > "$BACKUP_DIR/runtime-created-files.txt"
  printf '%s\n' "$BUNDLE_ID" > "$BACKUP_DIR/bundle-id.txt"
  while IFS=$'\t' read -r rel baseline final mode; do
    [[ "$rel" == "path" ]] && continue
    target="$WORKSPACE_ROOT/$rel"
    if [[ -e "$target" ]]; then
      mkdir -p "$BACKUP_DIR/files/$(dirname "$rel")"
      cp -a "$target" "$BACKUP_DIR/files/$rel"
      printf '%s\n' "$rel" >> "$BACKUP_DIR/backed-up-files.txt"
    else
      printf '%s\n' "$rel" >> "$BACKUP_DIR/created-files.txt"
    fi
  done < "$MANIFEST"
  for rel in automation-platform/.env automation-platform/.env.rabbitmq-preflight.bak; do
    target="$WORKSPACE_ROOT/$rel"
    if [[ -e "$target" ]]; then
      mkdir -p "$BACKUP_DIR/files/$(dirname "$rel")"
      cp -a "$target" "$BACKUP_DIR/files/$rel"
      printf '%s\n' "$rel" >> "$BACKUP_DIR/backed-up-files.txt"
    else
      printf '%s\n' "$rel" >> "$BACKUP_DIR/runtime-created-files.txt"
    fi
  done
  printf 'Backup: %s\n' "$BACKUP_DIR"
}

install_payload() {
  local rel baseline final mode source target
  while IFS=$'\t' read -r rel baseline final mode; do
    [[ "$rel" == "path" ]] && continue
    source="$PAYLOAD/$rel"
    target="$WORKSPACE_ROOT/$rel"
    [[ -f "$source" ]] || fail "Bundle payload is missing: $rel"
    mkdir -p "$(dirname "$target")"
    cp -a "$source" "$target"
    chmod "$mode" "$target"
    [[ "$(sha256_file "$target")" == "$final" ]] || fail "Installed checksum mismatch: $rel"
  done < "$MANIFEST"
}

migrate_local_env() {
  local python_bin
  python_bin="$(command -v python3 || true)"
  [[ -n "$python_bin" ]] || fail "python3 is required for the safe .env migration."
  "$python_bin" "$PLATFORM_ROOT/scripts/ensure_rabbitmq_broker.py" \
    --compose-file "$PLATFORM_ROOT/compose.yaml" \
    --env-file "$PLATFORM_ROOT/.env" \
    --rewrite-only
}

run_project_pytest() {
  resolve_uv
  local project_pythonpath="$PLATFORM_ROOT"
  if [[ -n "${PYTHONPATH:-}" ]]; then
    project_pythonpath="$project_pythonpath:$PYTHONPATH"
  fi
  (
    cd "$PLATFORM_ROOT"
    env -u UV_NO_DEV -u UV_NO_SYNC \
      PYTHONPATH="$project_pythonpath" \
      "$UV" run --frozen --group dev python -m pytest "$@"
  )
}

portable_verification() {
  info "Static and focused regression checks"
  bash -n "$PLATFORM_ROOT/scripts/dev.sh"
  python3 -m py_compile \
    "$PLATFORM_ROOT/scripts/ensure_rabbitmq_broker.py" \
    "$PLATFORM_ROOT/tests/test_rabbitmq_broker_preflight.py"
  node --check "$BRIDGE_ROOT/bridge.js"
  for file in "$BRIDGE_ROOT"/lib/*.js; do node --check "$file"; done
  (cd "$BRIDGE_ROOT" && node --test test/*.test.mjs)

  local isolated status
  isolated="$PLATFORM_ROOT/.bundle-focused-tests-$$"
  rm -rf "$isolated"
  mkdir -p "$isolated"
  cp "$PLATFORM_ROOT/tests/test_rabbitmq_broker_preflight.py" "$isolated/"
  status=0
  run_project_pytest \
    -q \
    --confcutdir="$isolated" \
    "$isolated/test_rabbitmq_broker_preflight.py" || status=$?
  rm -rf "$isolated"
  [[ "$status" -eq 0 ]] || return "$status"
}

full_verification() {
  resolve_uv
  info "Python compile and migration-head checks"
  (
    cd "$PLATFORM_ROOT"
    "$UV" run --frozen python -m compileall -q apps/api apps/worker packages scripts
    "$UV" run --frozen alembic heads
  )
  info "Targeted broker regression tests"
  run_project_pytest -q tests/test_rabbitmq_broker_preflight.py
  info "Complete Automation Platform test suite"
  run_project_pytest -q
  info "Astro production build"
  (cd "$PLATFORM_ROOT/apps/web" && npm run build)
  if git -C "$WORKSPACE_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    info "Git whitespace validation"
    git -C "$WORKSPACE_ROOT" diff --check
  fi
}

main() {
  trap on_exit EXIT
  resolve_roots
  [[ -f "$MANIFEST" ]] || fail "Bundle manifest is missing."
  [[ -d "$PAYLOAD" ]] || fail "Bundle payload is missing."
  [[ -x "$SCRIPT_DIR/bundle-tests/test_apply_update_runner.sh" ]] \
    || fail "Bundle installer regression test is missing or not executable."
  info "Bundle installer regression check"
  "$SCRIPT_DIR/bundle-tests/test_apply_update_runner.sh"
  printf 'Bundle: %s\nPlatform: %s\nWorkspace: %s\n' "$BUNDLE_ID" "$PLATFORM_ROOT" "$WORKSPACE_ROOT"
  info "Exact baseline preflight"
  preflight_files
  if [[ "$ALREADY_APPLIED" == true ]]; then
    printf 'Bundle files are already at the final checksums. Running verification only.\n'
  else
    info "Transactional file backup"
    create_backup
    APPLY_STARTED=true
    info "Installing broker-authentication and log-noise fixes"
    install_payload
    info "Migrating only the unsafe local guest broker URL"
    migrate_local_env
  fi
  portable_verification
  if [[ "$RUN_FULL_TESTS" == "1" ]]; then
    full_verification
  else
    printf '\nRUN_FULL_TESTS=%s: full uv/pytest/Astro verification was skipped by explicit override.\n' "$RUN_FULL_TESTS"
  fi
  APPLY_SUCCEEDED=true
  trap - EXIT
  printf '\n%s applied successfully.\n' "$BUNDLE_ID"
  if [[ -n "$BACKUP_DIR" ]]; then
    printf 'Rollback backup: %s\n' "$BACKUP_DIR"
  fi
  printf 'Next start: cd %q && ./scripts/dev.sh\n' "$PLATFORM_ROOT"
}

if [[ "${APPLY_UPDATE_LIB_ONLY:-0}" != "1" ]]; then
  main "$@"
fi

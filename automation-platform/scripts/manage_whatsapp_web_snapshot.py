#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import time
from dataclasses import dataclass
from pathlib import Path

LOCK_NAMES = ("SingletonCookie", "SingletonLock", "SingletonSocket")
BROWSER_NAMES = ("brave", "brave-browser", "chromium", "chromium-browser", "chrome", "google-chrome")


@dataclass(frozen=True)
class Snapshot:
    metadata_path: Path
    root: Path
    profile_directory: str

    @property
    def profile_path(self) -> Path:
        return self.root / self.profile_directory

    @property
    def lock_paths(self) -> tuple[Path, ...]:
        return tuple(self.root / name for name in LOCK_NAMES)


def load_snapshot(metadata_path: Path) -> Snapshot:
    metadata_path = metadata_path.expanduser().resolve()
    if not metadata_path.is_file():
        raise RuntimeError(f"Visible-profile metadata was not found: {metadata_path}")
    data = json.loads(metadata_path.read_text(encoding="utf-8"))
    root_raw = str(data.get("userDataDir") or "").strip()
    profile = str(data.get("profileDirectory") or "").strip()
    if not root_raw or not profile:
        raise RuntimeError(f"Visible-profile metadata is incomplete: {metadata_path}")
    root = Path(root_raw).expanduser().resolve()
    snapshot = Snapshot(metadata_path=metadata_path, root=root, profile_directory=profile)
    if not snapshot.root.is_dir() or not snapshot.profile_path.is_dir():
        raise RuntimeError(
            "Visible-profile metadata points to a missing snapshot: "
            f"root={snapshot.root}, profile={snapshot.profile_directory!r}"
        )
    return snapshot


def _read_cmdline(path: Path) -> list[str]:
    try:
        raw = path.read_bytes()
    except (OSError, PermissionError):
        return []
    return [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]


def _user_data_dir(args: list[str]) -> Path | None:
    for index, arg in enumerate(args):
        if arg.startswith("--user-data-dir="):
            value = arg.split("=", 1)[1]
            return Path(value).expanduser().resolve() if value else None
        if arg == "--user-data-dir" and index + 1 < len(args):
            return Path(args[index + 1]).expanduser().resolve()
    return None


def find_managed_browser_pids(snapshot: Snapshot, proc_root: Path = Path("/proc")) -> list[int]:
    pids: list[int] = []
    if not proc_root.is_dir():
        return pids
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        args = _read_cmdline(entry / "cmdline")
        if not args:
            continue
        executable = Path(args[0]).name.lower()
        if not any(name in executable for name in BROWSER_NAMES):
            continue
        if _user_data_dir(args) == snapshot.root:
            pids.append(int(entry.name))
    return sorted(set(pids))


def existing_lock_paths(snapshot: Snapshot) -> list[Path]:
    return [path for path in snapshot.lock_paths if os.path.lexists(path)]


def remove_stale_locks(snapshot: Snapshot, proc_root: Path = Path("/proc")) -> list[Path]:
    live = find_managed_browser_pids(snapshot, proc_root)
    if live:
        raise RuntimeError(
            "Refusing to remove Chromium singleton markers while the managed browser is live: "
            + ", ".join(map(str, live))
        )
    removed: list[Path] = []
    for path in existing_lock_paths(snapshot):
        try:
            path.unlink()
            removed.append(path)
        except FileNotFoundError:
            continue
    return removed


def stop_managed_browser(snapshot: Snapshot, timeout_seconds: float = 10.0) -> dict[str, object]:
    pids = find_managed_browser_pids(snapshot)
    terminated: list[int] = []
    killed: list[int] = []
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            terminated.append(pid)
        except ProcessLookupError:
            pass
        except PermissionError as exc:
            raise RuntimeError(f"Cannot stop managed browser PID {pid}: {exc}") from exc

    deadline = time.monotonic() + max(0.5, timeout_seconds)
    remaining = list(pids)
    while remaining and time.monotonic() < deadline:
        next_remaining: list[int] = []
        for pid in remaining:
            try:
                os.kill(pid, 0)
                next_remaining.append(pid)
            except ProcessLookupError:
                pass
            except PermissionError:
                next_remaining.append(pid)
        remaining = next_remaining
        if remaining:
            time.sleep(0.2)

    for pid in remaining:
        try:
            os.kill(pid, signal.SIGKILL)
            killed.append(pid)
        except ProcessLookupError:
            pass
        except PermissionError as exc:
            raise RuntimeError(f"Cannot force-stop managed browser PID {pid}: {exc}") from exc

    if remaining:
        time.sleep(0.2)
    still_live = find_managed_browser_pids(snapshot)
    if still_live:
        raise RuntimeError("Managed browser did not stop: " + ", ".join(map(str, still_live)))
    removed = remove_stale_locks(snapshot)
    return {
        "terminated": terminated,
        "killed": killed,
        "removedLocks": [str(path) for path in removed],
    }


def status_payload(snapshot: Snapshot, proc_root: Path = Path("/proc")) -> dict[str, object]:
    pids = find_managed_browser_pids(snapshot, proc_root)
    locks = existing_lock_paths(snapshot)
    return {
        "metadataPath": str(snapshot.metadata_path),
        "userDataDir": str(snapshot.root),
        "profileDirectory": snapshot.profile_directory,
        "profilePath": str(snapshot.profile_path),
        "browserPids": pids,
        "locks": [str(path) for path in locks],
        "active": bool(pids),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage the private WhatsApp Web Brave snapshot safely")
    parser.add_argument("command", choices=("validate", "status", "cleanup-stale", "stop"))
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    snapshot = load_snapshot(args.metadata)
    if args.command == "validate":
        payload = status_payload(snapshot)
        state = "active" if payload["active"] else "available"
        print(f"Visible-profile snapshot {state}: {snapshot.profile_path}")
        return
    if args.command == "status":
        print(json.dumps(status_payload(snapshot), sort_keys=True))
        return
    if args.command == "cleanup-stale":
        removed = remove_stale_locks(snapshot)
        if removed:
            print("Removed stale Chromium singleton markers: " + ", ".join(map(str, removed)))
        else:
            print("No stale Chromium singleton markers were present.")
        return
    result = stop_managed_browser(snapshot, timeout_seconds=args.timeout)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "manage_whatsapp_web_snapshot.py"
SPEC = importlib.util.spec_from_file_location("managed_snapshot", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def make_snapshot(tmp_path: Path):
    root = tmp_path / "snapshot"
    profile = root / "Profile 1"
    profile.mkdir(parents=True)
    metadata = tmp_path / "visible-profile.json"
    metadata.write_text(
        json.dumps({"userDataDir": str(root), "profileDirectory": "Profile 1"}),
        encoding="utf-8",
    )
    return MODULE.load_snapshot(metadata)


def write_process(proc_root: Path, pid: int, args: list[str]) -> None:
    process = proc_root / str(pid)
    process.mkdir(parents=True)
    (process / "cmdline").write_bytes(b"\0".join(arg.encode() for arg in args) + b"\0")


def test_finds_only_browser_using_the_private_snapshot(tmp_path: Path) -> None:
    snapshot = make_snapshot(tmp_path)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    write_process(proc_root, 101, ["/usr/bin/brave", f"--user-data-dir={snapshot.root}"])
    write_process(proc_root, 102, ["/usr/bin/brave", "--user-data-dir=/tmp/other"])
    write_process(proc_root, 103, ["/usr/bin/node", f"--user-data-dir={snapshot.root}"])
    assert MODULE.find_managed_browser_pids(snapshot, proc_root) == [101]


def test_stale_singleton_markers_are_removed_when_no_browser_is_live(tmp_path: Path) -> None:
    snapshot = make_snapshot(tmp_path)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    lock = snapshot.root / "SingletonLock"
    socket = snapshot.root / "SingletonSocket"
    lock.write_text("stale", encoding="utf-8")
    socket.symlink_to(tmp_path / "missing-socket")
    removed = MODULE.remove_stale_locks(snapshot, proc_root)
    assert {path.name for path in removed} == {"SingletonLock", "SingletonSocket"}
    assert not os.path.lexists(lock)
    assert not os.path.lexists(socket)


def test_live_managed_browser_prevents_lock_deletion(tmp_path: Path) -> None:
    snapshot = make_snapshot(tmp_path)
    proc_root = tmp_path / "proc"
    proc_root.mkdir()
    write_process(proc_root, 201, ["/usr/bin/brave-browser", "--user-data-dir", str(snapshot.root)])
    (snapshot.root / "SingletonLock").write_text("live", encoding="utf-8")
    try:
        MODULE.remove_stale_locks(snapshot, proc_root)
    except RuntimeError as exc:
        assert "managed browser is live" in str(exc)
    else:
        raise AssertionError("expected live-browser protection")

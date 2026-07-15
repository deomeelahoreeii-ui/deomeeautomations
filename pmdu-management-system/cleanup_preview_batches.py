from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CleanupTarget:
    label: str
    path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preview or discard generated notification/follow-up preview batches."
    )
    parser.add_argument(
        "--kind",
        choices=("notifications", "followups", "all"),
        default="all",
        help="Which preview roots to clean.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be removed.")
    parser.add_argument("--yes", action="store_true", help="Actually remove the preview batches.")
    parser.add_argument(
        "--include-notification-downloads",
        action="store_true",
        help="Also remove notification_downloads staging files. These can be regenerated.",
    )
    return parser.parse_args()


def directory_size(path: Path) -> int:
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total += item.stat().st_size
            except OSError:
                pass
    return total


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"


def preview_roots(project_root: Path, kind: str) -> list[CleanupTarget]:
    roots: list[CleanupTarget] = []
    reports = project_root / "reports" / "pmdu"
    if kind in {"notifications", "all"}:
        roots.append(CleanupTarget("notification previews", reports / "notification_previews"))
    if kind in {"followups", "all"}:
        roots.append(CleanupTarget("follow-up previews", reports / "followup_previews"))
    return roots


def batch_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted({item.parent for item in root.rglob("preview.html")})


def remove_empty_parents(path: Path, stop_at: Path) -> None:
    current = path
    while current != stop_at and stop_at in current.parents:
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def main() -> None:
    args = parse_args()
    if not args.dry_run and not args.yes:
        raise SystemExit("Use --dry-run to preview or --yes to discard previews.")

    project_root = Path(__file__).resolve().parent
    targets = preview_roots(project_root, args.kind)
    total_batches = 0
    total_size = 0

    for target in targets:
        batches = batch_dirs(target.path)
        latest = target.path / "LATEST"
        print(f"{target.label}: {target.path}")
        if not batches and not latest.exists():
            print("  nothing to remove")
            continue
        for batch in batches:
            size = directory_size(batch)
            total_batches += 1
            total_size += size
            print(f"  batch {batch} ({human_size(size)})")
            if args.yes:
                shutil.rmtree(batch)
                remove_empty_parents(batch.parent, target.path)
        if latest.exists():
            print(f"  pointer {latest}")
            if args.yes:
                latest.unlink()

    if args.include_notification_downloads:
        staging = project_root / "notification_downloads"
        print(f"notification staging: {staging}")
        if staging.exists():
            size = directory_size(staging)
            total_size += size
            print(f"  staging files ({human_size(size)})")
            if args.yes:
                shutil.rmtree(staging)
        else:
            print("  nothing to remove")

    action = "Would remove" if args.dry_run else "Removed"
    print(f"{action} {total_batches} preview batch(es), {human_size(total_size)} total.")


if __name__ == "__main__":
    main()

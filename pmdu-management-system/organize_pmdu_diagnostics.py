from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


CAPTURE_RE = re.compile(
    r"^(?P<stamp>\d{8}-\d{6})-"
    r"(?P<name>login-filled|complaints-list|login-timeout)"
    r"\.(?P<ext>png|html)$"
)


@dataclass(frozen=True)
class MovePlan:
    source: Path
    target: Path
    stamp: str
    capture_name: str
    extension: str
    size_bytes: int
    mtime_iso: str
    sha256: str


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_stamp(stamp: str) -> datetime:
    return datetime.strptime(stamp, "%Y%m%d-%H%M%S")


def build_plan(project_root: Path, artifacts_dir: Path, diagnostics_root: Path) -> list[MovePlan]:
    plans: list[MovePlan] = []
    if not artifacts_dir.exists():
        return plans

    for source in sorted(artifacts_dir.iterdir()):
        if not source.is_file():
            continue
        match = CAPTURE_RE.match(source.name)
        if not match:
            continue

        stamp = match.group("stamp")
        captured_at = parse_stamp(stamp)
        target = (
            diagnostics_root
            / captured_at.strftime("%Y")
            / captured_at.strftime("%m")
            / captured_at.strftime("%d")
            / stamp
            / source.name
        )
        stat = source.stat()
        plans.append(
            MovePlan(
                source=source,
                target=target,
                stamp=stamp,
                capture_name=match.group("name"),
                extension=match.group("ext"),
                size_bytes=stat.st_size,
                mtime_iso=datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(),
                sha256=sha256_file(source),
            )
        )
    return plans


def write_manifest(
    project_root: Path,
    diagnostics_root: Path,
    plans: list[MovePlan],
    dry_run: bool,
) -> Path:
    batch_id = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    manifest_dir = diagnostics_root / "MIGRATIONS" / batch_id
    manifest_dir.mkdir(parents=True, exist_ok=True)

    files_csv = manifest_dir / "FILES.csv"
    with files_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "stamp",
                "capture_name",
                "extension",
                "size_bytes",
                "mtime_iso",
                "sha256",
                "source_path",
                "target_path",
            ],
        )
        writer.writeheader()
        for plan in plans:
            writer.writerow(
                {
                    "stamp": plan.stamp,
                    "capture_name": plan.capture_name,
                    "extension": plan.extension,
                    "size_bytes": plan.size_bytes,
                    "mtime_iso": plan.mtime_iso,
                    "sha256": plan.sha256,
                    "source_path": str(plan.source.relative_to(project_root)),
                    "target_path": str(plan.target.relative_to(project_root)),
                }
            )

    manifest = {
        "batch_id": batch_id,
        "created_at": datetime.now().astimezone().isoformat(),
        "dry_run": dry_run,
        "source": "artifacts root browser diagnostics",
        "file_count": len(plans),
        "captures": sorted({plan.stamp for plan in plans}),
        "files_csv": str(files_csv.relative_to(project_root)),
    }
    manifest_path = manifest_dir / "MANIFEST.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest_path


def move_files(plans: list[MovePlan]) -> None:
    for plan in plans:
        if plan.target.exists():
            raise FileExistsError(f"Target already exists: {plan.target}")
        plan.target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(plan.source), str(plan.target))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Move PMDU browser diagnostic captures out of artifacts/ into a "
            "date-wise diagnostics tree."
        )
    )
    parser.add_argument(
        "--artifacts-dir",
        default="artifacts",
        help="Folder containing generated PMDU case artifacts.",
    )
    parser.add_argument(
        "--diagnostics-root",
        default="diagnostics/pmdu",
        help="Destination root for browser diagnostics.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write a migration manifest but do not move files.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Move files. Required unless --dry-run is used.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parent
    artifacts_dir = (project_root / args.artifacts_dir).resolve()
    diagnostics_root = (project_root / args.diagnostics_root).resolve()

    if not args.dry_run and not args.yes:
        raise SystemExit("Use --dry-run to preview or --yes to move files.")

    plans = build_plan(project_root, artifacts_dir, diagnostics_root)
    manifest_path = write_manifest(project_root, diagnostics_root, plans, args.dry_run)

    if args.dry_run:
        print(f"Dry run: {len(plans)} diagnostic files found.")
        print(f"Manifest: {manifest_path}")
        return 0

    move_files(plans)
    print(f"Moved {len(plans)} diagnostic files.")
    print(f"Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

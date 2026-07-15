from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


ARCHIVE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ArchiveEntry:
    case_id: str
    source_kind: str
    source_rel: str
    archive_rel: str
    file_rel: str
    size_bytes: int
    sha256: str
    mtime_ns: int


@dataclass(frozen=True)
class ArchiveTarget:
    case_id: str
    source_kind: str
    source_path: Path
    archive_path: Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def relative(path: Path, root: Path) -> str:
    return str(path.resolve(strict=False).relative_to(root.resolve(strict=False)))


def iter_files(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    for item in sorted(path.rglob("*")):
        if item.is_file():
            yield item


def inventory_target(
    target: ArchiveTarget,
    project_root: Path,
    archive_batch_root: Path,
) -> list[ArchiveEntry]:
    rows: list[ArchiveEntry] = []
    for file_path in iter_files(target.source_path):
        stat = file_path.stat()
        if target.source_path.is_file():
            file_rel = file_path.name
            archived_file = target.archive_path
        else:
            file_rel = str(file_path.relative_to(target.source_path))
            archived_file = target.archive_path / file_rel
        rows.append(
            ArchiveEntry(
                case_id=target.case_id,
                source_kind=target.source_kind,
                source_rel=relative(file_path, project_root),
                archive_rel=relative(archived_file, archive_batch_root),
                file_rel=file_rel,
                size_bytes=stat.st_size,
                sha256=sha256_file(file_path),
                mtime_ns=stat.st_mtime_ns,
            )
        )
    return rows


def unique_batch_root(archive_root: Path, label: str) -> Path:
    now = datetime.now()
    safe_label = "".join(
        char if char.isalnum() or char in {"-", "_"} else "-"
        for char in label.strip().lower()
    ).strip("-")
    suffix = f"-{safe_label}" if safe_label else ""
    base = archive_root / f"{now:%Y}" / f"{now:%m}" / f"{now:%d}"
    batch_id = f"{now:%Y%m%d-%H%M%S}{suffix}"
    candidate = base / batch_id
    counter = 1
    while candidate.exists() or candidate.with_name(candidate.name + ".in-progress").exists():
        candidate = base / f"{batch_id}-{counter:02d}"
        counter += 1
    return candidate


def discover_targets(
    project_root: Path,
    archive_batch_root: Path,
    selection: str,
) -> tuple[list[ArchiveTarget], dict[str, list[str]]]:
    raw_dir = project_root / "raw_html"
    artifacts_dir = project_root / "artifacts"

    raw_cases = {
        path.stem: path for path in sorted(raw_dir.glob("*.html")) if path.is_file()
    }
    artifact_cases = {
        path.name: path for path in sorted(artifacts_dir.iterdir()) if path.is_dir()
    } if artifacts_dir.exists() else {}

    if selection == "processed":
        selected_raw_cases = set(raw_cases) & set(artifact_cases)
        selected_artifact_cases = selected_raw_cases
    elif selection == "all":
        selected_raw_cases = set(raw_cases)
        selected_artifact_cases = set(artifact_cases)
    elif selection == "raw":
        selected_raw_cases = set(raw_cases)
        selected_artifact_cases = set()
    elif selection == "artifacts":
        selected_raw_cases = set()
        selected_artifact_cases = set(artifact_cases)
    else:
        raise ValueError(f"Unsupported selection: {selection}")

    targets: list[ArchiveTarget] = []
    for case_id in sorted(selected_raw_cases):
        targets.append(
            ArchiveTarget(
                case_id=case_id,
                source_kind="raw_html",
                source_path=raw_cases[case_id],
                archive_path=archive_batch_root / "raw_html" / raw_cases[case_id].name,
            )
        )
    for case_id in sorted(selected_artifact_cases):
        targets.append(
            ArchiveTarget(
                case_id=case_id,
                source_kind="artifacts",
                source_path=artifact_cases[case_id],
                archive_path=archive_batch_root / "artifacts" / case_id,
            )
        )

    summary = {
        "raw_cases": sorted(raw_cases),
        "artifact_cases": sorted(artifact_cases),
        "selected_raw_cases": sorted(selected_raw_cases),
        "selected_artifact_cases": sorted(selected_artifact_cases),
        "unprocessed_raw_cases": sorted(set(raw_cases) - set(artifact_cases)),
        "artifact_only_cases": sorted(set(artifact_cases) - set(raw_cases)),
    }
    return targets, summary


def write_manifest(
    batch_root: Path,
    project_root: Path,
    final_root: Path,
    selection: str,
    summary: dict[str, list[str]],
    entries: list[ArchiveEntry],
    dry_run: bool,
) -> None:
    manifest = {
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "created_at": datetime.now().astimezone().isoformat(),
        "project_root": str(project_root),
        "archive_root": str(final_root),
        "selection": selection,
        "dry_run": dry_run,
        "counts": {
            "raw_cases_total": len(summary["raw_cases"]),
            "artifact_cases_total": len(summary["artifact_cases"]),
            "selected_raw_cases": len(summary["selected_raw_cases"]),
            "selected_artifact_cases": len(summary["selected_artifact_cases"]),
            "unprocessed_raw_cases_left_in_place": len(summary["unprocessed_raw_cases"]),
            "artifact_only_cases": len(summary["artifact_only_cases"]),
            "files_indexed": len(entries),
            "bytes_indexed": sum(entry.size_bytes for entry in entries),
        },
        "cases": summary,
        "files_csv": "FILES.csv",
    }
    batch_root.mkdir(parents=True, exist_ok=True)
    (batch_root / "MANIFEST.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    with (batch_root / "FILES.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(asdict(entries[0]).keys()) if entries else [
            "case_id",
            "source_kind",
            "source_rel",
            "archive_rel",
            "file_rel",
            "size_bytes",
            "sha256",
            "mtime_ns",
        ])
        writer.writeheader()
        for entry in entries:
            writer.writerow(asdict(entry))
    (batch_root / "README.txt").write_text(
        "\n".join(
            [
                "PMDU archive batch",
                f"Created: {manifest['created_at']}",
                f"Selection: {selection}",
                "",
                "This archive is intentionally stored as normal folders and files.",
                "Use MANIFEST.json for batch-level audit data and FILES.csv for per-file SHA256 checksums.",
                "The default 'processed' selection archives raw HTML only when a matching artifact case exists,",
                "leaving unprocessed raw HTML in the live working folder for the next artifact build.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def move_targets(targets: list[ArchiveTarget]) -> None:
    for target in targets:
        target.archive_path.parent.mkdir(parents=True, exist_ok=True)
        if target.archive_path.exists():
            raise FileExistsError(f"Archive destination already exists: {target.archive_path}")
        shutil.move(str(target.source_path), str(target.archive_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Archive PMDU raw HTML and artifact history with manifests and checksums."
    )
    parser.add_argument(
        "--selection",
        choices=("processed", "all", "raw", "artifacts"),
        default="processed",
        help="processed archives only raw HTML cases that already have artifact folders, plus those artifacts.",
    )
    parser.add_argument(
        "--archive-root",
        default="archives/pmdu",
        help="Archive root folder relative to the PMDU project.",
    )
    parser.add_argument("--label", default="history", help="Human-readable batch label.")
    parser.add_argument("--dry-run", action="store_true", help="Print and manifest the plan without moving files.")
    parser.add_argument("--yes", action="store_true", help="Required for non-dry-run moves.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parent
    archive_root = Path(args.archive_root).expanduser()
    if not archive_root.is_absolute():
        archive_root = project_root / archive_root
    final_root = unique_batch_root(archive_root, args.label)
    staging_root = final_root.with_name(final_root.name + ".in-progress")

    targets, summary = discover_targets(project_root, staging_root, args.selection)
    entries: list[ArchiveEntry] = []
    for target in targets:
        entries.extend(inventory_target(target, project_root, staging_root))

    print(f"Selection: {args.selection}")
    print(f"Archive batch: {final_root}")
    print(f"Raw HTML total: {len(summary['raw_cases'])}")
    print(f"Artifact cases total: {len(summary['artifact_cases'])}")
    print(f"Selected raw HTML cases: {len(summary['selected_raw_cases'])}")
    print(f"Selected artifact cases: {len(summary['selected_artifact_cases'])}")
    print(f"Unprocessed raw HTML left in place: {len(summary['unprocessed_raw_cases'])}")
    if summary["unprocessed_raw_cases"]:
        print("Unprocessed cases:")
        for case_id in summary["unprocessed_raw_cases"]:
            print(f"  - {case_id}")

    if not targets:
        print("Nothing to archive.")
        return 0

    if args.dry_run:
        dry_run_root = final_root.with_name(final_root.name + ".dry-run")
        write_manifest(
            dry_run_root,
            project_root,
            dry_run_root,
            args.selection,
            summary,
            entries,
            dry_run=True,
        )
        print(f"Dry-run manifest written: {dry_run_root}")
        return 0

    if not args.yes:
        print("Refusing to move files without --yes. Run with --dry-run first, then add --yes.")
        return 2

    write_manifest(
        staging_root,
        project_root,
        final_root,
        args.selection,
        summary,
        entries,
        dry_run=False,
    )
    move_targets(targets)
    staging_root.rename(final_root)
    print(f"Archived {len(targets)} top-level targets and {len(entries)} files.")
    print(f"Archive ready: {final_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

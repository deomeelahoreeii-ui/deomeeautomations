#!/usr/bin/env python3
"""
gitignore_zip.py

Create one ZIP containing immediate child folders of a directory while honoring
each folder's .gitignore files.

Features:
- Uses Git itself for .gitignore matching, including nested .gitignore files
  and negation rules such as !important.env.
- Treats every file as untracked, so a file is excluded when .gitignore matches
  it even if it is currently tracked in its real repository.
- Shows included size, file count, largest projects, largest subfolders, and
  largest files before creating the ZIP.
- Prompts before zipping unless --yes is used.
- Writes to a temporary .part file and renames it only after success.
- Skips symbolic links and Git metadata.
- Uses ZIP64, so archives larger than 4 GB are supported.

Git must be installed and available on PATH.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class FileEntry:
    source: Path
    archive_name: str
    project: str
    relative_path: Path
    size: int


def human_size(size: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    value = float(size)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{size} B"


def ensure_git_available() -> None:
    try:
        subprocess.run(
            ["git", "--version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise SystemExit(
            "Git is required because Git itself is used to evaluate .gitignore rules."
        ) from exc


def create_temporary_git_dir(parent: Path) -> tempfile.TemporaryDirectory[str]:
    temp = tempfile.TemporaryDirectory(prefix="gitignore-zip-", dir=None)
    subprocess.run(
        ["git", "init", "--quiet", "--bare", temp.name],
        check=True,
        cwd=parent,
    )
    return temp


def gitignore_included_paths(project: Path, temp_git_dir: Path) -> tuple[list[Path], list[str]]:
    """
    Return files not ignored by .gitignore.

    A temporary empty Git index is used so every worktree file is evaluated as
    untracked. This means ignored tracked files are excluded too.
    """
    command = [
        "git",
        f"--git-dir={temp_git_dir}",
        f"--work-tree={project}",
        "ls-files",
        "--others",
        "--exclude-per-directory=.gitignore",
        "-z",
        "--",
        ".",
    ]

    result = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    paths: list[Path] = []
    warnings: list[str] = []

    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue

        relative_text = os.fsdecode(raw)
        relative = Path(relative_text)
        full_path = project / relative

        # Git can return "nested-repository/" rather than descending into it.
        if relative_text.endswith("/") or full_path.is_dir():
            warnings.append(
                f"Nested repository or directory entry skipped: "
                f"{project.name}/{relative_text.rstrip('/')}"
            )
            continue

        if full_path.is_symlink():
            warnings.append(f"Symbolic link skipped: {project.name}/{relative_text}")
            continue

        if full_path.is_file():
            paths.append(relative)
        else:
            warnings.append(
                f"File disappeared or is not a regular file: "
                f"{project.name}/{relative_text}"
            )

    return paths, warnings


def choose_projects(
    root: Path,
    requested: list[str],
    skipped_names: set[str],
    output_path: Path,
) -> list[Path]:
    if requested:
        projects = []
        for name in requested:
            candidate = (root / name).resolve()
            try:
                candidate.relative_to(root)
            except ValueError as exc:
                raise SystemExit(f"Folder must be inside the source root: {name}") from exc

            if not candidate.is_dir():
                raise SystemExit(f"Folder does not exist or is not a directory: {candidate}")
            projects.append(candidate)
        return projects

    projects = []
    for child in sorted(root.iterdir(), key=lambda path: path.name.casefold()):
        if not child.is_dir():
            continue
        if child.name.startswith("."):
            continue
        if child.name in skipped_names:
            continue
        if child.resolve() == output_path.resolve():
            continue
        projects.append(child.resolve())

    return projects


def scan_projects(
    projects: Iterable[Path],
    temp_git_dir: Path,
    output_path: Path,
) -> tuple[list[FileEntry], list[str]]:
    entries: list[FileEntry] = []
    warnings: list[str] = []

    output_resolved = output_path.resolve()
    part_resolved = output_path.with_suffix(output_path.suffix + ".part").resolve()

    for project in projects:
        print(f"Scanning {project.name} ...", flush=True)
        relative_paths, project_warnings = gitignore_included_paths(
            project,
            temp_git_dir,
        )
        warnings.extend(project_warnings)

        for relative in relative_paths:
            source = project / relative
            resolved = source.resolve()

            # Protect against accidentally archiving the output itself.
            if resolved in {output_resolved, part_resolved}:
                continue

            try:
                stat = source.stat()
            except OSError as exc:
                warnings.append(f"Cannot stat {source}: {exc}")
                continue

            archive_name = (Path(project.name) / relative).as_posix()
            entries.append(
                FileEntry(
                    source=source,
                    archive_name=archive_name,
                    project=project.name,
                    relative_path=relative,
                    size=stat.st_size,
                )
            )

    return entries, warnings


def print_report(entries: list[FileEntry], top_n: int, output_path: Path) -> None:
    total_size = sum(entry.size for entry in entries)
    project_sizes: dict[str, int] = defaultdict(int)
    subfolder_sizes: dict[str, int] = defaultdict(int)

    for entry in entries:
        project_sizes[entry.project] += entry.size

        parts = entry.relative_path.parts
        if len(parts) >= 2:
            bucket = f"{entry.project}/{parts[0]}"
        else:
            bucket = f"{entry.project}/(root files)"
        subfolder_sizes[bucket] += entry.size

    print("\nAnalysis")
    print("=" * 72)
    print(f"Included files      : {len(entries):,}")
    print(f"Included source size: {human_size(total_size)}")
    print(f"ZIP output          : {output_path}")
    print(
        "Expected ZIP size   : Cannot be known exactly before compression; "
        "already-compressed files may shrink very little."
    )

    try:
        free_space = shutil.disk_usage(output_path.parent).free
        print(f"Free disk space     : {human_size(free_space)}")
        if free_space < total_size:
            print(
                "WARNING             : Free space is smaller than included source size. "
                "The ZIP may fail if the files do not compress enough."
            )
    except OSError:
        pass

    print("\nLargest project folders")
    print("-" * 72)
    for name, size in sorted(
        project_sizes.items(),
        key=lambda item: item[1],
        reverse=True,
    )[:top_n]:
        percentage = (size / total_size * 100.0) if total_size else 0.0
        print(f"{human_size(size):>12}  {percentage:6.2f}%  {name}")

    print("\nLargest contributing subfolders")
    print("-" * 72)
    for name, size in sorted(
        subfolder_sizes.items(),
        key=lambda item: item[1],
        reverse=True,
    )[:top_n]:
        percentage = (size / total_size * 100.0) if total_size else 0.0
        print(f"{human_size(size):>12}  {percentage:6.2f}%  {name}")

    print("\nLargest included files")
    print("-" * 72)
    for entry in sorted(entries, key=lambda item: item.size, reverse=True)[:top_n]:
        print(f"{human_size(entry.size):>12}  {entry.archive_name}")


def create_zip(
    entries: list[FileEntry],
    output_path: Path,
    compression_level: int,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = output_path.with_suffix(output_path.suffix + ".part")

    if part_path.exists():
        part_path.unlink()

    total_files = len(entries)
    total_bytes = sum(entry.size for entry in entries)
    processed_bytes = 0
    next_percentage = 5

    try:
        with zipfile.ZipFile(
            part_path,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=compression_level,
            allowZip64=True,
        ) as archive:
            archive.comment = (
                f"Created by gitignore_zip.py on "
                f"{datetime.now().isoformat(timespec='seconds')}"
            ).encode("utf-8")

            for index, entry in enumerate(entries, start=1):
                archive.write(entry.source, arcname=entry.archive_name)
                processed_bytes += entry.size

                if total_bytes:
                    percentage = int(processed_bytes * 100 / total_bytes)
                else:
                    percentage = int(index * 100 / max(total_files, 1))

                if percentage >= next_percentage or index == total_files:
                    print(
                        f"Progress: {percentage:3d}% "
                        f"({index:,}/{total_files:,} files)",
                        flush=True,
                    )
                    while next_percentage <= percentage:
                        next_percentage += 5

        os.replace(part_path, output_path)

    except BaseException:
        if part_path.exists():
            part_path.unlink()
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze and ZIP immediate child folders while honoring each "
            "folder's .gitignore files."
        )
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        help="Parent directory containing the project folders. Default: current directory.",
    )
    parser.add_argument(
        "folders",
        nargs="*",
        help=(
            "Specific immediate child folders to include. "
            "When omitted, all non-hidden immediate child folders are included."
        ),
    )
    parser.add_argument(
        "-o",
        "--output",
        help=(
            "Output ZIP path. Default: a timestamped ZIP beside the source root."
        ),
    )
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        help="Immediate child folder name to skip. May be repeated.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=15,
        help="Number of largest folders/files to show. Default: 15.",
    )
    parser.add_argument(
        "--level",
        type=int,
        choices=range(0, 10),
        default=6,
        metavar="0-9",
        help="ZIP compression level. Default: 6.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only analyze; do not create a ZIP.",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Create the ZIP without asking for confirmation.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ensure_git_available()

    root = Path(args.root).expanduser().resolve()
    if not root.is_dir():
        raise SystemExit(f"Source root is not a directory: {root}")

    if args.output:
        output_path = Path(args.output).expanduser()
        if not output_path.is_absolute():
            output_path = (Path.cwd() / output_path).resolve()
        else:
            output_path = output_path.resolve()
    else:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_path = root.parent / f"{root.name}-{timestamp}.zip"

    if output_path.suffix.lower() != ".zip":
        output_path = output_path.with_suffix(".zip")

    projects = choose_projects(
        root=root,
        requested=args.folders,
        skipped_names=set(args.skip),
        output_path=output_path,
    )

    if not projects:
        raise SystemExit("No project folders were selected.")

    print(f"Source root: {root}")
    print(f"Projects   : {len(projects)}")
    for project in projects:
        print(f"  - {project.name}")

    with create_temporary_git_dir(root) as temp_name:
        entries, warnings = scan_projects(
            projects=projects,
            temp_git_dir=Path(temp_name),
            output_path=output_path,
        )

    entries.sort(key=lambda entry: entry.archive_name.casefold())

    if not entries:
        raise SystemExit("No files remain after applying .gitignore rules.")

    print_report(entries, max(args.top, 1), output_path)

    if warnings:
        print(f"\nWarnings: {len(warnings):,}")
        for warning in warnings[:20]:
            print(f"  - {warning}")
        if len(warnings) > 20:
            print(f"  ... and {len(warnings) - 20:,} more")

    if args.dry_run:
        print("\nDry run complete. No ZIP was created.")
        return 0

    if output_path.exists() and not args.yes:
        answer = input(f"\n{output_path} already exists. Replace it? [y/N] ").strip().lower()
        if answer not in {"y", "yes"}:
            print("Cancelled.")
            return 0

    if not args.yes:
        answer = input("\nCreate the ZIP now? [y/N] ").strip().lower()
        if answer not in {"y", "yes"}:
            print("Cancelled.")
            return 0

    print(f"\nCreating {output_path} ...")
    create_zip(entries, output_path, args.level)

    final_size = output_path.stat().st_size
    source_size = sum(entry.size for entry in entries)
    saved = source_size - final_size
    ratio = (final_size / source_size * 100.0) if source_size else 0.0

    print("\nCompleted")
    print("=" * 72)
    print(f"ZIP path           : {output_path}")
    print(f"ZIP size           : {human_size(final_size)}")
    print(f"Included source    : {human_size(source_size)}")
    print(f"ZIP/source ratio   : {ratio:.2f}%")
    if saved > 0:
        print(f"Space reduced by   : {human_size(saved)}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nCancelled.", file=sys.stderr)
        raise SystemExit(130)
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode(errors="replace") if exc.stderr else str(exc)
        print(f"\nGit command failed:\n{stderr}", file=sys.stderr)
        raise SystemExit(exc.returncode or 1)

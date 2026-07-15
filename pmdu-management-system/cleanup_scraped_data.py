from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preview or discard PMDU scraped intake data."
    )
    parser.add_argument("--raw-html-dir", default="raw_html")
    parser.add_argument("--artifact-dir", default="artifacts")
    parser.add_argument("--duckdb", default="pmdu_scrape.duckdb")
    parser.add_argument(
        "--include-artifacts",
        action="store_true",
        help="Also remove generated artifact folders. Leave off to discard only scraped intake.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    return parser.parse_args()


def file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def tree_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return file_size(path)
    return sum(file_size(item) for item in path.rglob("*") if item.is_file())


def human_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"


def queue_count(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    conn = duckdb.connect(str(db_path))
    try:
        try:
            row = conn.execute("SELECT COUNT(*) FROM scrape_queue").fetchone()
        except duckdb.CatalogException:
            return 0
        return int(row[0] or 0)
    finally:
        conn.close()


def clear_queue(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    conn = duckdb.connect(str(db_path))
    try:
        try:
            count = int(conn.execute("SELECT COUNT(*) FROM scrape_queue").fetchone()[0] or 0)
        except duckdb.CatalogException:
            return 0
        conn.execute("DELETE FROM scrape_queue")
        return count
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    if not args.dry_run and not args.yes:
        raise SystemExit("Use --dry-run to preview or --yes to discard scraped data.")

    project_root = Path(__file__).resolve().parent
    raw_html_dir = (project_root / args.raw_html_dir).resolve()
    artifact_dir = (project_root / args.artifact_dir).resolve()
    db_path = (project_root / args.duckdb).resolve()

    raw_files = sorted(raw_html_dir.glob("*.html")) if raw_html_dir.exists() else []
    artifact_dirs = [
        item
        for item in sorted(artifact_dir.iterdir())
        if item.is_dir()
    ] if artifact_dir.exists() else []
    queue_rows = queue_count(db_path)

    total_size = sum(file_size(item) for item in raw_files)
    if args.include_artifacts:
        total_size += sum(tree_size(item) for item in artifact_dirs)

    print(f"Raw HTML directory: {raw_html_dir}")
    print(f"Raw HTML files: {len(raw_files)} ({human_size(sum(file_size(item) for item in raw_files))})")
    print(f"Scrape queue rows: {queue_rows}")
    print(f"Artifact folders: {len(artifact_dirs)} ({human_size(sum(tree_size(item) for item in artifact_dirs))})")
    if not args.include_artifacts:
        print("Artifact folders will be kept. Use --include-artifacts to remove them too.")

    action = "Would discard" if args.dry_run else "Discarding"
    print(f"{action} raw HTML files and scrape queue rows.")
    if args.include_artifacts:
        print(f"{action} artifact folders too.")

    if args.dry_run:
        return

    for path in raw_files:
        path.unlink()
    removed_queue_rows = clear_queue(db_path)
    removed_artifacts = 0
    if args.include_artifacts:
        for path in artifact_dirs:
            shutil.rmtree(path)
            removed_artifacts += 1

    print(
        "Discarded "
        f"{len(raw_files)} raw HTML file(s), "
        f"{removed_queue_rows} queue row(s), "
        f"{removed_artifacts} artifact folder(s), "
        f"{human_size(total_size)} total."
    )


if __name__ == "__main__":
    main()

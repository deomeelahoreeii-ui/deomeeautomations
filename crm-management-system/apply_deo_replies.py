from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
import xml.etree.ElementTree as ET

import pandas as pd

from main import configure_logging
from generate_crm_compliance_folders import (
    NS,
    TABLE_ROW_TAG,
    TEXT_P_TAG,
    cell_text,
    expanded_cells,
    normalize_label_text,
)


LOGGER_NAME = "pmdu_automation"
DEFAULT_PENDING_DIR = "crm-compliances/pending"
DEFAULT_REPLIES_FILE = (
    "crm-compliances/pending/pending-complaint-replies-two-columns.csv"
)
COMPLAINT_NUMBER_RE = re.compile(r"\b\d{3}-\d{4,}\b")


@dataclass(frozen=True)
class ReplyRow:
    complaint_number: str
    reply: str
    row_number: int


@dataclass
class ApplyStats:
    workbook_rows: int = 0
    replies_loaded: int = 0
    updated: int = 0
    dry_run_updates: int = 0
    skipped_no_reply: int = 0
    skipped_no_folder: int = 0
    skipped_no_deo_report: int = 0
    skipped_no_remarks_cell: int = 0
    duplicate_rows: int = 0


@dataclass
class RunReport:
    report_dir: Path
    details: list[dict[str, str]]

    def add(self, complaint_number: str, status: str, message: str, **extra: Any) -> None:
        row = {
            "complaint_number": complaint_number,
            "status": status,
            "message": message,
        }
        row.update({key: "" if value is None else str(value) for key, value in extra.items()})
        self.details.append(row)


class FileLogHandler(logging.Handler):
    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(message + "\n")
        except Exception:
            self.handleError(record)


def resolve_project_path(project_root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def make_report_dir(pending_dir: Path) -> Path:
    report_dir = (
        pending_dir
        / "reply-apply-reports"
        / datetime.now().strftime("%Y%m%d-%H%M%S")
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir


def attach_file_logger(logger: logging.Logger, report_dir: Path) -> FileLogHandler:
    handler = FileLogHandler(report_dir / "run.log")
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)
    return handler


def clean_reply_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).replace("\xa0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines: list[str] = []
    previous_blank = False
    for raw_line in text.split("\n"):
        line = re.sub(r"[ \t\f\v]+", " ", raw_line).strip()
        if not line:
            if lines and not previous_blank:
                lines.append("")
            previous_blank = True
            continue
        lines.append(line)
        previous_blank = False
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines).strip()


def complaint_number_from_value(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    match = COMPLAINT_NUMBER_RE.search(str(value))
    return match.group(0) if match else ""


def read_replies_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        dataframe = pd.read_csv(path, encoding="utf-8-sig", dtype=object)
    elif suffix in {".xlsx", ".xls", ".xlsm"}:
        dataframe = pd.read_excel(path, dtype=object)
    else:
        raise ValueError(
            f"Unsupported replies file type {suffix or '<none>'!r}. "
            "Use CSV, XLSX, XLS, or XLSM."
        )

    # Spreadsheet exports sometimes contain blank trailing columns. They are not
    # part of the two-column reply contract and can be discarded safely.
    dataframe = dataframe.dropna(axis="columns", how="all")
    if len(dataframe.columns) != 2:
        raise ValueError(
            "Replies file must contain exactly two non-empty columns: "
            "column 1 = complaint number, column 2 = reply. "
            f"Found {len(dataframe.columns)} columns: "
            + ", ".join(repr(str(column)) for column in dataframe.columns)
        )
    return dataframe


def load_replies(workbook_path: Path, stats: ApplyStats) -> dict[str, ReplyRow]:
    dataframe = read_replies_file(workbook_path)
    stats.workbook_rows = len(dataframe)
    complaint_col, reply_col = dataframe.columns

    replies: dict[str, ReplyRow] = {}
    logger = logging.getLogger(LOGGER_NAME)
    for index, row in dataframe.iterrows():
        row_number = int(index) + 2
        complaint_number = complaint_number_from_value(row.get(complaint_col))
        reply = clean_reply_text(row.get(reply_col))
        if not complaint_number:
            logger.warning("Workbook row %s skipped: no complaint number.", row_number)
            continue
        if not reply:
            stats.skipped_no_reply += 1
            logger.info(
                "Workbook row %s skipped for %s: reply is empty.",
                row_number,
                complaint_number,
            )
            continue
        if complaint_number in replies:
            stats.duplicate_rows += 1
            logger.warning(
                "Duplicate workbook reply for %s at row %s; keeping the later row.",
                complaint_number,
                row_number,
            )
        replies[complaint_number] = ReplyRow(complaint_number, reply, row_number)

    stats.replies_loaded = len(replies)
    return replies


def set_cell_text(cell: ET.Element, value: str) -> None:
    for child in list(cell):
        cell.remove(child)

    paragraphs = value.split("\n")
    if not paragraphs:
        paragraphs = [""]
    for paragraph_text in paragraphs:
        paragraph = ET.Element(TEXT_P_TAG)
        paragraph.text = paragraph_text
        cell.append(paragraph)


def find_target_cell(root: ET.Element, heading: str) -> ET.Element | None:
    heading_key = normalize_label_text(heading)
    for table in root.findall(".//table:table", NS):
        rows = table.findall(TABLE_ROW_TAG)
        for row_index, row in enumerate(rows):
            cells = expanded_cells(row)
            for col_index, cell in enumerate(cells):
                text_key = normalize_label_text(cell_text(cell))
                if heading_key and heading_key in text_key:
                    for next_row in rows[row_index + 1 :]:
                        target_cells = expanded_cells(next_row)
                        if col_index < len(target_cells):
                            return target_cells[col_index]
    return None


def update_odt_remarks(odt_path: Path, reply: str, *, dry_run: bool) -> bool:
    with TemporaryDirectory() as temp_name:
        temp_dir = Path(temp_name)
        with zipfile.ZipFile(odt_path, "r") as source_zip:
            source_zip.extractall(temp_dir)

        content_xml = temp_dir / "content.xml"
        if not content_xml.exists():
            return False

        tree = ET.parse(content_xml)
        target_cell = find_target_cell(tree.getroot(), "Remarks")
        if target_cell is None:
            return False

        if dry_run:
            return True

        set_cell_text(target_cell, reply)
        tree.write(content_xml, encoding="utf-8", xml_declaration=True)

        temp_output = odt_path.with_suffix(odt_path.suffix + ".tmp")
        try:
            with zipfile.ZipFile(temp_output, "w", compression=zipfile.ZIP_DEFLATED) as out_zip:
                mimetype = temp_dir / "mimetype"
                if mimetype.exists():
                    out_zip.write(mimetype, "mimetype", compress_type=zipfile.ZIP_STORED)
                for item in sorted(temp_dir.rglob("*")):
                    if not item.is_file() or item.name == "mimetype":
                        continue
                    out_zip.write(item, item.relative_to(temp_dir).as_posix())
            temp_output.replace(odt_path)
        finally:
            if temp_output.exists():
                temp_output.unlink()
    return True


def find_deo_report(folder: Path, complaint_number: str) -> Path | None:
    expected = folder / f"{complaint_number} - DEO Report.odt"
    if expected.exists():
        return expected
    candidates = sorted(folder.glob("*DEO Report*.odt"))
    return candidates[0] if candidates else None


def backup_file(path: Path, backup_root: Path, pending_dir: Path) -> Path:
    relative = path.relative_to(pending_dir)
    target = backup_root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, target)
    return target


def apply_replies(
    *,
    project_root: Path,
    workbook_path: Path,
    pending_dir: Path,
    dry_run: bool,
    backup: bool,
    report: RunReport,
) -> ApplyStats:
    logger = logging.getLogger(LOGGER_NAME)
    stats = ApplyStats()
    replies = load_replies(workbook_path, stats)
    logger.info("Loaded %d usable reply row(s) from %s.", len(replies), workbook_path)

    folder_numbers = {
        path.name
        for path in pending_dir.iterdir()
        if path.is_dir() and COMPLAINT_NUMBER_RE.fullmatch(path.name)
    }
    backup_root = (
        pending_dir
        / ".reply-backups"
        / datetime.now().strftime("%Y%m%d-%H%M%S")
    )

    for complaint_number in sorted(folder_numbers):
        folder = pending_dir / complaint_number
        reply_row = replies.get(complaint_number)
        if reply_row is None:
            stats.skipped_no_reply += 1
            logger.info("Skipped %s: folder exists but workbook has no reply.", complaint_number)
            report.add(
                complaint_number,
                "skipped_no_reply",
                "Folder exists but workbook has no reply.",
                folder=folder,
            )
            continue

        deo_report = find_deo_report(folder, complaint_number)
        if deo_report is None:
            stats.skipped_no_deo_report += 1
            logger.warning("Skipped %s: no DEO Report ODT found in %s.", complaint_number, folder)
            report.add(
                complaint_number,
                "skipped_no_deo_report",
                "No DEO Report ODT found.",
                folder=folder,
                workbook_row=reply_row.row_number,
            )
            continue

        backup_path = None
        if backup and not dry_run:
            backup_path = backup_file(deo_report, backup_root, pending_dir)

        updated = update_odt_remarks(deo_report, reply_row.reply, dry_run=dry_run)
        if not updated:
            stats.skipped_no_remarks_cell += 1
            logger.warning("Skipped %s: could not find Remarks target cell in %s.", complaint_number, deo_report)
            report.add(
                complaint_number,
                "skipped_no_remarks_cell",
                "Could not find Remarks target cell in DEO Report.",
                report_file=deo_report,
                workbook_row=reply_row.row_number,
                backup_file=backup_path,
            )
            continue

        if dry_run:
            stats.dry_run_updates += 1
            logger.debug("DRY RUN: Would apply reply for %s to %s.", complaint_number, deo_report)
            report.add(
                complaint_number,
                "dry_run_update",
                "Would apply reply to DEO Report.",
                report_file=deo_report,
                workbook_row=reply_row.row_number,
            )
        else:
            if backup_path is not None:
                logger.debug("Backup saved for %s: %s", complaint_number, backup_path)
            stats.updated += 1
            logger.debug("Applied reply for %s to %s.", complaint_number, deo_report)
            report.add(
                complaint_number,
                "updated",
                "Applied reply to DEO Report.",
                report_file=deo_report,
                workbook_row=reply_row.row_number,
                backup_file=backup_path,
            )

    for complaint_number in sorted(set(replies) - folder_numbers):
        stats.skipped_no_folder += 1
        logger.info("Skipped %s: workbook has a reply but no matching pending folder.", complaint_number)
        report.add(
            complaint_number,
            "skipped_no_folder",
            "Workbook has a reply but no matching pending folder.",
            workbook_row=replies[complaint_number].row_number,
        )

    return stats


def stats_dict(stats: ApplyStats) -> dict[str, int]:
    return {
        "workbook_rows": stats.workbook_rows,
        "replies_loaded": stats.replies_loaded,
        "updated": stats.updated,
        "dry_run_updates": stats.dry_run_updates,
        "skipped_no_reply": stats.skipped_no_reply,
        "skipped_no_folder": stats.skipped_no_folder,
        "skipped_no_deo_report": stats.skipped_no_deo_report,
        "skipped_no_remarks_cell": stats.skipped_no_remarks_cell,
        "duplicate_rows": stats.duplicate_rows,
    }


def write_run_report(
    report: RunReport,
    *,
    stats: ApplyStats,
    workbook_path: Path,
    pending_dir: Path,
    dry_run: bool,
    backup: bool,
) -> None:
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "workbook_path": str(workbook_path),
        "pending_dir": str(pending_dir),
        "dry_run": dry_run,
        "backup_enabled": backup,
        "stats": stats_dict(stats),
        "report_dir": str(report.report_dir),
    }
    (report.report_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    lines = [
        "Apply DEO Replies Run Report",
        f"Generated at: {summary['generated_at']}",
        f"Workbook: {workbook_path}",
        f"Pending folder: {pending_dir}",
        f"Mode: {'dry run' if dry_run else 'apply'}",
        "",
        "Summary",
    ]
    for key, value in stats_dict(stats).items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "Files",
            "- summary.json",
            "- summary.txt",
            "- details.csv",
            "- run.log",
        ]
    )
    (report.report_dir / "summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    fieldnames = [
        "complaint_number",
        "status",
        "message",
        "report_file",
        "folder",
        "workbook_row",
        "backup_file",
    ]
    with (report.report_dir / "details.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(report.details)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply two-column CSV/Excel replies to pending CRM DEO Report ODT files."
    )
    parser.add_argument(
        "--replies-file",
        default=DEFAULT_REPLIES_FILE,
        help="Two-column CSV/Excel file: complaint number first, reply second.",
    )
    parser.add_argument("--pending-dir", default=DEFAULT_PENDING_DIR, help="Pending compliance folder containing complaint-number folders.")
    parser.add_argument("--dry-run", action="store_true", help="Preview matches without modifying ODT files.")
    parser.add_argument("--no-backup", action="store_true", help="Do not save backups before modifying ODT files.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent
    args = parse_args(argv)
    workbook_path = resolve_project_path(project_root, args.replies_file)
    pending_dir = resolve_project_path(project_root, args.pending_dir)

    try:
        if not workbook_path.exists():
            raise FileNotFoundError(f"Replies file not found: {workbook_path}")
        if not pending_dir.exists() or not pending_dir.is_dir():
            raise FileNotFoundError(f"Pending folder not found: {pending_dir}")

        report_dir = make_report_dir(pending_dir)
        file_handler = attach_file_logger(logger, report_dir)
        report = RunReport(report_dir=report_dir, details=[])
        logger.info("Run report folder: %s", report_dir)
        backup_enabled = not args.no_backup and not args.dry_run
        stats = apply_replies(
            project_root=project_root,
            workbook_path=workbook_path,
            pending_dir=pending_dir,
            dry_run=args.dry_run,
            backup=backup_enabled,
            report=report,
        )
        write_run_report(
            report,
            stats=stats,
            workbook_path=workbook_path,
            pending_dir=pending_dir,
            dry_run=args.dry_run,
            backup=backup_enabled,
        )
        logger.info(
            "Reply apply summary: workbook_rows=%d replies_loaded=%d updated=%d dry_run_updates=%d "
            "skipped_no_reply=%d skipped_no_folder=%d skipped_no_deo_report=%d "
            "skipped_no_remarks_cell=%d duplicate_rows=%d.",
            stats.workbook_rows,
            stats.replies_loaded,
            stats.updated,
            stats.dry_run_updates,
            stats.skipped_no_reply,
            stats.skipped_no_folder,
            stats.skipped_no_deo_report,
            stats.skipped_no_remarks_cell,
            stats.duplicate_rows,
        )
        logger.info("Run report written: %s", report_dir)
        logger.removeHandler(file_handler)
        return 0 if (stats.updated or stats.dry_run_updates) else 1
    except Exception:
        logger.exception("Failed to apply DEO replies.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from crm.extract import (
    COMPLAINT_OVERRIDES,
    CRM_COMPLAINT_RE,
    complaint_label_review_candidates,
)

LOGGER = logging.getLogger("crm_number_review")


def resolve_project_path(project_root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def load_existing_overrides(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("Could not read existing override file %s: %s", path, exc)
        return {}
    if not isinstance(raw, dict):
        LOGGER.warning("Override file is not a JSON object: %s", path)
        return {}
    return {str(key): str(value or "").strip() for key, value in raw.items()}


def open_folder(path: Path) -> None:
    opener = shutil.which("xdg-open")
    if not opener:
        LOGGER.warning("xdg-open is not available; open folder manually: %s", path)
        return
    subprocess.Popen(
        [opener, str(path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def review_entry(pdf_path: Path, existing_value: str) -> tuple[str, dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    error = ""
    try:
        candidates = complaint_label_review_candidates(pdf_path)
    except Exception as exc:
        error = str(exc)

    suggested = ""
    if candidates:
        first = candidates[0]
        second_score = float(candidates[1]["score"]) if len(candidates) > 1 else 0.0
        first_score = float(first["score"])
        if first_score >= 2.0 and first_score >= second_score + 1.0:
            suggested = str(first["complaint_number"])

    override_value = existing_value
    if not override_value and suggested:
        override_value = suggested

    return override_value, {
        "file": pdf_path.name,
        "reviewed_complaint_number": override_value,
        "suggested_complaint_number": suggested,
        "ocr_candidates": candidates,
        "status": "reviewed" if CRM_COMPLAINT_RE.fullmatch(override_value) else "needs_manual_review",
        "error": error,
    }


def run_review(review_dir: Path, should_open_folder: bool) -> int:
    review_dir.mkdir(parents=True, exist_ok=True)
    pdfs = sorted(path for path in review_dir.iterdir() if path.is_file() and path.suffix.lower() == ".pdf")
    overrides_path = review_dir / COMPLAINT_OVERRIDES
    review_path = review_dir / "crm-number-review.json"
    existing_overrides = load_existing_overrides(overrides_path)
    if not pdfs:
        review_path.write_text(
            json.dumps(
                {
                    "review_folder": str(review_dir),
                    "instructions": "No number-missing PDFs are currently in this folder.",
                    "files": [],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        LOGGER.info("No number-missing PDF files found in %s.", review_dir)
        LOGGER.info("OCR candidate report written: %s", review_path)
        if should_open_folder:
            open_folder(review_dir)
        return 0

    overrides: dict[str, str] = {}
    entries: list[dict[str, Any]] = []
    for pdf_path in pdfs:
        override_value, entry = review_entry(pdf_path, existing_overrides.get(pdf_path.name, ""))
        overrides[pdf_path.name] = override_value
        entries.append(entry)

    overrides_path.write_text(
        json.dumps(overrides, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    review_path.write_text(
        json.dumps(
            {
                "review_folder": str(review_dir),
                "instructions": (
                    "Open crm-complaint-overrides.json and correct each value. "
                    "Leave blank only when the complaint number is still unreadable."
                ),
                "files": entries,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    LOGGER.info("Found %d PDF(s) needing number review.", len(pdfs))
    LOGGER.info("Review override file written: %s", overrides_path)
    LOGGER.info("OCR candidate report written: %s", review_path)
    if should_open_folder:
        open_folder(review_dir)
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    parser = argparse.ArgumentParser(description="Prepare CRM number-missing PDFs for manual review.")
    parser.add_argument(
        "--review-dir",
        default="phase1-crm/unprocessed-crm/filtered/fresh/number-missing",
        help="Folder containing number-missing CRM PDFs.",
    )
    parser.add_argument("--open-folder", action="store_true", help="Open the review folder after writing files.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    return run_review(resolve_project_path(project_root, args.review_dir), args.open_folder)


if __name__ == "__main__":
    sys.exit(main())

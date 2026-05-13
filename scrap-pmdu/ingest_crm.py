"""
CRM Ingestion to Artifacts
Save as ingest_crm.py in deomeeautomations/scrap-pmdu/
Run with: uv run python ingest_crm.py
"""

import io
import json
import logging
import re
import shutil
import sys
from pathlib import Path

try:
    from pypdf import PdfReader
except ImportError:
    print("Please run: uv add pypdf")
    sys.exit(1)

# Basic logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger("crm_ingest")


def parse_crm_pdf(pdf_path: Path) -> dict[str, str]:
    """Extracts the Key-Value tabular data from the CRM PDF."""
    reader = PdfReader(str(pdf_path))
    full_text = "".join(page.extract_text() or "" for page in reader.pages)

    matches = re.findall(r'"([^"]+)"\s*,\s*"([^"]*)"', full_text)
    return {
        k.strip().replace("\n", "").replace("\r", ""): v.strip()
        .replace("\n", "")
        .replace("\r", "")
        for k, v in matches
        if k.strip()
    }


def main():
    project_root = Path(__file__).resolve().parent
    raw_crm_dir = project_root / "crm-main-complaints"
    artifacts_dir = project_root / "artifacts"

    if not raw_crm_dir.exists():
        logger.error(f"Directory not found: {raw_crm_dir}")
        return 1

    pdf_files = list(raw_crm_dir.glob("*.pdf"))
    if not pdf_files:
        logger.info("No PDFs found to process.")
        return 0

    logger.info(f"Found {len(pdf_files)} CRM PDFs. Building artifacts...")

    for pdf_path in pdf_files:
        try:
            # 1. Parse the raw PDF data
            extracted_data = parse_crm_pdf(pdf_path)

            # Use the extracted complaint number if available, otherwise use filename
            complaint_code = extracted_data.get("Complaint No")
            if not complaint_code:
                # Fallback: E.g., extract '104-5450529' from 'CRM - 104-5450529.pdf'
                match = re.search(r"\d{3}-\d+", pdf_path.name)
                complaint_code = (
                    match.group(0) if match else pdf_path.stem.replace(" ", "")
                )

            # 2. Build the exact folder structure the Paperless engine expects
            # e.g., artifacts/104-5450529/v1/
            case_dir = artifacts_dir / complaint_code / "v1"
            case_dir.mkdir(parents=True, exist_ok=True)

            # 3. Copy the PDF into the new artifact folder
            dest_pdf_path = case_dir / f"{complaint_code}.pdf"
            shutil.copy2(pdf_path, dest_pdf_path)

            # 4. Generate the snapshot.json mapping directly to your paperless.py structure
            # This maps to {identity.citizen_name}, {identity.category}, etc.
            snapshot_data = {
                "complaint_code": complaint_code,
                "version": 1,
                "source": "CRM",
                "generated_pdf": f"artifacts/{complaint_code}/v1/{complaint_code}.pdf",
                "identity": {
                    "citizen_name": extracted_data.get("Person Name", ""),
                    "citizen_contact": extracted_data.get("Mobile No", ""),
                    "category": extracted_data.get("Category", ""),
                    "level_one": "School Education Department",  # Example department
                },
                "attachments": [],  # CRM usually doesn't have split attachments right away
            }

            snapshot_path = case_dir / "snapshot.json"
            snapshot_path.write_text(
                json.dumps(snapshot_data, indent=2), encoding="utf-8"
            )

            logger.info(f"Successfully staged artifact for: {complaint_code}")

            # Optional: Move the processed raw PDF to an "archive" folder so you don't re-process it
            archive_dir = raw_crm_dir / "archived"
            archive_dir.mkdir(exist_ok=True)
            shutil.move(str(pdf_path), str(archive_dir / pdf_path.name))

        except Exception as e:
            logger.error(f"Failed to process {pdf_path.name}: {e}")

    logger.info("CRM ingestion complete! Ready for Paperless sync.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

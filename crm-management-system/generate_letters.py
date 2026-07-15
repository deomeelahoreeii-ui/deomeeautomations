from dotenv import load_dotenv

load_dotenv()  # This forces Python to read your .env file automatically

import asyncio
import os
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

from paperless import PaperlessClient, PaperlessSettings, clean_text


def normalize_label(value: Any) -> str:
    value = clean_text("" if value is None else str(value)).lower()
    value = value.replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "", value)


def option_maps(
    custom_fields: list[dict[str, Any]],
) -> tuple[dict[int, str], dict[tuple[int, str], str]]:
    field_names = {
        int(field["id"]): str(field.get("name", "")) for field in custom_fields
    }
    option_labels: dict[tuple[int, str], str] = {}
    for field in custom_fields:
        field_id = int(field["id"])
        for option in field.get("extra_data", {}).get("select_options", []):
            option_labels[(field_id, str(option.get("id")))] = str(
                option.get("label", "")
            )
    return field_names, option_labels


def decoded_custom_fields(
    document: dict[str, Any],
    field_names: dict[int, str],
    option_labels: dict[tuple[int, str], str],
) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for item in document.get("custom_fields", []):
        field_id = int(item["field"])
        name = field_names.get(field_id, str(field_id))
        value = item.get("value")
        if isinstance(value, str):
            value = option_labels.get((field_id, value), value)
        decoded[name] = value
    return decoded


def complaint_code(fields: dict[str, Any], document: dict[str, Any]) -> str:
    candidates = [
        clean_text(fields.get("Complaint Number")),
        clean_text(document.get("title")),
        str(document["id"]),
    ]
    for candidate in candidates:
        match = re.search(r"\b\d{3,}-\d{4,}\b", candidate)
        if match:
            return match.group(0)
    fallback = candidates[0] or candidates[1] or candidates[2]
    fallback = re.sub(
        r"\s+-\s+Main Complaint(?:\s+-\s+v\d+)?\s*$",
        "",
        fallback,
        flags=re.IGNORECASE,
    )
    return clean_text(fallback)


def generate_odt_letter(
    template_path: Path, output_path: Path, replacements: dict[str, str]
) -> None:
    """Reads an ODT template, replaces placeholders in content.xml, and saves as a new ODT."""
    with zipfile.ZipFile(template_path, "r") as template_zip:
        with zipfile.ZipFile(output_path, "w") as output_zip:
            for item in template_zip.infolist():
                content = template_zip.read(item.filename)
                # ODT text content is stored in content.xml
                if item.filename == "content.xml":
                    content_str = content.decode("utf-8")
                    for placeholder, actual_value in replacements.items():
                        # Handle XML escaping for special characters like & or <
                        safe_value = (
                            str(actual_value)
                            .replace("&", "&amp;")
                            .replace("<", "&lt;")
                            .replace(">", "&gt;")
                        )
                        content_str = content_str.replace(placeholder, safe_value)
                    content = content_str.encode("utf-8")
                output_zip.writestr(item, content)


async def main():
    # 1. Setup paths and directories
    project_root = Path(__file__).parent
    template_path = (
        project_root.parent / "sample_letter.odt"
    )  # Adjust if your sample is in a different location
    output_dir = project_root / "generated_letters"
    output_dir.mkdir(exist_ok=True)

    if not template_path.exists():
        print(f"Error: Template not found at {template_path}")
        return

    # 2. Configure Paperless Settings (pulling from your .env file)
    settings = PaperlessSettings(
        base_url=os.environ.get(
            "PAPERLESS_URL", "https://paperless.lab.internal/dashboard"
        ),
        username=os.environ.get("PAPERLESS_USERNAME", "admin"),
        password=os.environ.get("PAPERLESS_PASSWORD", "admin"),
        artifact_dir=project_root / "artifacts",
        duckdb_path=project_root / "paperless.duckdb",
        timeout_seconds=30,
        task_timeout_seconds=60,
        max_cases=None,
        document_type_complaint="Complaint",
        document_type_attachment="Attachment",
        correspondent_name="CEO, (DEA), Lahore",
        source_label="CRM Portal",
        field_config_path=project_root / "paperless_field_defaults.json",
        dry_run=False,
        verify_ssl=os.environ.get("PAPERLESS_VERIFY_SSL", "true").strip().lower()
        in {"1", "true", "yes", "y", "on"},
        sync_mode="upload",
    )

    print("Connecting to Paperless...")
    async with PaperlessClient(settings) as client:
        # Fetch Custom Fields to map IDs to Names
        custom_fields = await client.paginated_results(
            "/api/custom_fields/?page_size=100"
        )
        field_names, option_labels = option_maps(custom_fields)

        # Fetch all documents
        print("Fetching documents from Paperless...")
        all_documents = await client.paginated_results("/api/documents/?page_size=100")

        generated_count = 0
        today_date = datetime.now().strftime("%d/%m/%Y")

        for document in all_documents:
            fields = decoded_custom_fields(document, field_names, option_labels)

            # Filter: We only want Main Complaints that are Under Investigation
            doc_role = normalize_label(fields.get("Document Role", ""))
            status = normalize_label(fields.get("Status", ""))

            if doc_role == normalize_label(
                "Main Complaint"
            ) and status == normalize_label("Under Investigation"):
                c_code = complaint_code(fields, document)

                # Fetch raw values
                school_name = clean_text(fields.get("The Reported Entity Name"))
                school_address = clean_text(fields.get("The Reported Entity Address"))
                tehsil = clean_text(fields.get("Tehsil")) or "Unassigned"

                # Base replacements (Complaint code, date, and Tehsil)
                replacements = {
                    "_____________": c_code,
                    "07/05/2026": today_date,
                    "[Name of Tehsil]": tehsil,
                }

                # Handle Missing School Name
                if school_name:
                    replacements["[Name of Private School]"] = school_name
                else:
                    replacements["[Name of Private School]"] = (
                        "The Educational Institution Concerned"
                    )
                    # This changes the hardcoded "The Principal" in the template
                    replacements["The Principal"] = "The Principal Concerned"

                # Handle Missing Address
                if school_address:
                    replacements["[School Address / Area]"] = school_address
                else:
                    # If address is missing, fallback to the Tehsil so it reads: [Tehsil], Lahore.
                    replacements["[School Address / Area]"] = tehsil

                # Construct output file name safely
                if school_name:
                    safe_school_name = school_name.replace("/", "-").replace("\\", "-")
                    filename = f"Inquiry_Letter_{c_code}_{safe_school_name}.odt"
                else:
                    filename = f"Inquiry_Letter_{c_code}.odt"

                output_path = output_dir / filename

                # Generate the file
                generate_odt_letter(template_path, output_path, replacements)
                print(f"✅ Generated: {filename}")
                generated_count += 1

        print(
            f"\nFinished! Successfully generated {generated_count} letters in the '{output_dir.name}' folder."
        )


if __name__ == "__main__":
    asyncio.run(main())

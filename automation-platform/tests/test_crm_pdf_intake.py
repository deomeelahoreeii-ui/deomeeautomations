from __future__ import annotations

import zipfile
from pathlib import Path

from crm_filters.pdf_intake import (
    create_pdf_batch_archive,
    extract_pdf_batch,
    validate_pdf_batch_archive,
)


def write_pdf(path: Path, marker: bytes) -> None:
    path.write_bytes(b"%PDF-1.4\n" + marker + b"\n%%EOF\n")


def test_pdf_batch_archive_is_deterministic_and_detects_duplicates(tmp_path: Path) -> None:
    first = tmp_path / "first.pdf"
    duplicate = tmp_path / "duplicate.pdf"
    third = tmp_path / "third.pdf"
    write_pdf(first, b"same")
    duplicate.write_bytes(first.read_bytes())
    write_pdf(third, b"different")

    archive_one = tmp_path / "one.zip"
    archive_two = tmp_path / "two.zip"
    files = [("Complaint A.pdf", first), ("Complaint A.pdf", duplicate), ("Third.pdf", third)]

    digest_one, metadata_one, errors_one, warnings_one = create_pdf_batch_archive(files, archive_one)
    digest_two, metadata_two, errors_two, warnings_two = create_pdf_batch_archive(files, archive_two)

    assert errors_one == errors_two == []
    assert digest_one == digest_two
    assert archive_one.read_bytes() == archive_two.read_bytes()
    assert metadata_one["file_count"] == 3
    assert metadata_one["duplicate_file_count"] == 1
    assert metadata_two["files"][1]["stored_name"] == "Complaint A (2).pdf"
    assert any("exact duplicate" in warning for warning in warnings_one)
    assert warnings_one == warnings_two


def test_pdf_batch_validation_and_safe_extraction(tmp_path: Path) -> None:
    first = tmp_path / "first.pdf"
    write_pdf(first, b"content")
    archive = tmp_path / "batch.zip"
    create_pdf_batch_archive([("first.pdf", first)], archive)

    status, schema, metadata, errors, _warnings = validate_pdf_batch_archive(archive)
    extracted = extract_pdf_batch(archive, tmp_path / "extracted")

    assert status == "valid"
    assert schema == "crm_pdf_batch_v1"
    assert errors == []
    assert metadata["file_count"] == 1
    assert [path.name for path in extracted] == ["first.pdf"]
    assert extracted[0].read_bytes().startswith(b"%PDF-")


def test_pdf_batch_extraction_rejects_path_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.writestr("../escape.pdf", b"%PDF-1.4\n%%EOF\n")

    status, _schema, _metadata, errors, _warnings = validate_pdf_batch_archive(archive)

    assert status == "invalid"
    assert any("Unsafe archive member" in error for error in errors)

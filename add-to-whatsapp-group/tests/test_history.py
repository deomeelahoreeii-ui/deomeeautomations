from pathlib import Path

from whatsapp_group_importer.contacts import Contact
from whatsapp_group_importer.history import AttemptStore, file_sha256


def test_attempt_ledger_is_file_and_group_scoped(tmp_path: Path):
    source = tmp_path / "contacts.xlsx"
    source.write_bytes(b"workbook")
    contact = Contact(source_row=2, raw_value="03001234567", phone="923001234567")

    with AttemptStore(tmp_path / "history.sqlite3") as store:
        assert store.reserve_attempt(
            source_file=source,
            source_digest=file_sha256(source),
            group_jid="one@g.us",
            contact=contact,
            batch_id="batch-1",
            job_id="job-1",
        )
        assert not store.reserve_attempt(
            source_file=source,
            source_digest=file_sha256(source),
            group_jid="one@g.us",
            contact=contact,
            batch_id="batch-2",
            job_id="job-2",
        )
        assert store.reserve_attempt(
            source_file=source,
            source_digest=file_sha256(source),
            group_jid="two@g.us",
            contact=contact,
            batch_id="batch-3",
            job_id="job-3",
        )
        store.update_attempt(
            source_file=source,
            group_jid="one@g.us",
            phone=contact.phone,
            status="privacy_rejected",
            error_code="403",
        )
        records = store.attempted_contacts(source, "one@g.us", [contact])

    assert records[contact.phone].status == "privacy_rejected"
    assert records[contact.phone].error_code == "403"

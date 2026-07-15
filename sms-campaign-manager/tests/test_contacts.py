from sms_campaign_manager.contacts import normalize_phone, sms_segments, suggested_number_column
from sms_campaign_manager.service import campaign_key
from pathlib import Path


def test_pakistan_phone_normalization():
    assert normalize_phone("0300-1234567") == "+923001234567"
    assert normalize_phone("+92 300 1234567") == "+923001234567"


def test_column_suggestion():
    assert suggested_number_column(["name", "contact_no", "district"]) == "contact_no"


def test_segments():
    assert sms_segments("a" * 160)[0] == 1
    assert sms_segments("a" * 161)[0] == 2
    assert sms_segments("سلام")[1] == "Unicode"


def test_campaign_identity_ignores_file_contents_and_message(tmp_path: Path):
    path = tmp_path / "contacts.xlsx"
    path.write_text("first")
    first = campaign_key(path, "Sheet1", "contact_no", "old message")[0]
    path.write_text("changed")
    second = campaign_key(path, "Sheet1", "contact_no", "new message")[0]
    assert first == second


def test_explicit_campaign_identity_is_stable(tmp_path: Path):
    one = campaign_key(tmp_path / "one.xlsx", "A", "phone", "one", "office-test")[0]
    two = campaign_key(tmp_path / "two.xlsx", "B", "mobile", "two", "office-test")[0]
    assert one == two

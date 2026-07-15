from pathlib import Path

from openpyxl import Workbook

from whatsapp_group_importer.contacts import load_contacts, normalize_phone, select_contacts


def test_normalizes_pakistan_mobile_formats():
    assert normalize_phone("0321-4729842") == "923214729842"
    assert normalize_phone(3214729842) == "923214729842"
    assert normalize_phone("+92 321 4729842") == "923214729842"


def test_loads_named_column_and_selects_last(tmp_path: Path):
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["name", "contact_no"])
    sheet.append(["A", "03001234567"])
    sheet.append(["B", "03111234567"])
    sheet.append(["duplicate", "03111234567"])
    path = tmp_path / "contacts.xlsx"
    workbook.save(path)

    contacts, warnings = load_contacts(path)
    selected = select_contacts(contacts, "last", 1, None, None)

    assert [contact.phone for contact in selected] == ["923111234567"]
    assert "duplicate" in warnings[0]

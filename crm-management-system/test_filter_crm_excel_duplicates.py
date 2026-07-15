import tempfile
import unittest
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

from filter_crm_excel_duplicates import COMPLAINT_COL_NAME, read_crm_dataframe


class CrmSheetReaderTests(unittest.TestCase):
    def test_reads_standard_header_row(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "standard.xlsx"
            pd.DataFrame(
                [["104-6601116", "Overdue"]],
                columns=[COMPLAINT_COL_NAME, "Complaint_Status"],
            ).to_excel(path, index=False)

            dataframe, detected_header_row = read_crm_dataframe(path)

            self.assertIsNone(detected_header_row)
            self.assertIn(COMPLAINT_COL_NAME, dataframe.columns)
            self.assertEqual(dataframe.loc[0, COMPLAINT_COL_NAME], "104-6601116")

    def test_detects_header_below_merged_title_row(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "portal-export.xlsx"
            workbook = Workbook()
            worksheet = workbook.active
            worksheet.title = "OVERDUE CRM DEO MEE"
            worksheet.merge_cells("A1:B1")
            worksheet["A1"] = "Complaint Listing Data"
            worksheet["A2"] = COMPLAINT_COL_NAME
            worksheet["B2"] = "Complaint_Status"
            worksheet["A3"] = "104-6601116"
            worksheet["B3"] = "Overdue"
            workbook.save(path)

            dataframe, detected_header_row = read_crm_dataframe(path)

            self.assertEqual(detected_header_row, 1)
            self.assertIn(COMPLAINT_COL_NAME, dataframe.columns)
            self.assertEqual(dataframe.loc[0, COMPLAINT_COL_NAME], "104-6601116")


if __name__ == "__main__":
    unittest.main()

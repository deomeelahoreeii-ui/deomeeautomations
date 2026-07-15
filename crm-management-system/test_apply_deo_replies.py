import tempfile
import unittest
from pathlib import Path

import pandas as pd

from apply_deo_replies import ApplyStats, load_replies, read_replies_file


class RepliesFileTests(unittest.TestCase):
    def test_loads_two_column_csv_by_position(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "replies.csv"
            path.write_text(
                "Any complaint heading,Any reply heading\n"
                '104-6706553,"First line\nSecond line"\n',
                encoding="utf-8-sig",
            )

            stats = ApplyStats()
            replies = load_replies(path, stats)

            self.assertEqual(stats.workbook_rows, 1)
            self.assertEqual(replies["104-6706553"].reply, "First line\nSecond line")

    def test_loads_two_column_excel_by_position(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "replies.xlsx"
            pd.DataFrame(
                [["104-6706553", "Prepared response"]],
                columns=["Complaint", "Reply"],
            ).to_excel(path, index=False)

            replies = load_replies(path, ApplyStats())

            self.assertEqual(replies["104-6706553"].reply, "Prepared response")

    def test_rejects_more_than_two_nonempty_columns(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "replies.csv"
            path.write_text("Complaint,Reply,Notes\n104-6706553,Yes,Extra\n")

            with self.assertRaisesRegex(ValueError, "exactly two non-empty columns"):
                read_replies_file(path)


if __name__ == "__main__":
    unittest.main()

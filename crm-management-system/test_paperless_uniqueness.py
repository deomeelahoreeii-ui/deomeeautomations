import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from paperless import preflight_unique_pending_crm_snapshots


FIELDS = {
    "complaint number": {"id": 1},
    "source": {"id": 2},
    "document role": {"id": 3},
}


def custom_fields(number: str) -> list[dict[str, object]]:
    return [
        {"field": 1, "value": number},
        {"field": 2, "value": "CRM Portal"},
        {"field": 3, "value": "Main Complaint"},
    ]


class FakeClient:
    def __init__(self, documents: list[dict[str, object]]) -> None:
        self.documents = documents
        self.calls = 0

    async def paginated_results(self, path: str) -> list[dict[str, object]]:
        self.calls += 1
        return self.documents


class PendingCrmUniquenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_skips_remote_and_local_duplicates_before_upload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = []
            for index, code in enumerate(
                ["104-6601116", "104-6602222", "104-6602222"]
            ):
                path = root / f"snapshot-{index}.json"
                path.write_text(
                    json.dumps({"source": "CRM", "complaint_code": code}),
                    encoding="utf-8",
                )
                paths.append(path)

            client = FakeClient(
                [
                    {
                        "id": 42,
                        "added": "2026-06-19T01:00:00Z",
                        "custom_fields": custom_fields("104-6601116"),
                    }
                ]
            )
            accepted, remote, local = await preflight_unique_pending_crm_snapshots(
                client,
                SimpleNamespace(sync_mode="upload"),
                {"custom_fields": FIELDS, "complaint_type_id": 7},
                paths,
            )

            self.assertEqual(accepted, [paths[1]])
            self.assertEqual((remote, local), (1, 1))
            self.assertEqual(client.calls, 1)

    async def test_status_override_is_not_removed_by_pending_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot.json"
            path.write_text(
                json.dumps(
                    {
                        "source": "CRM",
                        "complaint_code": "104-6601116",
                        "paperless": {"status": "Not Relevant"},
                    }
                ),
                encoding="utf-8",
            )
            client = FakeClient([])
            accepted, remote, local = await preflight_unique_pending_crm_snapshots(
                client,
                SimpleNamespace(sync_mode="upload"),
                {"custom_fields": FIELDS, "complaint_type_id": 7},
                [path],
            )

            self.assertEqual(accepted, [path])
            self.assertEqual((remote, local), (0, 0))
            self.assertEqual(client.calls, 0)


if __name__ == "__main__":
    unittest.main()

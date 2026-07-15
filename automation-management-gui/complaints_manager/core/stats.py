from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import sqlite3

import duckdb

@dataclass
class Metric:
    label: str
    value: str


def count_files(path: Path, pattern: str = "*") -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.glob(pattern) if item.is_file())


def count_dirs(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.iterdir() if item.is_dir())


def count_files_recursive(path: Path, pattern: str = "*") -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.rglob(pattern) if item.is_file())


def system_root(gui_root: Path, system_id: str) -> Path:
    if system_id == "crm":
        return (gui_root.parent / "crm-management-system").resolve(strict=False)
    if system_id == "pmdu":
        return (gui_root.parent / "pmdu-management-system").resolve(strict=False)
    if system_id == "whatsapp_groups":
        return (gui_root.parent / "add-to-whatsapp-group").resolve(strict=False)
    if system_id == "whatsapp_accounts":
        return (gui_root.parent / "whatsappbot").resolve(strict=False)
    return gui_root.resolve(strict=False)


def workspace_root(gui_root: Path) -> Path:
    resolved = gui_root.resolve(strict=False)
    candidates = [resolved, *resolved.parents]
    for candidate in candidates[:6]:
        if (candidate / "antidengue").exists():
            return candidate
    return resolved.parent


def pmdu_metrics(project_root: Path) -> list[Metric]:
    root = system_root(project_root, "pmdu")
    metrics = [
        Metric("Raw HTML", str(count_files(root / "raw_html", "*.html"))),
        Metric("Artifacts", str(count_dirs(root / "artifacts"))),
    ]
    db_path = root / "pmdu_scrape.duckdb"
    if not db_path.exists():
        metrics.extend(
            [
                Metric("Queue", "0"),
                Metric("Pending", "0"),
                Metric("Scraped", "0"),
                Metric("Failed", "0"),
            ]
        )
        return metrics

    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT COALESCE(status, 'pending') AS status, COUNT(*)
                FROM scrape_queue
                GROUP BY 1
                """
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        metrics.append(Metric("DuckDB", f"Unavailable: {exc}"))
        return metrics

    counts = {"pending": 0, "scraped": 0, "failed": 0}
    total = 0
    for status, count in rows:
        normalized = str(status or "pending").lower()
        value = int(count)
        total += value
        if normalized in counts:
            counts[normalized] += value
        else:
            counts["pending"] += value

    metrics.extend(
        [
            Metric("Queue", str(total)),
            Metric("Pending", str(counts["pending"])),
            Metric("Scraped", str(counts["scraped"])),
            Metric("Failed", str(counts["failed"])),
        ]
    )
    return metrics


def crm_metrics(project_root: Path) -> list[Metric]:
    root = system_root(project_root, "crm")
    return [
        Metric("Main PDFs", str(count_files(root / "crm-main-complaints", "*.pdf"))),
        Metric("Compliance folders", str(count_dirs(root / "crm-compliances"))),
        Metric("Generated letters", str(count_files(root / "generated_letters", "*.pdf"))),
        Metric("Dispatch temp", str(count_files(root / "dispatch_temp", "*.pdf"))),
        Metric(
            "Fresh sheets",
            str(
                count_files(root / "phase1-crm/unprocessed-crm/filtered/fresh", "*.xlsx")
                + count_files(root / "phase1-crm/unprocessed-crm/filtered/fresh", "*.csv")
            ),
        ),
    ]


def maintenance_metrics(project_root: Path) -> list[Metric]:
    crm_root = system_root(project_root, "crm")
    pmdu_root = system_root(project_root, "pmdu")
    return [
        Metric("CRM root", "yes" if crm_root.exists() else "no"),
        Metric("PMDU root", "yes" if pmdu_root.exists() else "no"),
        Metric("PMDU DB", "yes" if (pmdu_root / "pmdu_scrape.duckdb").exists() else "no"),
        Metric(
            "CRM delivery DB",
            "yes" if (crm_root / "compliance_delivery.duckdb").exists() else "no",
        ),
        Metric("GUI settings", "yes" if (project_root / ".complaints_manager.json").exists() else "no"),
    ]


def antidengue_metrics(project_root: Path) -> list[Metric]:
    base = workspace_root(project_root) / "antidengue"
    return [
        Metric("Raw files", str(count_files(base / "drop-raw-files"))),
        Metric("Output files", str(count_files_recursive(base / "output-files"))),
        Metric("Archived files", str(count_files(base / "archived-files"))),
        Metric("Unmapped reports", str(count_files_recursive(base / "unmapped-officer-reports"))),
    ]


def whatsapp_group_metrics(project_root: Path) -> list[Metric]:
    root = system_root(project_root, "whatsapp_groups")
    database = root / "group-import-history.sqlite3"
    base = [
        Metric("Excel files", str(count_files(root, "*.xlsx"))),
        Metric("Importer", "ready" if (root / "pyproject.toml").exists() else "missing"),
    ]
    if not database.exists():
        return [*base, Metric("Attempts", "0"), Metric("Added", "0"), Metric("Rejected", "0")]
    try:
        with sqlite3.connect(database) as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) FROM contact_attempts GROUP BY status"
            ).fetchall()
    except sqlite3.Error:
        return [*base, Metric("Ledger", "unavailable")]
    counts = {str(status): int(count) for status, count in rows}
    return [
        *base,
        Metric("Attempts", str(sum(counts.values()))),
        Metric("Added", str(counts.get("added_or_already_member", 0))),
        Metric("Rejected", str(counts.get("privacy_rejected", 0))),
        Metric(
            "Other failures",
            str(sum(count for status, count in counts.items() if status not in {
                "added_or_already_member", "privacy_rejected", "queued", "reserved"
            })),
        ),
    ]


def whatsapp_account_metrics(project_root: Path) -> list[Metric]:
    root = system_root(project_root, "whatsapp_accounts")
    path = root / "accounts.json"
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            accounts = data.get("accounts", []) if isinstance(data, dict) else []
            accounts = [item for item in accounts if isinstance(item, dict)]
        except (OSError, json.JSONDecodeError):
            accounts = []
    else:
        accounts = [{"id": "default", "enabled": True}]
    enabled = [item for item in accounts if bool(item.get("enabled", True))]
    data_dir = root / "data"
    qr_count = len(list(data_dir.glob("whatsapp-login-qr*.png"))) if data_dir.exists() else 0
    auth_dirs = len(list(root.glob("auth_info_baileys*"))) if root.exists() else 0
    return [
        Metric("Accounts", str(len(accounts))),
        Metric("Enabled", str(len(enabled))),
        Metric("Auth folders", str(auth_dirs)),
        Metric("QR pending", str(qr_count)),
    ]


def services_metrics(project_root: Path) -> list[Metric]:
    nats_root = project_root.parent / "nats-server"
    whatsapp_root = project_root.parent / "whatsappbot"
    accounts_path = whatsapp_root / "accounts.json"
    account_count = 1
    enabled_count = 1
    if accounts_path.exists():
        try:
            data = json.loads(accounts_path.read_text(encoding="utf-8"))
            accounts = data.get("accounts", []) if isinstance(data, dict) else []
            accounts = [item for item in accounts if isinstance(item, dict)]
            account_count = len(accounts)
            enabled_count = sum(1 for item in accounts if bool(item.get("enabled", True)))
        except (OSError, json.JSONDecodeError):
            account_count = 0
            enabled_count = 0
    return [
        Metric("NATS folder", "yes" if nats_root.exists() else "no"),
        Metric("WhatsApp folder", "yes" if whatsapp_root.exists() else "no"),
        Metric("WhatsApp accounts", str(account_count)),
        Metric("Enabled accounts", str(enabled_count)),
    ]


def metrics_for(system_id: str, project_root: Path) -> list[Metric]:
    if system_id == "services":
        return services_metrics(project_root)
    if system_id == "crm":
        return crm_metrics(project_root)
    if system_id == "pmdu":
        return pmdu_metrics(project_root)
    if system_id == "antidengue":
        return antidengue_metrics(project_root)
    if system_id == "whatsapp_groups":
        return whatsapp_group_metrics(project_root)
    if system_id == "whatsapp_accounts":
        return whatsapp_account_metrics(project_root)
    return maintenance_metrics(project_root)

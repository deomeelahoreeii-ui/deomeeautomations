# PMDU Management System

Automation for PMDU portal scraping, local artifact generation, Paperless upload,
metadata repair, and under-investigation notifications.

## Common Commands

```bash
uv run python main.py phase1
uv run python main.py phase2
uv run python archive_pmdu_history.py --selection processed --dry-run
uv run python archive_pmdu_history.py --selection processed --yes
uv run python organize_pmdu_diagnostics.py --dry-run
uv run python organize_pmdu_diagnostics.py --yes
uv run python generate_inquiry_letters.py --source paperless
uv run python main.py paperless-check --artifact-dir artifacts
uv run python main.py paperless --artifact-dir artifacts
uv run python main.py notify-preview --to group
uv run python main.py notify-send --to group
```

## Main Folders

- `pmdu/`: PMDU artifact parsing and Phase 2 code.
- `raw_html/`: scraped PMDU complaint detail pages.
- `artifacts/`: generated PMDU snapshots, PDFs, and attachments.
- `diagnostics/pmdu/`: browser login/list screenshots, HTML captures, and metadata.
- `archives/pmdu/`: timestamped archive batches with manifests and checksums.
- `inquiry_letters_to_ddeos/`: DDEO inquiry letter template, generated ODT/PDF letters, and inquiry tracking SQLite database.
- `notification_downloads/`: downloaded files staged for notifications.
- `reports/pmdu/`: PMDU reporting output.
- `sample/`: sample PMDU complaint documents.
- `paperless/`: Paperless upload/check/patch client used by PMDU flows.

Configure local credentials in `.env` before scraping or uploading:

```env
PMDU_LOGIN_URL=https://admin.pmdu.gov.pk/guardian/users/login
PMDU_COMPLAINTS_URL=https://admin.pmdu.gov.pk/pkcp/complaints/list
PMDU_USERNAME=your-username
PMDU_PASSWORD=your-password
PAPERLESS_URL=http://10.200.0.1:8010/dashboard
PAPERLESS_USERNAME=your-username
PAPERLESS_PASSWORD=your-password
```

Use the sibling `../automation-management-gui` folder for the desktop GUI.

## Archiving Before New Artifact Builds

Use `archive_pmdu_history.py --selection processed` when the live `raw_html/`
and `artifacts/` folders contain older processed cases but you want to keep only
new unprocessed raw HTML in the working set.

The archive creates normal filesystem folders, not opaque ZIP files:

```text
archives/pmdu/YYYY/MM/DD/<batch-id>/
├── MANIFEST.json
├── FILES.csv
├── README.txt
├── raw_html/
└── artifacts/
```

`MANIFEST.json` has batch-level counts and case lists. `FILES.csv` records each
archived file with size, mtime, original path, archive path, and SHA256 checksum.

## Browser Diagnostics

Phase 1 saves browser-level evidence such as `login-filled` and
`complaints-list` screenshots/HTML. These are not complaint artifacts; they are
operational diagnostics that show what the portal looked like during a run.

New captures are stored date-wise:

```text
diagnostics/pmdu/YYYY/MM/DD/<run-id>/
├── YYYYMMDD-HHMMSS-login-filled.png
├── YYYYMMDD-HHMMSS-login-filled.html
└── YYYYMMDD-HHMMSS-login-filled.meta.json
```

To move older root-level diagnostic captures out of `artifacts/`, run:

```bash
uv run python organize_pmdu_diagnostics.py --dry-run
uv run python organize_pmdu_diagnostics.py --yes
```

Each cleanup writes `diagnostics/pmdu/MIGRATIONS/<batch-id>/MANIFEST.json` and
`FILES.csv` with original paths, new paths, file sizes, mtimes, and SHA256
checksums.

## DDEO Inquiry Letters

Generate inquiry letters from Paperless under-investigation complaints:

```bash
uv run python generate_inquiry_letters.py --source paperless
```

Paperless is the source of truth for corrected custom fields such as `Tehsil`.
The sample ODT template is preserved; generation only fills the complaint number,
Tehsil, and school/address placeholders. Complaint details are not inserted into
the letter because the original complaint PDF and attachments are sent alongside
the inquiry order.

Output is arranged for filesystem review:

```text
inquiry_letters_to_ddeos/YYYY/MM/DD/<TEHSIL>/
├── editable/
└── pdfs/
```

`inquiry_letters_to_ddeos/inquiry_letters.sqlite3` records issued inquiry orders,
DDEO routing, due dates, generated file paths, and status fields for future
reminder/warning workflows.

## Notification Preview Batches

`notify-preview` now creates a frozen review batch before anything is sent:

```bash
uv run python main.py notify-preview --to ddeo --files pdf --scope all
```

Preview batches are written to:

```text
reports/pmdu/notification_previews/YYYY/MM/DD/<batch-id>/
├── batch.sqlite3
├── preview.html
├── summary.json
├── recipients.csv
├── jobs.jsonl
└── issues.csv
```

The preview uses the same recipient/message builder as send mode. When files are
selected, it stages the same document payloads and validates file existence,
counts, sizes, and targets. Nothing is published to NATS and no delivery rows are
recorded.

Open the latest preview from the GUI with `Notifications -> Open Latest Preview`
or run:

```bash
uv run python open_latest_notification_preview.py
```

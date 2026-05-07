# scrap-pmdu

## Command reference

Normal scrape and sync flow:

```bash
# Phase 1: login, scrape complaint list/detail HTML, and fill raw_html/
uv run python main.py phase1

# Same as phase1
uv run python main.py scrape

# Phase 2: parse raw_html/, generate JSON snapshots, PDFs, and attachments
uv run python main.py phase2

# Same as phase2
uv run python main.py artifacts
uv run python main.py parse

# Phase 3: upload latest complaint PDFs and attachments to Paperless
uv run python main.py paperless

# Same as paperless
uv run python main.py phase3
uv run python main.py paperless-sync

# Check Paperless field/type/select-option setup before syncing
uv run python main.py paperless-check
```

Common end-to-end run:

```bash
uv run python main.py phase1
uv run python main.py phase2
uv run python main.py paperless-check
uv run python main.py paperless
uv run python main.py notify-preview --to group
uv run python main.py notify-send --to group
```

Short test runs:

```bash
# Limit Phase 1 detail-page scraping
PMDU_MAX_DETAILS=5 uv run python main.py phase1

# Limit Phase 2 local HTML processing
PMDU_PHASE2_MAX_FILES=5 uv run python main.py phase2

# Limit Phase 3 Paperless uploads
PAPERLESS_MAX_CASES=1 uv run python main.py paperless
```

Notification command shape:

```bash
# Preview jobs only; does not publish WhatsApp jobs or write delivery rows
uv run python main.py notify-preview --to group

# Send WhatsApp jobs through NATS
uv run python main.py notify-send --to group

# Send to one officer level
uv run python main.py notify-send --to aeo --scope all --files pdf-and-attachments
uv run python main.py notify-send --to ddeo --scope newly-under-investigation --files pdf

# Send only one tehsil
uv run python main.py notify-send --to aeo --tehsil "Model Town" --scope all --files pdf
```

## PMDU login automation

Configure credentials in a local `.env` file:

```env
PMDU_LOGIN_URL=https://admin.pmdu.gov.pk/guardian/users/login
PMDU_COMPLAINTS_URL=https://admin.pmdu.gov.pk/pkcp/complaints/list
PMDU_USERNAME=your-username
PMDU_PASSWORD=your-password
CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium
PMDU_HEADLESS=false
PMDU_BROWSER_WIDTH=1600
PMDU_BROWSER_HEIGHT=950
PMDU_RAW_HTML_DIR=raw_html
PMDU_DUCKDB_PATH=pmdu_scrape.duckdb
PMDU_SCRAPE_RETRIES=3
PMDU_SCRAPE_RATE_LIMIT_MS=1500
```

Run the automation:

```bash
uv run python main.py phase1
```

The script opens Chromium, fills the username and password, then waits for you to
manually verify the CAPTCHA and click **Sign In**. Once the URL changes away from
`/guardian/users/login`, it opens the complaints list page, queues complaint
detail links in DuckDB, and saves rendered detail pages under `raw_html/`.

Phase One uses `complaint_code` plus the listing page's received date/time to
detect reopened or returned complaints. If a known complaint appears with a new
received timestamp, it is queued again and `raw_html/{complaint_code}.html` is
refreshed. Phase Two then decides whether that refreshed HTML creates a new
version based on content hash.

DuckDB queue table:

```sql
CREATE TABLE scrape_queue (
    complaint_code TEXT PRIMARY KEY,
    detail_url TEXT,
    status TEXT,
    saved_path TEXT,
    scraped_at TIMESTAMP,
    listing_received_at TEXT,
    last_seen_at TIMESTAMP,
    last_scraped_listing_received_at TEXT
);
```

Set `PMDU_MAX_DETAILS=5` in `.env` for a short test run.

## Phase Two artifact generation

Generate parsed snapshots, PDFs, and attachment folders from existing
`raw_html/*.html` files:

```bash
uv run python main.py phase2
```

Phase Two does not require logging in. It reads local raw HTML files, parses them
with `lxml`, downloads public CDN attachments with `aiohttp`, generates PDFs from
the portal HTML using Playwright print CSS, writes `snapshot.json` with `orjson`,
and records complaint versions in DuckDB.

Phase Two records the raw HTML SHA256 in DuckDB. After the first indexed run,
unchanged raw HTML files are skipped immediately. PDF generation reuses one
Chromium browser for the run instead of launching Chromium once per complaint.

Output structure:

```text
artifacts/
└── PU180426-92524519/
    ├── meta.json
    └── v1/
        ├── snapshot.json
        ├── complaint.pdf
        └── attachments/
```

Useful Phase Two settings:

```env
PMDU_ATTACHMENT_RETRIES=3
PMDU_ATTACHMENT_CONCURRENCY=4
PMDU_ATTACHMENT_TIMEOUT_SECONDS=120
PMDU_PDF_ENABLED=true
PMDU_PDF_FORMAT=Letter
PMDU_JSON_INDENT=true
# PMDU_PHASE2_MAX_FILES=5
```

## Phase Three Paperless sync

Upload latest complaint artifacts to Paperless-NGX:

```bash
uv run python main.py paperless
```

Phase Three reads the latest `snapshot.json` for each complaint under
`artifacts/`, uploads `complaint.pdf` as the main Paperless document, uploads
attachments as `Attachment` documents, and sets the `Parent Case` document-link
field on each attachment to the main complaint document.

Paperless sync is tracked in DuckDB table `paperless_documents`, so reruns match
already uploaded files by complaint/version/role/SHA256 rather than absolute
local path. If a previous run uploaded a file but failed while patching metadata,
the next run tries to find the Paperless document by exact title and resumes
metadata patching. Metadata payloads are fingerprinted; unchanged documents skip
the Paperless PATCH request on later runs.

`Status` is treated as an operational field. Main complaints get the configured
default status, usually `Pending`, only on first upload or when a new PMDU
version is uploaded for the same complaint. Already-synced versions preserve the
current Paperless status. Each main-complaint sync writes an audit row to
DuckDB table `paperless_status_history` with `status_before`, `status_after`,
and reason: `first_upload`, `new_version`, or `preserved_existing`.

Paperless does not support every attachment type. Audio/video attachments are
kept under `artifacts/` but skipped during Paperless upload and recorded in
DuckDB as `skipped_unsupported`.

When Paperless rejects an upload as a duplicate and returns a related document,
the sync treats that as a successful match and patches/links the existing
Paperless document instead of failing the complaint.

Check Paperless setup before syncing:

```bash
uv run python main.py paperless-check
```

The check validates document types, correspondent, custom fields, and select
options used by `paperless_field_defaults.json`.

Useful Phase Three settings:

```env
PAPERLESS_URL=http://localhost:8000/
PAPERLESS_USERNAME=your-paperless-username
PAPERLESS_PASSWORD=your-paperless-password
PAPERLESS_DOCUMENT_TYPE_COMPLAINT=Complaint
PAPERLESS_DOCUMENT_TYPE_ATTACHMENT=Attachment
PAPERLESS_CORRESPONDENT_NAME=CEO, (DEA), Lahore
PAPERLESS_SOURCE_LABEL=PMDU Portal
PAPERLESS_FIELD_CONFIG=paperless_field_defaults.json
PAPERLESS_TIMEOUT_SECONDS=120
PAPERLESS_TASK_TIMEOUT_SECONDS=300
PAPERLESS_DRY_RUN=false
# PAPERLESS_MAX_CASES=1
```

Paperless custom field defaults and filters live in
`paperless_field_defaults.json`. Rerunning `uv run python main.py paperless`
refreshes metadata for already uploaded documents, so changing `"Status"` to
`"Pending"` or another Paperless select option will patch existing main
complaints without uploading duplicate files.

Config values can use placeholders from `snapshot.json`, for example
`"{complaint_code}"`, `"{version}"`, `"{identity.citizen_name}"`,
`"{identity.category}"`, and `"{parent_document_id}"`. A custom field is only
changed when it appears in the config. Fields not listed, such as `Tehsil`, are
preserved.

Useful config filters:

```json
{
  "filters": {
    "roles": ["main_complaint"],
    "paperless_added_date": "2026-05-01",
    "complaint_codes": []
  }
}
```

`paperless_added_date` filters by the date Paperless ingested the document.
`paperless_created_date` filters by the document date stored in Paperless.
Leave date filters as `null` for normal upload/sync runs.

The sync only sets select custom fields when the target option already exists in
Paperless. `Source` should have a `PMDU Portal` option. `Tehsil` is intentionally
left unset so it can be assigned manually before routing complaints to officers.
Attachment documents do not receive a `Status` custom field.

## Under Investigation WhatsApp notifications

Preview notification jobs for Paperless main complaints whose custom field
`Status` is `Under Investigation`:

```bash
uv run python main.py notify-preview --to group
```

Publish jobs to NATS only when you use `notify-send`:

```bash
uv run python main.py notify-send --to group
```

Recipient targets:

```bash
uv run python main.py notify-preview --to group
uv run python main.py notify-preview --to aeo
uv run python main.py notify-preview --to ddeo
uv run python main.py notify-preview --to deo
uv run python main.py notify-preview --to officers
uv run python main.py notify-preview --to all
```

File profiles:

```bash
# Text only
uv run python main.py notify-send --to aeo --files none --scope all

# Complaint PDF only
uv run python main.py notify-send --to ddeo --files pdf --scope new-or-updated

# Complaint PDF plus linked attachments
uv run python main.py notify-send --to ddeo --files pdf-and-attachments --scope new-or-updated
```

Tehsil filter:

```bash
# Preview only City AEO complaints
uv run python main.py notify-preview --to aeo --tehsil City --files pdf-and-attachments

# Send only Model Town AEO complaints
uv run python main.py notify-send --to aeo --tehsil "Model Town" --scope all --files pdf

# Send only unassigned complaints to DEO
uv run python main.py notify-send --to deo --tehsil unassigned --scope all --files none
```

`--scope` controls whether already-sent complaints are skipped:

```bash
# Send every complaint currently Under Investigation.
uv run python main.py notify-send --to aeo --scope all --files none

# Send only complaints never sent before to the same role and recipient.
uv run python main.py notify-send --to aeo --scope new --files pdf-and-attachments

# Send never-sent complaints, plus complaints whose selected files changed.
uv run python main.py notify-send --to aeo --scope new-or-updated --files pdf-and-attachments

# Send complaints whose status newly changed to Under Investigation since a previous scan.
uv run python main.py notify-send --to aeo --scope newly-under-investigation --files pdf-and-attachments
```

`notify-preview` never publishes jobs and never records delivery rows.
`notify-send` publishes to the WhatsApp bot subject configured in
`notification_config.json`. Paperless files are only downloaded for `notify-send`
and only after scope filtering decides a complaint should be sent.

The notifier reads `officers-data.csv`, `whatsapp_groups.csv`, and
`notification_config.json`:

```json
{
  "hierarchy": {
    "deo_mee": "summary",
    "ddeo": "full",
    "aeo": "full"
  },
  "message": {
    "mode": "per_complaint",
    "attachment_text_mode": "separate"
  },
  "templates": {
    "officer_full": "Under Investigation Complaint\nTo: {recipient_name} ({role})\nComplaint: {complaint_number}\nStatus: {status}\nTehsil: {tehsil}",
    "group_summary_title": "Under Investigation Complaints Summary"
  },
  "group_summary": {
    "enabled": true,
    "groups_csv": "whatsapp_groups.csv",
    "title": "Under Investigation Complaints Summary"
  },
  "resend_policy": {
    "min_minutes_between_same_recipient": 0
  }
}
```

`message.mode` can be `per_complaint` or `per_recipient`. Complaints without a
`Tehsil` are sent only to the DEO MEE focal person as unassigned when DEO is in
the selected `--to` target. Every actual delivery is tracked in DuckDB table
`whatsapp_notifications` with `sent_at` and file fingerprint.

Enabled rows in `whatsapp_groups.csv` receive one summary-only group message.
That group summary never includes attachments; it lists the total pending count
and complaint numbers grouped by source and tehsil.

Each run is tracked in `notification_batches`. Status observations are tracked
in `complaint_status_history`, which powers `--scope newly-under-investigation`.

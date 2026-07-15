# AntiDengue

This pipeline now publishes WhatsApp jobs through the same NATS queue format used by the `WhatsAppBot` worker.

## Highlights

- No hardcoded WhatsApp group IDs in Python code
- Master schools, officer jurisdictions, and fixed WhatsApp recipients are read from PocketBase first
- `officers_list.csv` and `whatsapp_recipients.csv` are sync/fallback inputs, not the runtime source of truth
- Portal credentials and app settings come from environment variables
- Screenshot sending is optional and only included when the file actually exists
- Every run writes `run_summary.json` beside the generated Excel report
- Data quality checks block WhatsApp delivery when a report shape looks unsafe
- WhatsApp worker readiness is checked before queueing messages
- delivery status is awaited by default, so a run is not reported successful just because jobs entered NATS
- Officer delivery audits show each named officer, number, job id, school list, and final delivery status
- Activity evidence workbooks show the raw activity values behind each inactive school
- `--dry-run` generates reports and WhatsApp payload counts without queueing messages

## Delivery Order

Each run sends in this order:

1. Dynamic personal messages to the matched `AEO` and `DDEO` contacts for the dormant schools in the current report
2. Fixed recipients from the PocketBase `whatsapp_recipients` collection

## Fixed Recipients

Runtime reads fixed recipients from PocketBase. The CSV is still useful for bulk sync through the database screen:

```csv
enabled,name,type,target,text,image_path,excel_path,excel_filename,attachment_text_mode,delay_ms
true,Anti Dengue Group,group,120363319976862432@g.us,,,,,caption,1500
```

Notes:

- `type=group` requires a full group JID ending in `@g.us`
- `type=contact` can be a plain phone number like `923001234567`
- If `text`, `image_path`, or `excel_path` are blank, the pipeline uses the files/text generated during the current run
- `attachment_text_mode` supports `caption` or `separate`
- if `attachment_text_mode` is blank, the pipeline uses `WA_ATTACHMENT_TEXT_MODE`
- fixed recipients still control their own attachments through `excel_path` and `image_path`

## Officers And Schools

Runtime reads schools, DDEOs, AEOs, and jurisdictions from PocketBase. The `officers_list.csv` file remains the bulk-sync input for schools, heads, DDEOs, AEOs, tehsils, markazes, and jurisdictions.

Expected columns include:

- `EMIS`
- `School Name`
- `DDEO Name`
- `DDEO CELL NUMBER`
- `AEO NAME`
- `AEO CELL NUMBER`

Phone normalization is handled in code for common Pakistani-number issues:

- leading `'` inserted by Excel
- missing leading `0`
- numbers already stored as `92...`
- numbers stored as `03...`
- numbers stored as `3...`

## Environment

Copy `.env.example` to `.env` and set your real values.

Important variables:

- `PORTAL_USER`
- `PORTAL_PASS`
- `REPORT_SOURCE`
- `FILTERED_TARGET_URL`
- `UNFILTERED_TARGET_URL`
- `NATS_URL`
- `NATS_SUBJECT`
- `WHATSAPP_RECIPIENTS_FILE`
- `OFFICERS_LIST_FILE`
- `POCKETBASE_ENABLED`
- `POCKETBASE_DB_PATH`
- `GENERATE_SCREENSHOT`
- `WA_ATTACHMENT_TEXT_MODE`
- `WA_INCLUDE_EXCEL_FOR_DDEO`
- `WA_INCLUDE_EXCEL_FOR_AEO`
- `WA_REQUIRE_WORKER_READY`
- `WA_WAIT_FOR_DELIVERY`
- `WA_HEALTH_TIMEOUT_SECONDS`
- `WA_HEALTH_WAIT_SECONDS`
- `WA_DELIVERY_TIMEOUT_SECONDS`
- `QUALITY_MAX_MASTER_COVERAGE`
- `QUALITY_MAX_UNMAPPED_RATIO`
- `PORTAL_DUPLICATE_RAW_POLICY`

Dynamic officer delivery rules:

- `WA_INCLUDE_EXCEL_FOR_DDEO=true` keeps the generated Excel attached for DDEO messages
- `WA_INCLUDE_EXCEL_FOR_AEO=false` sends AEO messages as text-only by default
- fixed group recipients are unaffected and still receive attachments based on the fixed recipient file

Report source rules:

- `REPORT_SOURCE=unfiltered` downloads the all-users report and filters dormant users locally
- `REPORT_SOURCE=filtered` downloads the portal-filtered dormant report
- `REPORT_SOURCE` may also be set to a full URL when you need a one-off report link
- local dormant filtering uses `Total Activities = 0` when that column exists
- if `Total Activities` is missing, local dormant filtering falls back to rows where all available detailed activity columns are `0`: `Simple Activities`, `Patient Activities`, `Vector Surveillance Activities`, `Larvae Case Response`, `TPV Activities`
- if the portal changes the export schema and an expected activity column is missing, the run logs a schema-drift warning and records which filter strategy was used
- if no activity columns exist, the report is treated as already filtered; use that shape only with `REPORT_SOURCE=filtered`
- `PORTAL_DUPLICATE_RAW_POLICY=block_send` is the default. When a portal run downloads the exact same raw file hash as an earlier portal run, the pipeline still writes the Excel/evidence/summary files, but blocks WhatsApp dispatch so old counts are not resent under a fresh timestamp. Use `warn` to send with a warning, or `allow` to ignore the duplicate check.

## Run

```bash
uv python install 3.12
uv sync
python main.py
```

If `WhatsAppBot/worker.js` is running and connected, the generated report will be queued to every enabled recipient in your recipients file.

Use dry-run mode before sending operationally sensitive reports:

```bash
python main.py --dry-run
python main.py manual-unfiltered --dry-run
```

Dry-run mode still generates Excel reports, officer audits, activity evidence, and `run_summary.json`, but it does not queue WhatsApp messages or archive the raw file.

## Data Quality Gate

Before WhatsApp delivery, the pipeline checks:

- whether local dormant filtering was applied for unfiltered reports
- whether expected activity columns are missing
- whether the inactive count suspiciously covers almost the full master list
- whether officer mapping coverage is below the configured threshold
- whether too many officer contact rows have invalid mobile numbers
- whether the WhatsApp worker is connected and answering `NATS_HEALTH_SUBJECT`

If the gate fails, reports and `run_summary.json` are still generated, but WhatsApp delivery is skipped and the raw file is left in place for review.

After queueing, delivery status is awaited when `WA_WAIT_FOR_DELIVERY=true`. If delivery does not complete for any fixed recipient or dynamic officer recipient, the run summary records the failure, the officer delivery audit records the affected person/number/job, and the raw file is not archived.

## Run Outputs

Each non-empty run creates:

- formatted inactive-school Excel report in `output-files/<timestamp>/`
- activity evidence workbook in `output-files/<timestamp>/`
- officer delivery audit in `output-files/<timestamp>/`
- `run_summary.json` in `output-files/<timestamp>/`
- officer mapping audit in `unmapped-officer-reports/<timestamp>/`

When the scraper hits login or empty-download failures, it saves page screenshots and HTML snapshots in `scraper-debug/`.

## Manual Unfiltered Files

Drop downloaded all-users files into `drop-manual-unfiltered-files`, then run:

```bash
python main.py manual-unfiltered
```

The older typo folder `drop-manually-unfilterd-files` is still supported for compatibility.

The command processes all files in that folder, skips temporary hidden lock files, applies the same zero-activity dormant filter, generates the normal Excel/audit outputs, queues WhatsApp jobs, and archives successfully processed raw files.

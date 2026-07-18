# AntiDengue

This pipeline now publishes WhatsApp jobs through the same NATS queue format used by the `WhatsAppBot` worker.

## Highlights

- One authenticated portal session can acquire multiple registered report types
- Scheduled portal windows are frozen in `Asia/Karachi` (for example `00:00–08:30`)
- Raw dormant and hotspot-distance exports are retained as independent immutable artifacts
- No hardcoded WhatsApp group IDs in Python code
- Platform runs consume an immutable PostgreSQL snapshot containing master schools, officer jurisdictions, routes, settings, and recent run fingerprints
- `officers_list.csv` and `whatsapp_recipients.csv` are standalone CLI fallbacks, not the platform runtime source of truth
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
2. Authorized recipients from the selected PostgreSQL dispatch profiles and audiences

## Fixed Recipients

Platform runtime reads recipients from PostgreSQL. The CSV remains available only for direct standalone CLI runs:

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

Platform runtime reads schools, DDEOs, AEOs, and effective jurisdictions from its immutable PostgreSQL snapshot. The `officers_list.csv` file remains a standalone fallback and import source.

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
- `PORTAL_NAVIGATION_RETRIES`
- `PORTAL_NAVIGATION_RETRY_DELAY_MS`
- `PORTAL_DISTRICT_ID` (defaults to Lahore district `18`)
- `PORTAL_REPORTS` (defaults to `dormant_users,hotspot_distance,simple_activity_list`)
- `PORTAL_REPORT_CUTOFF` (normally injected by the scheduler)
- `HOTSPOT_DISTANCE_REVIEW_THRESHOLD_METERS` (blank until policy is confirmed)
- `REPORT_SOURCE`
- `FILTERED_TARGET_URL`
- `UNFILTERED_TARGET_URL`
- `NATS_URL`
- `NATS_SUBJECT`
- `NATS_PUBLISH_ATTEMPTS`
- `NATS_PUBLISH_RETRY_DELAY_SECONDS`
- `WHATSAPP_RECIPIENTS_FILE`
- `OFFICERS_LIST_FILE`
- `ANTIDENGUE_RUNTIME_SNAPSHOT` (injected by the platform worker; do not set manually for normal service runs)
- `GENERATE_SCREENSHOT`
- `WA_ATTACHMENT_TEXT_MODE`
- `WA_INCLUDE_EXCEL_FOR_DDEO`

## Portal report registry

`portal_reports.py` defines portal data products independently from their analysis and
WhatsApp routing. The initial registry contains:

- `dormant_users`: required input for the existing dormant-school workflow
- `hotspot_distance`: optional cumulative activity export from local midnight to the
  frozen schedule cutoff

The hotspot export is validated against `hotspot_distance.v1`. It is described as a
distance-review data source; no activity is labelled fake automatically. Set
`HOTSPOT_DISTANCE_REVIEW_THRESHOLD_METERS` only after the department confirms the
distance policy. Until then the run summary records row counts, submitter counts and
distance distribution without producing accusations.
- `WA_INCLUDE_EXCEL_FOR_AEO`
- `WA_REQUIRE_WORKER_READY`
- `WA_WAIT_FOR_DELIVERY`
- `WA_HEALTH_TIMEOUT_SECONDS`
- `WA_HEALTH_WAIT_SECONDS`
- `WA_DELIVERY_TIMEOUT_SECONDS`
- `QUALITY_MAX_DORMANCY_RATIO`
- `QUALITY_MAX_UNMAPPED_RATIO`
- `QUALITY_MIN_EXPORT_COVERAGE`
- `QUALITY_DORMANCY_WARNING_AFTER_HOUR`
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
- if `Total Activities` is missing, local dormant filtering is accepted only when all detailed activity columns exist and are `0`: `Simple Activities`, `Patient Activities`, `Vector Surveillance Activities`, `Larvae Case Response`, `TPV Activities`
- when `Total Activities` is present it is authoritative; missing detail capabilities such as `TPV Activities` are recorded without creating an actionable warning
- if `Total Activities` is absent, every configured detailed activity column must be present before an unfiltered report can be classified safely
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

## Runtime Ownership

The AntiDengue script is a plain Python report executor. The automation platform
owns schedules, durable task publication, retries, execution history, previews,
approval, and delivery through PostgreSQL and Celery. No Prefect server,
deployment, work pool, or ephemeral orchestration process is required.

JetStream publication retries use the payload job ID as `Nats-Msg-Id`, so an
ambiguous acknowledgement can be retried without creating a second logical
WhatsApp job.

## Data Quality Gate

Before WhatsApp delivery, the pipeline checks:

- whether local dormant filtering was applied for unfiltered reports
- whether the export has a reliable activity schema
- portal/master EMIS reconciliation and export coverage
- whether the reconciled dormant rate is suspiciously high after the configured morning cutoff
- officer mapping coverage only when individual officer delivery can actually run
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

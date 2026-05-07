# AntiDengue

This pipeline now publishes WhatsApp jobs through the same NATS queue format used by the `WhatsAppBot` worker.

## Highlights

- No hardcoded WhatsApp group IDs in Python code
- Dynamic officer recipients are derived from `officers_list.csv` on every run
- Recipients come from `whatsapp_recipients.csv` or another file set through `WHATSAPP_RECIPIENTS_FILE`
- Portal credentials and app settings come from environment variables
- Screenshot sending is optional and only included when the file actually exists

## Delivery Order

Each run sends in this order:

1. Dynamic personal messages to the matched `AEO` and `DDEO` contacts for the dormant schools in the current report
2. Fixed recipients from `whatsapp_recipients.csv`

## Recipient File

Use `whatsapp_recipients.csv` with the same columns supported by `WhatsAppBot`:

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

## Officers File

The dynamic officer mapping comes from `officers_list.csv`.

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
- `GENERATE_SCREENSHOT`
- `WA_ATTACHMENT_TEXT_MODE`
- `WA_INCLUDE_EXCEL_FOR_DDEO`
- `WA_INCLUDE_EXCEL_FOR_AEO`

Dynamic officer delivery rules:

- `WA_INCLUDE_EXCEL_FOR_DDEO=true` keeps the generated Excel attached for DDEO messages
- `WA_INCLUDE_EXCEL_FOR_AEO=false` sends AEO messages as text-only by default
- fixed group recipients are unaffected and still receive attachments based on the fixed recipient file

Report source rules:

- `REPORT_SOURCE=unfiltered` downloads the all-users report and filters dormant users locally
- `REPORT_SOURCE=filtered` downloads the portal-filtered dormant report
- `REPORT_SOURCE` may also be set to a full URL when you need a one-off report link
- local dormant filtering is applied when the raw report contains all six activity columns and all are `0`: `Simple Activities`, `Patient Activities`, `Vector Surveillance Activities`, `Larvae Case Response`, `TPV Activities`, `Total Activities`

## Run

```bash
uv python install 3.12
uv sync
python main.py
```

If `WhatsAppBot/worker.js` is running and connected, the generated report will be queued to every enabled recipient in your recipients file.

## Manual Unfiltered Files

Drop downloaded all-users files into `drop-manually-unfilterd-files`, then run:

```bash
python main.py manual-unfiltered
```

The command processes all files in that folder, skips temporary hidden lock files, applies the same zero-activity dormant filter, generates the normal Excel/audit outputs, queues WhatsApp jobs, and archives successfully processed raw files.

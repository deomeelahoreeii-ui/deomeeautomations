# WhatsAppBot

`WhatsAppBot` is the delivery worker only.

It does not own recipient files or broadcast logic. Producers decide:

- who to send to
- what to send
- which files to attach

The worker only:

- maintains the WhatsApp session
- consumes jobs from NATS
- validates and delivers jobs
- retries transient failures
- dead-letters invalid or exhausted jobs
- can optionally record discovered group IDs in `data/discovered-groups.csv`

## Queue Contract

Producers publish JSON to `NATS_SUBJECT`.

Supported payload:

```json
{
  "job_id": "uuid",
  "target": "120363319976862432@g.us",
  "type": "group",
  "recipient_name": "Anti Dengue Group",
  "attachment_text_mode": "caption",
  "text": "Message body",
  "image_path": "C:/absolute/path/to/image.png",
  "excel_path": "C:/absolute/path/to/report.xlsx",
  "excel_filename": "Report.xlsx",
  "document_path": "C:/absolute/path/to/file.pdf",
  "document_filename": "Complaint.pdf",
  "document_mimetype": "application/pdf",
  "documents": [
    {
      "path": "C:/absolute/path/to/attachment.jpg",
      "filename": "attachment.jpg",
      "mimetype": "image/jpeg",
      "caption": "Attachment 1 of 104-5450529"
    }
  ],
  "delay_ms": 1500
}
```

Rules:

- `target` is required
- `type=group` requires a full `@g.us` JID
- `type=contact` may be digits or a full `@s.whatsapp.net` JID
- `attachment_text_mode` may be `caption` or `separate`
- at least one of `text`, `image_path`, or `excel_path` must be present
- `document_path` sends one generic document
- `documents` sends multiple generic documents
- producers should publish absolute file paths
- if a caption is too long, the worker automatically sends the text separately to avoid truncation

## Run

```bash
pnpm install
pnpm run worker
```

If the worker is not authenticated, scan the QR code shown in the terminal.

## Group Discovery

Set `WA_ENABLE_GROUP_DISCOVERY=true` to let the worker learn new WhatsApp groups
from inbound events and store them in `WA_GROUPS_FILE`. Set it to `false` to
disable the feature completely.

To avoid `rate-overlimit` responses from WhatsApp, group metadata lookups are:

- skipped for groups already recorded
- deduplicated while a lookup is already in flight
- paced using `WA_GROUP_METADATA_MIN_INTERVAL_MS`
- cooled down after failures using `WA_GROUP_METADATA_FAILURE_COOLDOWN_MS`
- cooled down longer after rate limits using `WA_GROUP_METADATA_RATELIMIT_COOLDOWN_MS`

## Best-Practice Boundary

- keep recipient files in each producer app
- keep producer-specific templating in each producer app
- keep WhatsApp delivery reliability here

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

Group participant jobs use the same durable queue and status flow:

```json
{
  "job_id": "uuid",
  "operation": "add_group_participants",
  "target": "120363319976862432@g.us",
  "type": "group",
  "participants": ["923214729842@s.whatsapp.net"],
  "status_subject": "whatsapp.status.some-batch",
  "delay_ms": 1500
}
```

Rules:

- `target` is required
- `operation` defaults to `send_message`; `add_group_participants` requires a group target and a non-empty `participants` array
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

If the worker is not authenticated, it writes a scannable PNG to
`data/whatsapp-login-qr.png`. The management GUI exposes an **Open QR Code**
button while that file exists. The image is removed after authentication.
Credentials are preserved on every disconnect, including `401` and connection
conflicts. There is no automatic reset path. Login rotation is available only
through the GUI's confirmed **Reset Login & Create QR** action, which archives
the previous state. A PID lock at `data/worker.lock` prevents two local workers
from opening the same session and causing `Stream Errored (conflict)`.
The browser identity is generated with Baileys' supported `Browsers` helpers;
the third tuple element is an operating-system version, not a Chrome version.
Credential updates are serialized and flushed before readiness, reconnect, and
process exit so a GUI shutdown cannot terminate an in-flight auth-state write.

## Group Discovery

Set `WA_ENABLE_GROUP_DISCOVERY=true` to let the worker learn new WhatsApp groups
from inbound events and store them in `WA_GROUPS_FILE`. Set it to `false` to
disable the feature completely.

The worker also provides a read-only request/reply endpoint on
`NATS_GROUP_MEMBERS_SUBJECT`. A request containing `group_jid` returns current
participant phone-number JIDs, LIDs, names, and admin roles for audit/reporting.

On connection, the worker also synchronizes all participating groups. Newly
created or joined groups are captured from live group events, so producers and
the management GUI can select them by name without manually finding a JID.

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

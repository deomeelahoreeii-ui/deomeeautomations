# Add contacts to a WhatsApp group

Reusable producer that reads the `contact_no` column from an Excel workbook,
normalizes and de-duplicates phone numbers, selects a configurable applicant
range, and publishes durable participant-add jobs to the existing NATS-backed
WhatsApp worker.

Preview the last 50 applicants (no WhatsApp changes):

```bash
/home/ahmad/.local/bin/uv run add-to-whatsapp-group \
  --file lahore-ht-test.xlsx --selection last --count 50
```

Execute after checking the preview:

```bash
/home/ahmad/.local/bin/uv run add-to-whatsapp-group \
  --file lahore-ht-test.xlsx --selection last --count 50 \
  --group-jid 'YOUR_GROUP_ID@g.us' --consent-confirmed --execute
```

Use `--selection first`, `--selection all`, or `--selection range --start 51
--end 100` for other batches. Start/end are 1-based positions among valid,
unique contacts. Use `--sheet` when the desired data is not on the first sheet.

The CLI is preview-only unless `--execute` is supplied. NATS and the WhatsApp
worker must be running for execution; the management GUI starts both services.

Direct group additions are blocked unless the operator confirms that recipients
were informed and consented. The producer queues one contact at a time and stops
on the first rejection or timeout; it does not preload an entire batch. This is
a damage-control mechanism, not a guarantee against WhatsApp enforcement. For
people who have not opted in, distribute the group's invite link through an
official office channel and let them join voluntarily.

Direct-add execution is currently disabled by default because both Baileys and
official WhatsApp Web sessions were invalidated after participant-add actions.
The supported workflow is now the read-only membership audit plus voluntary
invite-link distribution. No account rotation is performed.

A per-contact WhatsApp `403` is treated as a privacy rejection rather than a
batch failure. The import continues and writes a `group-import-*-not-added.csv`
report for people who must receive the group's invite link. Account-level,
rate-limit, connection, and unknown failures still halt the batch.

## Idempotency ledger

`group-import-history.sqlite3` records every contact before its job is
published. The unique key is the canonical source-file path, target group JID,
and normalized phone number. Every prior outcome—including added/already-member,
privacy-rejected, failed, publish-failed, or timed-out—is skipped on later runs.
Contacts halted before they were reserved remain eligible. Use `--database` to
place the ledger elsewhere. Renaming or moving a source file intentionally
creates a different file scope.

Before reserving the first contact, the importer requires a request/reply health
response from a WhatsApp-connected worker on `whatsapp.worker.health`. A running
NATS server without an authenticated worker therefore fails immediately instead
of hanging. Each queued contact prints live progress, and operator interruption
is recorded without a traceback.

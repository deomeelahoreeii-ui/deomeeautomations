# Automation Platform

API-first web control center for Deomee automations.

The first migrated slice runs the existing AntiDengue pipeline through FastAPI,
RabbitMQ/Celery, and an Astro-native interface. The desktop application and the
existing AntiDengue implementation remain unchanged.

## Current AntiDengue slice

- Dry run or live run from the web
- Portal login mode selection
- One active AntiDengue run at a time
- Live job status and log polling
- Generated artifact discovery and downloads
- Recent web-run history

## Native development

Install Python and web dependencies:

```bash
/home/ahmad/.local/bin/uv sync
cd apps/web && npm install
```

Start RabbitMQ, then use three terminals:

```bash
# API
/home/ahmad/.local/bin/uv run uvicorn automation_api.main:app --reload --port 8020

# Automation worker (AntiDengue, WhatsApp, and CRM queues)
/home/ahmad/.local/bin/uv run celery -A automation_worker.main.celery_app worker \
  --queues=antidengue,crm --concurrency=1 --hostname=automation@%h \
  --without-gossip --without-mingle --loglevel=INFO

# Astro
cd apps/web && npm run dev -- --host 0.0.0.0 --port 4321
```

Open <http://localhost:4321>.

PostgreSQL runs natively; Docker is not required. After creating the
`automation_platform` database and its user, copy `.env.example` to `.env`, set
the password, and apply the schema:

```bash
/home/ahmad/.local/bin/uv run alembic upgrade head
```

RabbitMQ is the default broker. For a local demo on a machine without RabbitMQ,
set these variables for both the API and worker:

```bash
CELERY_BROKER_URL=filesystem://
CELERY_FILESYSTEM_FOLDER=/tmp/automation-platform-celery
```

### Manual AntiDengue reports

Open <http://localhost:4321/antidengue/manual-reports> when portal collection is
unavailable. The intake service streams `.xls`, `.xlsx`, or `.csv` uploads into
immutable platform storage, calculates a SHA-256 checksum, rejects duplicate
content, and records validation metadata in PostgreSQL. Valid files are queued
as dry-run AntiDengue jobs and can proceed only to Dispatch Preview & Approval;
manual upload never provides a direct live-send path.

Each processing or reprocessing attempt is a separate job linked to the same
immutable source file. The current worker uses the legacy `manual-file` command
behind an adapter, allowing the processor to be replaced later without changing
the upload API, PostgreSQL records, or web UI. The default upload limit is 50 MB
and can be changed with `SOURCE_FILE_MAX_BYTES`.

## API surface

```text
POST /api/v1/antidengue/runs
GET  /api/v1/antidengue/overview
GET  /api/v1/jobs?job_type=antidengue.report
GET  /api/v1/jobs/{job_id}
GET  /api/v1/jobs/{job_id}/logs
GET  /api/v1/jobs/{job_id}/artifacts
GET  /api/v1/artifacts/{artifact_id}/download
```

## Native CRM sheet duplicate filter

Open <http://localhost:4321/crm/> to upload a CRM Excel/CSV sheet and compare each
`Complaint No` against Paperless. The intake service stores an immutable source,
calculates a SHA-256 checksum, detects the header within the first 20 rows, and
records validation findings before a job is queued on the dedicated `crm` queue.

The worker authenticates with Paperless once per run, loads the complaint list in
pages, and matches the sheet's unique complaint numbers locally. It only falls back
to exact per-number searches when an older Paperless list response omits both custom
fields and the complaint number from the title. Progress logs update the job heartbeat;
CRM runs that stop reporting progress are closed automatically instead of remaining
`running` forever. Output is isolated per job under
`data/artifacts/crm-sheet-filter/<job-id>/` and includes non-empty category workbooks,
a full classification audit, `run_summary.json`, and a ZIP bundle.
Paperless request failures, blank complaint numbers, and conflicting Submitted/Not
Relevant matches are placed in Manual Review rather than being treated as fresh.

Configure either `PAPERLESS_TOKEN`, or `PAPERLESS_USERNAME` and
`PAPERLESS_PASSWORD`, in `.env`. `PAPERLESS_URL` may point to the Paperless root or
its `/dashboard` URL. For an internal certificate, the preferred configuration is
`PAPERLESS_CA_BUNDLE=/path/to/internal-ca.pem`. When
`PAPERLESS_ALLOW_INSECURE_FALLBACK=true`, a certificate-validation failure may be
retried without verification only for loopback, private-address, or trusted internal
hostnames such as `.internal`; the worker records that fallback in the job log.

```text
GET  /api/v1/crm/overview
GET  /api/v1/crm/sheets
POST /api/v1/crm/sheets/uploads
GET  /api/v1/crm/sheets/{source_file_id}
POST /api/v1/crm/sheets/{source_file_id}/process
GET  /api/v1/crm/sheets/{source_file_id}/download
DELETE /api/v1/crm/sheets/{source_file_id}/hard
```

## Native CRM sheet rows to PDFs

Open <http://localhost:4321/crm/convert/> or select **Sheet Rows to PDFs** in the
CRM tabs. Choose any previously validated CRM sheet and start a background conversion.
The worker creates one PDF per non-blank complaint row, preserves duplicate complaint
rows with deterministic row suffixes, and generates a CSV manifest, JSON summary, and
downloadable ZIP bundle under `data/artifacts/crm-sheet-to-pdf/<job-id>/`. This workflow
does not require Paperless.

```text
POST /api/v1/crm/sheets/{source_file_id}/convert-to-pdfs
```

## Native CRM PDF duplicate filter

Open <http://localhost:4321/crm/pdfs/> to upload one or more complaint PDFs. The
browser sends them as an immutable, deterministic ZIP batch. Intake validates PDF
headers, assigns safe unique filenames, calculates per-file and batch checksums,
and reports exact duplicates and OCR-tool readiness before the job is queued.

The worker extracts embedded text with Poppler and falls back to first-page OCR with
Poppler plus Tesseract when needed. It builds a Paperless complaint index once per
run, then compares complaint number, applicant identity, and complaint remarks.
Results are separated into Fresh, Uploaded/Pending, Uploaded/Not Relevant,
Submitted, Duplicate in Batch, Manual Review, and OCR Failed. Every run produces a
CSV audit, an Excel audit, non-empty category ZIPs, `run_summary.json`, and a complete
result bundle under `data/artifacts/crm-pdf-filter/<job-id>/`.

Install the system `pdftotext` and `pdftoppm` tools from Poppler and the `tesseract`
OCR executable on the worker machine. The feature still starts without complete OCR
tools, but image-only PDFs may be classified as OCR Failed.

```text
GET  /api/v1/crm/pdf-batches
POST /api/v1/crm/pdf-batches/uploads
GET  /api/v1/crm/pdf-batches/{source_file_id}
POST /api/v1/crm/pdf-batches/{source_file_id}/process
GET  /api/v1/crm/pdf-batches/{source_file_id}/download
DELETE /api/v1/crm/pdf-batches/{source_file_id}/hard
```

## WhatsApp Gateway

Open <http://localhost:4321/whatsapp/> for the PostgreSQL-backed WhatsApp control
plane. It provides focused pages for connection health, Master Data recipients,
wing-specific groups, dispatch plans, deliveries, templates, safety settings,
and activity. NATS request/reply supplies live gateway health, while JetStream
persists approved test-message jobs. Live delivery is disabled by default and
must be explicitly enabled before a SweetAlert2-confirmed test can publish.

The platform stores no PocketBase WhatsApp records and does not read desktop GUI
configuration. The delivery runtime is accessed only through its NATS contract.

Routing configuration follows this order:

1. Define the module report type (for example, AntiDengue school activity).
2. Define recipient roles and hierarchy scopes for the individual and group
   channels (for example, AEO, school head, district, wing, tehsil, or markaz).
3. Create a report template for the exact report type, channel, and optional
   recipient scope.
4. Build a channel-pure audience from detected contacts or groups.
5. Combine the report type, scope, audience, template, and connection in a
   dispatch profile.

Roles and hierarchy scopes are PostgreSQL configuration, not hard-coded dispatch
branches, so CRM and PMDU can add their own recipient structures without changing
the gateway runtime. Stable keys are generated by the UI and remain durable API
identifiers even when a display name changes.

### Dispatch preview planner

Open <http://localhost:4321/whatsapp/previews> to compile a completed AntiDengue
dry run into an immutable review batch. The planner stores the exact rendered
message, resolved JID/LID, wing route, profile/audience/template snapshot, and
content-addressed copies plus SHA-256 checksums for every attachment. Compilation
runs as a Celery job rather than occupying a FastAPI request. Review is split into overview,
deliveries, validation issues, and report-file pages; all tables use server-side
10-row pagination.

Preview compilation never publishes to NATS and never creates delivery rows.
Cross-wing targets, targets outside the selected audience, missing or duplicate
routes, unregistered attachments, and unlinked individual recipients are
blocked. The planner verifies the actual school EMIS values inside each Excel
against PostgreSQL, so a MEE-labelled filename containing WEE schools is still
blocked. Intentionally skipped officer routes, offline connections, and
report-generation quality findings remain informational warnings. Changing the
profile, audience membership, template, account, or frozen file integrity makes
the existing preview stale.

A ready, non-stale preview can then be explicitly approved from its overview or
planned-deliveries page. Approval acknowledges warnings, records the operator and
frozen content hash, creates each delivery exactly once, and queues those exact
messages and attachment snapshots through JetStream. The live-delivery safety
switch must be enabled, the gateway must be ready, and an approved preview cannot
be approved or sent a second time.

Detected contacts can be linked to officers or school heads from
<http://localhost:4321/whatsapp/directory/contacts>. These verified links are
used only to validate individual dispatch scope; WhatsApp credentials and token
secrets remain outside master data.

## Master Data

Open <http://localhost:4321/master-data/> for school, head, officer, area,
jurisdiction, quality, import/export, and audit views. All UI tables use the
framework-agnostic TanStack Table core with a 10-row default page size. Browser
queries and CRUD operations are routed through typed Astro Actions; FastAPI
remains the source API for web and Android clients. PostgreSQL is the source of
truth for web Master Data; the desktop app continues using PocketBase during
the staged migration.

The Lahore WEE and Secondary workbook import is repeatable and previews by
default:

```bash
/home/ahmad/.local/bin/uv run python scripts/import_lahore_wings.py
/home/ahmad/.local/bin/uv run python scripts/import_lahore_wings.py --commit
```

The commit command creates a SQLite backup first. Secondary schools are imported
with an empty markaz reference until authoritative markaz mappings are supplied.

## PocketBase to PostgreSQL

The PostgreSQL migration is separate from the still-running desktop database.
The importer reads PocketBase in read-only mode, previews by default, and uses
stable IDs so committed imports are repeatable:

```bash
/home/ahmad/.local/bin/uv run python scripts/migrate_pocketbase_to_postgres.py
/home/ahmad/.local/bin/uv run python scripts/migrate_pocketbase_to_postgres.py --commit
```

Schools, school heads, officers, and jurisdictions are included. The schema
enforces matching wing scope for schools, markazes, officers, jurisdictions,
and overrides. Legacy WhatsApp routes are intentionally deferred because their
records do not yet contain a wing reference.

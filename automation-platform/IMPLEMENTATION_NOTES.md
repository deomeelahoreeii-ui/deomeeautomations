# Native CRM Sheet Duplicate Filter

## Scope implemented

The legacy `crm-management-system/filter_crm_excel_duplicates.py` workflow has been
ported into `automation-platform` as a native FastAPI, Celery, PostgreSQL/SQLite,
and Astro feature.

The sheet workflow no longer launches the legacy Python script or accepts arbitrary
server filesystem paths. CRM files are uploaded through the browser, stored as
immutable `SourceFile` records, validated, linked to each processing job, and
processed directly by the `crm_filters` package.

The legacy CRM PDF filter is intentionally unchanged and remains behind its existing
adapter.

## Native workflow

1. Open `http://localhost:4321/crm/`.
2. Upload an Excel or CSV CRM sheet.
3. The API stores an immutable copy, calculates SHA-256, rejects exact duplicate
   uploads, scans the first 20 rows for `Complaint No`, and records validation
   metadata.
4. A valid sheet is queued on the Celery `crm` queue.
5. The worker authenticates with Paperless once, loads document/custom-field metadata
   once, and caches repeated complaint-number lookups.
6. Results, live logs, counts, and downloads appear on the CRM page.

## Classifications

- `fresh`: no matching CRM main complaint exists in Paperless.
- `uploaded_pending`: a matching complaint exists with an active or unspecified
  non-terminal status.
- `uploaded_not_relevant`: a matching complaint has status `Not Relevant`.
- `submitted`: a matching complaint has status `Submitted`.
- `manual_review`: blank/invalid complaint number, Paperless lookup failure,
  conflicting Submitted and Not Relevant records, or an unrecognized internal
  classification.

Paperless failures are never converted to `fresh`.

## Generated artifacts

Each run writes to:

```text
data/artifacts/crm-sheet-filter/<job-id>/
```

The run contains non-empty category workbooks, a complete classification audit,
`run_summary.json`, and a ZIP bundle containing the generated reports.

## Configuration

Add one of the following authentication methods to `automation-platform/.env`:

```dotenv
PAPERLESS_URL=https://paperless.lab.internal/dashboard

# Preferred when available
PAPERLESS_TOKEN=

# Or username/password authentication
PAPERLESS_USERNAME=
PAPERLESS_PASSWORD=

PAPERLESS_VERIFY_SSL=true
PAPERLESS_TIMEOUT_SECONDS=15
PAPERLESS_DOCUMENT_TYPE_COMPLAINT=Complaint
PAPERLESS_MAX_PAGES=10
```

The URL can be the Paperless root or its `/dashboard` URL.

## Development command

The worker must consume both queues:

```bash
cd /home/ahmad/code/deomeeautomations/automation-platform && {
  trap 'kill $(jobs -p) 2>/dev/null' EXIT INT TERM;
  /home/ahmad/.local/bin/uv run uvicorn automation_api.main:app --reload --port 8020 &
  /home/ahmad/.local/bin/uv run celery -A automation_worker.main.celery_app worker \
    --queues=antidengue,crm --concurrency=1 --hostname=automation@%h --loglevel=INFO &
  (cd apps/web && PATH=/home/ahmad/.nvm/versions/node/v24.15.0/bin:$PATH \
    npm run dev -- --host 0.0.0.0 --port 4321) &
  wait;
}
```

## Affected files

### Modified

- `.env.example`
- `README.md`
- `apps/web/src/actions/index.ts`
- `apps/web/src/layouts/AppLayout.astro`
- `packages/automation_core/automation_core/config.py`
- `packages/crm_filters/crm_filters/api.py`
- `packages/crm_filters/crm_filters/schemas.py`
- `packages/crm_filters/crm_filters/tasks.py`
- `tests/test_api.py`

### Added

- `IMPLEMENTATION_NOTES.md`
- `apps/web/src/layouts/CrmLayout.astro`
- `apps/web/src/pages/crm/index.astro`
- `apps/web/src/styles/crm.css`
- `packages/crm_filters/crm_filters/intake.py`
- `packages/crm_filters/crm_filters/paperless.py`
- `packages/crm_filters/crm_filters/sheet_filter.py`
- `tests/test_crm_paperless.py`
- `tests/test_crm_sheet_filter.py`
- `tests/test_crm_sheet_intake.py`

No database migration is required because the existing `SourceFile`,
`SourceFileRun`, `Job`, `JobLog`, and `Artifact` tables cover this workflow.

## Validation performed

- Python source compilation completed successfully.
- Six focused CRM unit tests pass, covering header detection, validation, duplicate
  complaint caching, safe Manual Review behavior, Paperless matching, conflicting
  statuses, artifact generation, audit output, manifest output, and ZIP output.
- The new CRM page client script and Astro Actions TypeScript were syntax/type parsed
  with the available TypeScript compiler.

A full FastAPI/Celery/Astro integration build could not be executed in the packaging
container because it does not contain the project's Python 3.14 environment and has
no network access to download it. Run `uv sync`, `pytest`, and `npm run build` in the
project's normal development environment before deployment.

# Native CRM Filters

## Implemented

- Native CRM sheet duplicate filtering through FastAPI, Celery, Paperless, and Astro.
- Native CRM PDF duplicate filtering with immutable multi-file intake, text extraction,
  OCR fallback, Paperless indexing, fuzzy comparison, categorized PDF bundles, audit
  reports, live logs, source history, reprocessing, and permanent managed deletion.
- Paperless TLS support for an internal certificate through `PAPERLESS_CA_BUNDLE`.
- A guarded development fallback through `PAPERLESS_ALLOW_INSECURE_FALLBACK=true`.
  It applies only to loopback, private-address, and trusted internal hostnames and is
  recorded in the job log/result whenever used.
- RabbitMQ-compatible local worker startup using one worker for the `antidengue` and
  `crm` queues with Celery mingle and gossip disabled.

## CRM PDF decisions

- `fresh`
- `uploaded_pending`
- `uploaded_not_relevant`
- `submitted`
- `duplicate_in_batch`
- `manual_review`
- `ocr_failed`

PDF runs create CSV and Excel audits, non-empty per-category ZIP files,
`run_summary.json`, and `crm_pdf_filter_results.zip` under:

```text
data/artifacts/crm-pdf-filter/<job-id>/
```

## Pages

```text
http://localhost:4321/crm/
http://localhost:4321/crm/pdfs/
```

## Local requirements

- RabbitMQ on port 5672
- Poppler executables: `pdftotext` and `pdftoppm`
- Tesseract with English language data for image-only PDFs

Use:

```bash
./scripts/dev.sh
```

## Validation performed

- Python source compilation completed successfully.
- 15 focused CRM sheet, Paperless TLS, PDF intake, extraction, matching, safety, and
  artifact tests passed.
- FastAPI health and OpenAPI route tests passed.
- A multipart PDF upload, queueing, listing, and parameter-persistence API integration
  smoke test passed with the Celery dispatch mocked.
- A real Poppler/Tesseract image-only PDF OCR smoke test passed.
- The Astro production build completed successfully.
- `pyproject.toml` and `uv.lock` parse as valid TOML.

No database migration is required because existing `SourceFile`, `SourceFileRun`,
`Job`, `JobLog`, and `Artifact` tables cover both CRM workflows.

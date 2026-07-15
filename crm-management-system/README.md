# CRM Management System

Automation for CRM complaint intake, duplicate filtering, Paperless upload,
inquiry letters, compliance folders, and compliance dispatch.

## Common Commands

```bash
uv run python filter_crm_pdf_duplicates.py --input-dir crm-main-complaints
uv run python prepare_crm_upload.py --input-dir phase1-crm/unprocessed-crm/filtered/fresh/number-found --artifact-dir artifacts/crm-pending --crm-cache-db crm-cache.sqlite --cache-only
uv run python main.py paperless --artifact-dir artifacts/crm-pending
uv run python generate_crm_compliance_folders.py
uv run python upload_compliances.py
uv run python send_compliances.py --csv crm-compliances/crm-dispatch-list.csv
```

## Main Folders

- `crm/`: CRM parsing, OCR, duplicate filtering, and reply knowledge modules.
- `phase1-crm/`: raw and filtered CRM intake files.
- `crm-main-complaints/`: source CRM complaint PDFs.
- `crm-not-relevant/`: source CRM PDFs to upload as Not Relevant.
- `crm-compliances/`: compliance folders and dispatch lists.
- `crm-reply-context-packs/`: generated reply context JSON/prompt packs.
- `artifacts/`: Paperless-ready CRM artifacts.
- `paperless/`: shared Paperless upload/check/patch client used by CRM flows.

Configure Paperless credentials in `.env` before upload commands:

```env
PAPERLESS_URL=http://10.200.0.1:8010/dashboard
PAPERLESS_USERNAME=your-username
PAPERLESS_PASSWORD=your-password
```

The duplicate filter OCR cache is `crm-cache.sqlite`. Paperless upload state is
stored separately in `paperless.duckdb` by default; only set `CRM_DUCKDB_PATH`
to a DuckDB file, not to `crm-cache.sqlite`.

Before a CRM Pending upload starts, the uploader reads existing CRM main
complaint numbers from the Paperless API. Complaints already present in
Paperless, including their attachments, are skipped. Repeated complaint numbers
inside the selected artifact batch are also uploaded only once; both skip counts
are shown in the command and GUI activity summary.

Use the sibling `../automation-management-gui` folder for the desktop GUI.

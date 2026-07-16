# WhatsApp Gateway Refactor R4 + R5

This refactor is behavior preserving. `preview_api.py` and `preview_service.py` remain compatibility facades.

## R4 — Preview API

- Schemas: `previews/schemas.py`
- Serialization: `previews/serialization.py`
- Options: `previews/options.py`
- Creation: `previews/creation.py`
- Queries: `previews/queries.py`
- Approval: `previews/approval.py`
- Deletion: `previews/deletion.py`
- Deliveries: `previews/deliveries.py`
- Artifacts and issues: `previews/artifacts.py`
- Contact links: `previews/contact_links.py`
- Router composition: `previews/router.py`

## R5A — Preview service helpers

- Managed artifact storage: `previews/artifact_storage.py`
- Staleness, serialization and cleanup: `previews/maintenance.py`
- Compiler errors: `previews/compiler/errors.py`
- Message rendering: `previews/compiler/messages.py`
- Route classification: `previews/compiler/routes.py`
- Attachment classification: `previews/compiler/attachments.py`

## R5B — Staged compiler

`compile_antidengue_preview()` is now a small orchestrator with explicit stages:

1. Load and validate source/profile context.
2. Build audience-owned delivery plans.
3. Persist the immutable preview header and configuration snapshot.
4. Freeze content-addressed artifacts.
5. Validate and persist each delivery.
6. Finalize counts, status and content SHA-256.

The detailed symbol mapping is stored in `MOVED_SYMBOLS_R4_R5.json`.

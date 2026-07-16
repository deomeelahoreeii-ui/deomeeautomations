# WhatsApp Gateway Refactor Map

This refactor is intentionally behavior-preserving. Existing imports from
`whatsapp_gateway.models` remain supported.

## Bundle R0 + R1

| Previous symbol | Implementation after R1 |
|---|---|
| `WhatsAppAccount` | `persistence/account.py` |
| Directory groups, contacts, aliases, members, authorized groups and contact links | `persistence/directory.py` |
| Applications, report types, recipient scopes, audiences, templates, dispatch profiles and settings | `persistence/configuration.py` |
| Dispatch previews, preview artifacts, preview deliveries and approvals | `persistence/previews.py` |
| Deliveries and activity records | `persistence/deliveries.py` |
| Inbound messages, attachments, history requests and export records | `persistence/inbound.py` |

`whatsapp_gateway/models.py` is now a small compatibility facade. It must remain
stable because Alembic, other packages and tests import model classes from it.

## Safety contracts

The installer captures the pre-refactor runtime contract and verifies that the
refactored package preserves:

- all WhatsApp OpenAPI paths and schemas;
- WhatsApp SQLModel tables, columns, constraints and indexes;
- public imports used by the repository;
- Celery task names and queue routes;
- configured WhatsApp NATS subjects.

## Bundle R2A

Inbound request schemas, worker authentication, account/contact resolution and
media handling were extracted without changing API paths or payloads.

| Previous symbol or module | Implementation after R2A |
|---|---|
| Inbound Pydantic request/event classes in `inbound_api.py` | `inbound/schemas.py` |
| `_verify_worker_token` | `inbound/authentication.py` |
| `_resolve_account`, `_resolve_contact_id` | `inbound/accounts.py` |
| `upload_attachment_content` | `inbound/media_upload.py` |
| File type detection previously in `inbound_media.py` | `inbound/media_types.py` |

`inbound_api.py` continues to expose the moved classes/functions and registers
the same routes. `inbound_media.py` remains a compatibility facade, including
its former private helpers for code that may still import them.

## Bundle R2B

Inbound event ingestion, atomic database upserts and history-progress tracking
were extracted without changing the worker event endpoint or data behavior.

| Previous symbol or responsibility | Implementation after R2B |
|---|---|
| `_insert_idempotently` | `inbound/upserts.py` |
| `_upsert_inbound_message` | `inbound/upserts.py` |
| `_upsert_inbound_attachment` | `inbound/upserts.py` |
| `ingest_event` | `inbound/ingestion.py` |
| Message and attachment value construction | `inbound/ingestion.py` |
| History request counters and status reconciliation | `inbound/history_tracking.py` |

`inbound_api.py` keeps compatibility aliases for every moved pre-R2B symbol and
registers the same `/api/v1/whatsapp/inbound/events` endpoint with the same
worker-token dependency. Existing idempotency and history-tracking regression
tests remain mandatory.

## Combined Bundle R2C + R2D + R2E + R2F

The remaining inbound history, export, worker-task and identity-repair code was
split into responsibility-focused modules in one combined delivery.

| Previous responsibility | Implementation after R2C-R2F |
|---|---|
| History request endpoints | `inbound/history.py` |
| Export API endpoints | `inbound/export_api.py` |
| Inbound status endpoint | `inbound/status.py` |
| Filter normalization and contact loading | `inbound/common.py` |
| Account-scoped contact matching and coverage | `inbound/contact_matching.py` |
| Export preview construction | `inbound/export_preview.py` |
| Export-run creation and serialization | `inbound/export_runs.py` |
| ZIP/manifests and package naming | `inbound/export_packaging.py` |
| Immutable ORM value snapshots | `inbound/task_snapshots.py` |
| NATS media client | `inbound/worker_client.py` |
| Export-item database loading | `inbound/export_items.py` |
| Celery export orchestration | `inbound/export_task.py` |
| Identity repair row type | `inbound/identity_types.py` |
| Contact identity index | `inbound/identity_index.py` |
| Message identity resolution | `inbound/identity_resolution.py` |
| Identity audit/repair execution | `inbound/identity_repair_service.py` |

The former `inbound_api.py`, `inbound_service.py`, `inbound_tasks.py` and
`identity_repair.py` modules remain small compatibility facades. All original
symbols from the pre-refactor inventory are mapped in `MOVED_SYMBOLS.json`, and
the combined regression test verifies that each legacy import resolves to the
exact moved object.

## Combined Bundle R3A + R3B + R3C

The former 3,114-line `api.py` is now a compatibility facade and router
aggregator. Connection, directory, configuration and dispatch responsibilities
are isolated into small modules.

| Previous responsibility | Implementation after R3A-R3C |
|---|---|
| Connection overview, QR and worker/NATS client | `gateway/` |
| Directory groups, contacts, identity synchronization and token refresh | `directory/` |
| Defaults, report types, scopes, audiences and dispatch profiles | `configuration/` |
| Test messages, deliveries, recipients, groups, templates and settings | `dispatch/` |

All previous imports from `whatsapp_gateway.api` remain supported. The parent
router includes the four responsibility routers under the unchanged
`/api/v1/whatsapp` prefix. Runtime contract snapshots and movement tests protect
OpenAPI, models, Celery, NATS and legacy imports.


## R6-R8 final refactor

- `antidengue_renderer.py` -> `rendering/antidengue/*` with a stable explicit facade.
- `tasks.py::_publish_approved_deliveries` -> `dispatch/approved_delivery.py`.
- Registered dispatch task entrypoints -> `dispatch/task_entrypoints.py`, re-exported by `tasks.py`.
- Final architecture limits and movement contracts -> `ARCHITECTURE.md`, `MOVED_SYMBOLS_R6_R8.json`, and `SYMBOL_CONTRACTS_R6_R8.json`.

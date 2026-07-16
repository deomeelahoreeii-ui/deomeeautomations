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

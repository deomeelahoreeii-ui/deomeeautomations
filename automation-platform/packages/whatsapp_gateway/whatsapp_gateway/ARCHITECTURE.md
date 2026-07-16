# WhatsApp Gateway Package Architecture

The package uses stable top-level compatibility facades and responsibility-focused implementation packages.

## Stable facades

`api.py`, `models.py`, `preview_api.py`, `preview_service.py`, `inbound_api.py`, `inbound_service.py`, `inbound_tasks.py`, `identity_repair.py`, `antidengue_renderer.py`, and `tasks.py` preserve existing import paths. New implementation code must not be added to these files.

## Dependency direction

API/router facades depend on feature packages. Feature packages may depend on persistence and shared helpers. Feature packages must not import a compatibility facade from their own subsystem. Renderer implementation imports must point to `rendering.antidengue`, never back to `antidengue_renderer`. Dispatch task implementation must not import `whatsapp_gateway.tasks`.

## Size budgets

- Compatibility facade: at most 80 lines.
- Newly refactored implementation module: at most 250 lines.
- Any WhatsApp Gateway implementation module: at most 450 lines.

These limits are enforced by `scripts/refactor/check_whatsapp_architecture.py` and the R6-R8 regression tests.

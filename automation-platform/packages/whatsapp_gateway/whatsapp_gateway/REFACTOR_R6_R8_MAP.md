# WhatsApp Gateway Refactor R6-R8

## R6: AntiDengue renderer

`antidengue_renderer.py` is a compatibility facade. Implementation is divided into `rendering/antidengue/` modules for models, source workbooks, formatting, presentation policy, messages, Excel, images, and wing/tehsil orchestration.

## R7: dispatch tasks

`tasks.py` is a stable Celery facade. The delivery publisher lives in `dispatch/approved_delivery.py`; registered task entrypoints live in `dispatch/task_entrypoints.py`. Explicit Celery task names are unchanged.

## R8: architecture guardrails

- Every legacy symbol is mapped and re-exported by identity.
- Every moved class/function has a stored AST structural hash.
- Compatibility facades are capped at 80 lines.
- New implementation modules are capped at 250 lines.
- The complete package is capped at 450 lines per implementation module, preventing new mega-files.
- Implementation modules may not import the compatibility facades.
- Runtime OpenAPI, SQLModel, Celery and NATS contracts are compared before and after installation.

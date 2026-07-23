# AntiDengue Runtime Failure Audit

## Incident conclusion

- The manual preview execution was committed correctly, but its Celery outbox publication failed with RabbitMQ `ACCESS_REFUSED` before the AntiDengue task could start.
- The local Docker broker was process-healthy while the application used an invalid `guest` login. The existing startup check tested only `rabbitmq-diagnostics ping`, not AMQP authentication.
- The repeated `Closing session:` and `Removing old closed session:` object dumps came from the managed WhatsApp Web dependency path and were unrelated to the AntiDengue task failure, but they exposed noisy cryptographic session internals and hid the actionable Celery error.
- Existing durable recovery is sufficient after authentication is repaired: the failed outbox row remains retryable and the scheduler republishes it without deleting the job or execution.

## Patch scope

1. Reconcile the durable local RabbitMQ application user with the effective Celery broker URL.
2. Migrate only unsafe local `guest` URLs to the non-guest application account, preserving custom/remote brokers.
3. Verify a real AMQP login before migrations, recovery, API, scheduler, or Celery startup.
4. Suppress only the two known repetitive WhatsApp session-dump prefixes; warnings and errors remain visible.
5. Add unit and contract tests for migration, credential reconciliation order, URL redaction, startup ordering, default consistency, and console filtering.

## Files audited

- **Legacy AntiDengue executor:** 26 files
- **Platform AntiDengue UI:** 11 files
- **Platform AntiDengue orchestration:** 21 files
- **Platform AntiDengue tests:** 12 files
- **Shared runtime/startup dependency:** 19 files
- **WhatsApp history bridge:** 27 files
- **Total:** 116 files

The CSV manifest records every audited path, size, SHA-256, parsing/syntax result, and whether it was changed. Source/config files were read and parsed; CSV/XLSX/JSON/runtime data files were structurally inventoried. Dependency lock files were inventoried rather than semantically reviewed line by line.

## Verification performed in the build sandbox

- Python compile pass across the legacy executor, platform AntiDengue package, shared core, API/worker entrypoints, and recovery/preflight scripts.
- Shell syntax pass for `dev.sh` and bridge scripts.
- JavaScript syntax pass for the bridge entrypoint and all bridge modules.
- 10 isolated RabbitMQ preflight tests passed.
- 35 WhatsApp history bridge tests passed, including 3 new noise-filter tests.
- The uploaded baseline already recorded 516 platform tests and the Astro production build passing before this patch.

## Full verification on the target machine

`apply-update.sh` runs the targeted tests, complete platform test suite, bridge suite, Python compile, Alembic-head check, and Astro build. Any failure triggers automatic file rollback.

## B4.4.1 installer regression correction

The first B4.4 target run exposed an installer-only issue after the JavaScript
tests passed: the isolated Python test was invoked from the patch directory, so
`uv` could not discover `automation-platform/pyproject.toml` and attempted to
spawn an unavailable standalone `pytest` executable. The transactional rollback
completed successfully and left the target at the original baseline.

B4.4.1 changes no application payload. It now:

- changes into the Automation Platform root before invoking `uv`;
- explicitly enables the locked `dev` dependency group;
- invokes pytest as `python -m pytest` rather than relying on an executable shim;
- applies the same runner to focused and full Python suites; and
- includes a bundle regression test that verifies the working directory, exact
  `uv` arguments, and project `PYTHONPATH`.

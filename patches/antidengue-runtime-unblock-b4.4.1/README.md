# AntiDengue Runtime Unblock B4.4.1

This bundle fixes the uploaded incident without changing AntiDengue report,
portal, routing, storage, preview, approval, or delivery business logic.

## B4.4.1 installer correction

The first B4.4 installer correctly rolled back on the target machine, but its
focused Python test command ran from the patch directory. That prevented `uv`
from discovering the Automation Platform project and its `dev` dependency group,
so it reported `Failed to spawn: pytest`. B4.4.1 runs every Python test from the
project root through `uv run --frozen --group dev python -m pytest` and includes
a bundle-level regression test for the working directory and exact command.

## Root cause

The stuck manual preview was persisted correctly, but its task outbox failed
before execution because Celery used `guest:guest@localhost` while the durable
Docker RabbitMQ instance used the `automation` application account. The old
startup check only proved that RabbitMQ was alive; it did not prove that Celery
could log in.

The repeated `Closing session:` / `Removing old closed session:` dumps were a
separate WhatsApp Web console-noise issue. They are now filtered narrowly while
normal structured bridge logs, warnings, and errors remain visible.

## Apply

```bash
chmod +x apply-update.sh rollback-update.sh
./apply-update.sh /home/ahmad/code/deomeeautomations/automation-platform
```

The installer:

- checks exact uploaded baseline hashes and refuses unknown files;
- creates a transactional backup under `.update-backups/`;
- migrates only an unsafe local `guest` broker URL;
- runs focused tests, the full platform suite, bridge tests, Alembic-head check,
  Python compilation, Astro production build, and `git diff --check`;
- automatically restores files if any verification fails.

After it succeeds, start normally:

```bash
cd /home/ahmad/code/deomeeautomations/automation-platform
./scripts/dev.sh
```

At startup, the new preflight reconciles the durable RabbitMQ application user,
verifies a real AMQP login, and only then allows recovery and workers to start.
The existing failed AntiDengue outbox row remains retryable and should resume
through the current recovery/scheduler flow.

## Rollback

```bash
./rollback-update.sh /home/ahmad/code/deomeeautomations/automation-platform
```

See `AUDIT_REPORT.md` and `AUDIT_MANIFEST.csv` for the 116-file audit.

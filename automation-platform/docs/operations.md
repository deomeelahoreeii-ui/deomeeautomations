# Automation Platform operations

## Exposure boundary

Application authentication is intentionally deferred. Until it is added, keep
the API, web UI, Flower, PostgreSQL, RabbitMQ, Redis, NATS, and ntfy bound to
loopback or a separately authenticated private tunnel. The checked-in Compose
defaults bind host ports to `127.0.0.1`.

Do not change these bindings to `0.0.0.0` on a shared or internet-reachable host.

## Start and upgrade

Create `.env` from `.env.example`, replace every `change-me` value, and start the
stack:

```bash
docker compose up --build -d
docker compose ps
```

The `migrate` service must complete successfully before the API, scheduler, or
workers start. A failed migration leaves application services stopped instead
of running against an incompatible schema.

Compose runs four independent worker pools:

| Service | Queue | Concurrency | Workload |
|---|---|---:|---|
| `worker` | `antidengue` | 1 | Report runs and approved dispatch |
| `worker-preview` | `antidengue-preview-v2` | 1 | Frozen preview compilation |
| `worker-crm` | `crm` | 1 | Spreadsheet, PDF, Paperless, and CRM work |
| `worker-whatsapp` | `whatsapp` | 1 | Inbound processing and exports |

This prevents OCR or large spreadsheet jobs from blocking preview compilation
or WhatsApp export work. The CRM and WhatsApp workers use the OCR-enabled Python
image; the API, scheduler, and other workers use the smaller base image. Scale a
queue only after verifying that its tasks and external systems support parallel
execution.

## Internal and public service addresses

`PUBLIC_API_BASE_URL` is embedded into browser bundles at image build time.
`API_INTERNAL_BASE_URL` is read by Astro actions at runtime and defaults to
`http://api:8000` in Compose.

Host-native integrations need a container-reachable address:

```dotenv
FRAPPE_HELPDESK_CONTAINER_URL=http://host.docker.internal:8082
OBJECT_STORAGE_CONTAINER_ENDPOINT_URL=http://host.docker.internal:9000
```

Remote integration URLs can be used directly. `host.docker.internal` is mapped
to the Docker host gateway by the Python services.

## Health and logs

- `/livez` confirms that the API process can serve requests.
- `/readyz` verifies database connectivity and returns `503` when unavailable.
- `/health` remains a backward-compatible alias of readiness.
- Each Celery pool has a queue-specific remote-control health check.
- Every API response includes `x-request-id`.
- API, scheduler, and worker service events use JSON logs by default.

Use `LOG_FORMAT=text` for local interactive work or `LOG_LEVEL=DEBUG` while
diagnosing a specific problem. JSON is preferred for collected logs.

## Verification

Run the same primary checks as CI:

```bash
uv sync --locked --group dev
uv run ruff check apps packages tests scripts alembic
uv run pytest --cov --cov-report=term --cov-fail-under=60
cd apps/web
npm ci
npm run verify
npm audit --omit=dev --audit-level=high
```

CI applies every migration to a fresh PostgreSQL 17 database and verifies that
all heads are installed. `alembic check` currently reports pre-existing model
metadata differences—primarily historical constraint names, defaults, and
nullability. Reconcile that baseline before promoting autogenerate drift to a
blocking CI check.

## PostgreSQL backup and restore drill

Create a timestamped custom-format backup before upgrades:

```bash
mkdir -p backups
docker compose exec -T postgres pg_dump \
  --username automation --format=custom automation_platform \
  > backups/automation-platform.dump
```

Store backups outside the repository in encrypted, access-controlled storage.
Regularly restore a copy into a disposable database and run `alembic current`
plus representative read-only API checks. Never test restoration against the
live database.

Object-storage buckets and PostgreSQL must be backed up as one recovery set.
Database rows own object keys and checksums; restoring only one side can leave
artifacts unavailable.

## Repository data

`.update-backups/`, `raw-data/`, runtime databases, artifacts, coverage output,
and environment files are ignored for new additions. Historical tracked backup
files and the existing raw dataset were intentionally not deleted by the
hardening change; remove them from Git only after confirming retention and
privacy requirements.

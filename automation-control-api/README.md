# Automation Control API

Local FastAPI agent for controlling Deomee automation projects from Windmill, Tailscale, or another operator UI.

The API is intentionally thin: project logic stays inside the existing automation folders. This service starts local commands, tails logs, reads status files, and writes approval files.

## Run

For the first phase, the AntiDengue virtualenv already contains FastAPI/Uvicorn.

Preferred local controls:

```bash
cd /home/ahmad/code/deomeeautomations/automation-control-api
bin/start.sh
bin/status.sh
bin/stop.sh
```

Manual foreground run:

```bash
cd /home/ahmad/code/deomeeautomations/automation-control-api
../antidengue/.venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8787
```

Then open:

```text
http://127.0.0.1:8787/docs
```

## Mobile Dashboard

Tailnet dashboard:

```text
https://hustle03.tailbc3cfc.ts.net/control-api
```

Generate a one-tap pairing URL for an already trusted device:

```bash
cd /home/ahmad/code/deomeeautomations/automation-control-api
bin/pair-url.sh
```

The token is carried in the URL fragment (`#token=...`), so it is not sent to the server in the HTTP request. The dashboard stores it in that browser's local storage.

## Security

Set `CONTROL_API_TOKEN` before exposing through Tailscale Serve. Mutating endpoints accept either:

```text
Authorization: Bearer <token>
```

or:

```text
X-API-Token: <token>
```

Without `CONTROL_API_TOKEN`, the API is useful for local development only.

## Phase 1 Endpoints

```text
GET  /health
GET  /antidengue/status
POST /antidengue/run
POST /antidengue/stop
GET  /antidengue/logs
GET  /antidengue/latest-run
GET  /portal-login/status
POST /portal-login/approve
POST /portal-login/deny
```

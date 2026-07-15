# Windmill Control

Windmill operator layer for Deomee automations.

This folder keeps Windmill-facing scripts and notes separate from the local execution API. Windmill should call `automation-control-api`; it should not directly own local Playwright sessions, WhatsApp auth files, or project internals.

## Local Windmill

The official Windmill compose files are kept in `vendor/` as a reference. The runnable compose in this folder is adapted for this laptop:

- no Caddy container, because ports 80/443 are already used by the existing local stack
- Windmill server exposed only on `127.0.0.1:8088`
- one default worker and one native worker
- Postgres state stored in Docker volumes

Start it with:

```bash
cd /home/ahmad/code/deomeeautomations/windmill-control
docker compose up -d
```

Then open:

```text
http://127.0.0.1:8088
```

## Environment Variables

Configure these as Windmill resources/secrets:

```text
CONTROL_API_BASE_URL=http://127.0.0.1:8787
CONTROL_API_TOKEN=<same token as automation-control-api>
```

When exposed through Tailscale Serve, replace the base URL with the Tailscale HTTPS URL.

## First Scripts

```text
scripts/antidengue/run_now.py
scripts/antidengue/status.py
scripts/antidengue/approve_login.py
```

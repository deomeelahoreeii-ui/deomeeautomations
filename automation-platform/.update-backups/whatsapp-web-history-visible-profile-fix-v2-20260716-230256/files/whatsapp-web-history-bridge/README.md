# WhatsApp Web History Bridge

This service handles **historical** WhatsApp messages and files with `whatsapp-web.js` 1.34.7. The existing Baileys worker remains responsible for live inbound capture and outbound delivery.

## Default mode: LocalAuth

The dependency is exactly pinned to `whatsapp-web.js` 1.34.7. `dev.sh` uses the installed Brave/Chromium executable and skips Puppeteer's separate browser download.

1. Copy `.env.example` to `.env`.
2. Set `WWEBJS_PLATFORM_TOKEN` to the same value as `WA_INBOUND_PLATFORM_TOKEN` in `whatsappbot/.env`.
3. Start `automation-platform/scripts/dev.sh`.
4. On first run, scan the QR printed in the terminal or open `data/login-qr.png`. The linked session is then stored under `data/auth/`.

## Optional existing-browser mode

Start a dedicated Brave instance with remote debugging:

```bash
./scripts/launch-brave-debug.sh
```

Log into WhatsApp Web in that Brave window and confirm the target chat/files are visible, then set:

```env
WWEBJS_MODE=browser_url
WWEBJS_BROWSER_URL=http://127.0.0.1:9222
```

The bridge creates a new WhatsApp Web tab inside that authenticated browser context. It never closes the whole attached browser during normal shutdown.

## Responsibilities

- Resolve the selected direct chat.
- Load earlier messages through WhatsApp Web's own earlier-message loader.
- Ingest normalized history into Automation Platform.
- Immediately archive supported images, PDFs and spreadsheets.
- Retry a historical media download later when an export asks for it.
- Report durable request lifecycle over NATS.

## Provider rollback

The original Baileys history implementation remains live in `whatsappbot` and is also snapshotted under `whatsappbot/archive/baileys-history-v2/`. Set `WHATSAPP_INBOUND_HISTORY_PROVIDER=baileys` in `automation-platform/.env` to route history requests back to it.

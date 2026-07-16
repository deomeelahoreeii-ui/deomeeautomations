# Preserved Baileys history provider

The Baileys on-demand history implementation remains intact in:

- `lib/inbound-history.js`
- `lib/inbound-message-store.js`
- `lib/inbound-capture.js`
- `worker.js`

It continues to subscribe to:

```text
whatsapp.worker.inbound.history.<worker-id>
```

Automation Platform now defaults to the `wwebjs` provider because existing linked Baileys sessions can accept `fetchMessageHistory()` without receiving any history event. To temporarily reactivate the preserved implementation, set this in `automation-platform/.env` and restart the stack:

```env
WHATSAPP_INBOUND_HISTORY_PROVIDER=baileys
```

To return to the normal WhatsApp Web bridge:

```env
WHATSAPP_INBOUND_HISTORY_PROVIDER=wwebjs
```

Do not delete the Baileys history tables or worker code. They remain useful for future upstream fixes, initial linked-device synchronization, and regression comparison.

An immutable source snapshot is also stored under:

```text
whatsappbot/archive/baileys-history-v2/
```

Run `sha256sum -c MANIFEST.sha256` inside that directory to verify it before using it for future comparison or restoration.

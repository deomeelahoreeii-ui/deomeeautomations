# Baileys inbound-history v2 source snapshot

This directory is an immutable source snapshot of the Baileys on-demand history provider that existed immediately before the Automation Platform switched its default provider to `whatsapp-web.js`.

The active Baileys implementation is still present in `whatsappbot`; this copy exists for regression comparison and future restoration if upstream history delivery becomes reliable.

To select the still-active provider without restoring this snapshot:

```env
WHATSAPP_INBOUND_HISTORY_PROVIDER=baileys
```

The snapshot includes the history responder, durable store/capture dependencies, worker wiring, configuration, and focused tests. Verify it with `sha256sum -c MANIFEST.sha256` from this directory.

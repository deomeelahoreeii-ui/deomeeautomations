# WhatsApp Web History Bridge

This service handles **historical** WhatsApp messages and files with exactly pinned `whatsapp-web.js` 1.34.7. The existing Baileys worker remains responsible for live inbound capture and outbound delivery.

## Required mode: the visible Brave profile

Historical retrieval defaults to `WWEBJS_MODE=browser_url`. The launcher makes a private local snapshot of the Brave profile in which the target chat and files are already visible, then opens that snapshot with loopback-only remote debugging. A separate `LocalAuth` linked-device session is not treated as equivalent and is blocked for historical requests by default.

One-time startup:

1. Save your work and close every Brave window.
2. Create and start the private debug snapshot of that same profile:

   ```bash
   cd whatsapp-web-history-bridge
   ./scripts/launch-brave-debug.sh
   ```

3. In that Brave window, open WhatsApp Web and verify the required chat/files are visible.
4. Keep that Brave process open.
5. In another terminal, start `automation-platform/scripts/dev.sh`.

The launcher reads Brave's last-used profile from `~/.config/BraveSoftware/Brave-Browser/Local State`, copies it only after Brave is closed, excludes disposable caches, and leaves the normal profile untouched. The private snapshot is stored under `~/.local/share/deomee-wwebjs-brave-visible`. Override the profile when needed:

```bash
WWEBJS_BRAVE_PROFILE_DIRECTORY="Profile 1" ./scripts/launch-brave-debug.sh
```

To reuse the existing private snapshot without refreshing it from the normal profile:

```bash
WWEBJS_REFRESH_PROFILE_SNAPSHOT=false ./scripts/launch-brave-debug.sh
```

## Loader strategy

The bridge first asks the official `Chat.syncHistory()` API to refresh the selected chat. It then uses `Chat.fetchMessages()`. If WhatsApp Web changes its internal earlier-message function and the official call throws an opaque minified error, the bridge runs a compatibility loader inside the attached page. That loader tries both known `loadEarlierMsgs` signatures, observes the actual chat store, reloads messages by stable ID, and records detailed diagnostics.

Failures now include their phase, error type, stack excerpt, chat candidates, loader exports and attempted signatures instead of only `r` or another one-character browser exception.

## Responsibilities

- Resolve the selected direct chat, including phone-number and LID mappings.
- Load earlier messages from the attached WhatsApp Web browser profile.
- Ingest normalized history into Automation Platform.
- Immediately archive supported images, PDFs and spreadsheets.
- Retry a historical media download later when an export asks for it.
- Report durable request lifecycle and diagnostics over NATS.

## Preserved implementations

The LocalAuth data directory is not deleted. To experiment with it, explicitly set both:

```env
WWEBJS_MODE=local_auth
WWEBJS_ALLOW_LOCAL_AUTH_HISTORY=true
```

The original Baileys history implementation remains in `whatsappbot` and is snapshotted under `whatsappbot/archive/baileys-history-v2/`. Set `WHATSAPP_INBOUND_HISTORY_PROVIDER=baileys` in `automation-platform/.env` to route history requests back to it.

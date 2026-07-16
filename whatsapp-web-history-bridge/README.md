# WhatsApp Web history bridge

This service retrieves historical files from the same local Brave profile in which they are already visible, then sends their metadata and bytes to the Automation Platform.

## Normal architecture

`WWEBJS_MODE=visible_profile` is the supported history mode.

1. Close normal Brave once.
2. Run `./scripts/launch-brave-debug.sh`.
3. The script copies the selected Brave profile to a private runtime snapshot and exits. Despite its legacy filename, it no longer launches a debug browser.
4. Start `automation-platform/scripts/dev.sh`.
5. `whatsapp-web.js` 1.34.7 launches and owns exactly one Brave page using that snapshot.

This replaces the earlier `browser_url` design. In whatsapp-web.js 1.34.7, browser URL attachment connects to the browser and then calls `browser.newPage()`. That produced a second WhatsApp Web tab beside the visible tab and allowed WhatsApp navigation to invalidate the frame used by `getChats()`.

## Detached-frame recovery

Protocol v3 also protects operations that can race with a WhatsApp Web navigation:

- contact/LID mapping;
- number and chat lookup;
- broad chat listing;
- `syncHistory()`;
- official `fetchMessages()`;
- compatibility pagination;
- message hydration;
- media download.

Only known Puppeteer navigation failures are retried. Ordinary application errors are returned immediately. If the current page closes, the bridge rebinds to the live WhatsApp Web page owned by its browser.

## Preparing or refreshing the snapshot

```bash
cd /home/ahmad/code/deomeeautomations/whatsapp-web-history-bridge
./scripts/launch-brave-debug.sh
```

The profile is auto-detected from Brave's `Local State`. Override it when necessary:

```bash
WWEBJS_VISIBLE_PROFILE_DIRECTORY="Profile 1" ./scripts/launch-brave-debug.sh
```

The script writes `data/visible-profile.json`. Authentication data and browser storage remain local and are not included in patch bundles.

## Preserved implementations

- `browser_url` remains in source for diagnostics but history is blocked unless `WWEBJS_ALLOW_BROWSER_URL_HISTORY=true` is deliberately set.
- `local_auth` remains available for experiments but is blocked for normal historical retrieval.
- The existing Baileys implementation and `whatsappbot/archive/baileys-history-v2/` remain unchanged.

## Commands

```bash
npm run check
npm test
npm start
```

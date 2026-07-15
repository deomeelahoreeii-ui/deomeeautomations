import { createGroupRegistry } from "./groups.js";
import { sleep } from "./config.js";

export function createGroupDiscovery({ config, log }) {
  const groupRegistry = createGroupRegistry(config.groupsFile);
  const groupMetadataState = new Map();
  const participatingGroups = new Map();
  let groupMetadataQueue = Promise.resolve();
  let lastGroupMetadataLookupAt = 0;

  function summarizeGroup(group) {
    const jid = group?.id || group?.jid || "";
    if (!jid.endsWith("@g.us")) {
      return null;
    }
    const creation = Number(group?.creation || 0);
    return {
      jid,
      name: group?.subject || group?.name || "",
      description: group?.desc || "",
      ownerJid: group?.owner || null,
      participantCount: Number(group?.size ?? group?.participants?.length ?? 0),
      createdAt: creation > 0 ? new Date(creation * 1000).toISOString() : null,
    };
  }

  function cacheGroup(group) {
    const summary = summarizeGroup(group);
    if (summary) {
      participatingGroups.set(summary.jid, summary);
    }
    return summary;
  }

  function recordGroup(jid, groupName = "") {
    if (!jid || !jid.endsWith("@g.us")) {
      return false;
    }

    const wasAdded = groupRegistry.record({
      discoveredAt: new Date().toISOString(),
      groupId: jid,
      groupName,
    });

    if (wasAdded) {
      log.info("Discovered a new WhatsApp group", {
        groupId: jid,
        groupName,
        groupsFile: config.groupsFile,
      });
    }
    return wasAdded;
  }

  function getGroupMetadataCooldownMs(error) {
    if (error?.message === "rate-overlimit") {
      return config.groupMetadataRateLimitCooldownMs;
    }

    return config.groupMetadataFailureCooldownMs;
  }

  async function captureGroupMetadata(sock, jid, fallbackGroupName = "") {
    if (!jid || !jid.endsWith("@g.us") || groupRegistry.has(jid)) {
      return;
    }

    const now = Date.now();
    const state = groupMetadataState.get(jid);

    if (state?.inFlight) {
      return state.inFlight;
    }

    if (state?.nextAttemptAt && state.nextAttemptAt > now) {
      log.debug("Skipping group metadata lookup during cooldown", {
        groupId: jid,
        retryAfterMs: state.nextAttemptAt - now,
      });
      return;
    }

    const inFlight = (async () => {
      const waitMs = Math.max(
        0,
        lastGroupMetadataLookupAt + config.groupMetadataMinIntervalMs - Date.now(),
      );

      if (waitMs > 0) {
        await sleep(waitMs);
      }

      lastGroupMetadataLookupAt = Date.now();

      try {
        const metadata = await sock.groupMetadata(jid);
        const groupName = metadata?.subject || fallbackGroupName || "";
        recordGroup(jid, groupName);

        groupMetadataState.delete(jid);
      } catch (error) {
        const cooldownMs = getGroupMetadataCooldownMs(error);

        groupMetadataState.set(jid, {
          nextAttemptAt: Date.now() + cooldownMs,
        });

        log.warn("Failed to capture group metadata", {
          groupId: jid,
          error: error.message,
          retryAfterMs: cooldownMs,
        });
      } finally {
        const current = groupMetadataState.get(jid);

        if (current?.inFlight === inFlight) {
          delete current.inFlight;
        }
      }
    })();

    groupMetadataState.set(jid, {
      ...state,
      inFlight,
    });

    return inFlight;
  }

  function queueGroupMetadataCapture(sock, jid, fallbackGroupName = "") {
    groupMetadataQueue = groupMetadataQueue
      .then(() => captureGroupMetadata(sock, jid, fallbackGroupName))
      .catch((error) => {
        log.warn("Group metadata queue step failed", {
          groupId: jid,
          error: error.message,
        });
      });

    return groupMetadataQueue;
  }

  function handleMessagesUpsert(sock, { messages }) {
    const pendingGroups = new Map();

    for (const message of messages || []) {
      const jid = message?.key?.remoteJid;

      if (!jid || !jid.endsWith("@g.us") || groupRegistry.has(jid)) {
        continue;
      }

      if (!pendingGroups.has(jid)) {
        pendingGroups.set(jid, message.pushName || "");
      }
    }

    for (const [jid, fallbackGroupName] of pendingGroups) {
      void queueGroupMetadataCapture(sock, jid, fallbackGroupName);
    }
  }

  function handleGroupsUpsert(groups) {
    for (const group of groups || []) {
      cacheGroup(group);
      recordGroup(group?.id, group?.subject || group?.name || "");
    }
  }

  async function syncParticipatingGroups(sock) {
    try {
      const groups = await sock.groupFetchAllParticipating();
      participatingGroups.clear();
      handleGroupsUpsert(Object.values(groups || {}));
      log.info("Synchronized WhatsApp group registry", {
        groupCount: Object.keys(groups || {}).length,
        groupsFile: config.groupsFile,
      });
    } catch (error) {
      log.warn("Failed to synchronize participating WhatsApp groups", {
        error: error.message,
      });
    }
  }

  async function listParticipatingGroups(sock, { refresh = true } = {}) {
    if (refresh || participatingGroups.size === 0) {
      await syncParticipatingGroups(sock);
    }
    return [...participatingGroups.values()].sort((left, right) =>
      String(left.name || left.jid).localeCompare(String(right.name || right.jid)),
    );
  }

  function reset() {
    groupMetadataState.clear();
    participatingGroups.clear();
    groupMetadataQueue = Promise.resolve();
    lastGroupMetadataLookupAt = 0;
  }

  return {
    handleGroupsUpsert,
    handleMessagesUpsert,
    listParticipatingGroups,
    reset,
    syncParticipatingGroups,
  };
}

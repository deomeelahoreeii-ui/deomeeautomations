import fs from "fs";
import path from "path";

const MAX_EVIDENCE_ITEMS = 25;

export function isPhoneJid(value) {
  return /^\d+@s\.whatsapp\.net$/.test(String(value || ""));
}

export function isLidJid(value) {
  return /^\d+(?::\d+)?@lid$/.test(String(value || ""));
}

export function canonicalPhoneJid(value) {
  const text = String(value || "").trim();
  if (isPhoneJid(text)) {
    return text;
  }
  const digits = text.replace(/\D/g, "");
  return digits ? `${digits}@s.whatsapp.net` : null;
}

export function canonicalLidJid(value) {
  const match = String(value || "").trim().match(/^(\d+)(?::\d+)?@lid$/);
  return match ? `${match[1]}@lid` : null;
}

function emptyState() {
  return {
    version: 1,
    updatedAt: null,
    identities: {},
    lidToPhoneJid: {},
  };
}

function normalizeState(raw) {
  if (!raw || typeof raw !== "object") {
    return emptyState();
  }
  return {
    version: 1,
    updatedAt: raw.updatedAt || null,
    identities:
      raw.identities && typeof raw.identities === "object"
        ? raw.identities
        : {},
    lidToPhoneJid:
      raw.lidToPhoneJid && typeof raw.lidToPhoneJid === "object"
        ? raw.lidToPhoneJid
        : {},
  };
}

function confidenceValue(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0.5;
  }
  return Math.min(1, Math.max(0, parsed));
}

function sortedLidEntries(identity) {
  return Object.entries(identity.lidJids || {}).sort((left, right) => {
    const leftConfidence = confidenceValue(left[1]?.confidence);
    const rightConfidence = confidenceValue(right[1]?.confidence);
    if (rightConfidence !== leftConfidence) {
      return rightConfidence - leftConfidence;
    }
    return String(right[1]?.lastSeenAt || "").localeCompare(
      String(left[1]?.lastSeenAt || ""),
    );
  });
}

export function createWhatsAppIdentityStore({ filePath, authDir, log }) {
  let state = emptyState();
  let loaded = false;
  let seededFromAuthMappings = false;

  function save() {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    state.updatedAt = new Date().toISOString();
    const tempPath = `${filePath}.${process.pid}.tmp`;
    fs.writeFileSync(tempPath, `${JSON.stringify(state, null, 2)}\n`, "utf8");
    fs.renameSync(tempPath, filePath);
  }

  function load() {
    if (loaded) {
      return state;
    }

    try {
      if (fs.existsSync(filePath)) {
        state = normalizeState(
          JSON.parse(fs.readFileSync(filePath, "utf8") || "{}"),
        );
      }
    } catch (error) {
      log?.warn?.("Could not load WhatsApp identity store", {
        error: error.message,
        filePath,
      });
      state = emptyState();
    }

    loaded = true;
    return state;
  }

  function applyObservation({
    phoneJid,
    lidJid,
    source,
    evidence = {},
    confidence = 0.7,
    contactName = "",
  }) {
    const canonicalPhone = canonicalPhoneJid(phoneJid);
    const canonicalLid = canonicalLidJid(lidJid);
    if (!canonicalPhone || !canonicalLid) {
      return null;
    }

    const now = new Date().toISOString();
    const phone = canonicalPhone.split("@", 1)[0];
    const identity = state.identities[canonicalPhone] || {
      phone,
      phoneJid: canonicalPhone,
      primaryLidJid: "",
      lidJids: {},
      contactName: "",
      firstSeenAt: now,
      lastSeenAt: now,
      lastSource: "",
      confidence: 0,
      active: true,
      evidence: [],
    };

    const alias = identity.lidJids[canonicalLid] || {
      firstSeenAt: now,
      lastSeenAt: now,
      sources: [],
      confidence: 0,
    };
    alias.lastSeenAt = now;
    alias.confidence = Math.max(
      confidenceValue(alias.confidence),
      confidenceValue(confidence),
    );
    if (source && !alias.sources.includes(source)) {
      alias.sources.push(source);
    }

    identity.lidJids[canonicalLid] = alias;
    identity.lastSeenAt = now;
    identity.lastSource = source || identity.lastSource || "unknown";
    identity.confidence = Math.max(
      confidenceValue(identity.confidence),
      confidenceValue(confidence),
    );
    identity.active = true;
    if (contactName) {
      identity.contactName = contactName;
    }
    identity.evidence = [
      {
        observedAt: now,
        source: source || "unknown",
        lidJid: canonicalLid,
        ...evidence,
      },
      ...(identity.evidence || []),
    ].slice(0, MAX_EVIDENCE_ITEMS);

    const [primaryLid] = sortedLidEntries(identity)[0] || [];
    identity.primaryLidJid = primaryLid || canonicalLid;
    state.identities[canonicalPhone] = identity;
    state.lidToPhoneJid[canonicalLid] = canonicalPhone;
    return identity;
  }

  function observe(observation) {
    load();
    const identity = applyObservation(observation);
    if (identity) {
      save();
    }
    return identity;
  }

  function seedFromAuthMappings() {
    load();
    if (seededFromAuthMappings) {
      return false;
    }
    seededFromAuthMappings = true;

    let changed = false;
    try {
      for (const filename of fs.readdirSync(authDir)) {
        const match = filename.match(/^lid-mapping-(\d+)_reverse\.json$/);
        if (!match) {
          continue;
        }
        const rawPhone = JSON.parse(
          fs.readFileSync(path.join(authDir, filename), "utf8"),
        );
        const phoneJid = canonicalPhoneJid(String(rawPhone || ""));
        const lidJid = canonicalLidJid(`${match[1]}@lid`);
        if (!phoneJid || !lidJid) {
          continue;
        }
        const before = state.lidToPhoneJid[lidJid];
        if (before === phoneJid) {
          continue;
        }
        applyObservation({
          phoneJid,
          lidJid,
          source: "baileys_auth_reverse_mapping",
          confidence: 0.85,
          evidence: { authFile: filename },
        });
        changed = true;
      }
    } catch (error) {
      log?.warn?.("Could not seed WhatsApp identities from Baileys mappings", {
        error: error.message,
        authDir,
      });
    }
    if (changed) {
      save();
    }
    return changed;
  }

  function findLidForPhoneJid(phoneJid) {
    load();
    const canonicalPhone = canonicalPhoneJid(phoneJid);
    if (!canonicalPhone) {
      return null;
    }
    const identity = state.identities[canonicalPhone];
    if (!identity) {
      return null;
    }
    return identity.primaryLidJid || sortedLidEntries(identity)[0]?.[0] || null;
  }

  function findPhoneForLidJid(lidJid) {
    load();
    const canonicalLid = canonicalLidJid(lidJid);
    return canonicalLid ? state.lidToPhoneJid[canonicalLid] || null : null;
  }

  function stats() {
    load();
    return {
      identities: Object.keys(state.identities).length,
      lidAliases: Object.keys(state.lidToPhoneJid).length,
      filePath,
    };
  }

  function directory() {
    load();
    return Object.values(state.identities)
      .map((identity) => ({
        phoneJid: identity.phoneJid || null,
        primaryLidJid: identity.primaryLidJid || null,
        displayName: identity.contactName || "",
        firstSeenAt: identity.firstSeenAt || null,
        lastSeenAt: identity.lastSeenAt || null,
        source: identity.lastSource || "gateway",
        confidence: confidenceValue(identity.confidence),
        active: identity.active !== false,
        aliases: sortedLidEntries(identity).map(([lidJid, alias]) => ({
          lidJid,
          firstSeenAt: alias.firstSeenAt || null,
          lastSeenAt: alias.lastSeenAt || null,
          source: alias.sources?.at(-1) || identity.lastSource || "gateway",
          confidence: confidenceValue(alias.confidence),
        })),
      }))
      .sort((left, right) =>
        String(left.displayName || left.phoneJid || "").localeCompare(
          String(right.displayName || right.phoneJid || ""),
        ),
      );
  }

  return {
    directory,
    findLidForPhoneJid,
    findPhoneForLidJid,
    observe,
    seedFromAuthMappings,
    stats,
  };
}

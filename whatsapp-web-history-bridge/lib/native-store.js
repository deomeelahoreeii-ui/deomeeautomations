import { formatError, phaseError } from "./errors.js";
import { withPageRecovery } from "./page-session.js";

function normalizeId(value) {
  const text = String(value?._serialized || value || "").trim();
  if (!text) return "";
  if (text.endsWith("@s.whatsapp.net")) return `${text.split("@")[0]}@c.us`;
  return text;
}

function digits(value) {
  const local = String(value?._serialized || value || "").split("@")[0];
  const output = local.replace(/\D/g, "");
  return output.length >= 7 ? output : "";
}

function nativeMessage(client, record, pageRecoveryTimeoutMs) {
  const serialized = String(record?.id || "");
  return {
    id: {
      _serialized: serialized,
      id: String(record?.keyId || serialized),
      remote: String(record?.remote || ""),
      fromMe: Boolean(record?.fromMe),
    },
    timestamp: Number(record?.timestamp || 0),
    fromMe: Boolean(record?.fromMe),
    from: String(record?.from || record?.remote || ""),
    to: String(record?.to || ""),
    author: record?.author || null,
    type: String(record?.type || "unknown"),
    body: String(record?.body || ""),
    hasMedia: Boolean(record?.hasMedia),
    _data: {
      id: {
        _serialized: serialized,
        id: String(record?.keyId || serialized),
        remote: String(record?.remote || ""),
        fromMe: Boolean(record?.fromMe),
      },
      t: Number(record?.timestamp || 0),
      type: String(record?.type || "unknown"),
      caption: String(record?.body || ""),
      body: String(record?.body || ""),
      mimetype: record?.mimetype || null,
      filename: record?.filename || null,
      size: Number(record?.filesize || 0) || null,
      notifyName: record?.notifyName || null,
      directPath: record?.hasMedia ? "native-store" : null,
    },
    async downloadMedia() {
      return downloadNativeMediaById(client, serialized, { pageRecoveryTimeoutMs });
    },
  };
}

async function evaluateNative(client, phase, evaluator, args, pageRecoveryTimeoutMs) {
  if (!client?.pupPage?.evaluate) {
    throw new Error("WhatsApp Web native Store access requires an attached browser page");
  }
  return withPageRecovery(
    client,
    phase,
    () => client.pupPage.evaluate(evaluator, ...args),
    { pageTimeoutMs: pageRecoveryTimeoutMs },
  );
}

export async function inspectNativeDirectChats(client, remoteJids, {
  pageRecoveryTimeoutMs = 30000,
} = {}) {
  const requested = [...new Set((remoteJids || []).map(normalizeId).filter(Boolean))];
  const result = await evaluateNative(
    client,
    "native_chat_lookup_page",
    async (requestedIds) => {
      const serialize = (value) => {
        if (!value) return "";
        if (typeof value === "string") return value;
        if (typeof value._serialized === "string") return value._serialized;
        try {
          const text = value.toString?.();
          return text && text !== "[object Object]" ? String(text) : "";
        } catch {
          return "";
        }
      };
      const normalize = (value) => {
        const text = serialize(value).trim();
        if (!text) return "";
        if (text.endsWith("@s.whatsapp.net")) return `${text.split("@")[0]}@c.us`;
        return text;
      };
      const numberDigits = (value) => {
        const output = normalize(value).split("@")[0].replace(/\D/g, "");
        return output.length >= 7 ? output : "";
      };
      const errorText = (error) => {
        if (typeof error === "string") return error;
        return String(error?.message || error || "Unknown browser error");
      };
      const requestedSet = new Set(requestedIds.map(normalize).filter(Boolean));
      const requestedDigits = new Set([...requestedSet].map(numberDigits).filter(Boolean));
      const mappingFailures = [];
      const mappings = [];

      if (typeof globalThis.WWebJS?.enforceLidAndPnRetrieval === "function") {
        for (const requestedId of [...requestedSet]) {
          try {
            const mapped = await globalThis.WWebJS.enforceLidAndPnRetrieval(requestedId);
            const lid = normalize(mapped?.lid);
            const phone = normalize(mapped?.phone);
            if (lid) requestedSet.add(lid);
            if (phone) requestedSet.add(phone);
            const phoneValue = numberDigits(phone);
            if (phoneValue) requestedDigits.add(phoneValue);
            mappings.push({ requestedId, lid: lid || null, phone: phone || null });
          } catch (error) {
            mappingFailures.push(`${requestedId}: ${errorText(error)}`);
          }
        }
      }

      const activeIds = new Set();
      try {
        for (const element of globalThis.document?.querySelectorAll?.("#main [data-id]") || []) {
          const dataId = String(element.getAttribute?.("data-id") || "");
          const match = dataId.match(/^(?:true|false)_([^_]+)_/);
          if (match?.[1]) activeIds.add(normalize(match[1]));
        }
      } catch {}

      let chatCollection;
      try {
        chatCollection = globalThis.require("WAWebCollections").Chat;
      } catch (error) {
        return {
          matched: null,
          diagnostics: {
            requested: [...requestedSet],
            mappings,
            mappingFailures,
            activeIds: [...activeIds],
            error: `WAWebCollections.Chat is unavailable: ${errorText(error)}`,
          },
        };
      }

      const models = typeof chatCollection?.getModelsArray === "function"
        ? chatCollection.getModelsArray()
        : Array.isArray(chatCollection?.models)
          ? chatCollection.models
          : [];
      const summaries = [];

      for (const chat of models || []) {
        try {
          const identities = new Set();
          const add = (value) => {
            const id = normalize(value);
            if (id) identities.add(id);
          };
          add(chat?.id);
          add(chat?.contact?.id);
          add(chat?.contact?.phoneNumber);
          add(chat?.contact?.lid);
          add(chat?.contact?.pn);
          add(chat?.contact?.phone);
          add(chat?.contact?.wid);
          try {
            const apiContact = globalThis.require("WAWebApiContact");
            if (typeof apiContact?.getPhoneNumber === "function" && chat?.id) {
              add(await Promise.resolve(apiContact.getPhoneNumber(chat.id)));
            }
          } catch {}

          const id = normalize(chat?.id);
          const server = String(chat?.id?.server || id.split("@")[1] || "");
          const isGroup = Boolean(chat?.isGroup || server === "g.us");
          if (!id || isGroup) continue;
          const identityDigits = new Set([...identities].map(numberDigits).filter(Boolean));
          const exact = [...identities].some((item) => requestedSet.has(item));
          const digitMatch = [...identityDigits].some((item) => requestedDigits.has(item));
          const active = activeIds.has(id) || [...identities].some((item) => activeIds.has(item));
          let score = exact ? 100 : digitMatch ? 60 : 0;
          // The visible/open chat is only a tie-breaker. It must never turn an
          // unrelated active chat into a match for the selected directory contact.
          if (score > 0 && active) score += 10;
          summaries.push({
            id,
            user: String(chat?.id?.user || id.split("@")[0] || ""),
            title: String(chat?.formattedTitle || chat?.name || chat?.contact?.formattedName || chat?.contact?.pushname || ""),
            identities: [...identities],
            exact,
            digitMatch,
            active,
            score,
          });
        } catch (error) {
          summaries.push({ id: null, score: -1, error: errorText(error) });
        }
      }

      const ranked = summaries
        .filter((item) => item.id && item.score > 0)
        .sort((left, right) => right.score - left.score || Number(right.active) - Number(left.active));
      const top = ranked[0] || null;
      const ambiguous = Boolean(top && ranked[1] && ranked[1].score === top.score && ranked[1].id !== top.id);
      return {
        matched: ambiguous ? null : top,
        diagnostics: {
          requested: [...requestedSet],
          requestedDigits: [...requestedDigits],
          mappings,
          mappingFailures,
          activeIds: [...activeIds],
          scannedChats: summaries.length,
          topCandidates: ranked.slice(0, 8),
          ambiguous,
        },
      };
    },
    [requested],
    pageRecoveryTimeoutMs,
  );
  return result;
}

async function nativeFetchMessages(client, chatId, searchOptions, pageRecoveryTimeoutMs) {
  const limit = Math.max(1, Number(searchOptions?.limit || 100));
  const fromMe = searchOptions?.fromMe;
  const result = await evaluateNative(
    client,
    `native_history_page:${chatId}`,
    async (targetChatId, targetLimit, targetFromMe) => {
      const serialize = (value) => {
        if (!value) return "";
        if (typeof value === "string") return value;
        if (typeof value._serialized === "string") return value._serialized;
        try {
          const text = value.toString?.();
          return text && text !== "[object Object]" ? String(text) : "";
        } catch {
          return "";
        }
      };
      const errorText = (error) => typeof error === "string" ? error : String(error?.message || error || "Unknown browser error");
      const collections = globalThis.require("WAWebCollections");
      const chats = collections.Chat;
      const widFactory = globalThis.require("WAWebWidFactory");
      let chat = null;
      try { chat = chats.get(targetChatId); } catch {}
      if (!chat) {
        try { chat = chats.get(widFactory.createWid(targetChatId)); } catch {}
      }
      if (!chat) {
        const models = typeof chats.getModelsArray === "function" ? chats.getModelsArray() : chats.models || [];
        chat = models.find((item) => serialize(item?.id) === targetChatId) || null;
      }
      if (!chat) throw new Error(`Native Store could not find chat ${targetChatId}`);

      const include = (message) => {
        if (!message || message.isNotification) return false;
        if (targetFromMe === true && message?.id?.fromMe !== true) return false;
        if (targetFromMe === false && message?.id?.fromMe === true) return false;
        return true;
      };
      const messageId = (message) => serialize(message?.id);
      const collect = () => {
        let source = chat?.msgs?.getModelsArray?.() || [];
        if (!source.length && typeof collections.Msg?.getModelsArray === "function") {
          source = collections.Msg.getModelsArray().filter((message) => serialize(message?.id?.remote) === targetChatId);
        }
        const unique = new Map();
        for (const message of source || []) {
          const id = messageId(message);
          if (id && include(message)) unique.set(id, message);
        }
        return [...unique.values()];
      };

      let messages = collect();
      let rounds = 0;
      let stagnant = 0;
      const failures = [];
      while (messages.length < targetLimit && rounds < Math.max(6, Math.min(300, Math.ceil(targetLimit / 15) + 15))) {
        rounds += 1;
        const before = messages.length;
        let loader;
        try {
          loader = globalThis.require("WAWebChatLoadMessages")?.loadEarlierMsgs;
        } catch (error) {
          failures.push(`loader_module: ${errorText(error)}`);
          break;
        }
        if (typeof loader !== "function") {
          failures.push("WAWebChatLoadMessages.loadEarlierMsgs is unavailable");
          break;
        }
        let loaded = null;
        let success = false;
        for (const [variant, invoke] of [
          ["object_chat", () => loader({ chat })],
          ["direct_chat", () => loader(chat)],
        ]) {
          let roundTimer = null;
          try {
            loaded = await Promise.race([
              invoke(),
              new Promise((_, reject) => {
                roundTimer = setTimeout(() => reject(new Error("earlier-message loader round timed out")), 30000);
              }),
            ]);
            success = true;
            break;
          } catch (error) {
            failures.push(`${variant}: ${errorText(error)}`);
          } finally {
            if (roundTimer) clearTimeout(roundTimer);
          }
        }
        if (!success) break;
        await new Promise((resolve) => setTimeout(resolve, 250));
        const merged = new Map(messages.map((message) => [messageId(message), message]));
        for (const message of Array.isArray(loaded) ? loaded : []) {
          const id = messageId(message);
          if (id && include(message)) merged.set(id, message);
        }
        for (const message of collect()) merged.set(messageId(message), message);
        messages = [...merged.values()];
        stagnant = messages.length <= before ? stagnant + 1 : 0;
        if (stagnant >= 2) break;
      }

      messages.sort((left, right) => Number(left?.t || 0) - Number(right?.t || 0));
      if (messages.length > targetLimit) messages = messages.slice(messages.length - targetLimit);
      const records = messages.map((message) => {
        const id = messageId(message);
        const remote = serialize(message?.id?.remote || message?.from || message?.to);
        return {
          id,
          keyId: String(message?.id?.id || id),
          remote,
          fromMe: Boolean(message?.id?.fromMe),
          from: serialize(message?.from) || (!message?.id?.fromMe ? remote : ""),
          to: serialize(message?.to) || (message?.id?.fromMe ? remote : ""),
          author: serialize(message?.author) || null,
          timestamp: Number(message?.t || 0),
          type: String(message?.type || "unknown"),
          body: String(message?.caption || message?.body || message?.pollName || ""),
          hasMedia: Boolean(message?.directPath || message?.mimetype || message?.mediaData),
          mimetype: message?.mimetype || null,
          filename: message?.filename || null,
          filesize: Number(message?.size || 0) || null,
          notifyName: message?.notifyName || null,
        };
      });
      return {
        records,
        diagnostics: {
          chatId: targetChatId,
          requestedLimit: targetLimit,
          rounds,
          finalCount: records.length,
          failures: failures.slice(-12),
        },
      };
    },
    [chatId, limit, fromMe],
    pageRecoveryTimeoutMs,
  );
  return (result?.records || []).map((record) => nativeMessage(client, record, pageRecoveryTimeoutMs));
}

async function nativeSyncHistory(client, chatId, pageRecoveryTimeoutMs) {
  if (typeof client?.syncHistory === "function") {
    return withPageRecovery(
      client,
      `native_sync_history:${chatId}`,
      () => client.syncHistory(chatId),
      { pageTimeoutMs: pageRecoveryTimeoutMs },
    );
  }
  return false;
}

export function createNativeDirectChat(client, record, {
  pageRecoveryTimeoutMs = 30000,
} = {}) {
  const chatId = normalizeId(record?.id);
  if (!chatId) throw new Error("Native Store chat record has no id");
  return {
    id: { _serialized: chatId, user: String(record?.user || chatId.split("@")[0] || "") },
    name: String(record?.title || ""),
    isGroup: false,
    __nativeStore: true,
    async syncHistory() {
      return nativeSyncHistory(client, chatId, pageRecoveryTimeoutMs);
    },
    async fetchMessages(searchOptions = {}) {
      return nativeFetchMessages(client, chatId, searchOptions, pageRecoveryTimeoutMs);
    },
  };
}

export async function resolveNativeDirectChat(client, remoteJids, options = {}) {
  try {
    const result = await inspectNativeDirectChats(client, remoteJids, options);
    if (!result?.matched) return { chat: null, diagnostics: result?.diagnostics || null };
    return {
      chat: createNativeDirectChat(client, result.matched, options),
      diagnostics: result.diagnostics,
    };
  } catch (error) {
    throw phaseError("native_chat_resolution", error);
  }
}

export async function downloadNativeMediaById(client, messageId, {
  pageRecoveryTimeoutMs = 30000,
} = {}) {
  const id = String(messageId || "").trim();
  if (!id) throw new Error("Native media download requires a message id");
  return evaluateNative(
    client,
    `native_media_download:${id}`,
    async (targetMessageId) => {
      const collections = globalThis.require("WAWebCollections");
      let message = collections.Msg.get(targetMessageId);
      if (!message && typeof collections.Msg.getMessagesById === "function") {
        message = (await collections.Msg.getMessagesById([targetMessageId]))?.messages?.[0] || null;
      }
      if (!message || !message.mediaData || message.mediaData.mediaStage === "REUPLOADING") return null;
      if (message.mediaData.mediaStage !== "RESOLVED") {
        await message.downloadMedia({ downloadEvenIfExpensive: true, rmrReason: 1 });
      }
      if (String(message.mediaData.mediaStage || "").includes("ERROR") || message.mediaData.mediaStage === "FETCHING") {
        return null;
      }
      const mockQpl = {
        addAnnotations() { return this; },
        addPoint() { return this; },
      };
      const decrypted = await globalThis.require("WAWebDownloadManager").downloadManager.downloadAndMaybeDecrypt({
        directPath: message.directPath,
        encFilehash: message.encFilehash,
        filehash: message.filehash,
        mediaKey: message.mediaKey,
        mediaKeyTimestamp: message.mediaKeyTimestamp,
        type: message.type,
        signal: new AbortController().signal,
        downloadQpl: mockQpl,
      });
      const data = await globalThis.WWebJS.arrayBufferToBase64Async(decrypted);
      return {
        data,
        mimetype: message.mimetype || null,
        filename: message.filename || null,
        filesize: Number(message.size || 0) || null,
      };
    },
    [id],
    pageRecoveryTimeoutMs,
  ).catch((error) => {
    throw phaseError("native_media_download", new Error(formatError(error, "browser_media")));
  });
}

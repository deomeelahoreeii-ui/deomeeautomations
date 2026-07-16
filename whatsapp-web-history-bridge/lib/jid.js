import { formatError } from "./errors.js";

export function phoneDigits(value) {
  const local = String(value || "").split("@")[0];
  const digits = local.replace(/\D/g, "");
  return digits.length >= 7 ? digits : "";
}

export function normalizePlatformJid(value) {
  const text = String(value || "").trim();
  const digits = phoneDigits(text);
  if (!digits) return text;
  if (text.endsWith("@lid")) return text;
  return `${digits}@s.whatsapp.net`;
}

function normalizeWwebId(value) {
  const text = String(value?._serialized || value || "").trim();
  if (!text) return "";
  if (text.endsWith("@s.whatsapp.net")) return `${phoneDigits(text)}@c.us`;
  return text;
}

export function wwebCandidates(values) {
  const result = [];
  for (const value of values || []) {
    const text = String(value || "").trim();
    if (!text) continue;
    if (text.endsWith("@c.us") || text.endsWith("@lid") || text.endsWith("@g.us")) result.push(text);
    const digits = phoneDigits(text);
    if (digits) result.push(`${digits}@c.us`);
  }
  return [...new Set(result.filter(Boolean))];
}

function appendMappingCandidates(candidates, mappings) {
  for (const mapping of mappings || []) {
    for (const value of [mapping?.lid, mapping?.pn, mapping?.phone, mapping?.phoneNumber]) {
      const normalized = normalizeWwebId(value);
      if (normalized) candidates.push(normalized);
    }
  }
}

export async function resolveDirectChat(client, remoteJids) {
  const candidates = wwebCandidates(remoteJids);
  const digits = [...new Set((remoteJids || []).map(phoneDigits).filter(Boolean))];
  const diagnostics = {
    requested: [...(remoteJids || [])],
    candidates: [],
    mappings: [],
    failures: [],
    matchedBy: null,
    matchedChatId: null,
  };

  if (typeof client.getContactLidAndPhone === "function" && candidates.length) {
    try {
      const mappings = await client.getContactLidAndPhone([...candidates]);
      diagnostics.mappings = mappings || [];
      appendMappingCandidates(candidates, mappings);
    } catch (error) {
      diagnostics.failures.push(formatError(error, "contact_lid_phone"));
    }
  }

  for (const number of digits) {
    try {
      const id = await client.getNumberId(number);
      const serialized = normalizeWwebId(id?._serialized || id?.id?._serialized || id);
      if (serialized) candidates.unshift(serialized);
    } catch (error) {
      diagnostics.failures.push(formatError(error, `number_lookup:${number}`));
    }
  }

  const uniqueCandidates = [...new Set(candidates.filter(Boolean))];
  diagnostics.candidates = uniqueCandidates;
  for (const candidate of uniqueCandidates) {
    try {
      const chat = await client.getChatById(candidate);
      if (chat && !chat.isGroup) {
        diagnostics.matchedBy = "getChatById";
        diagnostics.matchedChatId = String(chat?.id?._serialized || candidate);
        return { chat, diagnostics };
      }
    } catch (error) {
      diagnostics.failures.push(formatError(error, `get_chat:${candidate}`));
    }
  }

  let chats;
  try {
    chats = await client.getChats();
  } catch (error) {
    throw new Error(`WhatsApp Web could not list chats. ${formatError(error, "get_chats")}`);
  }
  for (const chat of chats || []) {
    if (chat?.isGroup) continue;
    const serialized = String(chat?.id?._serialized || "");
    const user = String(chat?.id?.user || "");
    if (uniqueCandidates.includes(serialized) || digits.includes(user) || digits.includes(phoneDigits(serialized))) {
      diagnostics.matchedBy = "getChats";
      diagnostics.matchedChatId = serialized;
      return { chat, diagnostics };
    }
  }

  const attempted = uniqueCandidates.length ? uniqueCandidates.join(", ") : "no valid candidates";
  const detail = diagnostics.failures.slice(-4).join(" || ");
  throw new Error(`WhatsApp Web direct chat was not found. Requested: ${(remoteJids || []).join(", ")}. Tried: ${attempted}${detail ? `. Diagnostics: ${detail}` : ""}`);
}

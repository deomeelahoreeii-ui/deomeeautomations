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

export function wwebCandidates(values) {
  const result = [];
  for (const value of values || []) {
    const text = String(value || "").trim();
    if (!text) continue;
    if (text.endsWith("@c.us") || text.endsWith("@lid") || text.endsWith("@g.us")) result.push(text);
    const digits = phoneDigits(text);
    if (digits) result.push(`${digits}@c.us`);
  }
  return [...new Set(result)];
}

export async function resolveDirectChat(client, remoteJids) {
  const candidates = wwebCandidates(remoteJids);
  const digits = [...new Set((remoteJids || []).map(phoneDigits).filter(Boolean))];

  for (const number of digits) {
    try {
      const id = await client.getNumberId(number);
      const serialized = id?._serialized || id?.id?._serialized;
      if (serialized) candidates.unshift(serialized);
    } catch {
      // Some linked sessions cannot resolve a number until its chat is loaded.
    }
  }

  for (const candidate of [...new Set(candidates)]) {
    try {
      const chat = await client.getChatById(candidate);
      if (chat && !chat.isGroup) return chat;
    } catch {
      // Continue through candidates and finally inspect loaded chats.
    }
  }

  const chats = await client.getChats();
  for (const chat of chats) {
    if (chat?.isGroup) continue;
    const serialized = String(chat?.id?._serialized || "");
    const user = String(chat?.id?.user || "");
    if (candidates.includes(serialized) || digits.includes(user) || digits.includes(phoneDigits(serialized))) {
      return chat;
    }
  }
  throw new Error(`WhatsApp Web direct chat was not found for ${remoteJids.join(", ")}`);
}

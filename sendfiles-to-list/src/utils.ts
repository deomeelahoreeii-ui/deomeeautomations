export const delay = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));
export const randomDelay = (min: number, max: number) =>
  delay(Math.floor(Math.random() * (max - min + 1) + min));

export function formatWhatsAppNumber(phone: string | number): string | null {
  if (!phone) return null;
  let numStr = String(phone).replace(/\D/g, "");

  if (numStr.length === 10 && numStr.startsWith("3")) {
    numStr = "92" + numStr;
  } else if (numStr.length === 11 && numStr.startsWith("0")) {
    numStr = "92" + numStr.substring(1);
  }

  if (numStr.length >= 11) {
    return `${numStr}@s.whatsapp.net`;
  }
  return null;
}

export function parseMarkdownTemplate(
  template: string,
  contactData: Record<string, any>,
): string {
  let message = template;

  // Replace variables like {{Name}} with the corresponding column from Excel
  for (const [key, value] of Object.entries(contactData)) {
    const regex = new RegExp(`{{${key}}}`, "gi");
    message = message.replace(regex, String(value || ""));
  }

  // Fallback if {{Name}} was used but column was empty
  message = message.replace(/{{Name}}/gi, "Sir/Madam");

  // Convert standard Markdown bold/italic to WhatsApp format
  message = message.replace(/\*\*(.*?)\*\*/g, "*$1*"); // **bold** to *bold*

  return message;
}

import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import qrcode from "qrcode-terminal";
import pino from "pino";
import * as fs from "fs";
import * as path from "path";
import * as mime from "mime-types";
import { PATHS, DELAYS } from "./config.js";
import { delay } from "./utils.js";

export async function initializeWhatsApp(
  onReady: (sock: any) => Promise<void>,
) {
  const { state, saveCreds } = await useMultiFileAuthState(PATHS.AUTH);

  const sock = makeWASocket({
    auth: state,
    logger: pino({ level: "silent" }),
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      console.log("\n📱 Please scan the QR code below with WhatsApp:\n");
      qrcode.generate(qr, { small: true });
    }

    if (connection === "close") {
      const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

      console.log(`Connection closed. Reconnecting: ${shouldReconnect}`);
      if (shouldReconnect) setTimeout(() => initializeWhatsApp(onReady), 2000);
    } else if (connection === "open") {
      console.log("✅ Connected to WhatsApp securely.");
      await onReady(sock);
    }
  });
}

export async function sendTextMessage(sock: any, jid: string, text: string) {
  await sock.sendPresenceUpdate("composing", jid);
  await delay(DELAYS.PRESENCE);
  await sock.sendMessage(jid, { text });
  console.log(`   ✅ Sent Message`);
}

export async function sendFileMessage(
  sock: any,
  jid: string,
  filePath: string,
) {
  await sock.sendPresenceUpdate("composing", jid);
  await delay(DELAYS.PRESENCE);

  const fileName = path.basename(filePath);
  const mimeType = mime.lookup(filePath) || "application/octet-stream";
  let messagePayload: any = {};

  if (mimeType.startsWith("image/")) {
    messagePayload = { image: fs.readFileSync(filePath), caption: fileName };
  } else if (mimeType.startsWith("video/")) {
    messagePayload = { video: fs.readFileSync(filePath), caption: fileName };
  } else {
    messagePayload = {
      document: fs.readFileSync(filePath),
      mimetype: mimeType,
      fileName: fileName,
      caption: fileName,
    };
  }

  await sock.sendMessage(jid, messagePayload);
  console.log(`   ✅ Sent File: ${fileName}`);
}

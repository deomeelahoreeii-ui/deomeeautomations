import qrcode from "qrcode-terminal";
import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import * as fs from "fs";
import * as path from "path";
import xlsx from "xlsx";
import * as mime from "mime-types";
import pino from "pino";
import { fileURLToPath } from "url";
import { dirname } from "path";

// ES Module __dirname reconstruction
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration Paths
const CONTACTS_DIR = path.join(__dirname, "contacts");
const FILES_DIR = path.join(__dirname, "files");

// Helper: Staggered Delay (Anti-Ban feature)
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const randomDelay = (min = 8000, max = 15000) =>
  delay(Math.floor(Math.random() * (max - min + 1) + min));

// Helper: Format Pakistani Mobile Numbers for WhatsApp
function formatWhatsAppNumber(phone: string | number): string | null {
  if (!phone) return null;

  let numStr = String(phone).replace(/\D/g, ""); // Remove all non-numeric characters

  // Fix 1: Excel stripped the leading '0' (e.g., 3085171856 -> 10 digits)
  if (numStr.length === 10 && numStr.startsWith("3")) {
    numStr = "92" + numStr; // Just '92', no '+' or '00'
  }
  // Fix 2: Standard local format (e.g., 03085171856 -> 11 digits)
  else if (numStr.length === 11 && numStr.startsWith("0")) {
    numStr = "92" + numStr.substring(1);
  }

  // Ensure it's a valid international length before proceeding
  if (numStr.length >= 11) {
    return `${numStr}@s.whatsapp.net`;
  }

  return null;
}

// Helper: Read Contacts from Excel/CSV (Auto-detects the file)
function getContacts() {
  console.log("Loading contacts...");

  if (!fs.existsSync(CONTACTS_DIR)) {
    console.error("❌ Contacts directory does not exist.");
    return [];
  }

  // Auto-detect any Excel or CSV file in the directory
  const availableFiles = fs.readdirSync(CONTACTS_DIR);
  const contactFile = availableFiles.find(
    (file) => file.endsWith(".xlsx") || file.endsWith(".csv"),
  );

  if (!contactFile) {
    console.error("❌ No .xlsx or .csv file found in the contacts/ directory.");
    return [];
  }

  const targetPath = path.join(CONTACTS_DIR, contactFile);
  console.log(`📄 Using contacts file: ${contactFile}`);

  const workbook = xlsx.readFile(targetPath);
  const sheetName = workbook.SheetNames[0]; // Gets the first sheet

  if (!sheetName) {
    console.error("❌ No sheets found in the Excel/CSV file.");
    return [];
  }

  const sheet = workbook.Sheets[sheetName];

  // Converts sheet to JSON array
  const data: any[] = xlsx.utils.sheet_to_json(sheet);
  return data;
}

// Helper: Read Files from Directory
function getFilesToSend(): string[] {
  console.log("Scanning files directory...");
  if (!fs.existsSync(FILES_DIR)) return [];

  return fs
    .readdirSync(FILES_DIR)
    .map((file) => path.join(FILES_DIR, file))
    .filter((filePath) => fs.statSync(filePath).isFile());
}

// Main Dispatch Function
async function startDispatcher() {
  // 1. Setup Baileys Auth & Socket
  const { state, saveCreds } = await useMultiFileAuthState("auth_info_baileys");

  const sock = makeWASocket({
    auth: state,
    logger: pino({ level: "silent" }), // Keeps the terminal clean during sending
  });

  // CRITICAL: Saves session so you do not have to scan QR code again
  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    // --- Render the QR code manually if needed ---
    if (qr) {
      console.log("\n📱 Please scan the QR code below with WhatsApp:\n");
      qrcode.generate(qr, { small: true });
    }

    if (connection === "close") {
      const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

      console.log(
        `Connection closed (Code: ${statusCode}). Reconnecting: ${shouldReconnect}`,
      );

      if (shouldReconnect) {
        // Wait 2 seconds before reconnecting to let auth files save to disk safely
        setTimeout(() => {
          startDispatcher();
        }, 2000);
      }
    } else if (connection === "open") {
      console.log("✅ Connected to WhatsApp securely.");
      await processBroadcast(sock); // Start sending once connected
    }
  });
}

// The Core Broadcast Logic
async function processBroadcast(sock: any) {
  const contacts = getContacts();
  const files = getFilesToSend();

  if (files.length === 0) {
    console.log("❌ No files found in the files/ directory to send.");
    process.exit(1);
    return;
  }

  if (contacts.length === 0) {
    console.log("❌ No valid contacts loaded to send messages to.");
    process.exit(1);
    return;
  }

  console.log(`\n🚀 Starting broadcast to ${contacts.length} contacts...`);

  for (const [index, contact] of contacts.entries()) {
    // Standardizing column names
    const rawPhone = contact["Cell No"];
    const name = contact["Name"] || "Sir/Madam";
    const customMessage = contact["Message"] || ""; // Fallback if no message column

    const jid = formatWhatsAppNumber(rawPhone);
    if (!jid) {
      console.log(`⚠️ Skipping row ${index + 2}: Invalid number (${rawPhone})`);
      continue;
    }

    console.log(
      `\n[${index + 1}/${contacts.length}] Processing ${name} (${jid})...`,
    );

    // Optional but recommended: Check if number exists on WhatsApp
    try {
      const [result] = await sock.onWhatsApp(jid);
      if (!result?.exists) {
        console.log(`⏭️ Number not registered on WhatsApp: ${jid}`);
        continue;
      }
    } catch (err) {
      console.log(
        `⚠️ Could not verify WhatsApp registration for ${jid}. Attempting to send anyway.`,
      );
    }

    // Send all files to this specific contact
    for (const filePath of files) {
      const fileName = path.basename(filePath);
      const mimeType = mime.lookup(filePath) || "application/octet-stream";

      // Personalize the caption
      const caption = customMessage
        ? `Dear ${name},\n\n${customMessage}`
        : `Dear ${name},\n\nPlease find the attached document: ${fileName}`;

      try {
        // Simulate human presence (typing/recording)
        await sock.sendPresenceUpdate("composing", jid);
        await delay(2000);

        // Determine Baileys message payload based on mime-type
        let messagePayload: any = {};

        if (mimeType.startsWith("image/")) {
          messagePayload = {
            image: fs.readFileSync(filePath),
            caption: caption,
          };
        } else if (mimeType.startsWith("video/")) {
          messagePayload = {
            video: fs.readFileSync(filePath),
            caption: caption,
          };
        } else {
          // For PDFs, Word docs, etc.
          messagePayload = {
            document: fs.readFileSync(filePath),
            mimetype: mimeType,
            fileName: fileName,
            caption: caption,
          };
        }

        await sock.sendMessage(jid, messagePayload);
        console.log(`   ✅ Sent: ${fileName}`);

        // Pause slightly between files to the SAME person
        await delay(3000);
      } catch (error) {
        console.error(`   ❌ Failed to send ${fileName}:`, error);
      }
    }

    // Critical: Staggered delay before messaging the NEXT person
    console.log("   Waiting before next contact (Anti-Ban safety)...");
    await randomDelay(8000, 16000);
  }

  console.log("\n🎉 Broadcast completed successfully!");
  process.exit(0);
}

// Execute
startDispatcher();

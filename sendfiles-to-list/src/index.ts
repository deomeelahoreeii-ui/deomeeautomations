import { getContacts, getFilesToSend, getBroadcastMessage } from "./data.ts";
import {
  formatWhatsAppNumber,
  parseMarkdownTemplate,
  randomDelay,
  delay,
} from "./utils.ts";
import {
  initializeWhatsApp,
  sendTextMessage,
  sendFileMessage,
} from "./whatsapp.ts";
import { DELAYS } from "./config.ts";

// --- Parse Command Line Arguments ---
const args = process.argv.slice(2);
let sendText = args.includes("--message") || args.includes("-m");
let sendFiles = args.includes("--files") || args.includes("-f");

// If no flags are provided, default to both for backward compatibility
if (!sendText && !sendFiles) {
  console.log(
    "ℹ️  No flags provided. Defaulting to sending BOTH messages and files.",
  );
  console.log(
    "💡 Tip: Use --message to send only text, or --files to send only files.",
  );
  sendText = true;
  sendFiles = true;
}

async function runBroadcast(sock: any) {
  const contacts = getContacts();
  // Only load the resources if the respective flags are active
  const files = sendFiles ? getFilesToSend() : [];
  const rawMessageTemplate = sendText ? getBroadcastMessage() : null;

  if (contacts.length === 0) {
    console.log("❌ No valid contacts loaded. Exiting.");
    process.exit(1);
  }

  // Pre-flight Validation
  if (sendText && !rawMessageTemplate) {
    console.log(
      "❌ You requested to send messages (--message), but no markdown/txt file was found in messages/. Exiting.",
    );
    process.exit(1);
  }

  if (sendFiles && files.length === 0) {
    console.log(
      "❌ You requested to send files (--files), but the files/ directory is empty. Exiting.",
    );
    process.exit(1);
  }

  console.log(`\n🚀 Starting broadcast to ${contacts.length} contacts...`);
  console.log(
    `📡 Modes active: ${sendText ? "[Text]" : ""} ${sendFiles ? "[Files]" : ""}\n`,
  );

  for (const [index, contact] of contacts.entries()) {
    const rawPhone = contact["Cell No"];
    const jid = formatWhatsAppNumber(rawPhone);
    const name = contact["Name"] || "Unknown";

    if (!jid) {
      console.log(
        `⚠️  Skipping row ${index + 2}: Invalid number (${rawPhone})`,
      );
      continue;
    }

    console.log(
      `\n[${index + 1}/${contacts.length}] Processing ${name} (${jid})...`,
    );

    // 1. Verify Registration
    try {
      const [result] = await sock.onWhatsApp(jid);
      if (!result?.exists) {
        console.log(`⏭️  Number not registered on WhatsApp: ${jid}`);
        continue;
      }
    } catch (err) {
      console.log(
        `⚠️  Could not verify registration for ${jid}. Attempting anyway.`,
      );
    }

    // 2. Send Markdown Message
    if (sendText && rawMessageTemplate) {
      try {
        const personalizedMessage = parseMarkdownTemplate(
          rawMessageTemplate,
          contact,
        );
        await sendTextMessage(sock, jid, personalizedMessage);

        // Add a small pause if we are about to send files right after, ensuring order
        if (sendFiles && files.length > 0) {
          await delay(DELAYS.BETWEEN_FILES);
        }
      } catch (error) {
        console.error(`   ❌ Failed to send text message:`, error);
      }
    }

    // 3. Send Files
    if (sendFiles && files.length > 0) {
      for (const filePath of files) {
        try {
          await sendFileMessage(sock, jid, filePath);
          await delay(DELAYS.BETWEEN_FILES); // Pause between multiple files
        } catch (error) {
          console.error(`   ❌ Failed to send file:`, error);
        }
      }
    }

    // 4. Staggered Delay for Anti-Ban
    console.log("   Waiting before next contact...");
    await randomDelay(DELAYS.MIN_CONTACT, DELAYS.MAX_CONTACT);
  }

  console.log("\n🎉 Broadcast completed successfully!");
  process.exit(0);
}

// Start the application
console.log("Initializing Script...");
initializeWhatsApp(runBroadcast);

import * as path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");

export const PATHS = {
  CONTACTS: path.join(ROOT_DIR, "contacts"),
  FILES: path.join(ROOT_DIR, "files"),
  MESSAGES: path.join(ROOT_DIR, "messages"),
  AUTH: path.join(ROOT_DIR, "auth_info_baileys"),
};

export const DELAYS = {
  PRESENCE: 2000, // Time to simulate "typing..."
  BETWEEN_FILES: 3000, // Pause between multiple files to the same person
  MIN_CONTACT: 8000, // Anti-ban: Minimum wait before next contact
  MAX_CONTACT: 15000, // Anti-ban: Maximum wait before next contact
};

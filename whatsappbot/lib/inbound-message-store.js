import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

const SCHEMA_VERSION = 1;

function json(value) {
  return JSON.stringify(value ?? null);
}

export class InboundMessageStore {
  constructor({ filePath, workerId, log }) {
    this.filePath = filePath;
    this.workerId = workerId;
    this.log = log;
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    this.db = new DatabaseSync(filePath);
    this.db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;");
    this.migrate();
  }

  migrate() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS inbound_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        worker_id TEXT NOT NULL,
        message_id TEXT NOT NULL,
        remote_jid TEXT NOT NULL,
        participant_jid TEXT,
        sender_jid TEXT NOT NULL,
        from_me INTEGER NOT NULL DEFAULT 0,
        chat_scope TEXT NOT NULL,
        message_timestamp TEXT NOT NULL,
        push_name TEXT,
        text_content TEXT,
        message_type TEXT NOT NULL,
        ingestion_source TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
        raw_payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(worker_id, remote_jid, message_id)
      );
      CREATE INDEX IF NOT EXISTS ix_inbound_messages_sender_time ON inbound_messages(sender_jid, message_timestamp);
      CREATE INDEX IF NOT EXISTS ix_inbound_messages_remote_time ON inbound_messages(remote_jid, message_timestamp);
      CREATE TABLE IF NOT EXISTS inbound_attachments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_row_id INTEGER NOT NULL REFERENCES inbound_messages(id) ON DELETE CASCADE,
        media_kind TEXT NOT NULL,
        message_key TEXT NOT NULL,
        original_filename TEXT,
        mime_type TEXT,
        declared_size INTEGER,
        media_sha256 TEXT,
        caption TEXT,
        download_status TEXT NOT NULL DEFAULT 'metadata_only',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(message_row_id)
      );
      CREATE TABLE IF NOT EXISTS inbound_outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_key TEXT NOT NULL UNIQUE,
        event_type TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        available_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_error TEXT,
        delivered_at TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS ix_inbound_outbox_pending ON inbound_outbox(delivered_at, available_at, id);
    `);
    this.db.prepare("INSERT OR REPLACE INTO metadata(key, value) VALUES('schema_version', ?)").run(String(SCHEMA_VERSION));
  }

  upsert(normalized) {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const item = normalized;
      this.db.prepare(`
        INSERT INTO inbound_messages (
          worker_id, message_id, remote_jid, participant_jid, sender_jid, from_me,
          chat_scope, message_timestamp, push_name, text_content, message_type,
          ingestion_source, payload_sha256, raw_payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(worker_id, remote_jid, message_id) DO UPDATE SET
          participant_jid=excluded.participant_jid,
          sender_jid=excluded.sender_jid,
          from_me=excluded.from_me,
          chat_scope=excluded.chat_scope,
          message_timestamp=excluded.message_timestamp,
          push_name=COALESCE(excluded.push_name, inbound_messages.push_name),
          text_content=COALESCE(excluded.text_content, inbound_messages.text_content),
          message_type=excluded.message_type,
          ingestion_source=excluded.ingestion_source,
          payload_sha256=excluded.payload_sha256,
          raw_payload_json=excluded.raw_payload_json,
          updated_at=CURRENT_TIMESTAMP
      `).run(
        this.workerId, item.messageId, item.remoteJid, item.participantJid,
        item.senderJid, item.fromMe ? 1 : 0, item.chatScope, item.messageTimestamp,
        item.pushName, item.text, item.messageType, item.ingestionSource,
        item.payloadSha256, json(item.rawPayload),
      );

      const row = this.db.prepare("SELECT id FROM inbound_messages WHERE worker_id=? AND remote_jid=? AND message_id=?").get(this.workerId, item.remoteJid, item.messageId);
      if (item.attachment) {
        const a = item.attachment;
        this.db.prepare(`
          INSERT INTO inbound_attachments (
            message_row_id, media_kind, message_key, original_filename, mime_type,
            declared_size, media_sha256, caption
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(message_row_id) DO UPDATE SET
            media_kind=excluded.media_kind,
            message_key=excluded.message_key,
            original_filename=COALESCE(excluded.original_filename, inbound_attachments.original_filename),
            mime_type=COALESCE(excluded.mime_type, inbound_attachments.mime_type),
            declared_size=COALESCE(excluded.declared_size, inbound_attachments.declared_size),
            media_sha256=COALESCE(excluded.media_sha256, inbound_attachments.media_sha256),
            caption=COALESCE(excluded.caption, inbound_attachments.caption),
            updated_at=CURRENT_TIMESTAMP
        `).run(row.id, a.mediaKind, a.messageKey, a.originalFilename, a.mimeType, a.declaredSize, a.mediaSha256, a.caption);
      }

      const eventKey = `${this.workerId}:${item.remoteJid}:${item.messageId}`;
      const payload = { ...item, workerId: this.workerId };
      this.db.prepare(`
        INSERT INTO inbound_outbox(event_key, event_type, payload_json)
        VALUES (?, 'whatsapp.inbound.message.upserted', ?)
        ON CONFLICT(event_key) DO UPDATE SET payload_json=excluded.payload_json,
          delivered_at=NULL, available_at=CURRENT_TIMESTAMP
      `).run(eventKey, json(payload));
      this.db.exec("COMMIT");
      return row.id;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  pendingOutbox(limit = 100) {
    return this.db.prepare(`SELECT id, event_key, event_type, payload_json, attempts FROM inbound_outbox WHERE delivered_at IS NULL AND available_at <= CURRENT_TIMESTAMP ORDER BY id LIMIT ?`).all(limit);
  }

  markDelivered(id) {
    this.db.prepare("UPDATE inbound_outbox SET delivered_at=CURRENT_TIMESTAMP, last_error=NULL WHERE id=?").run(id);
  }

  markFailed(id, error) {
    this.db.prepare(`UPDATE inbound_outbox SET attempts=attempts+1, last_error=?, available_at=datetime('now', '+' || MIN(300, (attempts + 1) * 5) || ' seconds') WHERE id=?`).run(String(error || "unknown error").slice(0, 2000), id);
  }

  getMessage(remoteJid, messageId) {
    const row = this.db.prepare("SELECT raw_payload_json FROM inbound_messages WHERE worker_id=? AND remote_jid=? AND message_id=?").get(this.workerId, remoteJid, messageId);
    return row ? JSON.parse(row.raw_payload_json) : undefined;
  }

  stats() {
    const messages = this.db.prepare("SELECT COUNT(*) AS count, MIN(message_timestamp) AS earliest, MAX(message_timestamp) AS latest FROM inbound_messages WHERE worker_id=?").get(this.workerId);
    const attachments = this.db.prepare("SELECT COUNT(*) AS count FROM inbound_attachments a JOIN inbound_messages m ON m.id=a.message_row_id WHERE m.worker_id=?").get(this.workerId);
    const pending = this.db.prepare("SELECT COUNT(*) AS count FROM inbound_outbox WHERE delivered_at IS NULL").get();
    return { schemaVersion: SCHEMA_VERSION, filePath: this.filePath, messages: Number(messages.count), attachments: Number(attachments.count), outboxPending: Number(pending.count), earliestMessageAt: messages.earliest || null, latestMessageAt: messages.latest || null };
  }

  close() { this.db.close(); }
}

migrate((app) => {
  const adminOnly = null

  function collection(name) {
    try {
      return app.findCollectionByNameOrId(name)
    } catch {
      return null
    }
  }

  function exec(sql) {
    try {
      app.db().newQuery(sql).execute()
    } catch {
      // Best-effort backfill/index creation for partially migrated databases.
    }
  }

  function saveCollectionIfMissing(definition) {
    const existing = collection(definition.name)
    if (existing) {
      return existing
    }
    const created = new Collection(definition)
    app.save(created)
    return app.findCollectionByNameOrId(definition.name)
  }

  function addRelationField(collectionName, fieldName, targetCollectionName) {
    const owner = collection(collectionName)
    const target = collection(targetCollectionName)
    if (!owner || !target) {
      return
    }
    const fields = owner.fields.asMap()
    if (fields[fieldName]) {
      return
    }
    owner.fields.add(new RelationField({
      name: fieldName,
      collectionId: target.id,
      maxSelect: 1,
    }))
    app.save(owner)
  }

  saveCollectionIfMissing({
    type: "base",
    name: "dispatch_settings",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "active", type: "bool" },
      { name: "name", type: "text", required: true, presentable: true, max: 120 },
      { name: "allow_individual", type: "bool" },
      { name: "allow_groups", type: "bool" },
      { name: "manual_only", type: "bool" },
      { name: "attach_excel", type: "bool" },
      { name: "send_failed_only", type: "bool" },
      { name: "require_preview", type: "bool" },
      { name: "notes", type: "text", max: 1000 },
    ],
    indexes: [
      "CREATE UNIQUE INDEX idx_dispatch_settings_name ON dispatch_settings (name)",
    ],
  })

  saveCollectionIfMissing({
    type: "base",
    name: "whatsapp_groups",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "enabled", type: "bool" },
      { name: "name", type: "text", required: true, presentable: true, max: 160 },
      { name: "target", type: "text", required: true, max: 120 },
      { name: "route_kind", type: "select", required: true, maxSelect: 1, values: ["district", "tehsil", "markaz", "fallback", "custom"] },
      { name: "message_mode", type: "select", required: true, maxSelect: 1, values: ["full_report", "tehsil_summary", "ddeo_wise", "aeo_wise", "failed_fallback"] },
      { name: "attach_excel", type: "bool" },
      { name: "manual_only", type: "bool" },
      { name: "attachment_text_mode", type: "select", maxSelect: 1, values: ["caption", "separate"] },
      { name: "delay_ms", type: "number" },
      { name: "last_verified_at", type: "date" },
      { name: "notes", type: "text", max: 1000 },
    ],
    indexes: [
      "CREATE UNIQUE INDEX idx_whatsapp_groups_target ON whatsapp_groups (target)",
      "CREATE INDEX idx_whatsapp_groups_enabled ON whatsapp_groups (enabled)",
      "CREATE INDEX idx_whatsapp_groups_route ON whatsapp_groups (route_kind, message_mode)",
    ],
  })

  addRelationField("whatsapp_groups", "tehsil_ref", "tehsils")
  addRelationField("whatsapp_groups", "markaz_ref", "markazes")

  saveCollectionIfMissing({
    type: "base",
    name: "message_templates",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "active", type: "bool" },
      { name: "key", type: "text", required: true, presentable: true, max: 120 },
      { name: "name", type: "text", required: true, max: 160 },
      { name: "channel", type: "select", required: true, maxSelect: 1, values: ["individual", "group"] },
      { name: "message_mode", type: "select", maxSelect: 1, values: ["individual", "full_report", "tehsil_summary", "ddeo_wise", "aeo_wise", "failed_fallback"] },
      { name: "body", type: "text", max: 20000 },
      { name: "notes", type: "text", max: 1000 },
    ],
    indexes: [
      "CREATE UNIQUE INDEX idx_message_templates_key ON message_templates (key)",
      "CREATE INDEX idx_message_templates_channel ON message_templates (channel, active)",
    ],
  })

  saveCollectionIfMissing({
    type: "base",
    name: "dispatch_events",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "run_key", type: "text", max: 160 },
      { name: "channel", type: "select", required: true, maxSelect: 1, values: ["individual", "group", "system"] },
      { name: "action", type: "select", required: true, maxSelect: 1, values: ["copied", "marked_sent", "queued", "skipped", "failed", "configured"] },
      { name: "target", type: "text", max: 120 },
      { name: "recipient_name", type: "text", max: 160 },
      { name: "status", type: "select", required: true, maxSelect: 1, values: ["pending", "copied", "manual_sent", "queued", "delivered", "failed", "skipped"] },
      { name: "message_preview", type: "text", max: 1000 },
      { name: "payload", type: "json" },
      { name: "created_at", type: "date" },
      { name: "notes", type: "text", max: 1000 },
    ],
    indexes: [
      "CREATE INDEX idx_dispatch_events_run ON dispatch_events (run_key)",
      "CREATE INDEX idx_dispatch_events_status ON dispatch_events (status)",
      "CREATE INDEX idx_dispatch_events_target ON dispatch_events (target)",
    ],
  })

  exec(`
    INSERT OR IGNORE INTO dispatch_settings
    (id, active, name, allow_individual, allow_groups, manual_only, attach_excel, send_failed_only, require_preview, notes)
    VALUES
    ('b50691f465c2e1a', 1, 'default', 1, 1, 0, 1, 0, 0, 'Default AntiDengue dispatch controls.')
  `)

  exec(`
    INSERT OR IGNORE INTO message_templates
    (id, active, key, name, channel, message_mode, body, notes)
    VALUES
    ('6720a7d97ba5d5f', 1, 'individual_dormant', 'Individual dormant officer message', 'individual', 'individual', '', 'Generated by application code.'),
    ('99afe5ec1efc882', 1, 'group_full_report', 'Group full dormant report', 'group', 'full_report', '', 'Generated by application code.'),
    ('d1a1f2eb35ea10a', 1, 'group_ddeo_wise', 'Group DDEO-wise dormant report', 'group', 'ddeo_wise', '', 'Generated by application code.'),
    ('7f4ca4d50ef7e02', 1, 'group_aeo_wise', 'Group AEO-wise dormant report', 'group', 'aeo_wise', '', 'Generated by application code.'),
    ('a699d0ee53b7b81', 1, 'failed_fallback', 'Failed direct-delivery fallback', 'group', 'failed_fallback', '', 'Generated by application code.')
  `)

  exec(`
    INSERT OR IGNORE INTO whatsapp_groups
    (id, enabled, name, target, route_kind, message_mode, attach_excel, manual_only, attachment_text_mode, delay_ms, notes)
    SELECT
      id,
      enabled,
      name,
      target,
      'district',
      'full_report',
      1,
      0,
      COALESCE(attachment_text_mode, ''),
      COALESCE(delay_ms, 1500),
      'Migrated from whatsapp_recipients.'
    FROM whatsapp_recipients
    WHERE type = 'group'
      AND target LIKE '%@g.us'
  `)
}, (app) => {
  function collection(name) {
    try {
      return app.findCollectionByNameOrId(name)
    } catch {
      return null
    }
  }

  for (const name of [
    "dispatch_events",
    "message_templates",
    "whatsapp_groups",
    "dispatch_settings",
  ]) {
    const existing = collection(name)
    if (existing) {
      app.delete(existing)
    }
  }
})

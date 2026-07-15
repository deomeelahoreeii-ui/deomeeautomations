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
      // Optional compatibility index/backfill.
    }
  }

  function addTextField(collectionName, fieldName, max) {
    const owner = collection(collectionName)
    if (!owner) {
      return
    }
    const fields = owner.fields.asMap()
    if (fields[fieldName]) {
      return
    }
    owner.fields.add(new TextField({ name: fieldName, max }))
    app.save(owner)
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

  addTextField("schools", "school_type", 80)
  addTextField("schools", "deos_wise", 80)
  addTextField("schools", "school_level", 80)

  saveCollectionIfMissing({
    type: "base",
    name: "whatsapp_recipients",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "enabled", type: "bool" },
      { name: "name", type: "text", required: true, presentable: true, max: 160 },
      { name: "type", type: "select", required: true, maxSelect: 1, values: ["group", "contact"] },
      { name: "target", type: "text", required: true, max: 120 },
      { name: "text", type: "text", max: 5000 },
      { name: "image_path", type: "text", max: 500 },
      { name: "excel_path", type: "text", max: 500 },
      { name: "excel_filename", type: "text", max: 255 },
      { name: "attachment_text_mode", type: "select", maxSelect: 1, values: ["caption", "separate"] },
      { name: "delay_ms", type: "number" },
      { name: "notes", type: "text", max: 500 },
    ],
    indexes: [
      "CREATE UNIQUE INDEX idx_whatsapp_recipients_target ON whatsapp_recipients (target)",
      "CREATE INDEX idx_whatsapp_recipients_enabled ON whatsapp_recipients (enabled)",
    ],
  })

  exec("UPDATE schools SET school_type = 'Male' WHERE COALESCE(school_type, '') = ''")
  exec("UPDATE schools SET deos_wise = 'M-EE' WHERE COALESCE(deos_wise, '') = ''")
  exec("UPDATE schools SET school_level = 'Primary' WHERE COALESCE(school_level, '') = '' AND UPPER(name) LIKE 'GPS %'")
  exec("UPDATE schools SET school_level = 'Middle' WHERE COALESCE(school_level, '') = '' AND UPPER(name) LIKE 'GES %'")
  exec("UPDATE schools SET school_level = 'sMosque' WHERE COALESCE(school_level, '') = '' AND UPPER(name) LIKE 'GMMS %'")
  exec("CREATE INDEX IF NOT EXISTS idx_schools_school_level ON schools (school_level)")
}, (app) => {
  function collection(name) {
    try {
      return app.findCollectionByNameOrId(name)
    } catch {
      return null
    }
  }

  const recipients = collection("whatsapp_recipients")
  if (recipients) {
    app.delete(recipients)
  }

  const schools = collection("schools")
  if (schools) {
    for (const fieldName of ["school_type", "deos_wise", "school_level"]) {
      if (schools.fields.asMap()[fieldName]) {
        schools.fields.removeByName(fieldName)
      }
    }
    app.save(schools)
  }
})

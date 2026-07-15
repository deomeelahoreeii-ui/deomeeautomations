migrate((app) => {
  function collection(name) {
    try {
      return app.findCollectionByNameOrId(name)
    } catch {
      return null
    }
  }

  function ensureSelectValues(collectionName, fieldName, values) {
    const owner = collection(collectionName)
    if (!owner) {
      return
    }
    const field = owner.fields.asMap()[fieldName]
    if (!field || !Array.isArray(field.values)) {
      return
    }
    let changed = false
    for (const value of values) {
      if (!field.values.includes(value)) {
        field.values.push(value)
        changed = true
      }
    }
    if (changed) {
      app.save(owner)
    }
  }

  const groupMessageModes = [
    "full_report",
    "tehsil_summary",
    "markaz_summary",
    "tehsil_markaz_summary",
    "ddeo_wise",
    "aeo_wise",
    "failed_fallback",
  ]

  ensureSelectValues("whatsapp_groups", "message_mode", groupMessageModes)
  ensureSelectValues("message_templates", "message_mode", [
    "individual",
    ...groupMessageModes,
  ])
}, (app) => {
  // Non-destructive rollback: existing records may already use the new modes.
})

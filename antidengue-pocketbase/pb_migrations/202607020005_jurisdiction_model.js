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
      // Optional compatibility cleanup/index creation.
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

  function baseJurisdictionCollection(name) {
    return saveCollectionIfMissing({
      type: "base",
      name,
      listRule: adminOnly,
      viewRule: adminOnly,
      createRule: adminOnly,
      updateRule: adminOnly,
      deleteRule: adminOnly,
      fields: [
        { name: "active", type: "bool" },
        { name: "effective_from", type: "date" },
        { name: "effective_to", type: "date" },
        { name: "notes", type: "text", max: 500 },
      ],
      indexes: [],
    })
  }

  function baseOverrideCollection(name) {
    return saveCollectionIfMissing({
      type: "base",
      name,
      listRule: adminOnly,
      viewRule: adminOnly,
      createRule: adminOnly,
      updateRule: adminOnly,
      deleteRule: adminOnly,
      fields: [
        { name: "active", type: "bool" },
        { name: "reason", type: "text", max: 500 },
        { name: "effective_from", type: "date" },
        { name: "effective_to", type: "date" },
      ],
      indexes: [],
    })
  }

  baseJurisdictionCollection("ddeo_jurisdictions")
  baseJurisdictionCollection("aeo_jurisdictions")
  baseOverrideCollection("school_ddeo_overrides")
  baseOverrideCollection("school_aeo_overrides")

  for (const collectionName of ["ddeo_jurisdictions", "aeo_jurisdictions"]) {
    addRelationField(collectionName, "district_ref", "districts")
    addRelationField(collectionName, "department_ref", "departments")
    addRelationField(collectionName, "wing_ref", "wings")
    addRelationField(collectionName, "tehsil_ref", "tehsils")
  }
  addRelationField("ddeo_jurisdictions", "ddeo_ref", "ddeo_officers")
  addRelationField("aeo_jurisdictions", "aeo_ref", "aeo_officers")
  addRelationField("aeo_jurisdictions", "markaz_ref", "markazes")

  addRelationField("school_ddeo_overrides", "school_ref", "schools")
  addRelationField("school_ddeo_overrides", "ddeo_ref", "ddeo_officers")
  addRelationField("school_aeo_overrides", "school_ref", "schools")
  addRelationField("school_aeo_overrides", "aeo_ref", "aeo_officers")

  const oldAssignments = collection("school_assignments")
  if (oldAssignments) {
    app.delete(oldAssignments)
  }

  exec("DELETE FROM dormant_records")
  exec("DELETE FROM delivery_events")
  exec("DELETE FROM data_quality_issues")
  exec("DELETE FROM report_runs")

  exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_ddeo_jurisdictions_unique ON ddeo_jurisdictions (ddeo_ref, wing_ref, tehsil_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_ddeo_jurisdictions_lookup ON ddeo_jurisdictions (wing_ref, tehsil_ref, active)")
  exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_aeo_jurisdictions_unique ON aeo_jurisdictions (aeo_ref, wing_ref, markaz_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_aeo_jurisdictions_lookup ON aeo_jurisdictions (wing_ref, markaz_ref, active)")
  exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_school_ddeo_overrides_unique ON school_ddeo_overrides (school_ref, ddeo_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_school_ddeo_overrides_lookup ON school_ddeo_overrides (school_ref, active)")
  exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_school_aeo_overrides_unique ON school_aeo_overrides (school_ref, aeo_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_school_aeo_overrides_lookup ON school_aeo_overrides (school_ref, active)")
}, (app) => {
  // Restore from a SQLite backup for rollback; this migration intentionally
  // replaces per-school assignments with jurisdiction records.
})

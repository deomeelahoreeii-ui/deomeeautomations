migrate((app) => {
  const adminOnly = null

  function collection(name) {
    try {
      return app.findCollectionByNameOrId(name)
    } catch {
      return null
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

  function createIndex(sql) {
    try {
      app.db().newQuery(sql).execute()
    } catch {
      // Index may already exist if the migration is replayed during development.
    }
  }

  saveCollectionIfMissing({
    type: "base",
    name: "districts",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "name", type: "text", required: true, presentable: true, max: 120 },
      { name: "code", type: "text", max: 40 },
      { name: "active", type: "bool" },
    ],
    indexes: [
      "CREATE UNIQUE INDEX idx_districts_name ON districts (name)",
      "CREATE INDEX idx_districts_code ON districts (code)",
    ],
  })

  saveCollectionIfMissing({
    type: "base",
    name: "departments",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "name", type: "text", required: true, presentable: true, max: 160 },
      { name: "code", type: "text", max: 40 },
      { name: "active", type: "bool" },
    ],
    indexes: [
      "CREATE UNIQUE INDEX idx_departments_name ON departments (name)",
      "CREATE INDEX idx_departments_code ON departments (code)",
    ],
  })

  saveCollectionIfMissing({
    type: "base",
    name: "wings",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "name", type: "text", required: true, presentable: true, max: 120 },
      { name: "code", type: "text", max: 40 },
      { name: "active", type: "bool" },
    ],
    indexes: [
      "CREATE INDEX idx_wings_name ON wings (name)",
      "CREATE INDEX idx_wings_code ON wings (code)",
    ],
  })

  saveCollectionIfMissing({
    type: "base",
    name: "tehsils",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "name", type: "text", required: true, presentable: true, max: 120 },
      { name: "active", type: "bool" },
    ],
    indexes: [
      "CREATE INDEX idx_tehsils_name ON tehsils (name)",
    ],
  })

  saveCollectionIfMissing({
    type: "base",
    name: "markazes",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "name", type: "text", required: true, presentable: true, max: 160 },
      { name: "active", type: "bool" },
    ],
    indexes: [
      "CREATE INDEX idx_markazes_name ON markazes (name)",
    ],
  })

  saveCollectionIfMissing({
    type: "base",
    name: "ddeo_officers",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "name", type: "text", required: true, presentable: true, max: 160 },
      { name: "mobile", type: "text", required: true, max: 32 },
      { name: "normalized_mobile", type: "text", required: true, max: 32 },
      { name: "active", type: "bool" },
    ],
    indexes: [
      "CREATE UNIQUE INDEX idx_ddeo_officers_mobile ON ddeo_officers (normalized_mobile)",
    ],
  })

  saveCollectionIfMissing({
    type: "base",
    name: "aeo_officers",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "name", type: "text", required: true, presentable: true, max: 160 },
      { name: "mobile", type: "text", required: true, max: 32 },
      { name: "normalized_mobile", type: "text", required: true, max: 32 },
      { name: "active", type: "bool" },
    ],
    indexes: [
      "CREATE UNIQUE INDEX idx_aeo_officers_mobile ON aeo_officers (normalized_mobile)",
    ],
  })

  addRelationField("wings", "district", "districts")
  addRelationField("wings", "department", "departments")
  addRelationField("tehsils", "district", "districts")
  addRelationField("markazes", "tehsil", "tehsils")
  addRelationField("markazes", "wing", "wings")
  addRelationField("ddeo_officers", "district", "districts")
  addRelationField("ddeo_officers", "department", "departments")
  addRelationField("ddeo_officers", "wing", "wings")
  addRelationField("ddeo_officers", "tehsil", "tehsils")
  addRelationField("aeo_officers", "district", "districts")
  addRelationField("aeo_officers", "department", "departments")
  addRelationField("aeo_officers", "wing", "wings")
  addRelationField("aeo_officers", "tehsil", "tehsils")
  addRelationField("aeo_officers", "markaz", "markazes")

  addRelationField("schools", "district_ref", "districts")
  addRelationField("schools", "department_ref", "departments")
  addRelationField("schools", "wing_ref", "wings")
  addRelationField("schools", "tehsil_ref", "tehsils")
  addRelationField("schools", "markaz_ref", "markazes")

  addRelationField("officers", "district_ref", "districts")
  addRelationField("officers", "department_ref", "departments")
  addRelationField("officers", "wing_ref", "wings")
  addRelationField("officers", "tehsil_ref", "tehsils")
  addRelationField("officers", "markaz_ref", "markazes")

  addRelationField("school_assignments", "school_ref", "schools")
  addRelationField("school_assignments", "ddeo_ref", "ddeo_officers")
  addRelationField("school_assignments", "aeo_ref", "aeo_officers")

  createIndex("CREATE UNIQUE INDEX IF NOT EXISTS idx_wings_name_district_department ON wings (name, district, department)")
  createIndex("CREATE INDEX IF NOT EXISTS idx_wings_department ON wings (department)")
  createIndex("CREATE UNIQUE INDEX IF NOT EXISTS idx_tehsils_name_district ON tehsils (name, district)")
  createIndex("CREATE INDEX IF NOT EXISTS idx_tehsils_district ON tehsils (district)")
  createIndex("CREATE UNIQUE INDEX IF NOT EXISTS idx_markazes_name_tehsil_wing ON markazes (name, tehsil, wing)")
  createIndex("CREATE INDEX IF NOT EXISTS idx_markazes_tehsil ON markazes (tehsil)")
  createIndex("CREATE INDEX IF NOT EXISTS idx_markazes_wing ON markazes (wing)")
  createIndex("CREATE INDEX IF NOT EXISTS idx_ddeo_officers_wing_tehsil ON ddeo_officers (wing, tehsil)")
  createIndex("CREATE INDEX IF NOT EXISTS idx_aeo_officers_wing_markaz ON aeo_officers (wing, markaz)")
}, (app) => {
  function collection(name) {
    try {
      return app.findCollectionByNameOrId(name)
    } catch {
      return null
    }
  }

  function removeField(collectionName, fieldName) {
    const owner = collection(collectionName)
    if (!owner) {
      return
    }
    const fields = owner.fields.asMap()
    if (!fields[fieldName]) {
      return
    }
    owner.fields.removeByName(fieldName)
    app.save(owner)
  }

  for (const [collectionName, fields] of [
    ["school_assignments", ["school_ref", "ddeo_ref", "aeo_ref"]],
    ["officers", ["district_ref", "department_ref", "wing_ref", "tehsil_ref", "markaz_ref"]],
    ["schools", ["district_ref", "department_ref", "wing_ref", "tehsil_ref", "markaz_ref"]],
  ]) {
    for (const field of fields) {
      removeField(collectionName, field)
    }
  }

  for (const name of [
    "aeo_officers",
    "ddeo_officers",
    "markazes",
    "tehsils",
    "wings",
    "departments",
    "districts",
  ]) {
    const existing = collection(name)
    if (existing) {
      app.delete(existing)
    }
  }
})

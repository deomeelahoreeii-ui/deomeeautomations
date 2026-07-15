migrate((app) => {
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
      // Best effort for optional indexes/columns across partially migrated DBs.
    }
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

  function removeCollectionIndexes(collectionName, indexNames) {
    const owner = collection(collectionName)
    if (!owner) {
      return
    }
    const names = new Set(indexNames)
    owner.indexes = (owner.indexes || []).filter((sql) => {
      const text = String(sql || "")
      for (const name of names) {
        if (text.includes(name)) {
          return false
        }
      }
      return true
    })
    app.save(owner)
  }

  function copyColumn(table, from, to) {
    exec(`UPDATE ${table} SET ${to} = ${from} WHERE COALESCE(${to}, '') = '' AND COALESCE(${from}, '') != ''`)
  }

  exec("DELETE FROM dormant_records")
  exec("DELETE FROM delivery_events")
  exec("DELETE FROM data_quality_issues")
  exec("DELETE FROM report_runs")

  addRelationField("wings", "district_ref", "districts")
  addRelationField("wings", "department_ref", "departments")
  copyColumn("wings", "district", "district_ref")
  copyColumn("wings", "department", "department_ref")

  addRelationField("tehsils", "district_ref", "districts")
  copyColumn("tehsils", "district", "district_ref")

  addRelationField("markazes", "tehsil_ref", "tehsils")
  addRelationField("markazes", "wing_ref", "wings")
  copyColumn("markazes", "tehsil", "tehsil_ref")
  copyColumn("markazes", "wing", "wing_ref")

  addRelationField("ddeo_officers", "district_ref", "districts")
  addRelationField("ddeo_officers", "department_ref", "departments")
  addRelationField("ddeo_officers", "wing_ref", "wings")
  addRelationField("ddeo_officers", "tehsil_ref", "tehsils")
  copyColumn("ddeo_officers", "district", "district_ref")
  copyColumn("ddeo_officers", "department", "department_ref")
  copyColumn("ddeo_officers", "wing", "wing_ref")
  copyColumn("ddeo_officers", "tehsil", "tehsil_ref")

  addRelationField("aeo_officers", "district_ref", "districts")
  addRelationField("aeo_officers", "department_ref", "departments")
  addRelationField("aeo_officers", "wing_ref", "wings")
  addRelationField("aeo_officers", "tehsil_ref", "tehsils")
  addRelationField("aeo_officers", "markaz_ref", "markazes")
  copyColumn("aeo_officers", "district", "district_ref")
  copyColumn("aeo_officers", "department", "department_ref")
  copyColumn("aeo_officers", "wing", "wing_ref")
  copyColumn("aeo_officers", "tehsil", "tehsil_ref")
  copyColumn("aeo_officers", "markaz", "markaz_ref")

  addRelationField("dormant_records", "school_ref", "schools")

  for (const indexName of [
    "idx_schools_tehsil_markaz",
    "idx_officers_mobile_role",
    "idx_officers_role_location",
    "idx_school_assignments_unique",
    "idx_school_assignments_officer",
    "idx_dormant_records_emis",
    "idx_dormant_records_location",
    "idx_wings_name_district_department",
    "idx_wings_department",
    "idx_tehsils_name_district",
    "idx_tehsils_district",
    "idx_markazes_name_tehsil_wing",
    "idx_markazes_tehsil",
    "idx_markazes_wing",
    "idx_ddeo_officers_wing_tehsil",
    "idx_aeo_officers_wing_markaz",
  ]) {
    exec(`DROP INDEX IF EXISTS ${indexName}`)
  }

  removeCollectionIndexes("schools", ["idx_schools_tehsil_markaz"])
  removeCollectionIndexes("officers", ["idx_officers_mobile_role", "idx_officers_role_location"])
  removeCollectionIndexes("school_assignments", ["idx_school_assignments_unique", "idx_school_assignments_officer"])
  removeCollectionIndexes("dormant_records", ["idx_dormant_records_emis", "idx_dormant_records_location"])
  removeCollectionIndexes("wings", ["idx_wings_name_district_department", "idx_wings_department"])
  removeCollectionIndexes("tehsils", ["idx_tehsils_name_district", "idx_tehsils_district"])
  removeCollectionIndexes("markazes", ["idx_markazes_name_tehsil_wing", "idx_markazes_tehsil", "idx_markazes_wing"])
  removeCollectionIndexes("ddeo_officers", ["idx_ddeo_officers_wing_tehsil"])
  removeCollectionIndexes("aeo_officers", ["idx_aeo_officers_wing_markaz"])

  for (const fieldName of ["district", "wing", "tehsil", "markaz"]) {
    removeField("schools", fieldName)
  }
  for (const fieldName of ["school_emis", "officer_mobile", "district", "wing", "tehsil", "markaz"]) {
    removeField("school_assignments", fieldName)
  }
  for (const fieldName of ["district", "department"]) {
    removeField("wings", fieldName)
  }
  removeField("tehsils", "district")
  for (const fieldName of ["tehsil", "wing"]) {
    removeField("markazes", fieldName)
  }
  for (const fieldName of ["district", "department", "wing", "tehsil"]) {
    removeField("ddeo_officers", fieldName)
  }
  for (const fieldName of ["district", "department", "wing", "tehsil", "markaz"]) {
    removeField("aeo_officers", fieldName)
  }
  for (const fieldName of ["school_emis", "school_name", "tehsil", "markaz"]) {
    removeField("dormant_records", fieldName)
  }

  const officers = collection("officers")
  if (officers) {
    app.delete(officers)
  }

  exec("CREATE INDEX IF NOT EXISTS idx_schools_tehsil_markaz_refs ON schools (tehsil_ref, markaz_ref)")
  exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_wings_name_district_department_refs ON wings (name, district_ref, department_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_wings_department_ref ON wings (department_ref)")
  exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_tehsils_name_district_ref ON tehsils (name, district_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_tehsils_district_ref ON tehsils (district_ref)")
  exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_markazes_name_tehsil_wing_refs ON markazes (name, tehsil_ref, wing_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_markazes_tehsil_ref ON markazes (tehsil_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_markazes_wing_ref ON markazes (wing_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_ddeo_officers_wing_tehsil_refs ON ddeo_officers (wing_ref, tehsil_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_aeo_officers_wing_markaz_refs ON aeo_officers (wing_ref, markaz_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_school_assignments_school_ref ON school_assignments (school_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_school_assignments_ddeo_ref ON school_assignments (ddeo_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_school_assignments_aeo_ref ON school_assignments (aeo_ref)")
  exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_school_assignments_role_refs ON school_assignments (school_ref, officer_role, ddeo_ref, aeo_ref)")
  exec("CREATE INDEX IF NOT EXISTS idx_dormant_records_school_ref ON dormant_records (school_ref)")
}, (app) => {
  // This migration intentionally clears report history and removes legacy
  // duplicate columns. Restore from a pre-migration SQLite backup if rollback
  // of operational data is needed.
})

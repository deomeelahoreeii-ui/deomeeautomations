migrate((app) => {
  function collection(name) {
    try {
      return app.findCollectionByNameOrId(name)
    } catch {
      return null
    }
  }

  function requireFields(collectionName, fieldNames) {
    const owner = collection(collectionName)
    if (!owner) {
      return
    }

    const fields = owner.fields.asMap()
    let changed = false
    for (const fieldName of fieldNames) {
      const field = fields[fieldName]
      if (!field) {
        continue
      }
      if (!field.required) {
        field.required = true
        changed = true
      }
      if (field.type && field.type() === "relation" && !field.minSelect) {
        field.minSelect = 1
        changed = true
      }
    }

    if (changed) {
      app.save(owner)
    }
  }

  requireFields("wings", ["district_ref", "department_ref"])
  requireFields("tehsils", ["district_ref"])
  requireFields("markazes", ["tehsil_ref", "wing_ref"])
  requireFields("schools", [
    "district_ref",
    "department_ref",
    "wing_ref",
    "tehsil_ref",
    "markaz_ref",
  ])
  requireFields("ddeo_officers", [
    "district_ref",
    "department_ref",
    "wing_ref",
    "tehsil_ref",
  ])
  requireFields("aeo_officers", [
    "district_ref",
    "department_ref",
    "wing_ref",
    "tehsil_ref",
    "markaz_ref",
  ])
  requireFields("ddeo_jurisdictions", [
    "ddeo_ref",
    "district_ref",
    "department_ref",
    "wing_ref",
    "tehsil_ref",
  ])
  requireFields("aeo_jurisdictions", [
    "aeo_ref",
    "district_ref",
    "department_ref",
    "wing_ref",
    "tehsil_ref",
    "markaz_ref",
  ])
  requireFields("school_ddeo_overrides", ["school_ref", "ddeo_ref"])
  requireFields("school_aeo_overrides", ["school_ref", "aeo_ref"])
}, (app) => {
  function collection(name) {
    try {
      return app.findCollectionByNameOrId(name)
    } catch {
      return null
    }
  }

  function loosenFields(collectionName, fieldNames) {
    const owner = collection(collectionName)
    if (!owner) {
      return
    }

    const fields = owner.fields.asMap()
    let changed = false
    for (const fieldName of fieldNames) {
      const field = fields[fieldName]
      if (!field) {
        continue
      }
      if (field.required) {
        field.required = false
        changed = true
      }
      if (field.type && field.type() === "relation" && field.minSelect === 1) {
        field.minSelect = 0
        changed = true
      }
    }

    if (changed) {
      app.save(owner)
    }
  }

  loosenFields("wings", ["district_ref", "department_ref"])
  loosenFields("tehsils", ["district_ref"])
  loosenFields("markazes", ["tehsil_ref", "wing_ref"])
  loosenFields("schools", [
    "district_ref",
    "department_ref",
    "wing_ref",
    "tehsil_ref",
    "markaz_ref",
  ])
  loosenFields("ddeo_officers", [
    "district_ref",
    "department_ref",
    "wing_ref",
    "tehsil_ref",
  ])
  loosenFields("aeo_officers", [
    "district_ref",
    "department_ref",
    "wing_ref",
    "tehsil_ref",
    "markaz_ref",
  ])
  loosenFields("ddeo_jurisdictions", [
    "ddeo_ref",
    "district_ref",
    "department_ref",
    "wing_ref",
    "tehsil_ref",
  ])
  loosenFields("aeo_jurisdictions", [
    "aeo_ref",
    "district_ref",
    "department_ref",
    "wing_ref",
    "tehsil_ref",
    "markaz_ref",
  ])
  loosenFields("school_ddeo_overrides", ["school_ref", "ddeo_ref"])
  loosenFields("school_aeo_overrides", ["school_ref", "aeo_ref"])
})

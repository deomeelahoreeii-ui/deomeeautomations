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
      // Optional compatibility backfill for partially migrated databases.
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

  addTextField("schools", "source", 80)
  addTextField("schools", "notes", 1000)

  exec("UPDATE schools SET source = 'officers_list_csv' WHERE COALESCE(source, '') = ''")
}, (app) => {
  function collection(name) {
    try {
      return app.findCollectionByNameOrId(name)
    } catch {
      return null
    }
  }

  const schools = collection("schools")
  if (!schools) {
    return
  }
  for (const fieldName of ["source", "notes"]) {
    if (schools.fields.asMap()[fieldName]) {
      schools.fields.removeByName(fieldName)
    }
  }
  app.save(schools)
})

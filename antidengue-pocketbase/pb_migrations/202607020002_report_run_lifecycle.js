migrate((app) => {
  const collection = app.findCollectionByNameOrId("report_runs")
  const fieldMap = collection.fields.asMap()

  if (!fieldMap.archived) {
    collection.fields.add(new BoolField({ name: "archived" }))
  }
  if (!fieldMap.archived_at) {
    collection.fields.add(new DateField({ name: "archived_at" }))
  }

  app.save(collection)
}, (app) => {
  const collection = app.findCollectionByNameOrId("report_runs")
  collection.fields.removeByName("archived")
  collection.fields.removeByName("archived_at")
  app.save(collection)
})

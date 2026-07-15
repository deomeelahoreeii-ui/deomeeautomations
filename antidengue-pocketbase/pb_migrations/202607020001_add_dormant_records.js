migrate((app) => {
  const collection = new Collection({
    type: "base",
    name: "dormant_records",
    listRule: null,
    viewRule: null,
    createRule: null,
    updateRule: null,
    deleteRule: null,
    fields: [
      { name: "run_key", type: "text", required: true, max: 160 },
      { name: "school_emis", type: "text", max: 32 },
      { name: "school_name", type: "text", required: true, presentable: true, max: 255 },
      { name: "tehsil", type: "text", max: 120 },
      { name: "markaz", type: "text", max: 160 },
      { name: "row_data", type: "json" },
    ],
    indexes: [
      "CREATE INDEX idx_dormant_records_run ON dormant_records (run_key)",
      "CREATE INDEX idx_dormant_records_emis ON dormant_records (school_emis)",
      "CREATE INDEX idx_dormant_records_location ON dormant_records (tehsil, markaz)",
    ],
  })

  app.save(collection)
}, (app) => {
  try {
    app.delete(app.findCollectionByNameOrId("dormant_records"))
  } catch {
    // Already removed.
  }
})

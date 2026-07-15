migrate((app) => {
  const adminOnly = null

  const collections = [
    new Collection({
      type: "base",
      name: "schools",
      listRule: adminOnly,
      viewRule: adminOnly,
      createRule: adminOnly,
      updateRule: adminOnly,
      deleteRule: adminOnly,
      fields: [
        { name: "emis", type: "text", required: true, presentable: true, max: 32 },
        { name: "name", type: "text", required: true, presentable: true, max: 255 },
        { name: "district", type: "text", max: 120 },
        { name: "wing", type: "text", max: 120 },
        { name: "tehsil", type: "text", max: 120 },
        { name: "markaz", type: "text", max: 160 },
        { name: "shift", type: "text", max: 80 },
        { name: "head_name", type: "text", max: 160 },
        { name: "head_contact", type: "text", max: 32 },
        { name: "active", type: "bool" },
        { name: "source_row_hash", type: "text", max: 64 },
      ],
      indexes: [
        "CREATE UNIQUE INDEX idx_schools_emis ON schools (emis)",
        "CREATE INDEX idx_schools_tehsil_markaz ON schools (tehsil, markaz)",
      ],
    }),
    new Collection({
      type: "base",
      name: "officers",
      listRule: adminOnly,
      viewRule: adminOnly,
      createRule: adminOnly,
      updateRule: adminOnly,
      deleteRule: adminOnly,
      fields: [
        { name: "name", type: "text", required: true, presentable: true, max: 160 },
        { name: "role", type: "select", required: true, maxSelect: 1, values: ["DDEO", "AEO", "HEAD", "OTHER"] },
        { name: "mobile", type: "text", required: true, max: 32 },
        { name: "normalized_mobile", type: "text", required: true, max: 32 },
        { name: "district", type: "text", max: 120 },
        { name: "wing", type: "text", max: 120 },
        { name: "tehsil", type: "text", max: 120 },
        { name: "markaz", type: "text", max: 160 },
        { name: "active", type: "bool" },
      ],
      indexes: [
        "CREATE UNIQUE INDEX idx_officers_mobile_role ON officers (normalized_mobile, role)",
        "CREATE INDEX idx_officers_role_location ON officers (role, tehsil, markaz)",
      ],
    }),
    new Collection({
      type: "base",
      name: "school_assignments",
      listRule: adminOnly,
      viewRule: adminOnly,
      createRule: adminOnly,
      updateRule: adminOnly,
      deleteRule: adminOnly,
      fields: [
        { name: "school_emis", type: "text", required: true, max: 32 },
        { name: "officer_mobile", type: "text", required: true, max: 32 },
        { name: "officer_role", type: "select", required: true, maxSelect: 1, values: ["DDEO", "AEO", "HEAD", "OTHER"] },
        { name: "district", type: "text", max: 120 },
        { name: "wing", type: "text", max: 120 },
        { name: "tehsil", type: "text", max: 120 },
        { name: "markaz", type: "text", max: 160 },
        { name: "active", type: "bool" },
      ],
      indexes: [
        "CREATE UNIQUE INDEX idx_school_assignments_unique ON school_assignments (school_emis, officer_mobile, officer_role)",
        "CREATE INDEX idx_school_assignments_officer ON school_assignments (officer_mobile, officer_role)",
      ],
    }),
    new Collection({
      type: "base",
      name: "officer_import_batches",
      listRule: adminOnly,
      viewRule: adminOnly,
      createRule: adminOnly,
      updateRule: adminOnly,
      deleteRule: adminOnly,
      fields: [
        { name: "file_name", type: "text", required: true, max: 255 },
        { name: "sha256", type: "text", required: true, max: 64 },
        { name: "row_count", type: "number" },
        { name: "school_count", type: "number" },
        { name: "officer_count", type: "number" },
        { name: "assignment_count", type: "number" },
        { name: "status", type: "select", required: true, maxSelect: 1, values: ["imported", "failed"] },
        { name: "notes", type: "text", max: 500 },
      ],
      indexes: [
        "CREATE INDEX idx_officer_import_batches_sha ON officer_import_batches (sha256)",
      ],
    }),
    new Collection({
      type: "base",
      name: "report_runs",
      listRule: adminOnly,
      viewRule: adminOnly,
      createRule: adminOnly,
      updateRule: adminOnly,
      deleteRule: adminOnly,
      fields: [
        { name: "started_at", type: "date" },
        { name: "finished_at", type: "date" },
        { name: "source", type: "select", maxSelect: 1, values: ["portal", "manual", "replay"] },
        { name: "status", type: "select", maxSelect: 1, values: ["running", "completed", "failed", "partial"] },
        { name: "raw_file_name", type: "text", max: 255 },
        { name: "raw_file_sha256", type: "text", max: 64 },
        { name: "summary", type: "json" },
      ],
      indexes: [
        "CREATE INDEX idx_report_runs_started_at ON report_runs (started_at)",
        "CREATE INDEX idx_report_runs_status ON report_runs (status)",
      ],
    }),
    new Collection({
      type: "base",
      name: "delivery_events",
      listRule: adminOnly,
      viewRule: adminOnly,
      createRule: adminOnly,
      updateRule: adminOnly,
      deleteRule: adminOnly,
      fields: [
        { name: "run_key", type: "text", max: 160 },
        { name: "job_id", type: "text", max: 160 },
        { name: "target", type: "text", required: true, max: 80 },
        { name: "recipient_name", type: "text", max: 160 },
        { name: "role", type: "text", max: 40 },
        { name: "status", type: "select", required: true, maxSelect: 1, values: ["queued", "delivered", "failed", "pending", "manual"] },
        { name: "cause", type: "text", max: 500 },
        { name: "payload", type: "json" },
      ],
      indexes: [
        "CREATE INDEX idx_delivery_events_run ON delivery_events (run_key)",
        "CREATE INDEX idx_delivery_events_status ON delivery_events (status)",
        "CREATE INDEX idx_delivery_events_target ON delivery_events (target)",
      ],
    }),
    new Collection({
      type: "base",
      name: "data_quality_issues",
      listRule: adminOnly,
      viewRule: adminOnly,
      createRule: adminOnly,
      updateRule: adminOnly,
      deleteRule: adminOnly,
      fields: [
        { name: "run_key", type: "text", max: 160 },
        { name: "severity", type: "select", required: true, maxSelect: 1, values: ["info", "warning", "error"] },
        { name: "category", type: "text", required: true, max: 120 },
        { name: "message", type: "text", required: true, max: 1000 },
        { name: "context", type: "json" },
      ],
      indexes: [
        "CREATE INDEX idx_data_quality_issues_run ON data_quality_issues (run_key)",
        "CREATE INDEX idx_data_quality_issues_severity ON data_quality_issues (severity)",
      ],
    }),
  ]

  for (const collection of collections) {
    app.save(collection)
  }
}, (app) => {
  for (const name of [
    "data_quality_issues",
    "delivery_events",
    "report_runs",
    "officer_import_batches",
    "school_assignments",
    "officers",
    "schools",
  ]) {
    try {
      app.delete(app.findCollectionByNameOrId(name))
    } catch {
      // Already removed.
    }
  }
})

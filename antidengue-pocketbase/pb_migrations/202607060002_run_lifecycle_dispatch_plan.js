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
    if (collection(definition.name)) {
      return
    }
    app.save(new Collection(definition))
  }

  saveCollectionIfMissing({
    type: "base",
    name: "run_stage_events",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "run_key", type: "text", required: true, max: 160 },
      { name: "stage", type: "select", required: true, maxSelect: 1, values: [
        "started",
        "downloaded",
        "validated",
        "processed",
        "dispatch_planned",
        "dispatch_started",
        "dispatch_blocked",
        "dispatch_finished",
        "summary_saved",
        "completed",
        "failed",
      ] },
      { name: "status", type: "select", required: true, maxSelect: 1, values: [
        "started",
        "ok",
        "warning",
        "blocked",
        "failed",
      ] },
      { name: "message", type: "text", max: 1200 },
      { name: "payload", type: "json" },
      { name: "created_at", type: "date" },
    ],
    indexes: [
      "CREATE INDEX idx_run_stage_events_run ON run_stage_events (run_key)",
      "CREATE INDEX idx_run_stage_events_stage ON run_stage_events (stage)",
      "CREATE INDEX idx_run_stage_events_status ON run_stage_events (status)",
    ],
  })

  saveCollectionIfMissing({
    type: "base",
    name: "dispatch_plan_items",
    listRule: adminOnly,
    viewRule: adminOnly,
    createRule: adminOnly,
    updateRule: adminOnly,
    deleteRule: adminOnly,
    fields: [
      { name: "run_key", type: "text", required: true, max: 160 },
      { name: "job_id", type: "text", max: 80 },
      { name: "channel", type: "select", required: true, maxSelect: 1, values: [
        "individual",
        "group",
        "fallback",
      ] },
      { name: "recipient_name", type: "text", max: 160 },
      { name: "target", type: "text", max: 140 },
      { name: "route_kind", type: "select", maxSelect: 1, values: [
        "district",
        "tehsil",
        "markaz",
        "fallback",
        "custom",
      ] },
      { name: "message_mode", type: "select", maxSelect: 1, values: [
        "individual",
        "full_report",
        "tehsil_summary",
        "markaz_summary",
        "tehsil_markaz_summary",
        "ddeo_wise",
        "aeo_wise",
        "failed_fallback",
      ] },
      { name: "row_count", type: "number" },
      { name: "excel_path", type: "text", max: 600 },
      { name: "status", type: "select", required: true, maxSelect: 1, values: [
        "planned",
        "queued",
        "delivered",
        "sent_pending_confirmation",
        "manual_pending",
        "preview_required",
        "skipped",
        "blocked",
        "failed",
        "missing_status",
      ] },
      { name: "cause", type: "text", max: 1200 },
      { name: "payload", type: "json" },
      { name: "created_at", type: "date" },
      { name: "updated_at", type: "date" },
    ],
    indexes: [
      "CREATE INDEX idx_dispatch_plan_items_run ON dispatch_plan_items (run_key)",
      "CREATE INDEX idx_dispatch_plan_items_status ON dispatch_plan_items (status)",
      "CREATE INDEX idx_dispatch_plan_items_channel ON dispatch_plan_items (channel)",
      "CREATE INDEX idx_dispatch_plan_items_target ON dispatch_plan_items (target)",
    ],
  })
}, (app) => {
  function collection(name) {
    try {
      return app.findCollectionByNameOrId(name)
    } catch {
      return null
    }
  }

  for (const name of ["dispatch_plan_items", "run_stage_events"]) {
    const existing = collection(name)
    if (existing) {
      app.delete(existing)
    }
  }
})
